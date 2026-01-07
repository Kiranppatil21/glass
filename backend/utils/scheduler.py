"""
Scheduler - Background task scheduler for automated jobs
Uses APScheduler for cron-like scheduling
"""
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime, timezone
import logging
import asyncio

logger = logging.getLogger(__name__)

# Scheduler instance
scheduler = None
_db = None


def init_scheduler(database):
    """Initialize scheduler with database reference"""
    global scheduler, _db
    _db = database
    
    scheduler = AsyncIOScheduler(timezone="Asia/Kolkata")
    
    # Add payment alerts job - runs daily at 9:00 AM IST
    scheduler.add_job(
        run_payment_alerts_job,
        CronTrigger(hour=9, minute=0),
        id="payment_alerts_daily",
        name="Daily Payment Alerts",
        replace_existing=True
    )
    
    # Add weekly vendor payment summary - runs every Monday at 10:00 AM IST
    scheduler.add_job(
        run_weekly_vendor_summary,
        CronTrigger(day_of_week="mon", hour=10, minute=0),
        id="vendor_summary_weekly",
        name="Weekly Vendor Payment Summary",
        replace_existing=True
    )
    
    logger.info("Scheduler initialized with jobs: payment_alerts_daily, vendor_summary_weekly")
    
    return scheduler


def start_scheduler():
    """Start the scheduler"""
    global scheduler
    if scheduler and not scheduler.running:
        scheduler.start()
        logger.info("Scheduler started")


def stop_scheduler():
    """Stop the scheduler"""
    global scheduler
    if scheduler and scheduler.running:
        scheduler.shutdown()
        logger.info("Scheduler stopped")


async def run_payment_alerts_job():
    """Job function to run payment alerts"""
    global _db
    logger.info("Running scheduled payment alerts job...")
    
    try:
        from utils.payment_alerts import run_payment_due_alerts
        result = await run_payment_due_alerts(_db)
        logger.info(f"Payment alerts completed: {result}")
        
        # Store job run log - ensure all values are serializable
        log_entry = {
            "job_id": "payment_alerts_daily",
            "job_name": "Daily Payment Alerts",
            "status": "success",
            "result": {
                "customer_alerts": result.get("customer_alerts", {}),
                "vendor_alerts": result.get("vendor_alerts", {})
            },
            "run_at": datetime.now(timezone.utc).isoformat()
        }
        await _db.scheduler_logs.insert_one(log_entry)
    except Exception as e:
        logger.error(f"Payment alerts job failed: {e}")
        log_entry = {
            "job_id": "payment_alerts_daily",
            "job_name": "Daily Payment Alerts",
            "status": "failed",
            "error": str(e),
            "run_at": datetime.now(timezone.utc).isoformat()
        }
        await _db.scheduler_logs.insert_one(log_entry)


async def run_weekly_vendor_summary():
    """Job function to send weekly vendor payment summary"""
    global _db
    logger.info("Running weekly vendor payment summary...")
    
    try:
        from datetime import timedelta
        from utils.notifications import send_email_notification
        
        # Get last week's vendor payments
        week_ago = datetime.now(timezone.utc) - timedelta(days=7)
        
        payments = await _db.vendor_payments.find({
            "status": "completed",
            "completed_at": {"$gte": week_ago.isoformat()}
        }, {"_id": 0}).to_list(1000)
        
        total_paid = sum(p.get("amount", 0) for p in payments)
        
        # Get outstanding
        outstanding_pos = await _db.purchase_orders.find({
            "outstanding_balance": {"$gt": 0},
            "status": "approved"
        }, {"_id": 0}).to_list(1000)
        
        total_outstanding = sum(p.get("outstanding_balance", 0) for p in outstanding_pos)
        
        # Get admin email
        admin = await _db.users.find_one({"role": {"$in": ["super_admin", "admin"]}}, {"_id": 0})
        admin_email = admin.get("email") if admin else None
        
        if admin_email:
            html = f"""
            <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
                <div style="background: #7c3aed; color: white; padding: 20px; text-align: center;">
                    <h2>Weekly Vendor Payment Summary</h2>
                    <p>{datetime.now().strftime('%d %B %Y')}</p>
                </div>
                <div style="padding: 20px;">
                    <div style="display: flex; gap: 20px; margin-bottom: 20px;">
                        <div style="flex: 1; background: #e8f5e9; padding: 15px; border-radius: 8px; text-align: center;">
                            <p style="font-size: 24px; font-weight: bold; color: #2e7d32;">₹{total_paid:,.2f}</p>
                            <p style="color: #666;">Paid This Week</p>
                            <p style="color: #888; font-size: 12px;">{len(payments)} transactions</p>
                        </div>
                        <div style="flex: 1; background: #fff3e0; padding: 15px; border-radius: 8px; text-align: center;">
                            <p style="font-size: 24px; font-weight: bold; color: #ef6c00;">₹{total_outstanding:,.2f}</p>
                            <p style="color: #666;">Outstanding</p>
                            <p style="color: #888; font-size: 12px;">{len(outstanding_pos)} POs</p>
                        </div>
                    </div>
                    <p style="color: #666;">This is an automated weekly summary from your ERP system.</p>
                </div>
            </div>
            """
            
            await send_email_notification(
                admin_email,
                "Weekly Vendor Payment Summary - Lucumaa Glass",
                html
            )
        
        await _db.scheduler_logs.insert_one({
            "job_id": "vendor_summary_weekly",
            "job_name": "Weekly Vendor Payment Summary",
            "status": "success",
            "result": {
                "total_paid": total_paid,
                "payments_count": len(payments),
                "total_outstanding": total_outstanding,
                "outstanding_pos": len(outstanding_pos)
            },
            "run_at": datetime.now(timezone.utc).isoformat()
        })
        
        logger.info(f"Weekly vendor summary sent: paid={total_paid}, outstanding={total_outstanding}")
        
    except Exception as e:
        logger.error(f"Weekly vendor summary failed: {e}")
        await _db.scheduler_logs.insert_one({
            "job_id": "vendor_summary_weekly",
            "job_name": "Weekly Vendor Payment Summary",
            "status": "failed",
            "error": str(e),
            "run_at": datetime.now(timezone.utc).isoformat()
        })


def get_scheduled_jobs():
    """Get list of all scheduled jobs"""
    global scheduler
    if not scheduler:
        return []
    
    jobs = []
    for job in scheduler.get_jobs():
        jobs.append({
            "id": job.id,
            "name": job.name,
            "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
            "trigger": str(job.trigger)
        })
    
    return jobs


async def run_job_manually(job_id: str):
    """Run a job manually"""
    global scheduler
    if not scheduler:
        raise ValueError("Scheduler not initialized")
    
    job = scheduler.get_job(job_id)
    if not job:
        raise ValueError(f"Job {job_id} not found")
    
    # Run the job function
    if job_id == "payment_alerts_daily":
        await run_payment_alerts_job()
    elif job_id == "vendor_summary_weekly":
        await run_weekly_vendor_summary()
    else:
        raise ValueError(f"Unknown job: {job_id}")
    
    return {"message": f"Job {job_id} executed successfully"}
