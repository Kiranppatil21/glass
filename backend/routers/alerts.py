"""
Alerts Router - Payment due alerts and notification management
"""
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from datetime import datetime, timezone
from .base import get_erp_user, get_db
import sys
sys.path.insert(0, '/app/backend')
from utils.payment_alerts import run_payment_due_alerts, check_customer_payment_dues, check_vendor_payment_dues
from utils.notifications import send_notification_with_fallback

alerts_router = APIRouter(prefix="/alerts", tags=["Alerts"])


@alerts_router.post("/payment-dues/run")
async def trigger_payment_due_alerts(
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_erp_user)
):
    """Manually trigger payment due alerts"""
    if current_user.get("role") not in ["super_admin", "admin", "finance"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    # Run in background
    background_tasks.add_task(run_payment_due_alerts, db)
    
    return {
        "message": "Payment due alerts triggered",
        "status": "processing",
        "triggered_at": datetime.now(timezone.utc).isoformat()
    }


@alerts_router.get("/payment-dues/preview")
async def preview_payment_due_alerts(
    current_user: dict = Depends(get_erp_user)
):
    """Preview pending payment alerts without sending"""
    if current_user.get("role") not in ["super_admin", "admin", "finance"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    from datetime import timedelta
    
    today = datetime.now(timezone.utc)
    
    # Find customer orders with remaining balance
    customer_dues = []
    orders = await db.orders.find({
        "remaining_amount": {"$gt": 0},
        "status": {"$nin": ["cancelled", "returned"]}
    }, {"_id": 0}).to_list(500)
    
    for order in orders:
        created_at = order.get("created_at", "")
        if isinstance(created_at, str):
            created_date = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        else:
            created_date = created_at
        
        is_credit = order.get("is_credit_customer", False)
        payment_days = 30 if is_credit else 7
        due_date = created_date + timedelta(days=payment_days)
        days_until_due = (due_date.date() - today.date()).days
        
        if days_until_due <= 3:
            customer_dues.append({
                "order_number": order.get("order_number"),
                "customer_name": order.get("customer_name"),
                "customer_phone": order.get("customer_phone"),
                "amount_due": order.get("remaining_amount"),
                "due_date": due_date.strftime("%Y-%m-%d"),
                "days_until_due": days_until_due,
                "status": "overdue" if days_until_due < 0 else "due_soon"
            })
    
    # Find vendor POs with outstanding balance
    vendor_dues = []
    pos = await db.purchase_orders.find({
        "outstanding_balance": {"$gt": 0},
        "status": "approved"
    }, {"_id": 0}).to_list(500)
    
    for po in pos:
        vendor = await db.vendors.find_one({"id": po.get("vendor_id")}, {"_id": 0})
        credit_days = vendor.get("credit_days", 30) if vendor else 30
        
        approved_at = po.get("approved_at") or po.get("created_at", "")
        if isinstance(approved_at, str):
            approved_date = datetime.fromisoformat(approved_at.replace("Z", "+00:00"))
        else:
            approved_date = approved_at
        
        due_date = approved_date + timedelta(days=credit_days)
        days_until_due = (due_date.date() - today.date()).days
        
        if days_until_due <= 3:
            vendor_dues.append({
                "po_number": po.get("po_number"),
                "vendor_name": po.get("vendor_name"),
                "amount_due": po.get("outstanding_balance"),
                "due_date": due_date.strftime("%Y-%m-%d"),
                "days_until_due": days_until_due,
                "status": "overdue" if days_until_due < 0 else "due_soon"
            })
    
    return {
        "customer_dues": customer_dues,
        "vendor_dues": vendor_dues,
        "summary": {
            "total_customer_alerts": len(customer_dues),
            "total_vendor_alerts": len(vendor_dues),
            "customer_overdue": len([d for d in customer_dues if d["status"] == "overdue"]),
            "vendor_overdue": len([d for d in vendor_dues if d["status"] == "overdue"])
        }
    }


@alerts_router.get("/payment-dues/history")
async def get_payment_alert_history(
    limit: int = 30,
    current_user: dict = Depends(get_erp_user)
):
    """Get payment alert history"""
    if current_user.get("role") not in ["super_admin", "admin", "finance"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    logs = await db.payment_alert_logs.find(
        {}, {"_id": 0}
    ).sort("timestamp", -1).limit(limit).to_list(limit)
    
    return {"logs": logs, "count": len(logs)}


@alerts_router.post("/test-notification")
async def test_notification(
    phone: str,
    message: str,
    email: str = None,
    current_user: dict = Depends(get_erp_user)
):
    """Test notification system with fallback"""
    if current_user.get("role") not in ["super_admin", "admin"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    result = await send_notification_with_fallback(
        phone=phone,
        message=message,
        email=email,
        email_subject="Test Notification from Lucumaa Glass",
        email_html=f"<p>{message}</p>",
        prefer_whatsapp=True
    )
    
    return {
        "message": "Test notification sent",
        "result": result
    }


@alerts_router.get("/settings")
async def get_alert_settings(current_user: dict = Depends(get_erp_user)):
    """Get alert settings"""
    if current_user.get("role") not in ["super_admin", "admin"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    settings = await db.alert_settings.find_one({"type": "payment_alerts"}, {"_id": 0})
    
    if not settings:
        settings = {
            "type": "payment_alerts",
            "enabled": True,
            "customer_alerts": {
                "enabled": True,
                "days_before_due": [3, 1, 0],
                "overdue_reminders": True,
                "channels": ["whatsapp", "sms", "email"]
            },
            "vendor_alerts": {
                "enabled": True,
                "days_before_due": [3, 1, 0],
                "send_to_admin": True,
                "admin_email": None
            },
            "schedule": {
                "enabled": True,
                "run_at": "09:00",
                "timezone": "Asia/Kolkata"
            }
        }
    
    return settings


@alerts_router.put("/settings")
async def update_alert_settings(
    settings: dict,
    current_user: dict = Depends(get_erp_user)
):
    """Update alert settings"""
    if current_user.get("role") not in ["super_admin", "admin"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    settings["type"] = "payment_alerts"
    settings["updated_at"] = datetime.now(timezone.utc).isoformat()
    settings["updated_by"] = current_user.get("name")
    
    await db.alert_settings.update_one(
        {"type": "payment_alerts"},
        {"$set": settings},
        upsert=True
    )
    
    return {"message": "Alert settings updated", "settings": settings}


# =============== SCHEDULER ENDPOINTS ===============

@alerts_router.get("/scheduler/jobs")
async def get_scheduler_jobs(current_user: dict = Depends(get_erp_user)):
    """Get list of all scheduled jobs"""
    if current_user.get("role") not in ["super_admin", "admin"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    try:
        from utils.scheduler import get_scheduled_jobs
        jobs = get_scheduled_jobs()
        return {"jobs": jobs}
    except Exception as e:
        return {"jobs": [], "error": str(e)}


@alerts_router.post("/scheduler/run/{job_id}")
async def run_scheduler_job(
    job_id: str,
    current_user: dict = Depends(get_erp_user)
):
    """Run a scheduled job manually"""
    if current_user.get("role") not in ["super_admin", "admin"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    try:
        from utils.scheduler import run_job_manually
        result = await run_job_manually(job_id)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@alerts_router.get("/scheduler/logs")
async def get_scheduler_logs(
    limit: int = 50,
    current_user: dict = Depends(get_erp_user)
):
    """Get scheduler job execution logs"""
    if current_user.get("role") not in ["super_admin", "admin"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    import json
    from bson import ObjectId
    
    def serialize(obj):
        """Recursively serialize MongoDB documents"""
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, dict):
            return {k: serialize(v) for k, v in obj.items() if k != "_id"}
        if isinstance(obj, list):
            return [serialize(item) for item in obj]
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        return obj
    
    try:
        cursor = db.scheduler_logs.find({}).sort("run_at", -1).limit(limit)
        logs = []
        async for log in cursor:
            logs.append(serialize(log))
        
        return {"logs": logs, "count": len(logs)}
    except Exception as e:
        return {"logs": [], "count": 0, "error": str(e)}
