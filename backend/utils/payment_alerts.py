"""
Payment Due Alerts - Background task for sending payment reminders
"""
from datetime import datetime, timezone, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
import os
import asyncio
import logging
from .notifications import send_payment_due_alert, send_vendor_payment_due_alert

logger = logging.getLogger(__name__)


async def check_customer_payment_dues(db):
    """
    Check for customer orders with pending payments and send alerts.
    Runs daily to identify:
    - Payments due in 3 days
    - Payments due tomorrow
    - Overdue payments
    """
    today = datetime.now(timezone.utc)
    
    # Find orders with remaining balance
    orders = await db.orders.find({
        "remaining_amount": {"$gt": 0},
        "status": {"$nin": ["cancelled", "returned"]}
    }, {"_id": 0}).to_list(1000)
    
    alerts_sent = {"due_soon": 0, "due_tomorrow": 0, "overdue": 0, "failed": 0}
    
    for order in orders:
        try:
            # Calculate due date (30 days from order for credit customers, 7 days for others)
            created_at = order.get("created_at", "")
            if isinstance(created_at, str):
                created_date = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            else:
                created_date = created_at
            
            # Check if customer is credit customer
            is_credit = order.get("is_credit_customer", False)
            payment_days = 30 if is_credit else 7
            
            due_date = created_date + timedelta(days=payment_days)
            days_until_due = (due_date.date() - today.date()).days
            
            # Skip if already notified today
            last_reminder = order.get("last_payment_reminder")
            if last_reminder:
                last_reminder_date = datetime.fromisoformat(last_reminder.replace("Z", "+00:00")).date()
                if last_reminder_date == today.date():
                    continue
            
            should_notify = False
            
            # Overdue
            if days_until_due < 0:
                should_notify = True
                alerts_sent["overdue"] += 1
            # Due tomorrow
            elif days_until_due == 1:
                should_notify = True
                alerts_sent["due_tomorrow"] += 1
            # Due in 3 days
            elif days_until_due == 3:
                should_notify = True
                alerts_sent["due_soon"] += 1
            
            if should_notify:
                result = await send_payment_due_alert(
                    phone=order.get("customer_phone", ""),
                    customer_name=order.get("customer_name", "Customer"),
                    order_number=order.get("order_number", ""),
                    amount_due=order.get("remaining_amount", 0),
                    due_date=due_date.strftime("%d-%m-%Y"),
                    days_overdue=abs(days_until_due) if days_until_due < 0 else 0,
                    email=order.get("customer_email")
                )
                
                if result.get("any_success"):
                    # Update last reminder date
                    await db.orders.update_one(
                        {"id": order.get("id")},
                        {"$set": {"last_payment_reminder": today.isoformat()}}
                    )
                else:
                    alerts_sent["failed"] += 1
                    
        except Exception as e:
            logger.error(f"Error processing payment due for order {order.get('order_number')}: {e}")
            alerts_sent["failed"] += 1
    
    return alerts_sent


async def check_vendor_payment_dues(db):
    """
    Check for vendor POs with pending payments and send alerts to finance team.
    Alerts for:
    - Payments due in 3 days
    - Payments due tomorrow
    - Overdue payments
    """
    today = datetime.now(timezone.utc)
    
    # Find POs with outstanding balance
    pos = await db.purchase_orders.find({
        "outstanding_balance": {"$gt": 0},
        "status": "approved"
    }, {"_id": 0}).to_list(1000)
    
    alerts_sent = {"due_soon": 0, "due_tomorrow": 0, "overdue": 0, "failed": 0}
    
    # Get admin email for notifications
    admin = await db.users.find_one({"role": {"$in": ["super_admin", "admin", "finance"]}}, {"_id": 0})
    admin_email = admin.get("email") if admin else None
    
    for po in pos:
        try:
            # Get vendor credit days
            vendor = await db.vendors.find_one({"id": po.get("vendor_id")}, {"_id": 0})
            credit_days = vendor.get("credit_days", 30) if vendor else 30
            
            # Calculate due date
            approved_at = po.get("approved_at") or po.get("created_at", "")
            if isinstance(approved_at, str):
                approved_date = datetime.fromisoformat(approved_at.replace("Z", "+00:00"))
            else:
                approved_date = approved_at
            
            due_date = approved_date + timedelta(days=credit_days)
            days_until_due = (due_date.date() - today.date()).days
            
            # Skip if already notified today
            last_reminder = po.get("last_payment_reminder")
            if last_reminder:
                last_reminder_date = datetime.fromisoformat(last_reminder.replace("Z", "+00:00")).date()
                if last_reminder_date == today.date():
                    continue
            
            should_notify = False
            
            # Overdue
            if days_until_due < 0:
                should_notify = True
                alerts_sent["overdue"] += 1
            # Due tomorrow
            elif days_until_due == 1:
                should_notify = True
                alerts_sent["due_tomorrow"] += 1
            # Due in 3 days
            elif days_until_due == 3:
                should_notify = True
                alerts_sent["due_soon"] += 1
            
            if should_notify and admin_email:
                result = await send_vendor_payment_due_alert(
                    vendor_name=po.get("vendor_name", "Unknown"),
                    po_number=po.get("po_number", ""),
                    amount_due=po.get("outstanding_balance", 0),
                    due_date=due_date.strftime("%d-%m-%Y"),
                    days_until_due=days_until_due,
                    admin_email=admin_email
                )
                
                if result.get("any_success"):
                    # Update last reminder date
                    await db.purchase_orders.update_one(
                        {"id": po.get("id")},
                        {"$set": {"last_payment_reminder": today.isoformat()}}
                    )
                else:
                    alerts_sent["failed"] += 1
                    
        except Exception as e:
            logger.error(f"Error processing vendor payment due for PO {po.get('po_number')}: {e}")
            alerts_sent["failed"] += 1
    
    return alerts_sent


async def run_payment_due_alerts(db):
    """
    Main function to run all payment due alert checks
    """
    logger.info("Running payment due alerts...")
    
    customer_alerts = await check_customer_payment_dues(db)
    vendor_alerts = await check_vendor_payment_dues(db)
    
    summary = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "customer_alerts": customer_alerts,
        "vendor_alerts": vendor_alerts
    }
    
    # Log summary
    await db.payment_alert_logs.insert_one(summary)
    
    logger.info(f"Payment alerts sent - Customers: {customer_alerts}, Vendors: {vendor_alerts}")
    
    return summary


async def schedule_payment_alerts(db, interval_hours: int = 24):
    """
    Schedule payment alerts to run periodically.
    Default: Once every 24 hours.
    """
    while True:
        try:
            await run_payment_due_alerts(db)
        except Exception as e:
            logger.error(f"Payment alert scheduler error: {e}")
        
        # Wait for next run
        await asyncio.sleep(interval_hours * 3600)
