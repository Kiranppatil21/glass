"""
Admin Dashboard Router - Real-time metrics and overview
"""
from fastapi import APIRouter, Depends
from datetime import datetime, timezone, timedelta
from .base import get_erp_user, get_db

admin_router = APIRouter(prefix="/admin", tags=["Admin"])


@admin_router.get("/dashboard")
async def get_admin_dashboard(current_user: dict = Depends(get_erp_user)):
    """Get real-time admin dashboard metrics"""
    db = get_db()
    today = datetime.now(timezone.utc).date()
    today_start = datetime.combine(today, datetime.min.time()).replace(tzinfo=timezone.utc)
    
    # Today's orders
    orders_today = await db.production_orders.count_documents({
        "created_at": {"$gte": today_start.isoformat()}
    })
    
    # Production status counts
    production_stats = {}
    stages = ['pending', 'cutting', 'polishing', 'grinding', 'toughening', 'quality_check', 'packing', 'dispatched']
    for stage in stages:
        count = await db.production_orders.count_documents({"current_stage": stage})
        production_stats[stage] = count
    
    # Breakage today
    breakage_pipeline = [
        {"$match": {"created_at": {"$gte": today_start.isoformat()}}},
        {"$group": {"_id": None, "total_loss": {"$sum": "$total_loss"}}}
    ]
    breakage_result = await db.breakage_entries.aggregate(breakage_pipeline).to_list(1)
    breakage_today = breakage_result[0]['total_loss'] if breakage_result else 0
    
    # Low stock
    low_stock = await db.raw_materials.count_documents({"$expr": {"$lte": ["$current_stock", "$minimum_stock"]}})
    
    # Pending POs
    pending_pos = await db.purchase_orders.count_documents({"status": "pending"})
    
    # Present employees
    present_today = await db.attendance.count_documents({
        "date": today.isoformat(),
        "status": "present"
    })
    
    # Job Work Stats
    job_work_today = await db.job_work_orders.count_documents({
        "created_at": {"$gte": today_start.isoformat()}
    })
    job_work_pending = await db.job_work_orders.count_documents({
        "status": {"$in": ["pending", "accepted", "material_received", "in_process"]}
    })
    job_work_revenue_pipeline = [
        {"$match": {"payment_status": "completed"}},
        {"$group": {"_id": None, "total": {"$sum": "$paid_amount"}}}
    ]
    job_work_revenue_result = await db.job_work_orders.aggregate(job_work_revenue_pipeline).to_list(1)
    job_work_revenue = job_work_revenue_result[0]["total"] if job_work_revenue_result else 0
    
    return {
        "orders_today": orders_today,
        "production_stats": production_stats,
        "breakage_today": round(breakage_today, 2),
        "low_stock_items": low_stock,
        "pending_pos": pending_pos,
        "present_employees": present_today,
        # Job Work Stats
        "job_work": {
            "today": job_work_today,
            "pending": job_work_pending,
            "total_revenue": round(job_work_revenue, 2)
        }
    }


@admin_router.get("/charts/revenue")
async def get_revenue_chart_data(
    months: int = 6,
    current_user: dict = Depends(get_erp_user)
):
    """Get monthly revenue data for charts (last N months)"""
    db = get_db()
    today = datetime.now(timezone.utc).date()
    
    revenue_data = []
    for i in range(months - 1, -1, -1):
        # Calculate month start/end
        month_date = today.replace(day=1) - timedelta(days=i * 30)
        month_start = month_date.replace(day=1).isoformat()
        
        # Get next month
        if month_date.month == 12:
            next_month = month_date.replace(year=month_date.year + 1, month=1, day=1)
        else:
            next_month = month_date.replace(month=month_date.month + 1, day=1)
        month_end = next_month.isoformat()
        
        # Get invoices for this month
        invoices = await db.invoices.find({
            "created_at": {"$gte": month_start, "$lt": month_end}
        }, {"_id": 0, "subtotal": 1, "total": 1}).to_list(1000)
        
        total_revenue = sum(inv.get("subtotal", 0) for inv in invoices)
        
        # Get payments (collections) for this month
        payments = await db.payments.find({
            "created_at": {"$gte": month_start, "$lt": month_end}
        }, {"_id": 0, "amount": 1}).to_list(1000)
        
        total_collections = sum(p.get("amount", 0) for p in payments)
        
        revenue_data.append({
            "month": month_date.strftime("%b %Y"),
            "revenue": round(total_revenue, 2),
            "collections": round(total_collections, 2)
        })
    
    return revenue_data


@admin_router.get("/charts/production")
async def get_production_chart_data(current_user: dict = Depends(get_erp_user)):
    """Get production stage distribution for charts"""
    db = get_db()
    
    stages = [
        {'id': 'pending', 'name': 'Pending', 'color': '#94a3b8'},
        {'id': 'cutting', 'name': 'Cutting', 'color': '#3b82f6'},
        {'id': 'polishing', 'name': 'Polishing', 'color': '#8b5cf6'},
        {'id': 'grinding', 'name': 'Grinding', 'color': '#f59e0b'},
        {'id': 'toughening', 'name': 'Toughening', 'color': '#ef4444'},
        {'id': 'quality_check', 'name': 'QC', 'color': '#10b981'},
        {'id': 'packing', 'name': 'Packing', 'color': '#06b6d4'},
        {'id': 'dispatched', 'name': 'Dispatched', 'color': '#22c55e'}
    ]
    
    chart_data = []
    for stage in stages:
        count = await db.production_orders.count_documents({"current_stage": stage['id']})
        if count > 0:
            chart_data.append({
                "name": stage['name'],
                "value": count,
                "color": stage['color']
            })
    
    return chart_data


@admin_router.get("/charts/expenses")
async def get_expense_chart_data(current_user: dict = Depends(get_erp_user)):
    """Get expense breakdown for pie chart"""
    db = get_db()
    today = datetime.now(timezone.utc).date()
    month_start = today.replace(day=1).isoformat()
    
    # Purchase expenses
    pos = await db.purchase_orders.find({
        "status": "received",
        "received_at": {"$gte": month_start}
    }, {"_id": 0, "subtotal": 1}).to_list(500)
    purchase_total = sum(po.get("subtotal", 0) for po in pos)
    
    # Salary expenses
    salaries = await db.salary_payments.find({
        "created_at": {"$gte": month_start},
        "payment_status": {"$in": ["approved", "paid"]}
    }, {"_id": 0, "net_salary": 1}).to_list(500)
    salary_total = sum(s.get("net_salary", 0) for s in salaries)
    
    # Breakage costs
    breakages = await db.breakage_entries.find({
        "created_at": {"$gte": month_start}
    }, {"_id": 0, "total_loss": 1}).to_list(500)
    breakage_total = sum(b.get("total_loss", 0) for b in breakages)
    
    return [
        {"name": "Raw Materials", "value": round(purchase_total, 2), "color": "#3b82f6"},
        {"name": "Salaries", "value": round(salary_total, 2), "color": "#10b981"},
        {"name": "Breakage Loss", "value": round(breakage_total, 2), "color": "#ef4444"},
    ]


@admin_router.get("/charts/top-customers")
async def get_top_customers_chart(current_user: dict = Depends(get_erp_user)):
    """Get top customers by revenue"""
    db = get_db()
    
    pipeline = [
        {"$group": {
            "_id": "$customer_name",
            "total_revenue": {"$sum": "$subtotal"},
            "invoice_count": {"$sum": 1}
        }},
        {"$sort": {"total_revenue": -1}},
        {"$limit": 5}
    ]
    
    result = await db.invoices.aggregate(pipeline).to_list(5)
    
    return [
        {
            "name": r["_id"] if r["_id"] else "Unknown",
            "revenue": round(r["total_revenue"], 2),
            "orders": r["invoice_count"]
        }
        for r in result if r["_id"]
    ]

