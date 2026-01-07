"""
Audit Trail & Activity Logging Router
Tracks all user actions: Add, Edit, Delete, Approve/Reject, Login/Logout, Orders
Generates Daily/Monthly/Yearly reports for compliance and MIS
"""
from fastapi import APIRouter, Depends, HTTPException, Request
from typing import Optional, List
from datetime import datetime, timezone, timedelta
from .base import get_erp_user, get_db
import uuid

audit_router = APIRouter(prefix="/audit", tags=["Audit Trail & MIS"])

# Action Types
ACTION_TYPES = {
    "LOGIN": "login",
    "LOGOUT": "logout",
    "CREATE": "create",
    "UPDATE": "update",
    "DELETE": "delete",
    "APPROVE": "approve",
    "REJECT": "reject",
    "ORDER_CONFIRM": "order_confirm",
    "PAYMENT": "payment",
    "STATUS_CHANGE": "status_change",
    "EXPORT": "export",
    "PRINT": "print",
    "VIEW": "view"
}

# Module Names
MODULES = [
    "auth", "users", "leads", "production", "inventory", "purchase",
    "hr", "accounts", "payouts", "expenses", "assets", "holidays",
    "wallet", "orders", "invoices", "customers", "settings"
]


async def log_action(
    user_id: str,
    user_name: str,
    user_role: str,
    action: str,
    module: str,
    details: dict,
    ip_address: str = None,
    record_id: str = None,
    old_data: dict = None,
    new_data: dict = None
):
    """Log an action to the audit trail"""
    db = get_db()
    
    log_entry = {
        "id": str(uuid.uuid4()),
        "user_id": user_id,
        "user_name": user_name,
        "user_role": user_role,
        "action": action,
        "module": module,
        "record_id": record_id,
        "details": details,
        "old_data": old_data,
        "new_data": new_data,
        "ip_address": ip_address,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "month": datetime.now(timezone.utc).strftime("%Y-%m"),
        "year": datetime.now(timezone.utc).strftime("%Y")
    }
    
    await db.audit_logs.insert_one(log_entry)
    return log_entry


@audit_router.get("/logs")
async def get_audit_logs(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    user_id: Optional[str] = None,
    action: Optional[str] = None,
    module: Optional[str] = None,
    limit: int = 100,
    skip: int = 0,
    current_user: dict = Depends(get_erp_user)
):
    """Get audit logs with filters"""
    if current_user.get("role") not in ["super_admin", "admin", "owner"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    query = {}
    
    if start_date:
        query["date"] = {"$gte": start_date}
    if end_date:
        if "date" in query:
            query["date"]["$lte"] = end_date
        else:
            query["date"] = {"$lte": end_date}
    if user_id:
        query["user_id"] = user_id
    if action:
        query["action"] = action
    if module:
        query["module"] = module
    
    logs = await db.audit_logs.find(query, {"_id": 0}).sort("timestamp", -1).skip(skip).limit(limit).to_list(limit)
    total = await db.audit_logs.count_documents(query)
    
    return {
        "logs": logs,
        "total": total,
        "limit": limit,
        "skip": skip
    }


@audit_router.get("/daily-activity")
async def get_daily_activity(
    date: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get daily activity log - Aaj kisne kya kiya"""
    if current_user.get("role") not in ["super_admin", "admin", "owner"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    target_date = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    # Get all logs for the day
    logs = await db.audit_logs.find(
        {"date": target_date},
        {"_id": 0}
    ).sort("timestamp", -1).to_list(1000)
    
    # Group by user
    user_activity = {}
    for log in logs:
        uid = log["user_id"]
        if uid not in user_activity:
            user_activity[uid] = {
                "user_id": uid,
                "user_name": log["user_name"],
                "user_role": log["user_role"],
                "total_actions": 0,
                "actions": {
                    "create": 0, "update": 0, "delete": 0,
                    "approve": 0, "reject": 0, "login": 0, "logout": 0,
                    "order_confirm": 0, "view": 0, "export": 0, "print": 0
                },
                "modules_accessed": set(),
                "first_action": log["timestamp"],
                "last_action": log["timestamp"]
            }
        
        user_activity[uid]["total_actions"] += 1
        action_type = log["action"]
        if action_type in user_activity[uid]["actions"]:
            user_activity[uid]["actions"][action_type] += 1
        user_activity[uid]["modules_accessed"].add(log["module"])
        user_activity[uid]["last_action"] = log["timestamp"]
    
    # Convert sets to lists
    for uid in user_activity:
        user_activity[uid]["modules_accessed"] = list(user_activity[uid]["modules_accessed"])
    
    # Summary stats
    summary = {
        "date": target_date,
        "total_actions": len(logs),
        "active_users": len(user_activity),
        "actions_breakdown": {
            "create": sum(1 for l in logs if l["action"] == "create"),
            "update": sum(1 for l in logs if l["action"] == "update"),
            "delete": sum(1 for l in logs if l["action"] == "delete"),
            "approve": sum(1 for l in logs if l["action"] == "approve"),
            "reject": sum(1 for l in logs if l["action"] == "reject"),
            "login": sum(1 for l in logs if l["action"] == "login"),
            "logout": sum(1 for l in logs if l["action"] == "logout"),
            "order_confirm": sum(1 for l in logs if l["action"] == "order_confirm"),
        },
        "modules_breakdown": {}
    }
    
    for log in logs:
        module = log["module"]
        if module not in summary["modules_breakdown"]:
            summary["modules_breakdown"][module] = 0
        summary["modules_breakdown"][module] += 1
    
    return {
        "summary": summary,
        "user_activity": list(user_activity.values()),
        "recent_logs": logs[:50]
    }


@audit_router.get("/monthly-mis")
async def get_monthly_mis_report(
    month: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get Monthly MIS Report - Management Information System"""
    if current_user.get("role") not in ["super_admin", "admin", "owner"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    target_month = month or datetime.now(timezone.utc).strftime("%Y-%m")
    
    # Get all logs for the month
    logs = await db.audit_logs.find(
        {"month": target_month},
        {"_id": 0}
    ).to_list(10000)
    
    # User performance metrics
    user_metrics = {}
    daily_activity = {}
    
    for log in logs:
        uid = log["user_id"]
        date = log["date"]
        
        # User metrics
        if uid not in user_metrics:
            user_metrics[uid] = {
                "user_id": uid,
                "user_name": log["user_name"],
                "user_role": log["user_role"],
                "total_actions": 0,
                "active_days": set(),
                "actions": {"create": 0, "update": 0, "delete": 0, "approve": 0, "reject": 0},
                "modules_used": set()
            }
        
        user_metrics[uid]["total_actions"] += 1
        user_metrics[uid]["active_days"].add(date)
        if log["action"] in user_metrics[uid]["actions"]:
            user_metrics[uid]["actions"][log["action"]] += 1
        user_metrics[uid]["modules_used"].add(log["module"])
        
        # Daily breakdown
        if date not in daily_activity:
            daily_activity[date] = {"date": date, "total": 0, "users": set()}
        daily_activity[date]["total"] += 1
        daily_activity[date]["users"].add(uid)
    
    # Convert sets
    for uid in user_metrics:
        user_metrics[uid]["active_days"] = len(user_metrics[uid]["active_days"])
        user_metrics[uid]["modules_used"] = list(user_metrics[uid]["modules_used"])
    
    for date in daily_activity:
        daily_activity[date]["users"] = len(daily_activity[date]["users"])
    
    # Top performers
    top_performers = sorted(user_metrics.values(), key=lambda x: x["total_actions"], reverse=True)[:10]
    
    # Module usage
    module_usage = {}
    for log in logs:
        module = log["module"]
        if module not in module_usage:
            module_usage[module] = 0
        module_usage[module] += 1
    
    return {
        "month": target_month,
        "summary": {
            "total_actions": len(logs),
            "total_users": len(user_metrics),
            "avg_actions_per_user": round(len(logs) / max(len(user_metrics), 1), 2),
            "avg_actions_per_day": round(len(logs) / max(len(daily_activity), 1), 2)
        },
        "top_performers": top_performers,
        "daily_breakdown": sorted(daily_activity.values(), key=lambda x: x["date"]),
        "module_usage": module_usage,
        "action_breakdown": {
            "create": sum(1 for l in logs if l["action"] == "create"),
            "update": sum(1 for l in logs if l["action"] == "update"),
            "delete": sum(1 for l in logs if l["action"] == "delete"),
            "approve": sum(1 for l in logs if l["action"] == "approve"),
            "reject": sum(1 for l in logs if l["action"] == "reject")
        }
    }


@audit_router.get("/yearly-audit")
async def get_yearly_audit_trail(
    year: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get Yearly Audit Trail - Compliance & Review"""
    if current_user.get("role") not in ["super_admin", "admin", "owner"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    target_year = year or datetime.now(timezone.utc).strftime("%Y")
    
    # Aggregate monthly data
    pipeline = [
        {"$match": {"year": target_year}},
        {"$group": {
            "_id": "$month",
            "total_actions": {"$sum": 1},
            "unique_users": {"$addToSet": "$user_id"},
            "creates": {"$sum": {"$cond": [{"$eq": ["$action", "create"]}, 1, 0]}},
            "updates": {"$sum": {"$cond": [{"$eq": ["$action", "update"]}, 1, 0]}},
            "deletes": {"$sum": {"$cond": [{"$eq": ["$action", "delete"]}, 1, 0]}},
            "approvals": {"$sum": {"$cond": [{"$eq": ["$action", "approve"]}, 1, 0]}},
            "rejections": {"$sum": {"$cond": [{"$eq": ["$action", "reject"]}, 1, 0]}}
        }},
        {"$sort": {"_id": 1}}
    ]
    
    monthly_data = await db.audit_logs.aggregate(pipeline).to_list(12)
    
    # Process results
    for item in monthly_data:
        item["month"] = item.pop("_id")
        item["unique_users"] = len(item["unique_users"])
    
    # Get total counts
    total_logs = await db.audit_logs.count_documents({"year": target_year})
    
    # Get deleted records
    deleted_records = await db.audit_logs.find(
        {"year": target_year, "action": "delete"},
        {"_id": 0, "timestamp": 1, "user_name": 1, "module": 1, "details": 1, "old_data": 1}
    ).sort("timestamp", -1).limit(100).to_list(100)
    
    # User performance summary
    user_pipeline = [
        {"$match": {"year": target_year}},
        {"$group": {
            "_id": {"user_id": "$user_id", "user_name": "$user_name", "user_role": "$user_role"},
            "total_actions": {"$sum": 1},
            "creates": {"$sum": {"$cond": [{"$eq": ["$action", "create"]}, 1, 0]}},
            "updates": {"$sum": {"$cond": [{"$eq": ["$action", "update"]}, 1, 0]}},
            "deletes": {"$sum": {"$cond": [{"$eq": ["$action", "delete"]}, 1, 0]}}
        }},
        {"$sort": {"total_actions": -1}},
        {"$limit": 20}
    ]
    
    user_summary = await db.audit_logs.aggregate(user_pipeline).to_list(20)
    for item in user_summary:
        item["user_id"] = item["_id"]["user_id"]
        item["user_name"] = item["_id"]["user_name"]
        item["user_role"] = item["_id"]["user_role"]
        del item["_id"]
    
    return {
        "year": target_year,
        "summary": {
            "total_actions": total_logs,
            "months_active": len(monthly_data)
        },
        "monthly_breakdown": monthly_data,
        "user_performance": user_summary,
        "deleted_records": deleted_records
    }


@audit_router.get("/user-report/{user_id}")
async def get_user_action_report(
    user_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get detailed report for a specific user"""
    if current_user.get("role") not in ["super_admin", "admin", "owner"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    query = {"user_id": user_id}
    if start_date:
        query["date"] = {"$gte": start_date}
    if end_date:
        if "date" in query:
            query["date"]["$lte"] = end_date
        else:
            query["date"] = {"$lte": end_date}
    
    logs = await db.audit_logs.find(query, {"_id": 0}).sort("timestamp", -1).to_list(1000)
    
    if not logs:
        return {"user_id": user_id, "message": "No activity found"}
    
    # Calculate metrics
    user_info = {
        "user_id": user_id,
        "user_name": logs[0]["user_name"],
        "user_role": logs[0]["user_role"]
    }
    
    action_counts = {}
    module_counts = {}
    daily_counts = {}
    
    for log in logs:
        action = log["action"]
        module = log["module"]
        date = log["date"]
        
        action_counts[action] = action_counts.get(action, 0) + 1
        module_counts[module] = module_counts.get(module, 0) + 1
        daily_counts[date] = daily_counts.get(date, 0) + 1
    
    return {
        "user": user_info,
        "period": {
            "start": start_date or logs[-1]["date"],
            "end": end_date or logs[0]["date"]
        },
        "summary": {
            "total_actions": len(logs),
            "active_days": len(daily_counts),
            "avg_actions_per_day": round(len(logs) / max(len(daily_counts), 1), 2)
        },
        "action_breakdown": action_counts,
        "module_breakdown": module_counts,
        "daily_activity": [{"date": k, "count": v} for k, v in sorted(daily_counts.items())],
        "recent_actions": logs[:50]
    }


@audit_router.get("/approval-history")
async def get_approval_history(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    module: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get approval/rejection history"""
    if current_user.get("role") not in ["super_admin", "admin", "owner"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    query = {"action": {"$in": ["approve", "reject"]}}
    if start_date:
        query["date"] = {"$gte": start_date}
    if end_date:
        if "date" in query:
            query["date"]["$lte"] = end_date
        else:
            query["date"] = {"$lte": end_date}
    if module:
        query["module"] = module
    
    logs = await db.audit_logs.find(query, {"_id": 0}).sort("timestamp", -1).to_list(500)
    
    approved = [l for l in logs if l["action"] == "approve"]
    rejected = [l for l in logs if l["action"] == "reject"]
    
    return {
        "summary": {
            "total_approvals": len(approved),
            "total_rejections": len(rejected),
            "approval_rate": round(len(approved) / max(len(logs), 1) * 100, 2)
        },
        "approvals": approved[:100],
        "rejections": rejected[:100]
    }


@audit_router.get("/deleted-records")
async def get_deleted_records(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    module: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get deleted records log - For compliance and recovery"""
    if current_user.get("role") not in ["super_admin", "admin", "owner"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    query = {"action": "delete"}
    if start_date:
        query["date"] = {"$gte": start_date}
    if end_date:
        if "date" in query:
            query["date"]["$lte"] = end_date
        else:
            query["date"] = {"$lte": end_date}
    if module:
        query["module"] = module
    
    logs = await db.audit_logs.find(query, {"_id": 0}).sort("timestamp", -1).to_list(500)
    
    # Group by module
    by_module = {}
    for log in logs:
        mod = log["module"]
        if mod not in by_module:
            by_module[mod] = []
        by_module[mod].append(log)
    
    return {
        "total_deleted": len(logs),
        "by_module": {k: len(v) for k, v in by_module.items()},
        "records": logs
    }


@audit_router.get("/employee-performance")
async def get_employee_performance_report(
    month: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get Employee Performance Report - Individual evaluation"""
    if current_user.get("role") not in ["super_admin", "admin", "owner"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    target_month = month or datetime.now(timezone.utc).strftime("%Y-%m")
    
    # Get all employees
    employees = await db.users.find(
        {"role": {"$nin": ["customer", "dealer"]}},
        {"_id": 0, "id": 1, "name": 1, "email": 1, "role": 1}
    ).to_list(100)
    
    # Get activity for each employee
    performance = []
    for emp in employees:
        logs = await db.audit_logs.find(
            {"user_id": emp["id"], "month": target_month},
            {"_id": 0}
        ).to_list(1000)
        
        if not logs:
            performance.append({
                "employee": emp,
                "total_actions": 0,
                "active_days": 0,
                "efficiency_score": 0,
                "grade": "N/A"
            })
            continue
        
        active_days = len(set(l["date"] for l in logs))
        total_actions = len(logs)
        creates = sum(1 for l in logs if l["action"] == "create")
        updates = sum(1 for l in logs if l["action"] == "update")
        approvals = sum(1 for l in logs if l["action"] == "approve")
        
        # Calculate efficiency score (simple formula)
        efficiency_score = min(100, round((total_actions / max(active_days, 1)) * 5))
        
        # Grade based on score
        if efficiency_score >= 80:
            grade = "A"
        elif efficiency_score >= 60:
            grade = "B"
        elif efficiency_score >= 40:
            grade = "C"
        elif efficiency_score >= 20:
            grade = "D"
        else:
            grade = "F"
        
        performance.append({
            "employee": emp,
            "total_actions": total_actions,
            "active_days": active_days,
            "creates": creates,
            "updates": updates,
            "approvals": approvals,
            "avg_per_day": round(total_actions / max(active_days, 1), 2),
            "efficiency_score": efficiency_score,
            "grade": grade
        })
    
    # Sort by efficiency score
    performance.sort(key=lambda x: x["efficiency_score"], reverse=True)
    
    return {
        "month": target_month,
        "total_employees": len(employees),
        "active_employees": len([p for p in performance if p["total_actions"] > 0]),
        "performance": performance
    }


# Helper function to log actions from other routers
async def audit_log(request: Request, current_user: dict, action: str, module: str, details: dict, record_id: str = None, old_data: dict = None, new_data: dict = None):
    """Helper to log actions from other routers"""
    ip = request.client.host if request.client else None
    await log_action(
        user_id=current_user.get("id", "system"),
        user_name=current_user.get("name", "System"),
        user_role=current_user.get("role", "system"),
        action=action,
        module=module,
        details=details,
        ip_address=ip,
        record_id=record_id,
        old_data=old_data,
        new_data=new_data
    )
