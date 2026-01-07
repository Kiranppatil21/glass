"""
Super Admin Panel Router
Full control over all users, system settings, and reports
"""
from fastapi import APIRouter, Depends, HTTPException, Request
from typing import Optional, List
from datetime import datetime, timezone
from pydantic import BaseModel
from .base import get_erp_user, get_db
from .audit import log_action
import uuid
import hashlib

superadmin_router = APIRouter(prefix="/superadmin", tags=["Super Admin"])


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


def require_super_admin(current_user: dict = Depends(get_erp_user)):
    """Dependency to require super_admin role"""
    if current_user.get("role") != "super_admin":
        raise HTTPException(status_code=403, detail="Super Admin access required")
    return current_user


class CreateUserRequest(BaseModel):
    email: str
    name: str
    phone: str
    password: str
    role: str


class UpdateUserRequest(BaseModel):
    name: Optional[str] = None
    phone: Optional[str] = None
    role: Optional[str] = None
    is_active: Optional[bool] = None


# ==================== USER MANAGEMENT ====================

@superadmin_router.get("/users")
async def get_all_users(
    role: Optional[str] = None,
    status: Optional[str] = None,
    search: Optional[str] = None,
    current_user: dict = Depends(require_super_admin)
):
    """Get all users in the system"""
    db = get_db()
    
    query = {}
    if role:
        query["role"] = role
    if status == "active":
        query["is_active"] = {"$ne": False}
    elif status == "disabled":
        query["is_active"] = False
    if search:
        query["$or"] = [
            {"name": {"$regex": search, "$options": "i"}},
            {"email": {"$regex": search, "$options": "i"}}
        ]
    
    users = await db.users.find(query, {"_id": 0, "password_hash": 0}).sort("created_at", -1).to_list(500)
    
    # Get stats
    total = len(users)
    active = len([u for u in users if u.get("is_active", True)])
    disabled = total - active
    
    # Role breakdown
    role_counts = {}
    for user in users:
        r = user.get("role", "unknown")
        role_counts[r] = role_counts.get(r, 0) + 1
    
    return {
        "users": users,
        "stats": {
            "total": total,
            "active": active,
            "disabled": disabled
        },
        "role_breakdown": role_counts
    }


@superadmin_router.post("/users")
async def create_user(
    request: Request,
    user_data: CreateUserRequest,
    current_user: dict = Depends(require_super_admin)
):
    """Create a new user"""
    db = get_db()
    
    # Check if email exists
    existing = await db.users.find_one({"email": user_data.email})
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")
    
    # Validate role
    valid_roles = ["super_admin", "admin", "owner", "manager", "sales", 
                   "production_manager", "operator", "hr", "accountant", 
                   "store", "customer", "dealer"]
    if user_data.role not in valid_roles:
        raise HTTPException(status_code=400, detail=f"Invalid role. Valid roles: {valid_roles}")
    
    # Create user
    new_user = {
        "id": str(uuid.uuid4()),
        "email": user_data.email,
        "name": user_data.name,
        "phone": user_data.phone,
        "password_hash": hash_password(user_data.password),
        "role": user_data.role,
        "is_active": True,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "created_by": current_user["id"]
    }
    
    await db.users.insert_one(new_user)
    
    # Log action
    await log_action(
        user_id=current_user["id"],
        user_name=current_user["name"],
        user_role=current_user["role"],
        action="create",
        module="users",
        details={"created_user": user_data.email, "role": user_data.role},
        record_id=new_user["id"],
        ip_address=request.client.host if request.client else None
    )
    
    return {
        "message": "User created successfully",
        "user": {
            "id": new_user["id"],
            "email": new_user["email"],
            "name": new_user["name"],
            "role": new_user["role"]
        }
    }


@superadmin_router.put("/users/{user_id}")
async def update_user(
    request: Request,
    user_id: str,
    update_data: UpdateUserRequest,
    current_user: dict = Depends(require_super_admin)
):
    """Update a user's details"""
    db = get_db()
    
    user = await db.users.find_one({"id": user_id}, {"_id": 0, "password_hash": 0})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Prepare update
    updates = {}
    if update_data.name:
        updates["name"] = update_data.name
    if update_data.phone:
        updates["phone"] = update_data.phone
    if update_data.role:
        updates["role"] = update_data.role
    if update_data.is_active is not None:
        updates["is_active"] = update_data.is_active
    
    if updates:
        updates["updated_at"] = datetime.now(timezone.utc).isoformat()
        updates["updated_by"] = current_user["id"]
        
        await db.users.update_one({"id": user_id}, {"$set": updates})
        
        # Log action
        await log_action(
            user_id=current_user["id"],
            user_name=current_user["name"],
            user_role=current_user["role"],
            action="update",
            module="users",
            details={"updated_fields": list(updates.keys())},
            record_id=user_id,
            old_data=user,
            new_data=updates,
            ip_address=request.client.host if request.client else None
        )
    
    return {"message": "User updated successfully"}


@superadmin_router.post("/users/{user_id}/disable")
async def disable_user(
    request: Request,
    user_id: str,
    current_user: dict = Depends(require_super_admin)
):
    """Disable a user's login"""
    db = get_db()
    
    user = await db.users.find_one({"id": user_id}, {"_id": 0})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    if user.get("role") == "super_admin" and user_id != current_user["id"]:
        raise HTTPException(status_code=403, detail="Cannot disable another super admin")
    
    await db.users.update_one(
        {"id": user_id},
        {"$set": {
            "is_active": False,
            "disabled_at": datetime.now(timezone.utc).isoformat(),
            "disabled_by": current_user["id"]
        }}
    )
    
    # Log action
    await log_action(
        user_id=current_user["id"],
        user_name=current_user["name"],
        user_role=current_user["role"],
        action="update",
        module="users",
        details={"action": "disable_login", "target_user": user["email"]},
        record_id=user_id,
        ip_address=request.client.host if request.client else None
    )
    
    return {"message": f"User {user['email']} has been disabled"}


@superadmin_router.post("/users/{user_id}/enable")
async def enable_user(
    request: Request,
    user_id: str,
    current_user: dict = Depends(require_super_admin)
):
    """Enable a user's login"""
    db = get_db()
    
    user = await db.users.find_one({"id": user_id}, {"_id": 0})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    await db.users.update_one(
        {"id": user_id},
        {"$set": {
            "is_active": True,
            "enabled_at": datetime.now(timezone.utc).isoformat(),
            "enabled_by": current_user["id"]
        }}
    )
    
    # Log action
    await log_action(
        user_id=current_user["id"],
        user_name=current_user["name"],
        user_role=current_user["role"],
        action="update",
        module="users",
        details={"action": "enable_login", "target_user": user["email"]},
        record_id=user_id,
        ip_address=request.client.host if request.client else None
    )
    
    return {"message": f"User {user['email']} has been enabled"}


@superadmin_router.post("/users/{user_id}/reset-password")
async def reset_user_password(
    request: Request,
    user_id: str,
    new_password: str,
    current_user: dict = Depends(require_super_admin)
):
    """Reset a user's password"""
    db = get_db()
    
    user = await db.users.find_one({"id": user_id}, {"_id": 0})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    if len(new_password) < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters")
    
    await db.users.update_one(
        {"id": user_id},
        {"$set": {
            "password_hash": hash_password(new_password),
            "password_reset_at": datetime.now(timezone.utc).isoformat(),
            "password_reset_by": current_user["id"]
        }}
    )
    
    # Log action
    await log_action(
        user_id=current_user["id"],
        user_name=current_user["name"],
        user_role=current_user["role"],
        action="update",
        module="users",
        details={"action": "reset_password", "target_user": user["email"]},
        record_id=user_id,
        ip_address=request.client.host if request.client else None
    )
    
    return {"message": f"Password reset for {user['email']}"}


@superadmin_router.delete("/users/{user_id}")
async def delete_user(
    request: Request,
    user_id: str,
    current_user: dict = Depends(require_super_admin)
):
    """Delete a user (soft delete - marks as deleted)"""
    db = get_db()
    
    user = await db.users.find_one({"id": user_id}, {"_id": 0, "password_hash": 0})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    if user.get("role") == "super_admin":
        raise HTTPException(status_code=403, detail="Cannot delete super admin users")
    
    # Soft delete
    await db.users.update_one(
        {"id": user_id},
        {"$set": {
            "is_deleted": True,
            "is_active": False,
            "deleted_at": datetime.now(timezone.utc).isoformat(),
            "deleted_by": current_user["id"]
        }}
    )
    
    # Log action with old data for recovery
    await log_action(
        user_id=current_user["id"],
        user_name=current_user["name"],
        user_role=current_user["role"],
        action="delete",
        module="users",
        details={"deleted_user": user["email"], "role": user["role"]},
        record_id=user_id,
        old_data=user,
        ip_address=request.client.host if request.client else None
    )
    
    return {"message": f"User {user['email']} has been deleted"}


# ==================== SYSTEM OVERVIEW ====================

@superadmin_router.get("/dashboard")
async def get_super_admin_dashboard(current_user: dict = Depends(require_super_admin)):
    """Get Super Admin dashboard overview"""
    db = get_db()
    
    # User stats
    total_users = await db.users.count_documents({})
    active_users = await db.users.count_documents({"is_active": {"$ne": False}})
    disabled_users = await db.users.count_documents({"is_active": False})
    
    # Role breakdown
    roles = ["super_admin", "admin", "owner", "manager", "sales", "production_manager", 
             "operator", "hr", "accountant", "store", "customer", "dealer"]
    role_counts = {}
    for role in roles:
        role_counts[role] = await db.users.count_documents({"role": role})
    
    # Today's activity
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    today_logs = await db.audit_logs.count_documents({"date": today})
    today_logins = await db.audit_logs.count_documents({"date": today, "action": "login"})
    today_active_users = len(await db.audit_logs.distinct("user_id", {"date": today}))
    
    # Recent activity
    recent_logs = await db.audit_logs.find(
        {},
        {"_id": 0}
    ).sort("timestamp", -1).limit(20).to_list(20)
    
    # System stats
    collections_stats = {
        "leads": await db.erp_leads.count_documents({}),
        "production_orders": await db.production_orders.count_documents({}),
        "invoices": await db.invoices.count_documents({}),
        "employees": await db.erp_employees.count_documents({}),
        "materials": await db.materials.count_documents({}),
        "expenses": await db.expenses.count_documents({}),
        "assets": await db.company_assets.count_documents({}) + await db.rented_assets.count_documents({}),
        "holidays": await db.holidays.count_documents({})
    }
    
    return {
        "users": {
            "total": total_users,
            "active": active_users,
            "disabled": disabled_users,
            "by_role": role_counts
        },
        "today": {
            "total_actions": today_logs,
            "logins": today_logins,
            "active_users": today_active_users
        },
        "system": collections_stats,
        "recent_activity": recent_logs
    }


@superadmin_router.get("/login-history")
async def get_login_history(
    user_id: Optional[str] = None,
    days: int = 30,
    current_user: dict = Depends(require_super_admin)
):
    """Get login/logout history"""
    db = get_db()
    
    start_date = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    
    query = {
        "action": {"$in": ["login", "logout"]},
        "date": {"$gte": start_date}
    }
    if user_id:
        query["user_id"] = user_id
    
    logs = await db.audit_logs.find(query, {"_id": 0}).sort("timestamp", -1).to_list(500)
    
    # Group by user
    user_logins = {}
    for log in logs:
        uid = log["user_id"]
        if uid not in user_logins:
            user_logins[uid] = {
                "user_id": uid,
                "user_name": log["user_name"],
                "user_role": log["user_role"],
                "login_count": 0,
                "logout_count": 0,
                "last_login": None,
                "last_logout": None
            }
        
        if log["action"] == "login":
            user_logins[uid]["login_count"] += 1
            if not user_logins[uid]["last_login"]:
                user_logins[uid]["last_login"] = log["timestamp"]
        else:
            user_logins[uid]["logout_count"] += 1
            if not user_logins[uid]["last_logout"]:
                user_logins[uid]["last_logout"] = log["timestamp"]
    
    return {
        "period_days": days,
        "total_logins": sum(u["login_count"] for u in user_logins.values()),
        "unique_users": len(user_logins),
        "by_user": list(user_logins.values()),
        "recent_logins": [l for l in logs if l["action"] == "login"][:50]
    }


@superadmin_router.get("/system-settings")
async def get_system_settings(current_user: dict = Depends(require_super_admin)):
    """Get all system settings"""
    db = get_db()
    
    settings = await db.system_settings.find({}, {"_id": 0}).to_list(100)
    wallet_settings = await db.wallet_settings.find_one({}, {"_id": 0})
    holiday_settings = await db.holiday_settings.find_one({}, {"_id": 0})
    
    return {
        "general": settings,
        "wallet": wallet_settings,
        "holidays": holiday_settings
    }


@superadmin_router.put("/system-settings/{setting_key}")
async def update_system_setting(
    request: Request,
    setting_key: str,
    value: dict,
    current_user: dict = Depends(require_super_admin)
):
    """Update a system setting"""
    db = get_db()
    
    old_setting = await db.system_settings.find_one({"key": setting_key}, {"_id": 0})
    
    await db.system_settings.update_one(
        {"key": setting_key},
        {"$set": {
            "key": setting_key,
            "value": value,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "updated_by": current_user["id"]
        }},
        upsert=True
    )
    
    # Log action
    await log_action(
        user_id=current_user["id"],
        user_name=current_user["name"],
        user_role=current_user["role"],
        action="update",
        module="settings",
        details={"setting": setting_key},
        record_id=setting_key,
        old_data=old_setting,
        new_data=value,
        ip_address=request.client.host if request.client else None
    )
    
    return {"message": "Setting updated successfully"}


# ==================== REPORTS ====================

@superadmin_router.get("/reports/summary")
async def get_reports_summary(current_user: dict = Depends(require_super_admin)):
    """Get summary of all available reports"""
    db = get_db()
    
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    this_month = datetime.now(timezone.utc).strftime("%Y-%m")
    this_year = datetime.now(timezone.utc).strftime("%Y")
    
    return {
        "available_reports": [
            {
                "name": "Daily Activity Log",
                "endpoint": "/api/erp/audit/daily-activity",
                "description": "Aaj kisne kya kiya - User-wise daily breakdown"
            },
            {
                "name": "Monthly MIS Report",
                "endpoint": "/api/erp/audit/monthly-mis",
                "description": "Monthly performance, trends, and analysis"
            },
            {
                "name": "Yearly Audit Trail",
                "endpoint": "/api/erp/audit/yearly-audit",
                "description": "Compliance report with all actions and deletions"
            },
            {
                "name": "User Action Report",
                "endpoint": "/api/erp/audit/user-report/{user_id}",
                "description": "Detailed report for specific user"
            },
            {
                "name": "Approval History",
                "endpoint": "/api/erp/audit/approval-history",
                "description": "All approve/reject actions"
            },
            {
                "name": "Deleted Records Log",
                "endpoint": "/api/erp/audit/deleted-records",
                "description": "Track all deleted records for recovery"
            },
            {
                "name": "Employee Performance",
                "endpoint": "/api/erp/audit/employee-performance",
                "description": "Individual performance evaluation with grades"
            },
            {
                "name": "Login History",
                "endpoint": "/api/erp/superadmin/login-history",
                "description": "All login/logout events"
            }
        ],
        "quick_stats": {
            "today_actions": await db.audit_logs.count_documents({"date": today}),
            "this_month_actions": await db.audit_logs.count_documents({"month": this_month}),
            "this_year_actions": await db.audit_logs.count_documents({"year": this_year})
        }
    }
