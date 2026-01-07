"""
Daily Expense Management Router
Handles expense categories, entries, approvals, and reporting
"""
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import uuid
import os
import shutil
from .base import get_erp_user, get_db

expense_router = APIRouter(prefix="/expenses", tags=["Daily Expenses"])

# Expense Categories
DEFAULT_CATEGORIES = [
    {"id": "electricity", "name": "Electricity", "icon": "⚡", "color": "#f59e0b"},
    {"id": "fuel", "name": "Fuel", "icon": "⛽", "color": "#ef4444"},
    {"id": "repair", "name": "Repair & Maintenance", "icon": "🔧", "color": "#3b82f6"},
    {"id": "labour", "name": "Labour Charges", "icon": "👷", "color": "#8b5cf6"},
    {"id": "consumables", "name": "Consumables", "icon": "📦", "color": "#10b981"},
    {"id": "transport", "name": "Transport", "icon": "🚛", "color": "#06b6d4"},
    {"id": "raw_material", "name": "Raw Material", "icon": "🏭", "color": "#6366f1"},
    {"id": "office", "name": "Office Expenses", "icon": "🏢", "color": "#ec4899"},
    {"id": "misc", "name": "Miscellaneous", "icon": "📋", "color": "#64748b"},
]

PAYMENT_MODES = ["cash", "bank", "upi", "cheque", "credit"]

UPLOAD_DIR = "/app/backend/uploads/expenses"
os.makedirs(UPLOAD_DIR, exist_ok=True)


# =============== EXPENSE CATEGORIES ===============

@expense_router.get("/categories")
async def get_expense_categories(current_user: dict = Depends(get_erp_user)):
    """Get all expense categories"""
    db = get_db()
    categories = await db.expense_categories.find({}, {"_id": 0}).to_list(100)
    
    if not categories:
        # Initialize default categories
        for cat in DEFAULT_CATEGORIES:
            cat["created_at"] = datetime.now(timezone.utc).isoformat()
            await db.expense_categories.insert_one(cat)
        categories = DEFAULT_CATEGORIES
    
    return categories


@expense_router.post("/categories")
async def create_expense_category(
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Create new expense category (Admin only)"""
    if current_user.get("role") not in ["admin", "owner", "accountant"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    db = get_db()
    category = {
        "id": data.get("id") or str(uuid.uuid4())[:8],
        "name": data["name"],
        "icon": data.get("icon", "📋"),
        "color": data.get("color", "#64748b"),
        "budget_limit": data.get("budget_limit", 0),
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.expense_categories.insert_one(category)
    return {"message": "Category created", "category": category}


# =============== EXPENSE SETTINGS ===============

@expense_router.get("/settings")
async def get_expense_settings(current_user: dict = Depends(get_erp_user)):
    """Get expense module settings"""
    db = get_db()
    settings = await db.expense_settings.find_one({"id": "default"}, {"_id": 0})
    
    if not settings:
        settings = {
            "id": "default",
            "approval_enabled": True,
            "approval_levels": 2,  # 1=Direct, 2=Supervisor->Admin, 3=Supervisor->Accounts->Admin
            "admin_direct_approval": True,
            "daily_limit": 50000,
            "monthly_limit": 500000,
            "require_attachment_above": 1000,  # Require receipt above this amount
            "auto_post_to_ledger": True,
            "departments": ["Production", "Admin", "Sales", "HR", "Store", "Transport"],
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        await db.expense_settings.insert_one(settings)
    
    return settings


@expense_router.put("/settings")
async def update_expense_settings(
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Update expense settings (Admin only)"""
    if current_user.get("role") not in ["admin", "owner"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    db = get_db()
    allowed_fields = [
        "approval_enabled", "approval_levels", "admin_direct_approval",
        "daily_limit", "monthly_limit", "require_attachment_above",
        "auto_post_to_ledger", "departments"
    ]
    
    update_data = {k: v for k, v in data.items() if k in allowed_fields}
    update_data["updated_at"] = datetime.now(timezone.utc).isoformat()
    
    await db.expense_settings.update_one(
        {"id": "default"},
        {"$set": update_data},
        upsert=True
    )
    
    return {"message": "Settings updated"}


# =============== EXPENSE ENTRIES ===============

@expense_router.post("/entries")
async def create_expense_entry(
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Create new expense entry"""
    db = get_db()
    settings = await get_expense_settings(current_user)
    
    amount = float(data.get("amount", 0))
    if amount <= 0:
        raise HTTPException(status_code=400, detail="Invalid amount")
    
    # Check daily limit
    today = datetime.now(timezone.utc).date().isoformat()
    daily_total = await db.expense_entries.aggregate([
        {"$match": {"date": today, "status": {"$ne": "rejected"}}},
        {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
    ]).to_list(1)
    
    current_daily = daily_total[0]["total"] if daily_total else 0
    if current_daily + amount > settings.get("daily_limit", 50000):
        raise HTTPException(status_code=400, detail=f"Daily expense limit (₹{settings['daily_limit']}) exceeded")
    
    # Determine initial status
    user_role = current_user.get("role")
    if settings.get("admin_direct_approval") and user_role in ["admin", "owner"]:
        status = "approved"
        approved_by = current_user["id"]
    elif not settings.get("approval_enabled"):
        status = "approved"
        approved_by = None
    else:
        status = "pending"
        approved_by = None
    
    entry = {
        "id": str(uuid.uuid4()),
        "entry_number": f"EXP{datetime.now().strftime('%Y%m%d')}{str(uuid.uuid4())[:4].upper()}",
        "date": data.get("date", today),
        "category_id": data["category_id"],
        "category_name": data.get("category_name", ""),
        "amount": amount,
        "description": data.get("description", ""),
        "payment_mode": data.get("payment_mode", "cash"),
        "reference_number": data.get("reference_number", ""),
        "department": data.get("department", "Admin"),
        "project_id": data.get("project_id"),
        "order_id": data.get("order_id"),
        "vendor_name": data.get("vendor_name", ""),
        "attachments": data.get("attachments", []),
        "status": status,
        "created_by": current_user["id"],
        "created_by_name": current_user.get("name", current_user.get("email")),
        "approved_by": approved_by,
        "approval_history": [],
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    # Add to approval history if auto-approved
    if status == "approved":
        entry["approval_history"].append({
            "action": "approved",
            "by": current_user["id"],
            "by_name": current_user.get("name"),
            "role": user_role,
            "at": datetime.now(timezone.utc).isoformat(),
            "note": "Auto-approved (Admin direct approval)"
        })
        
        # Post to ledger if enabled
        if settings.get("auto_post_to_ledger"):
            await post_expense_to_ledger(db, entry)
    
    await db.expense_entries.insert_one(entry)
    
    return {"message": "Expense entry created", "entry": {k: v for k, v in entry.items() if k != "_id"}}


@expense_router.get("/entries")
async def get_expense_entries(
    status: Optional[str] = None,
    category_id: Optional[str] = None,
    department: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 100,
    current_user: dict = Depends(get_erp_user)
):
    """Get expense entries with filters"""
    db = get_db()
    
    query = {}
    if status:
        query["status"] = status
    if category_id:
        query["category_id"] = category_id
    if department:
        query["department"] = department
    if date_from:
        query["date"] = {"$gte": date_from}
    if date_to:
        if "date" in query:
            query["date"]["$lte"] = date_to
        else:
            query["date"] = {"$lte": date_to}
    
    # Non-admin users see only their own entries
    if current_user.get("role") not in ["admin", "owner", "accountant", "hr"]:
        query["created_by"] = current_user["id"]
    
    entries = await db.expense_entries.find(query, {"_id": 0}).sort("created_at", -1).to_list(limit)
    return entries


@expense_router.get("/entries/{entry_id}")
async def get_expense_entry(entry_id: str, current_user: dict = Depends(get_erp_user)):
    """Get single expense entry"""
    db = get_db()
    entry = await db.expense_entries.find_one({"id": entry_id}, {"_id": 0})
    if not entry:
        raise HTTPException(status_code=404, detail="Entry not found")
    return entry


@expense_router.post("/entries/{entry_id}/approve")
async def approve_expense(
    entry_id: str,
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Approve or reject expense entry"""
    db = get_db()
    entry = await db.expense_entries.find_one({"id": entry_id}, {"_id": 0})
    
    if not entry:
        raise HTTPException(status_code=404, detail="Entry not found")
    
    if entry["status"] not in ["pending", "supervisor_approved"]:
        raise HTTPException(status_code=400, detail="Entry cannot be approved/rejected")
    
    action = data.get("action", "approve")  # approve or reject
    note = data.get("note", "")
    user_role = current_user.get("role")
    settings = await get_expense_settings(current_user)
    
    # Determine new status based on approval level
    if action == "reject":
        new_status = "rejected"
    elif user_role in ["admin", "owner"]:
        new_status = "approved"
    elif user_role in ["accountant"] and settings.get("approval_levels") >= 3:
        new_status = "accounts_approved" if entry["status"] == "supervisor_approved" else "supervisor_approved"
    elif user_role in ["manager", "supervisor", "hr"]:
        if settings.get("approval_levels") == 1:
            new_status = "approved"
        else:
            new_status = "supervisor_approved"
    else:
        raise HTTPException(status_code=403, detail="Not authorized to approve")
    
    # Add to approval history
    approval_record = {
        "action": action,
        "by": current_user["id"],
        "by_name": current_user.get("name"),
        "role": user_role,
        "at": datetime.now(timezone.utc).isoformat(),
        "note": note
    }
    
    update_data = {
        "status": new_status,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    
    if new_status == "approved":
        update_data["approved_by"] = current_user["id"]
        update_data["approved_at"] = datetime.now(timezone.utc).isoformat()
        
        # Post to ledger
        if settings.get("auto_post_to_ledger"):
            entry["status"] = "approved"
            await post_expense_to_ledger(db, entry)
    
    await db.expense_entries.update_one(
        {"id": entry_id},
        {
            "$set": update_data,
            "$push": {"approval_history": approval_record}
        }
    )
    
    return {"message": f"Expense {action}d successfully", "new_status": new_status}


async def post_expense_to_ledger(db, entry: dict):
    """Post approved expense to accounting ledger"""
    ledger_entry = {
        "id": str(uuid.uuid4()),
        "date": entry["date"],
        "type": "expense",
        "category": entry["category_name"],
        "description": f"Expense: {entry['description']}",
        "debit": entry["amount"],
        "credit": 0,
        "reference_type": "expense",
        "reference_id": entry["id"],
        "reference_number": entry["entry_number"],
        "payment_mode": entry["payment_mode"],
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    await db.ledger.insert_one(ledger_entry)


# =============== FILE UPLOAD ===============

@expense_router.post("/upload")
async def upload_expense_attachment(
    file: UploadFile = File(...),
    current_user: dict = Depends(get_erp_user)
):
    """Upload expense receipt/bill attachment"""
    # Validate file type
    allowed_types = ["image/jpeg", "image/png", "image/webp", "application/pdf"]
    if file.content_type not in allowed_types:
        raise HTTPException(status_code=400, detail="Invalid file type. Allowed: JPG, PNG, WEBP, PDF")
    
    # Generate unique filename
    ext = file.filename.split(".")[-1] if "." in file.filename else "jpg"
    filename = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}.{ext}"
    filepath = os.path.join(UPLOAD_DIR, filename)
    
    # Save file
    with open(filepath, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    return {
        "filename": filename,
        "url": f"/api/erp/expenses/files/{filename}",
        "content_type": file.content_type
    }


@expense_router.get("/files/{filename}")
async def get_expense_file(filename: str):
    """Get uploaded expense file"""
    from fastapi.responses import FileResponse
    filepath = os.path.join(UPLOAD_DIR, filename)
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(filepath)


# =============== REPORTS & ANALYTICS ===============

@expense_router.get("/dashboard")
async def get_expense_dashboard(current_user: dict = Depends(get_erp_user)):
    """Get expense dashboard summary"""
    db = get_db()
    today = datetime.now(timezone.utc).date()
    month_start = today.replace(day=1).isoformat()
    today_str = today.isoformat()
    
    # Today's expenses
    today_pipeline = [
        {"$match": {"date": today_str, "status": "approved"}},
        {"$group": {"_id": None, "total": {"$sum": "$amount"}, "count": {"$sum": 1}}}
    ]
    today_result = await db.expense_entries.aggregate(today_pipeline).to_list(1)
    
    # Monthly expenses
    month_pipeline = [
        {"$match": {"date": {"$gte": month_start}, "status": "approved"}},
        {"$group": {"_id": None, "total": {"$sum": "$amount"}, "count": {"$sum": 1}}}
    ]
    month_result = await db.expense_entries.aggregate(month_pipeline).to_list(1)
    
    # Pending approvals
    pending_count = await db.expense_entries.count_documents({"status": {"$in": ["pending", "supervisor_approved"]}})
    
    # Category-wise breakdown (this month)
    category_pipeline = [
        {"$match": {"date": {"$gte": month_start}, "status": "approved"}},
        {"$group": {"_id": "$category_name", "total": {"$sum": "$amount"}, "count": {"$sum": 1}}},
        {"$sort": {"total": -1}}
    ]
    category_result = await db.expense_entries.aggregate(category_pipeline).to_list(20)
    
    # Department-wise breakdown
    dept_pipeline = [
        {"$match": {"date": {"$gte": month_start}, "status": "approved"}},
        {"$group": {"_id": "$department", "total": {"$sum": "$amount"}}},
        {"$sort": {"total": -1}}
    ]
    dept_result = await db.expense_entries.aggregate(dept_pipeline).to_list(10)
    
    # Recent entries
    recent = await db.expense_entries.find({}, {"_id": 0}).sort("created_at", -1).to_list(10)
    
    settings = await get_expense_settings(current_user)
    
    return {
        "today": {
            "total": today_result[0]["total"] if today_result else 0,
            "count": today_result[0]["count"] if today_result else 0,
            "limit": settings.get("daily_limit", 50000)
        },
        "month": {
            "total": month_result[0]["total"] if month_result else 0,
            "count": month_result[0]["count"] if month_result else 0,
            "limit": settings.get("monthly_limit", 500000)
        },
        "pending_approvals": pending_count,
        "by_category": [{"category": r["_id"], "total": r["total"], "count": r["count"]} for r in category_result],
        "by_department": [{"department": r["_id"], "total": r["total"]} for r in dept_result],
        "recent_entries": recent
    }


@expense_router.get("/reports/variance")
async def get_variance_report(
    month: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get planned vs actual expense variance report"""
    db = get_db()
    
    if not month:
        month = datetime.now(timezone.utc).strftime("%Y-%m")
    
    # Get category budgets
    categories = await db.expense_categories.find({}, {"_id": 0}).to_list(100)
    category_budgets = {c["id"]: c.get("budget_limit", 0) for c in categories}
    
    # Get actual expenses
    month_start = f"{month}-01"
    month_end = f"{month}-31"
    
    actual_pipeline = [
        {"$match": {"date": {"$gte": month_start, "$lte": month_end}, "status": "approved"}},
        {"$group": {"_id": "$category_id", "actual": {"$sum": "$amount"}}}
    ]
    actual_result = await db.expense_entries.aggregate(actual_pipeline).to_list(100)
    actual_map = {r["_id"]: r["actual"] for r in actual_result}
    
    # Build variance report
    variance_data = []
    for cat in categories:
        planned = cat.get("budget_limit", 0)
        actual = actual_map.get(cat["id"], 0)
        variance = planned - actual
        variance_pct = ((variance / planned) * 100) if planned > 0 else 0
        
        variance_data.append({
            "category_id": cat["id"],
            "category_name": cat["name"],
            "planned": planned,
            "actual": actual,
            "variance": variance,
            "variance_percentage": round(variance_pct, 1),
            "status": "under" if variance >= 0 else "over"
        })
    
    return {
        "month": month,
        "data": variance_data,
        "summary": {
            "total_planned": sum(v["planned"] for v in variance_data),
            "total_actual": sum(v["actual"] for v in variance_data),
            "total_variance": sum(v["variance"] for v in variance_data)
        }
    }


@expense_router.get("/reports/cash-flow")
async def get_cash_flow_report(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get expense impact on cash flow"""
    db = get_db()
    
    if not date_from:
        date_from = (datetime.now(timezone.utc) - timedelta(days=30)).date().isoformat()
    if not date_to:
        date_to = datetime.now(timezone.utc).date().isoformat()
    
    # Daily expense totals
    daily_pipeline = [
        {"$match": {"date": {"$gte": date_from, "$lte": date_to}, "status": "approved"}},
        {"$group": {"_id": "$date", "total": {"$sum": "$amount"}}},
        {"$sort": {"_id": 1}}
    ]
    daily_result = await db.expense_entries.aggregate(daily_pipeline).to_list(100)
    
    # Payment mode breakdown
    mode_pipeline = [
        {"$match": {"date": {"$gte": date_from, "$lte": date_to}, "status": "approved"}},
        {"$group": {"_id": "$payment_mode", "total": {"$sum": "$amount"}}}
    ]
    mode_result = await db.expense_entries.aggregate(mode_pipeline).to_list(10)
    
    return {
        "period": {"from": date_from, "to": date_to},
        "daily_expenses": [{"date": r["_id"], "amount": r["total"]} for r in daily_result],
        "by_payment_mode": [{"mode": r["_id"], "total": r["total"]} for r in mode_result],
        "total_outflow": sum(r["total"] for r in daily_result)
    }
