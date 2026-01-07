"""
SFA Expense & Reimbursement Router
Self-service expense upload with manager and accounts approval
Part of: Field Sales Attendance, Movement, Communication & Expense Intelligence System
"""
from fastapi import APIRouter, Depends, HTTPException, Request, UploadFile, File, Form
from typing import Optional, List
from datetime import datetime, timezone
from pydantic import BaseModel
import uuid
import os
import base64
from .base import get_erp_user, get_db
from .audit import log_action

sfa_expense_router = APIRouter(prefix="/sfa-expense", tags=["SFA Expense & Reimbursement"])


# ==================== MODELS ====================

class ExpenseSubmitRequest(BaseModel):
    category: str  # fuel, travel, food, accommodation, mobile, other
    amount: float
    description: str
    expense_date: str
    vehicle_type: Optional[str] = None
    distance_km: Optional[float] = None
    bill_number: Optional[str] = None


class ExpenseApprovalRequest(BaseModel):
    status: str  # approved, rejected
    remarks: Optional[str] = None
    approved_amount: Optional[float] = None


# ==================== EXPENSE CATEGORIES ====================

EXPENSE_CATEGORIES = {
    "fuel": {"name": "Fuel", "requires_bill": True, "auto_calculate": True},
    "travel": {"name": "Travel (Bus/Train/Auto)", "requires_bill": True, "auto_calculate": False},
    "food": {"name": "Food & Refreshments", "requires_bill": True, "auto_calculate": False},
    "accommodation": {"name": "Accommodation", "requires_bill": True, "auto_calculate": False},
    "mobile": {"name": "Mobile Recharge", "requires_bill": True, "auto_calculate": False},
    "parking": {"name": "Parking", "requires_bill": False, "auto_calculate": False},
    "toll": {"name": "Toll Charges", "requires_bill": True, "auto_calculate": False},
    "courier": {"name": "Courier/Postage", "requires_bill": True, "auto_calculate": False},
    "other": {"name": "Other", "requires_bill": True, "auto_calculate": False}
}


# ==================== EXPENSE SUBMISSION ====================

@sfa_expense_router.post("/submit")
async def submit_expense(
    request: Request,
    category: str = Form(...),
    amount: float = Form(...),
    description: str = Form(...),
    expense_date: str = Form(...),
    vehicle_type: Optional[str] = Form(None),
    distance_km: Optional[float] = Form(None),
    bill_number: Optional[str] = Form(None),
    bill_image: Optional[UploadFile] = File(None),
    current_user: dict = Depends(get_erp_user)
):
    """
    Submit expense with bill upload
    Employee self-service expense submission
    """
    db = get_db()
    
    if category not in EXPENSE_CATEGORIES:
        raise HTTPException(status_code=400, detail=f"Invalid category. Valid: {list(EXPENSE_CATEGORIES.keys())}")
    
    cat_config = EXPENSE_CATEGORIES[category]
    
    # Validate bill if required
    if cat_config["requires_bill"] and not bill_image:
        raise HTTPException(status_code=400, detail=f"Bill upload is mandatory for {cat_config['name']} expenses")
    
    now = datetime.now(timezone.utc)
    expense_id = str(uuid.uuid4())
    
    # Save bill image if provided
    bill_data = None
    if bill_image:
        content = await bill_image.read()
        bill_data = {
            "filename": bill_image.filename,
            "content_type": bill_image.content_type,
            "size": len(content),
            "data": base64.b64encode(content).decode('utf-8'),
            "uploaded_at": now.isoformat()
        }
    
    # Get manager for approval workflow
    # Find the user's manager (for now, default to sales_manager or admin)
    manager = await db.users.find_one(
        {"role": {"$in": ["sales_manager", "manager", "admin"]}},
        {"_id": 0, "id": 1, "name": 1, "email": 1}
    )
    
    expense_record = {
        "id": expense_id,
        "user_id": current_user["id"],
        "user_name": current_user["name"],
        "user_role": current_user.get("role"),
        "category": category,
        "category_name": cat_config["name"],
        "amount": amount,
        "description": description,
        "expense_date": expense_date,
        "vehicle_type": vehicle_type,
        "distance_km": distance_km,
        "bill_number": bill_number,
        "bill_image": bill_data,
        "has_bill": bill_data is not None,
        "status": "pending_manager",  # pending_manager, pending_accounts, approved, rejected
        "manager_approval": None,
        "accounts_approval": None,
        "approved_amount": None,
        "assigned_manager_id": manager["id"] if manager else None,
        "assigned_manager_name": manager["name"] if manager else None,
        "approval_history": [],
        "created_at": now.isoformat(),
        "month": now.strftime("%Y-%m")
    }
    
    await db.sfa_expenses.insert_one(expense_record)
    
    # Log action
    await log_action(
        user_id=current_user["id"],
        user_name=current_user["name"],
        user_role=current_user.get("role", "sales"),
        action="create",
        module="sfa_expenses",
        details={"category": category, "amount": amount, "description": description[:50]},
        record_id=expense_id,
        ip_address=request.client.host if request.client else None
    )
    
    return {
        "message": "Expense submitted successfully",
        "expense_id": expense_id,
        "status": "pending_manager",
        "assigned_manager": manager["name"] if manager else "Admin"
    }


@sfa_expense_router.get("/my-expenses")
async def get_my_expenses(
    status: Optional[str] = None,
    month: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """
    Get current user's expense submissions
    """
    db = get_db()
    
    query = {"user_id": current_user["id"]}
    if status:
        query["status"] = status
    if month:
        query["month"] = month
    
    expenses = await db.sfa_expenses.find(
        query,
        {"_id": 0, "bill_image.data": 0}  # Exclude bill data for list view
    ).sort("created_at", -1).to_list(100)
    
    # Calculate totals
    total_submitted = sum(e["amount"] for e in expenses)
    total_approved = sum(e.get("approved_amount", 0) or 0 for e in expenses if e["status"] == "approved")
    pending_count = len([e for e in expenses if e["status"].startswith("pending")])
    
    return {
        "expenses": expenses,
        "summary": {
            "total_submitted": round(total_submitted, 2),
            "total_approved": round(total_approved, 2),
            "pending_count": pending_count,
            "total_count": len(expenses)
        }
    }


@sfa_expense_router.get("/pending-approvals")
async def get_pending_approvals(
    approval_type: str = "manager",  # manager or accounts
    current_user: dict = Depends(get_erp_user)
):
    """
    Get expenses pending approval (for managers and accounts)
    """
    db = get_db()
    
    # Check role permissions
    if approval_type == "manager":
        if current_user.get("role") not in ["super_admin", "admin", "owner", "manager", "sales_manager"]:
            raise HTTPException(status_code=403, detail="Access denied")
        query = {"status": "pending_manager"}
    else:
        if current_user.get("role") not in ["super_admin", "admin", "owner", "accountant", "hr"]:
            raise HTTPException(status_code=403, detail="Access denied")
        query = {"status": "pending_accounts"}
    
    expenses = await db.sfa_expenses.find(
        query,
        {"_id": 0, "bill_image.data": 0}
    ).sort("created_at", 1).to_list(100)
    
    return {
        "approval_type": approval_type,
        "pending_count": len(expenses),
        "expenses": expenses
    }


@sfa_expense_router.post("/approve/{expense_id}/manager")
async def manager_approval(
    request: Request,
    expense_id: str,
    data: ExpenseApprovalRequest,
    current_user: dict = Depends(get_erp_user)
):
    """
    Manager approval for expense
    """
    if current_user.get("role") not in ["super_admin", "admin", "owner", "manager", "sales_manager"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    now = datetime.now(timezone.utc)
    
    expense = await db.sfa_expenses.find_one({"id": expense_id}, {"_id": 0})
    if not expense:
        raise HTTPException(status_code=404, detail="Expense not found")
    
    if expense["status"] != "pending_manager":
        raise HTTPException(status_code=400, detail="Expense not pending manager approval")
    
    approval_record = {
        "approver_id": current_user["id"],
        "approver_name": current_user["name"],
        "approver_role": "manager",
        "status": data.status,
        "remarks": data.remarks,
        "approved_amount": data.approved_amount if data.status == "approved" else None,
        "timestamp": now.isoformat()
    }
    
    new_status = "pending_accounts" if data.status == "approved" else "rejected"
    
    await db.sfa_expenses.update_one(
        {"id": expense_id},
        {
            "$set": {
                "status": new_status,
                "manager_approval": approval_record
            },
            "$push": {"approval_history": approval_record}
        }
    )
    
    # Log action
    await log_action(
        user_id=current_user["id"],
        user_name=current_user["name"],
        user_role=current_user.get("role"),
        action="approve" if data.status == "approved" else "reject",
        module="sfa_expenses",
        details={"expense_id": expense_id, "amount": expense["amount"], "status": data.status},
        record_id=expense_id,
        ip_address=request.client.host if request.client else None
    )
    
    return {
        "message": f"Expense {data.status}",
        "new_status": new_status
    }


@sfa_expense_router.post("/approve/{expense_id}/accounts")
async def accounts_approval(
    request: Request,
    expense_id: str,
    data: ExpenseApprovalRequest,
    current_user: dict = Depends(get_erp_user)
):
    """
    Accounts final approval for expense reimbursement
    """
    if current_user.get("role") not in ["super_admin", "admin", "owner", "accountant"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    now = datetime.now(timezone.utc)
    
    expense = await db.sfa_expenses.find_one({"id": expense_id}, {"_id": 0})
    if not expense:
        raise HTTPException(status_code=404, detail="Expense not found")
    
    if expense["status"] != "pending_accounts":
        raise HTTPException(status_code=400, detail="Expense not pending accounts approval")
    
    approved_amount = data.approved_amount if data.status == "approved" else 0
    
    approval_record = {
        "approver_id": current_user["id"],
        "approver_name": current_user["name"],
        "approver_role": "accounts",
        "status": data.status,
        "remarks": data.remarks,
        "approved_amount": approved_amount,
        "timestamp": now.isoformat()
    }
    
    await db.sfa_expenses.update_one(
        {"id": expense_id},
        {
            "$set": {
                "status": data.status,
                "accounts_approval": approval_record,
                "approved_amount": approved_amount,
                "final_approved_at": now.isoformat() if data.status == "approved" else None
            },
            "$push": {"approval_history": approval_record}
        }
    )
    
    # Log action
    await log_action(
        user_id=current_user["id"],
        user_name=current_user["name"],
        user_role=current_user.get("role"),
        action="approve" if data.status == "approved" else "reject",
        module="sfa_expenses",
        details={
            "expense_id": expense_id,
            "original_amount": expense["amount"],
            "approved_amount": approved_amount,
            "status": data.status
        },
        record_id=expense_id,
        ip_address=request.client.host if request.client else None
    )
    
    return {
        "message": f"Expense {data.status}",
        "approved_amount": approved_amount
    }


@sfa_expense_router.get("/bill/{expense_id}")
async def get_expense_bill(
    expense_id: str,
    current_user: dict = Depends(get_erp_user)
):
    """
    Get expense bill image
    """
    db = get_db()
    
    expense = await db.sfa_expenses.find_one({"id": expense_id}, {"_id": 0})
    if not expense:
        raise HTTPException(status_code=404, detail="Expense not found")
    
    # Check access
    if current_user["id"] != expense["user_id"]:
        if current_user.get("role") not in ["super_admin", "admin", "owner", "manager", "sales_manager", "accountant", "hr"]:
            raise HTTPException(status_code=403, detail="Access denied")
    
    if not expense.get("bill_image"):
        raise HTTPException(status_code=404, detail="No bill image found")
    
    return expense["bill_image"]


# ==================== REPORTS ====================

@sfa_expense_router.get("/reports/employee-wise")
async def get_employee_expense_report(
    month: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """
    Employee-wise expense report
    """
    if current_user.get("role") not in ["super_admin", "admin", "owner", "manager", "sales_manager", "accountant", "hr"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    target_month = month or datetime.now(timezone.utc).strftime("%Y-%m")
    
    pipeline = [
        {"$match": {"month": target_month}},
        {"$group": {
            "_id": {"user_id": "$user_id", "user_name": "$user_name"},
            "total_submitted": {"$sum": "$amount"},
            "total_approved": {"$sum": {"$cond": [{"$eq": ["$status", "approved"]}, "$approved_amount", 0]}},
            "pending_amount": {"$sum": {"$cond": [{"$in": ["$status", ["pending_manager", "pending_accounts"]]}, "$amount", 0]}},
            "rejected_amount": {"$sum": {"$cond": [{"$eq": ["$status", "rejected"]}, "$amount", 0]}},
            "expense_count": {"$sum": 1}
        }},
        {"$sort": {"total_submitted": -1}}
    ]
    
    results = await db.sfa_expenses.aggregate(pipeline).to_list(100)
    
    report = []
    for r in results:
        report.append({
            "employee_id": r["_id"]["user_id"],
            "employee_name": r["_id"]["user_name"],
            "total_submitted": round(r["total_submitted"], 2),
            "total_approved": round(r["total_approved"], 2),
            "pending_amount": round(r["pending_amount"], 2),
            "rejected_amount": round(r["rejected_amount"], 2),
            "expense_count": r["expense_count"]
        })
    
    return {
        "month": target_month,
        "report": report,
        "summary": {
            "total_submitted": round(sum(r["total_submitted"] for r in report), 2),
            "total_approved": round(sum(r["total_approved"] for r in report), 2),
            "total_pending": round(sum(r["pending_amount"] for r in report), 2)
        }
    }


@sfa_expense_router.get("/reports/category-wise")
async def get_category_expense_report(
    month: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """
    Category-wise expense report
    """
    if current_user.get("role") not in ["super_admin", "admin", "owner", "manager", "sales_manager", "accountant", "hr"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    target_month = month or datetime.now(timezone.utc).strftime("%Y-%m")
    
    pipeline = [
        {"$match": {"month": target_month}},
        {"$group": {
            "_id": {"category": "$category", "category_name": "$category_name"},
            "total_amount": {"$sum": "$amount"},
            "approved_amount": {"$sum": {"$cond": [{"$eq": ["$status", "approved"]}, "$approved_amount", 0]}},
            "count": {"$sum": 1}
        }},
        {"$sort": {"total_amount": -1}}
    ]
    
    results = await db.sfa_expenses.aggregate(pipeline).to_list(20)
    
    report = []
    for r in results:
        report.append({
            "category": r["_id"]["category"],
            "category_name": r["_id"]["category_name"],
            "total_amount": round(r["total_amount"], 2),
            "approved_amount": round(r["approved_amount"], 2),
            "count": r["count"]
        })
    
    return {
        "month": target_month,
        "report": report
    }


@sfa_expense_router.get("/reports/vehicle-wise")
async def get_vehicle_expense_report(
    month: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """
    Vehicle-wise expense report (fuel consumption)
    """
    if current_user.get("role") not in ["super_admin", "admin", "owner", "manager", "sales_manager", "accountant", "hr"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    target_month = month or datetime.now(timezone.utc).strftime("%Y-%m")
    
    # Get fuel expenses by vehicle type
    pipeline = [
        {"$match": {"month": target_month, "category": "fuel", "vehicle_type": {"$ne": None}}},
        {"$group": {
            "_id": "$vehicle_type",
            "total_amount": {"$sum": "$amount"},
            "total_distance": {"$sum": "$distance_km"},
            "count": {"$sum": 1}
        }},
        {"$sort": {"total_amount": -1}}
    ]
    
    results = await db.sfa_expenses.aggregate(pipeline).to_list(10)
    
    report = []
    for r in results:
        avg_per_km = round(r["total_amount"] / max(r["total_distance"], 1), 2) if r["total_distance"] else 0
        report.append({
            "vehicle_type": r["_id"],
            "total_fuel_expense": round(r["total_amount"], 2),
            "total_distance_km": round(r["total_distance"] or 0, 2),
            "avg_cost_per_km": avg_per_km,
            "count": r["count"]
        })
    
    return {
        "month": target_month,
        "report": report
    }
