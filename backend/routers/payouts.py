"""
Razorpay Payouts Router - Automated Salary Disbursement
Handles fund accounts, payouts, and payout status tracking

NOTE: Razorpay Payouts API (RazorpayX) requires special account activation.
Standard Razorpay keys won't work for Payouts. Contact Razorpay support to enable.
"""
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
import uuid
import razorpay
import requests
import os
from .base import get_erp_user, get_db

payouts_router = APIRouter(prefix="/payouts", tags=["Payouts"])

# Razorpay credentials
RAZORPAY_KEY_ID = os.environ.get('RAZORPAY_KEY_ID', '')
RAZORPAY_KEY_SECRET = os.environ.get('RAZORPAY_KEY_SECRET', '')

# Initialize Razorpay client for standard operations
razorpay_client = razorpay.Client(auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET))

# RazorpayX API base URL for Payouts
RAZORPAYX_BASE_URL = "https://api.razorpay.com/v1"


def razorpayx_request(method: str, endpoint: str, data: dict = None) -> dict:
    """Make authenticated request to RazorpayX API"""
    url = f"{RAZORPAYX_BASE_URL}/{endpoint}"
    auth = (RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET)
    
    try:
        if method == "POST":
            response = requests.post(url, json=data, auth=auth)
        elif method == "GET":
            response = requests.get(url, params=data, auth=auth)
        else:
            raise ValueError(f"Unsupported method: {method}")
        
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        error_detail = e.response.json() if e.response.content else {"error": str(e)}
        raise HTTPException(
            status_code=e.response.status_code,
            detail=f"Razorpay API error: {error_detail.get('error', {}).get('description', str(error_detail))}"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Razorpay API request failed: {str(e)}")


# =============== FUND ACCOUNTS (Employee Bank Details) ===============

@payouts_router.post("/fund-accounts")
async def create_fund_account(
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """
    Create a fund account (bank account) for an employee
    Required for payouts - links employee to their bank account
    """
    db = get_db()
    employee_id = data.get("employee_id")
    
    # Get employee details
    employee = await db.employees.find_one({"id": employee_id}, {"_id": 0})
    if not employee:
        raise HTTPException(status_code=404, detail="Employee not found")
    
    # Check if fund account already exists
    existing = await db.fund_accounts.find_one({"employee_id": employee_id}, {"_id": 0})
    if existing:
        return {"message": "Fund account already exists", "fund_account_id": existing.get("razorpay_fund_account_id")}
    
    try:
        # Create contact in Razorpay using RazorpayX API
        contact = razorpayx_request("POST", "contacts", {
            "name": employee.get("name"),
            "email": employee.get("email", ""),
            "contact": employee.get("phone", ""),
            "type": "employee",
            "reference_id": employee_id,
            "notes": {
                "emp_code": employee.get("emp_code", ""),
                "department": employee.get("department", "")
            }
        })
        
        # Create fund account (bank account) using RazorpayX API
        fund_account = razorpayx_request("POST", "fund_accounts", {
            "contact_id": contact["id"],
            "account_type": "bank_account",
            "bank_account": {
                "name": data.get("account_holder_name", employee.get("name")),
                "ifsc": data.get("ifsc_code"),
                "account_number": data.get("account_number")
            }
        })
        
        # Store in database
        fund_account_record = {
            "id": str(uuid.uuid4()),
            "employee_id": employee_id,
            "razorpay_contact_id": contact["id"],
            "razorpay_fund_account_id": fund_account["id"],
            "account_holder_name": data.get("account_holder_name", employee.get("name")),
            "ifsc_code": data.get("ifsc_code"),
            "account_number_last4": data.get("account_number")[-4:],
            "bank_name": fund_account.get("bank_account", {}).get("bank_name", ""),
            "status": "active",
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        
        await db.fund_accounts.insert_one(fund_account_record)
        
        # Update employee with fund account reference
        await db.employees.update_one(
            {"id": employee_id},
            {"$set": {
                "fund_account_id": fund_account["id"],
                "bank_linked": True
            }}
        )
        
        return {
            "message": "Fund account created successfully",
            "fund_account_id": fund_account["id"],
            "contact_id": contact["id"]
        }
        
    except razorpay.errors.BadRequestError as e:
        raise HTTPException(status_code=400, detail=f"Razorpay error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create fund account: {str(e)}")


@payouts_router.get("/fund-accounts")
async def get_fund_accounts(
    employee_id: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get all fund accounts or for a specific employee"""
    db = get_db()
    query = {}
    if employee_id:
        query["employee_id"] = employee_id
    
    accounts = await db.fund_accounts.find(query, {"_id": 0}).to_list(500)
    return accounts


# =============== SALARY PAYOUTS ===============

@payouts_router.post("/salary/process")
async def process_salary_payout(
    data: Dict[str, Any],
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_erp_user)
):
    """
    Process salary payout for a single employee
    Requires approved salary record and linked bank account
    """
    db = get_db()
    salary_id = data.get("salary_id")
    
    # Get salary record
    salary = await db.salary_payments.find_one({"id": salary_id}, {"_id": 0})
    if not salary:
        raise HTTPException(status_code=404, detail="Salary record not found")
    
    if salary.get("payment_status") == "paid":
        raise HTTPException(status_code=400, detail="Salary already paid")
    
    if salary.get("payment_status") != "approved":
        raise HTTPException(status_code=400, detail="Salary must be approved before payment")
    
    # Get employee fund account
    fund_account = await db.fund_accounts.find_one(
        {"employee_id": salary.get("employee_id")},
        {"_id": 0}
    )
    if not fund_account:
        raise HTTPException(status_code=400, detail="Employee bank account not linked. Please add fund account first.")
    
    # Get employee details
    employee = await db.employees.find_one({"id": salary.get("employee_id")}, {"_id": 0})
    
    try:
        # Create payout in Razorpay using RazorpayX API
        payout = razorpayx_request("POST", "payouts", {
            "account_number": os.environ.get("RAZORPAY_ACCOUNT_NUMBER", ""),  # Your Razorpay account
            "fund_account_id": fund_account.get("razorpay_fund_account_id"),
            "amount": int(salary.get("net_salary", 0) * 100),  # Convert to paise
            "currency": "INR",
            "mode": data.get("mode", "IMPS"),  # IMPS, NEFT, RTGS, UPI
            "purpose": "salary",
            "queue_if_low_balance": True,
            "reference_id": salary_id,
            "narration": f"Salary {salary.get('month')} - {employee.get('name', '')}",
            "notes": {
                "employee_id": salary.get("employee_id"),
                "month": salary.get("month"),
                "emp_code": employee.get("emp_code", "")
            }
        })
        
        # Update salary record
        await db.salary_payments.update_one(
            {"id": salary_id},
            {"$set": {
                "payment_status": "processing",
                "razorpay_payout_id": payout["id"],
                "payout_status": payout.get("status"),
                "payout_mode": data.get("mode", "IMPS"),
                "processed_at": datetime.now(timezone.utc).isoformat(),
                "processed_by": current_user.get("id", "system")
            }}
        )
        
        # Create payout record
        payout_record = {
            "id": str(uuid.uuid4()),
            "salary_id": salary_id,
            "employee_id": salary.get("employee_id"),
            "employee_name": employee.get("name"),
            "razorpay_payout_id": payout["id"],
            "amount": salary.get("net_salary"),
            "mode": data.get("mode", "IMPS"),
            "status": payout.get("status"),
            "utr": payout.get("utr"),
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        await db.payouts.insert_one(payout_record)
        
        return {
            "message": "Payout initiated successfully",
            "payout_id": payout["id"],
            "status": payout.get("status"),
            "amount": salary.get("net_salary")
        }
        
    except razorpay.errors.BadRequestError as e:
        raise HTTPException(status_code=400, detail=f"Razorpay error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Payout failed: {str(e)}")


@payouts_router.post("/salary/bulk-process")
async def bulk_process_salary_payouts(
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """
    Process salary payouts for all approved salaries in a month
    Admin-only function for bulk disbursement
    """
    db = get_db()
    
    if current_user.get("role") not in ["admin", "accountant"]:
        raise HTTPException(status_code=403, detail="Only admin/accountant can process bulk payouts")
    
    month = data.get("month")  # Format: YYYY-MM
    mode = data.get("mode", "IMPS")
    
    # Get all approved salaries for the month
    approved_salaries = await db.salary_payments.find({
        "month": month,
        "payment_status": "approved"
    }, {"_id": 0}).to_list(500)
    
    if not approved_salaries:
        raise HTTPException(status_code=400, detail=f"No approved salaries found for {month}")
    
    results = {
        "success": [],
        "failed": [],
        "skipped": []
    }
    
    for salary in approved_salaries:
        # Check if employee has fund account
        fund_account = await db.fund_accounts.find_one(
            {"employee_id": salary.get("employee_id")},
            {"_id": 0}
        )
        
        if not fund_account:
            results["skipped"].append({
                "salary_id": salary["id"],
                "employee_id": salary.get("employee_id"),
                "reason": "No bank account linked"
            })
            continue
        
        employee = await db.employees.find_one({"id": salary.get("employee_id")}, {"_id": 0})
        
        try:
            # Create payout using RazorpayX API
            payout = razorpayx_request("POST", "payouts", {
                "account_number": os.environ.get("RAZORPAY_ACCOUNT_NUMBER", ""),
                "fund_account_id": fund_account.get("razorpay_fund_account_id"),
                "amount": int(salary.get("net_salary", 0) * 100),
                "currency": "INR",
                "mode": mode,
                "purpose": "salary",
                "queue_if_low_balance": True,
                "reference_id": salary["id"],
                "narration": f"Salary {month} - {employee.get('name', '')}",
            })
            
            # Update salary record
            await db.salary_payments.update_one(
                {"id": salary["id"]},
                {"$set": {
                    "payment_status": "processing",
                    "razorpay_payout_id": payout["id"],
                    "payout_status": payout.get("status"),
                    "payout_mode": mode,
                    "processed_at": datetime.now(timezone.utc).isoformat()
                }}
            )
            
            # Create payout record
            payout_record = {
                "id": str(uuid.uuid4()),
                "salary_id": salary["id"],
                "employee_id": salary.get("employee_id"),
                "employee_name": employee.get("name") if employee else "",
                "razorpay_payout_id": payout["id"],
                "amount": salary.get("net_salary"),
                "mode": mode,
                "status": payout.get("status"),
                "created_at": datetime.now(timezone.utc).isoformat()
            }
            await db.payouts.insert_one(payout_record)
            
            results["success"].append({
                "salary_id": salary["id"],
                "employee_name": employee.get("name") if employee else "",
                "amount": salary.get("net_salary"),
                "payout_id": payout["id"]
            })
            
        except Exception as e:
            results["failed"].append({
                "salary_id": salary["id"],
                "employee_id": salary.get("employee_id"),
                "error": str(e)
            })
    
    # Create ledger entry for total salaries
    total_paid = sum(r["amount"] for r in results["success"])
    if total_paid > 0:
        ledger_entry = {
            "id": str(uuid.uuid4()),
            "date": datetime.now(timezone.utc).date().isoformat(),
            "type": "salary_payout",
            "reference": f"BULK_SALARY_{month}",
            "description": f"Bulk Salary Payout for {month} ({len(results['success'])} employees)",
            "debit": 0,
            "credit": round(total_paid, 2),
            "account": "Salary Expense",
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        await db.ledger.insert_one(ledger_entry)
    
    return {
        "message": "Bulk payout processing completed",
        "month": month,
        "summary": {
            "total_processed": len(results["success"]),
            "total_failed": len(results["failed"]),
            "total_skipped": len(results["skipped"]),
            "total_amount": total_paid
        },
        "details": results
    }


@payouts_router.get("/salary/status/{salary_id}")
async def get_payout_status(
    salary_id: str,
    current_user: dict = Depends(get_erp_user)
):
    """Get payout status for a salary payment"""
    db = get_db()
    
    salary = await db.salary_payments.find_one({"id": salary_id}, {"_id": 0})
    if not salary:
        raise HTTPException(status_code=404, detail="Salary record not found")
    
    payout_id = salary.get("razorpay_payout_id")
    if not payout_id:
        return {"status": salary.get("payment_status"), "message": "No payout initiated"}
    
    try:
        # Fetch latest status from Razorpay using RazorpayX API
        payout = razorpayx_request("GET", f"payouts/{payout_id}")
        
        # Update local record if status changed
        if payout.get("status") != salary.get("payout_status"):
            new_payment_status = "paid" if payout.get("status") == "processed" else salary.get("payment_status")
            
            await db.salary_payments.update_one(
                {"id": salary_id},
                {"$set": {
                    "payout_status": payout.get("status"),
                    "payment_status": new_payment_status,
                    "utr": payout.get("utr"),
                    "paid_at": datetime.now(timezone.utc).isoformat() if new_payment_status == "paid" else None
                }}
            )
        
        return {
            "salary_id": salary_id,
            "payout_id": payout_id,
            "status": payout.get("status"),
            "utr": payout.get("utr"),
            "failure_reason": payout.get("failure_reason"),
            "amount": salary.get("net_salary")
        }
        
    except HTTPException:
        raise
    except Exception as e:
        return {"status": salary.get("payout_status"), "error": str(e)}


# =============== PAYOUT HISTORY ===============

@payouts_router.get("/history")
async def get_payout_history(
    month: Optional[str] = None,
    employee_id: Optional[str] = None,
    status: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get payout history with filters"""
    db = get_db()
    
    query = {}
    if employee_id:
        query["employee_id"] = employee_id
    if status:
        query["status"] = status
    if month:
        # Filter by month in created_at
        query["created_at"] = {"$regex": f"^{month}"}
    
    payouts = await db.payouts.find(query, {"_id": 0}).sort("created_at", -1).to_list(500)
    return payouts


# =============== WEBHOOK HANDLER ===============

@payouts_router.post("/webhook")
async def handle_payout_webhook(
    payload: Dict[str, Any]
):
    """
    Handle Razorpay payout webhooks
    Updates payout and salary status based on webhook events
    """
    db = get_db()
    
    event = payload.get("event")
    payout_data = payload.get("payload", {}).get("payout", {}).get("entity", {})
    
    if not payout_data:
        return {"status": "ignored"}
    
    payout_id = payout_data.get("id")
    reference_id = payout_data.get("reference_id")  # This is our salary_id
    
    # Map Razorpay status to our status
    status_map = {
        "processed": "paid",
        "reversed": "failed",
        "cancelled": "cancelled",
        "queued": "processing",
        "pending": "processing",
        "processing": "processing"
    }
    
    new_status = status_map.get(payout_data.get("status"), payout_data.get("status"))
    
    # Update salary payment record
    if reference_id:
        update_data = {
            "payout_status": payout_data.get("status"),
            "payment_status": new_status,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        
        if payout_data.get("utr"):
            update_data["utr"] = payout_data.get("utr")
        
        if new_status == "paid":
            update_data["paid_at"] = datetime.now(timezone.utc).isoformat()
        
        if payout_data.get("failure_reason"):
            update_data["failure_reason"] = payout_data.get("failure_reason")
        
        await db.salary_payments.update_one(
            {"id": reference_id},
            {"$set": update_data}
        )
    
    # Update payout record
    await db.payouts.update_one(
        {"razorpay_payout_id": payout_id},
        {"$set": {
            "status": payout_data.get("status"),
            "utr": payout_data.get("utr"),
            "failure_reason": payout_data.get("failure_reason"),
            "updated_at": datetime.now(timezone.utc).isoformat()
        }}
    )
    
    return {"status": "processed", "event": event}


# =============== PAYOUT DASHBOARD ===============

@payouts_router.get("/dashboard")
async def get_payout_dashboard(
    current_user: dict = Depends(get_erp_user)
):
    """Get payout dashboard summary"""
    db = get_db()
    
    # Current month
    current_month = datetime.now(timezone.utc).strftime("%Y-%m")
    
    # Total employees with bank linked
    employees_with_bank = await db.employees.count_documents({"bank_linked": True})
    total_employees = await db.employees.count_documents({"status": "active"})
    
    # Current month salary stats
    salaries = await db.salary_payments.find({"month": current_month}, {"_id": 0}).to_list(500)
    
    pending_approval = len([s for s in salaries if s.get("payment_status") == "pending"])
    approved = len([s for s in salaries if s.get("payment_status") == "approved"])
    processing = len([s for s in salaries if s.get("payment_status") == "processing"])
    paid = len([s for s in salaries if s.get("payment_status") == "paid"])
    failed = len([s for s in salaries if s.get("payment_status") == "failed"])
    
    total_approved_amount = sum(s.get("net_salary", 0) for s in salaries if s.get("payment_status") == "approved")
    total_paid_amount = sum(s.get("net_salary", 0) for s in salaries if s.get("payment_status") == "paid")
    
    # Recent payouts
    recent_payouts = await db.payouts.find({}, {"_id": 0}).sort("created_at", -1).to_list(10)
    
    return {
        "current_month": current_month,
        "employees": {
            "total": total_employees,
            "bank_linked": employees_with_bank,
            "pending_bank_link": total_employees - employees_with_bank
        },
        "salaries": {
            "pending_approval": pending_approval,
            "approved": approved,
            "processing": processing,
            "paid": paid,
            "failed": failed
        },
        "amounts": {
            "ready_to_pay": round(total_approved_amount, 2),
            "paid_this_month": round(total_paid_amount, 2)
        },
        "recent_payouts": recent_payouts
    }
