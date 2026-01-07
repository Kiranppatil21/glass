"""
Ledger Management System
- Customer Ledger (Sales Ledger)
- Vendor Ledger (Purchase Ledger)
- General Ledger (GL)
- Auto-posting from sales, purchases, payments, receipts
- CA-ready reports with audit trail
"""
from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import uuid
import os

from .base import get_db, get_erp_user

ledger_router = APIRouter(prefix="/ledger", tags=["Ledger Management"])

# ================== PYDANTIC MODELS ==================

class OpeningBalanceCreate(BaseModel):
    party_type: str  # customer, vendor
    party_id: str
    amount: float
    balance_type: str  # debit, credit
    as_of_date: str  # YYYY-MM-DD
    notes: Optional[str] = None

class GLAccountCreate(BaseModel):
    account_code: str
    account_name: str
    account_type: str  # asset, liability, income, expense, equity
    parent_account: Optional[str] = None
    opening_balance: float = 0
    balance_type: str = "debit"  # debit or credit
    description: Optional[str] = None

class PeriodLockCreate(BaseModel):
    period_type: str  # quarterly, half_yearly, yearly
    period_start: str  # YYYY-MM-DD
    period_end: str  # YYYY-MM-DD
    locked_by_roles: List[str]  # ['hr', 'finance', 'accountant']
    notes: Optional[str] = None

class ReconciliationEntry(BaseModel):
    ledger_entry_id: str
    status: str  # confirmed, disputed, adjusted
    dispute_reason: Optional[str] = None
    adjustment_amount: Optional[float] = None
    notes: Optional[str] = None

# ================== GL ACCOUNT TYPES ==================

DEFAULT_GL_ACCOUNTS = [
    {"code": "1000", "name": "Cash in Hand", "type": "asset", "balance_type": "debit"},
    {"code": "1100", "name": "Bank Account", "type": "asset", "balance_type": "debit"},
    {"code": "1200", "name": "Accounts Receivable", "type": "asset", "balance_type": "debit"},
    {"code": "2000", "name": "Accounts Payable", "type": "liability", "balance_type": "credit"},
    {"code": "2100", "name": "GST Output (Payable)", "type": "liability", "balance_type": "credit"},
    {"code": "2200", "name": "GST Input (Receivable)", "type": "asset", "balance_type": "debit"},
    {"code": "3000", "name": "Sales Revenue", "type": "income", "balance_type": "credit"},
    {"code": "3100", "name": "Job Work Income", "type": "income", "balance_type": "credit"},
    {"code": "4000", "name": "Purchases", "type": "expense", "balance_type": "debit"},
    {"code": "4100", "name": "Operating Expenses", "type": "expense", "balance_type": "debit"},
    {"code": "4200", "name": "Salary & Wages", "type": "expense", "balance_type": "debit"},
    {"code": "4300", "name": "Transport & Logistics", "type": "expense", "balance_type": "debit"},
    {"code": "5000", "name": "Capital Account", "type": "equity", "balance_type": "credit"},
]

# ================== HELPER FUNCTIONS ==================

async def check_period_lock(db, date_str: str, user_role: str) -> dict:
    """Check if a period is locked for the given role"""
    if user_role in ["super_admin", "admin"]:
        return {"locked": False, "can_override": True}
    
    lock = await db.period_locks.find_one({
        "period_start": {"$lte": date_str},
        "period_end": {"$gte": date_str},
        "is_active": True
    }, {"_id": 0})
    
    if lock and user_role in lock.get("locked_by_roles", []):
        return {"locked": True, "can_override": False, "lock_info": lock}
    
    return {"locked": False, "can_override": False}


async def auto_post_to_ledger(
    db,
    entry_type: str,  # sales_invoice, purchase_bill, payment_received, payment_made, credit_note, debit_note
    reference_id: str,
    reference_number: str,
    party_type: str,  # customer, vendor
    party_id: str,
    party_name: str,
    amount: float,
    gst_amount: float = 0,
    description: str = "",
    transaction_date: str = None,
    created_by: str = None
):
    """
    Auto-post entry to appropriate ledgers
    This is called automatically from sales, purchase, payment modules
    NO MANUAL ENTRY ALLOWED - Only system-generated
    """
    print(f"[LEDGER] Auto-posting: {entry_type} for {party_name}, Amount: ₹{amount}, GST: ₹{gst_amount}")
    
    if db is None:
        print("[LEDGER] ERROR: Database is None!")
        return None
    
    if amount <= 0 and gst_amount <= 0:
        print("[LEDGER] Skipped - zero amount")
        return None
    
    if not transaction_date:
        transaction_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    entry_id = str(uuid.uuid4())
    timestamp = datetime.now(timezone.utc).isoformat()
    
    # Determine debit/credit based on entry type
    if entry_type == "sales_invoice":
        # Debit: Accounts Receivable, Credit: Sales + GST Output
        debit_account = "1200"  # Accounts Receivable
        credit_account = "3000"  # Sales Revenue
        party_effect = "debit"  # Customer owes us
    elif entry_type == "purchase_bill":
        # Debit: Purchases + GST Input, Credit: Accounts Payable
        debit_account = "4000"  # Purchases
        credit_account = "2000"  # Accounts Payable
        party_effect = "credit"  # We owe vendor
    elif entry_type == "payment_received":
        # Debit: Cash/Bank, Credit: Accounts Receivable
        debit_account = "1100"  # Bank
        credit_account = "1200"  # Accounts Receivable
        party_effect = "credit"  # Customer balance reduced
    elif entry_type == "payment_made":
        # Debit: Accounts Payable, Credit: Cash/Bank
        debit_account = "2000"  # Accounts Payable
        credit_account = "1100"  # Bank
        party_effect = "debit"  # Vendor balance reduced
    elif entry_type == "credit_note":
        # Sales return - reverse of sales
        debit_account = "3000"  # Sales (reduce)
        credit_account = "1200"  # Accounts Receivable (reduce)
        party_effect = "credit"  # Customer balance reduced
    elif entry_type == "debit_note":
        # Purchase return - reverse of purchase
        debit_account = "2000"  # Accounts Payable (reduce)
        credit_account = "4000"  # Purchases (reduce)
        party_effect = "debit"  # Vendor balance reduced
    else:
        raise ValueError(f"Unknown entry type: {entry_type}")
    
    # Create party ledger entry
    party_ledger_entry = {
        "id": entry_id,
        "party_type": party_type,
        "party_id": party_id,
        "party_name": party_name,
        "entry_type": entry_type,
        "reference_id": reference_id,
        "reference_number": reference_number,
        "transaction_date": transaction_date,
        "amount": amount,
        "gst_amount": gst_amount,
        "total_amount": amount + gst_amount,
        "effect": party_effect,  # debit increases balance, credit decreases
        "description": description,
        "gl_debit_account": debit_account,
        "gl_credit_account": credit_account,
        "reconciliation_status": "pending",
        "created_by": created_by,
        "created_at": timestamp,
        "is_system_generated": True
    }
    
    try:
        result = await db.party_ledger.insert_one(party_ledger_entry)
        print(f"[LEDGER] Party ledger inserted: {result.inserted_id}, acknowledged: {result.acknowledged}")
    except Exception as e:
        print(f"[LEDGER] ERROR inserting party_ledger: {e}")
        import traceback
        print(traceback.format_exc())
        return None
    
    # Create GL entries (double-entry)
    gl_entries = [
        {
            "id": str(uuid.uuid4()),
            "ledger_entry_id": entry_id,
            "account_code": debit_account,
            "entry_type": "debit",
            "amount": amount + gst_amount,
            "reference_id": reference_id,
            "reference_number": reference_number,
            "party_type": party_type,
            "party_id": party_id,
            "party_name": party_name,
            "transaction_date": transaction_date,
            "description": description,
            "created_at": timestamp
        },
        {
            "id": str(uuid.uuid4()),
            "ledger_entry_id": entry_id,
            "account_code": credit_account,
            "entry_type": "credit",
            "amount": amount + gst_amount,
            "reference_id": reference_id,
            "reference_number": reference_number,
            "party_type": party_type,
            "party_id": party_id,
            "party_name": party_name,
            "transaction_date": transaction_date,
            "description": description,
            "created_at": timestamp
        }
    ]
    
    # Add GST entries if applicable
    if gst_amount > 0:
        if entry_type in ["sales_invoice", "credit_note"]:
            gl_entries.append({
                "id": str(uuid.uuid4()),
                "ledger_entry_id": entry_id,
                "account_code": "2100",  # GST Output
                "entry_type": "credit" if entry_type == "sales_invoice" else "debit",
                "amount": gst_amount,
                "reference_id": reference_id,
                "reference_number": reference_number,
                "transaction_date": transaction_date,
                "description": f"GST on {description}",
                "created_at": timestamp
            })
        elif entry_type in ["purchase_bill", "debit_note"]:
            gl_entries.append({
                "id": str(uuid.uuid4()),
                "ledger_entry_id": entry_id,
                "account_code": "2200",  # GST Input
                "entry_type": "debit" if entry_type == "purchase_bill" else "credit",
                "amount": gst_amount,
                "reference_id": reference_id,
                "reference_number": reference_number,
                "transaction_date": transaction_date,
                "description": f"GST on {description}",
                "created_at": timestamp
            })
    
    await db.gl_entries.insert_many(gl_entries)
    
    # Log to audit trail
    await db.ledger_audit.insert_one({
        "id": str(uuid.uuid4()),
        "action": "auto_post",
        "entry_id": entry_id,
        "entry_type": entry_type,
        "party_type": party_type,
        "party_id": party_id,
        "amount": amount + gst_amount,
        "reference_number": reference_number,
        "created_by": created_by or "system",
        "created_at": timestamp,
        "details": f"Auto-posted {entry_type} for {party_name}"
    })
    
    return entry_id


def calculate_ageing(transaction_date: str) -> str:
    """Calculate ageing bucket for outstanding"""
    try:
        txn_date = datetime.strptime(transaction_date[:10], "%Y-%m-%d")
        today = datetime.now()
        days = (today - txn_date).days
        
        if days <= 30:
            return "0-30"
        elif days <= 60:
            return "31-60"
        elif days <= 90:
            return "61-90"
        else:
            return "90+"
    except ValueError:
        return "unknown"


# ================== INITIALIZATION ==================

@ledger_router.post("/init-gl-accounts")
async def initialize_gl_accounts(current_user: dict = Depends(get_erp_user)):
    """Initialize default GL accounts (run once)"""
    if current_user.get("role") not in ["super_admin", "admin"]:
        raise HTTPException(status_code=403, detail="Only admin can initialize GL accounts")
    
    db = get_db()
    
    # Check if already initialized
    existing = await db.gl_accounts.count_documents({})
    if existing > 0:
        return {"message": "GL accounts already initialized", "count": existing}
    
    accounts = []
    for acc in DEFAULT_GL_ACCOUNTS:
        accounts.append({
            "id": str(uuid.uuid4()),
            "code": acc["code"],
            "name": acc["name"],
            "type": acc["type"],
            "balance_type": acc["balance_type"],
            "opening_balance": 0,
            "current_balance": 0,
            "is_active": True,
            "created_at": datetime.now(timezone.utc).isoformat()
        })
    
    await db.gl_accounts.insert_many(accounts)
    
    return {"message": "GL accounts initialized", "count": len(accounts)}


# ================== OPENING BALANCE ==================

@ledger_router.post("/opening-balance")
async def set_opening_balance(
    data: OpeningBalanceCreate,
    current_user: dict = Depends(get_erp_user)
):
    """Set opening balance for customer or vendor (one-time setup)"""
    if current_user.get("role") not in ["super_admin", "admin", "finance", "accountant"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    # Check period lock
    lock_check = await check_period_lock(db, data.as_of_date, current_user.get("role"))
    if lock_check["locked"] and not lock_check.get("can_override"):
        raise HTTPException(status_code=400, detail="Period is locked. Cannot set opening balance.")
    
    # Verify party exists
    if data.party_type == "customer":
        party = await db.users.find_one({"id": data.party_id})
    else:
        party = await db.vendors.find_one({"id": data.party_id})
    
    if not party:
        raise HTTPException(status_code=404, detail=f"{data.party_type.title()} not found")
    
    # Check if opening balance already exists
    existing = await db.opening_balances.find_one({
        "party_type": data.party_type,
        "party_id": data.party_id
    })
    
    if existing:
        # Update existing
        await db.opening_balances.update_one(
            {"id": existing["id"]},
            {
                "$set": {
                    "amount": data.amount,
                    "balance_type": data.balance_type,
                    "as_of_date": data.as_of_date,
                    "notes": data.notes,
                    "updated_by": current_user.get("name"),
                    "updated_at": datetime.now(timezone.utc).isoformat()
                }
            }
        )
        balance_id = existing["id"]
    else:
        # Create new
        balance_id = str(uuid.uuid4())
        await db.opening_balances.insert_one({
            "id": balance_id,
            "party_type": data.party_type,
            "party_id": data.party_id,
            "party_name": party.get("name") or party.get("company_name"),
            "amount": data.amount,
            "balance_type": data.balance_type,
            "as_of_date": data.as_of_date,
            "notes": data.notes,
            "created_by": current_user.get("name"),
            "created_at": datetime.now(timezone.utc).isoformat()
        })
    
    # Audit log
    await db.ledger_audit.insert_one({
        "id": str(uuid.uuid4()),
        "action": "opening_balance_set",
        "party_type": data.party_type,
        "party_id": data.party_id,
        "amount": data.amount,
        "balance_type": data.balance_type,
        "created_by": current_user.get("name"),
        "created_at": datetime.now(timezone.utc).isoformat()
    })
    
    return {"message": "Opening balance set successfully", "id": balance_id}


@ledger_router.get("/opening-balances")
async def get_opening_balances(
    party_type: str = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get all opening balances"""
    if current_user.get("role") not in ["super_admin", "admin", "finance", "accountant", "ca"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    query = {}
    if party_type:
        query["party_type"] = party_type
    
    balances = await db.opening_balances.find(query, {"_id": 0}).to_list(1000)
    
    return {"opening_balances": balances, "count": len(balances)}


# ================== PERIOD LOCK ==================

@ledger_router.post("/period-lock")
async def create_period_lock(
    data: PeriodLockCreate,
    current_user: dict = Depends(get_erp_user)
):
    """Create period lock (quarterly/half-yearly)"""
    if current_user.get("role") not in ["super_admin", "admin"]:
        raise HTTPException(status_code=403, detail="Only Admin/Super Admin can create period locks")
    
    db = get_db()
    
    lock_id = str(uuid.uuid4())
    
    await db.period_locks.insert_one({
        "id": lock_id,
        "period_type": data.period_type,
        "period_start": data.period_start,
        "period_end": data.period_end,
        "locked_by_roles": data.locked_by_roles,
        "is_active": True,
        "notes": data.notes,
        "created_by": current_user.get("name"),
        "created_at": datetime.now(timezone.utc).isoformat()
    })
    
    # Audit log
    await db.ledger_audit.insert_one({
        "id": str(uuid.uuid4()),
        "action": "period_lock_created",
        "period_type": data.period_type,
        "period_start": data.period_start,
        "period_end": data.period_end,
        "locked_roles": data.locked_by_roles,
        "created_by": current_user.get("name"),
        "created_at": datetime.now(timezone.utc).isoformat()
    })
    
    return {"message": "Period lock created", "id": lock_id}


@ledger_router.get("/period-locks")
async def get_period_locks(current_user: dict = Depends(get_erp_user)):
    """Get all period locks"""
    if current_user.get("role") not in ["super_admin", "admin", "finance", "accountant", "ca"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    locks = await db.period_locks.find({}, {"_id": 0}).sort("period_start", -1).to_list(100)
    
    return {"locks": locks, "count": len(locks)}


@ledger_router.put("/period-lock/{lock_id}/toggle")
async def toggle_period_lock(
    lock_id: str,
    current_user: dict = Depends(get_erp_user)
):
    """Toggle period lock active/inactive"""
    if current_user.get("role") not in ["super_admin", "admin"]:
        raise HTTPException(status_code=403, detail="Only Admin/Super Admin can toggle locks")
    
    db = get_db()
    
    lock = await db.period_locks.find_one({"id": lock_id})
    if not lock:
        raise HTTPException(status_code=404, detail="Lock not found")
    
    new_status = not lock.get("is_active", True)
    
    await db.period_locks.update_one(
        {"id": lock_id},
        {"$set": {"is_active": new_status, "updated_at": datetime.now(timezone.utc).isoformat()}}
    )
    
    return {"message": f"Lock {'activated' if new_status else 'deactivated'}", "is_active": new_status}


@ledger_router.put("/period-lock/{lock_id}/grant-access")
async def grant_period_access(
    lock_id: str,
    user_id: str,
    current_user: dict = Depends(get_erp_user)
):
    """Grant a specific user access to a locked period"""
    if current_user.get("role") not in ["super_admin", "admin"]:
        raise HTTPException(status_code=403, detail="Only Admin/Super Admin can grant access")
    
    db = get_db()
    
    lock = await db.period_locks.find_one({"id": lock_id})
    if not lock:
        raise HTTPException(status_code=404, detail="Lock not found")
    
    # Add user to exemptions
    exemptions = lock.get("exempted_users", [])
    if user_id not in exemptions:
        exemptions.append(user_id)
        
        await db.period_locks.update_one(
            {"id": lock_id},
            {"$set": {"exempted_users": exemptions, "updated_at": datetime.now(timezone.utc).isoformat()}}
        )
    
    return {"message": "User granted access to locked period", "user_id": user_id}


# ================== CUSTOMER LEDGER ==================

@ledger_router.get("/customer/{customer_id}")
async def get_customer_ledger(
    customer_id: str,
    start_date: str = None,
    end_date: str = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get customer ledger with all transactions"""
    db = get_db()
    
    # Access control
    if current_user.get("role") == "customer":
        if current_user.get("id") != customer_id:
            raise HTTPException(status_code=403, detail="Can only view own ledger")
    elif current_user.get("role") not in ["super_admin", "admin", "finance", "accountant", "ca", "sales", "sales_manager"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    # Get customer details
    customer = await db.users.find_one({"id": customer_id}, {"_id": 0, "password": 0})
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")
    
    # Get opening balance
    opening = await db.opening_balances.find_one({
        "party_type": "customer",
        "party_id": customer_id
    }, {"_id": 0})
    
    opening_balance = opening.get("amount", 0) if opening else 0
    opening_type = opening.get("balance_type", "debit") if opening else "debit"
    
    # Build query for ledger entries
    query = {"party_type": "customer", "party_id": customer_id}
    if start_date:
        query["transaction_date"] = {"$gte": start_date}
    if end_date:
        if "transaction_date" in query:
            query["transaction_date"]["$lte"] = end_date
        else:
            query["transaction_date"] = {"$lte": end_date}
    
    # Get all ledger entries
    entries = await db.party_ledger.find(query, {"_id": 0}).sort("transaction_date", 1).to_list(10000)
    
    # Calculate running balance
    running_balance = opening_balance if opening_type == "debit" else -opening_balance
    
    ledger_entries = []
    total_debit = 0
    total_credit = 0
    
    for entry in entries:
        amount = entry.get("total_amount", entry.get("amount", 0))
        
        if entry.get("effect") == "debit":
            running_balance += amount
            total_debit += amount
            entry["debit"] = amount
            entry["credit"] = 0
        else:
            running_balance -= amount
            total_credit += amount
            entry["debit"] = 0
            entry["credit"] = amount
        
        entry["running_balance"] = round(running_balance, 2)
        entry["ageing"] = calculate_ageing(entry.get("transaction_date", ""))
        ledger_entries.append(entry)
    
    closing_balance = running_balance
    
    return {
        "customer": {
            "id": customer_id,
            "name": customer.get("name"),
            "email": customer.get("email"),
            "phone": customer.get("phone"),
            "gst_number": customer.get("gst_number"),
            "company_name": customer.get("company_name")
        },
        "opening_balance": {
            "amount": opening_balance,
            "type": opening_type,
            "as_of_date": opening.get("as_of_date") if opening else None
        },
        "entries": ledger_entries,
        "summary": {
            "total_debit": round(total_debit, 2),
            "total_credit": round(total_credit, 2),
            "closing_balance": round(closing_balance, 2),
            "balance_type": "Dr" if closing_balance >= 0 else "Cr",
            "entry_count": len(ledger_entries)
        },
        "period": {
            "start_date": start_date,
            "end_date": end_date
        },
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "disclaimer": "System Generated Ledger – Subject to Confirmation"
    }


@ledger_router.get("/customers/outstanding")
async def get_customer_outstanding(
    include_ageing: bool = True,
    current_user: dict = Depends(get_erp_user)
):
    """Get all customers with outstanding balances (Udhaari Report)"""
    if current_user.get("role") not in ["super_admin", "admin", "finance", "accountant", "ca", "sales_manager"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    # Aggregate customer balances
    pipeline = [
        {"$match": {"party_type": "customer"}},
        {"$group": {
            "_id": "$party_id",
            "party_name": {"$first": "$party_name"},
            "total_debit": {
                "$sum": {"$cond": [{"$eq": ["$effect", "debit"]}, "$total_amount", 0]}
            },
            "total_credit": {
                "$sum": {"$cond": [{"$eq": ["$effect", "credit"]}, "$total_amount", 0]}
            },
            "last_transaction": {"$max": "$transaction_date"},
            "entries": {"$push": {
                "transaction_date": "$transaction_date",
                "effect": "$effect",
                "amount": "$total_amount"
            }}
        }},
        {"$project": {
            "party_id": "$_id",
            "party_name": 1,
            "total_debit": 1,
            "total_credit": 1,
            "outstanding": {"$subtract": ["$total_debit", "$total_credit"]},
            "last_transaction": 1,
            "entries": 1
        }},
        {"$match": {"outstanding": {"$ne": 0}}},
        {"$sort": {"outstanding": -1}}
    ]
    
    results = await db.party_ledger.aggregate(pipeline).to_list(1000)
    
    # Add opening balances and calculate ageing
    customers = []
    total_outstanding = 0
    ageing_summary = {"0-30": 0, "31-60": 0, "61-90": 0, "90+": 0}
    
    for result in results:
        # Get opening balance
        opening = await db.opening_balances.find_one({
            "party_type": "customer",
            "party_id": result["party_id"]
        })
        
        opening_amount = opening.get("amount", 0) if opening else 0
        opening_type = opening.get("balance_type", "debit") if opening else "debit"
        
        if opening_type == "debit":
            result["outstanding"] += opening_amount
        else:
            result["outstanding"] -= opening_amount
        
        if result["outstanding"] <= 0:
            continue
        
        # Calculate ageing breakdown
        if include_ageing:
            ageing = {"0-30": 0, "31-60": 0, "61-90": 0, "90+": 0}
            for entry in result.get("entries", []):
                if entry.get("effect") == "debit":
                    bucket = calculate_ageing(entry.get("transaction_date", ""))
                    if bucket in ageing:
                        ageing[bucket] += entry.get("amount", 0)
            
            result["ageing"] = ageing
            for bucket, amount in ageing.items():
                ageing_summary[bucket] += amount
        
        del result["entries"]
        del result["_id"]
        
        result["outstanding"] = round(result["outstanding"], 2)
        total_outstanding += result["outstanding"]
        customers.append(result)
    
    return {
        "customers": customers,
        "summary": {
            "total_customers": len(customers),
            "total_outstanding": round(total_outstanding, 2),
            "ageing": ageing_summary if include_ageing else None
        },
        "generated_at": datetime.now(timezone.utc).isoformat()
    }


# ================== VENDOR LEDGER ==================

@ledger_router.get("/vendor/{vendor_id}")
async def get_vendor_ledger(
    vendor_id: str,
    start_date: str = None,
    end_date: str = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get vendor ledger with all transactions"""
    db = get_db()
    
    # Access control
    if current_user.get("role") == "vendor":
        # Vendors can only see their own ledger (if vendor login is implemented)
        pass
    elif current_user.get("role") not in ["super_admin", "admin", "finance", "accountant", "ca", "purchase"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    # Get vendor details
    vendor = await db.vendors.find_one({"id": vendor_id}, {"_id": 0})
    if not vendor:
        raise HTTPException(status_code=404, detail="Vendor not found")
    
    # Get opening balance
    opening = await db.opening_balances.find_one({
        "party_type": "vendor",
        "party_id": vendor_id
    }, {"_id": 0})
    
    opening_balance = opening.get("amount", 0) if opening else 0
    opening_type = opening.get("balance_type", "credit") if opening else "credit"
    
    # Build query
    query = {"party_type": "vendor", "party_id": vendor_id}
    if start_date:
        query["transaction_date"] = {"$gte": start_date}
    if end_date:
        if "transaction_date" in query:
            query["transaction_date"]["$lte"] = end_date
        else:
            query["transaction_date"] = {"$lte": end_date}
    
    entries = await db.party_ledger.find(query, {"_id": 0}).sort("transaction_date", 1).to_list(10000)
    
    # Calculate running balance (for vendors, credit = we owe them)
    running_balance = opening_balance if opening_type == "credit" else -opening_balance
    
    ledger_entries = []
    total_debit = 0
    total_credit = 0
    
    for entry in entries:
        amount = entry.get("total_amount", entry.get("amount", 0))
        
        if entry.get("effect") == "credit":
            running_balance += amount  # We owe more
            total_credit += amount
            entry["debit"] = 0
            entry["credit"] = amount
        else:
            running_balance -= amount  # We paid, owe less
            total_debit += amount
            entry["debit"] = amount
            entry["credit"] = 0
        
        entry["running_balance"] = round(running_balance, 2)
        entry["ageing"] = calculate_ageing(entry.get("transaction_date", ""))
        ledger_entries.append(entry)
    
    closing_balance = running_balance
    
    return {
        "vendor": {
            "id": vendor_id,
            "name": vendor.get("name"),
            "company_name": vendor.get("company_name"),
            "vendor_code": vendor.get("vendor_code"),
            "gst_number": vendor.get("gst_number"),
            "email": vendor.get("email"),
            "phone": vendor.get("phone")
        },
        "opening_balance": {
            "amount": opening_balance,
            "type": opening_type,
            "as_of_date": opening.get("as_of_date") if opening else None
        },
        "entries": ledger_entries,
        "summary": {
            "total_debit": round(total_debit, 2),
            "total_credit": round(total_credit, 2),
            "closing_balance": round(closing_balance, 2),
            "balance_type": "Cr" if closing_balance >= 0 else "Dr",
            "entry_count": len(ledger_entries)
        },
        "period": {
            "start_date": start_date,
            "end_date": end_date
        },
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "disclaimer": "System Generated Ledger – Subject to Confirmation"
    }


@ledger_router.get("/vendors/outstanding")
async def get_vendor_outstanding(
    include_ageing: bool = True,
    current_user: dict = Depends(get_erp_user)
):
    """Get all vendors with outstanding payables (Udhaari Report)"""
    if current_user.get("role") not in ["super_admin", "admin", "finance", "accountant", "ca"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    # Aggregate vendor balances
    pipeline = [
        {"$match": {"party_type": "vendor"}},
        {"$group": {
            "_id": "$party_id",
            "party_name": {"$first": "$party_name"},
            "total_credit": {
                "$sum": {"$cond": [{"$eq": ["$effect", "credit"]}, "$total_amount", 0]}
            },
            "total_debit": {
                "$sum": {"$cond": [{"$eq": ["$effect", "debit"]}, "$total_amount", 0]}
            },
            "last_transaction": {"$max": "$transaction_date"},
            "entries": {"$push": {
                "transaction_date": "$transaction_date",
                "effect": "$effect",
                "amount": "$total_amount"
            }}
        }},
        {"$project": {
            "party_id": "$_id",
            "party_name": 1,
            "total_debit": 1,
            "total_credit": 1,
            "outstanding": {"$subtract": ["$total_credit", "$total_debit"]},
            "last_transaction": 1,
            "entries": 1
        }},
        {"$match": {"outstanding": {"$ne": 0}}},
        {"$sort": {"outstanding": -1}}
    ]
    
    results = await db.party_ledger.aggregate(pipeline).to_list(1000)
    
    vendors = []
    total_outstanding = 0
    ageing_summary = {"0-30": 0, "31-60": 0, "61-90": 0, "90+": 0}
    
    for result in results:
        opening = await db.opening_balances.find_one({
            "party_type": "vendor",
            "party_id": result["party_id"]
        })
        
        opening_amount = opening.get("amount", 0) if opening else 0
        opening_type = opening.get("balance_type", "credit") if opening else "credit"
        
        if opening_type == "credit":
            result["outstanding"] += opening_amount
        else:
            result["outstanding"] -= opening_amount
        
        if result["outstanding"] <= 0:
            continue
        
        if include_ageing:
            ageing = {"0-30": 0, "31-60": 0, "61-90": 0, "90+": 0}
            for entry in result.get("entries", []):
                if entry.get("effect") == "credit":
                    bucket = calculate_ageing(entry.get("transaction_date", ""))
                    if bucket in ageing:
                        ageing[bucket] += entry.get("amount", 0)
            
            result["ageing"] = ageing
            for bucket, amount in ageing.items():
                ageing_summary[bucket] += amount
        
        del result["entries"]
        del result["_id"]
        
        result["outstanding"] = round(result["outstanding"], 2)
        total_outstanding += result["outstanding"]
        vendors.append(result)
    
    return {
        "vendors": vendors,
        "summary": {
            "total_vendors": len(vendors),
            "total_outstanding": round(total_outstanding, 2),
            "ageing": ageing_summary if include_ageing else None
        },
        "generated_at": datetime.now(timezone.utc).isoformat()
    }


# ================== GENERAL LEDGER ==================

@ledger_router.get("/gl/accounts")
async def get_gl_accounts(current_user: dict = Depends(get_erp_user)):
    """Get all GL accounts"""
    if current_user.get("role") not in ["super_admin", "admin", "finance", "accountant", "ca"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    accounts = await db.gl_accounts.find({"is_active": True}, {"_id": 0}).sort("code", 1).to_list(100)
    
    return {"accounts": accounts, "count": len(accounts)}


@ledger_router.get("/gl/account/{account_code}")
async def get_gl_account_ledger(
    account_code: str,
    start_date: str = None,
    end_date: str = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get GL account ledger with all entries"""
    if current_user.get("role") not in ["super_admin", "admin", "finance", "accountant", "ca"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    # Get account details
    account = await db.gl_accounts.find_one({"code": account_code}, {"_id": 0})
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    
    # Build query
    query = {"account_code": account_code}
    if start_date:
        query["transaction_date"] = {"$gte": start_date}
    if end_date:
        if "transaction_date" in query:
            query["transaction_date"]["$lte"] = end_date
        else:
            query["transaction_date"] = {"$lte": end_date}
    
    entries = await db.gl_entries.find(query, {"_id": 0}).sort("transaction_date", 1).to_list(10000)
    
    # Calculate running balance
    opening_balance = account.get("opening_balance", 0)
    is_debit_account = account.get("balance_type") == "debit"
    
    running_balance = opening_balance
    total_debit = 0
    total_credit = 0
    
    for entry in entries:
        amount = entry.get("amount", 0)
        
        if entry.get("entry_type") == "debit":
            if is_debit_account:
                running_balance += amount
            else:
                running_balance -= amount
            total_debit += amount
        else:
            if is_debit_account:
                running_balance -= amount
            else:
                running_balance += amount
            total_credit += amount
        
        entry["running_balance"] = round(running_balance, 2)
    
    return {
        "account": account,
        "opening_balance": opening_balance,
        "entries": entries,
        "summary": {
            "total_debit": round(total_debit, 2),
            "total_credit": round(total_credit, 2),
            "closing_balance": round(running_balance, 2)
        },
        "period": {
            "start_date": start_date,
            "end_date": end_date
        },
        "generated_at": datetime.now(timezone.utc).isoformat()
    }


@ledger_router.get("/gl/trial-balance")
async def get_trial_balance(
    as_of_date: str = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get trial balance as of a date"""
    if current_user.get("role") not in ["super_admin", "admin", "finance", "accountant", "ca"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    if not as_of_date:
        as_of_date = datetime.now().strftime("%Y-%m-%d")
    
    # Get all accounts
    accounts = await db.gl_accounts.find({"is_active": True}, {"_id": 0}).to_list(100)
    
    trial_balance = []
    total_debit = 0
    total_credit = 0
    
    for account in accounts:
        # Get all entries up to as_of_date
        pipeline = [
            {"$match": {
                "account_code": account["code"],
                "transaction_date": {"$lte": as_of_date}
            }},
            {"$group": {
                "_id": "$entry_type",
                "total": {"$sum": "$amount"}
            }}
        ]
        
        results = await db.gl_entries.aggregate(pipeline).to_list(2)
        
        debit_total = 0
        credit_total = 0
        
        for r in results:
            if r["_id"] == "debit":
                debit_total = r["total"]
            else:
                credit_total = r["total"]
        
        opening = account.get("opening_balance", 0)
        is_debit_account = account.get("balance_type") == "debit"
        
        if is_debit_account:
            balance = opening + debit_total - credit_total
            if balance >= 0:
                debit_balance = balance
                credit_balance = 0
            else:
                debit_balance = 0
                credit_balance = abs(balance)
        else:
            balance = opening + credit_total - debit_total
            if balance >= 0:
                credit_balance = balance
                debit_balance = 0
            else:
                credit_balance = 0
                debit_balance = abs(balance)
        
        if debit_balance > 0 or credit_balance > 0:
            trial_balance.append({
                "account_code": account["code"],
                "account_name": account["name"],
                "account_type": account["type"],
                "debit": round(debit_balance, 2),
                "credit": round(credit_balance, 2)
            })
            
            total_debit += debit_balance
            total_credit += credit_balance
    
    return {
        "trial_balance": trial_balance,
        "totals": {
            "debit": round(total_debit, 2),
            "credit": round(total_credit, 2),
            "difference": round(total_debit - total_credit, 2)
        },
        "as_of_date": as_of_date,
        "is_balanced": abs(total_debit - total_credit) < 0.01,
        "generated_at": datetime.now(timezone.utc).isoformat()
    }


# ================== RECONCILIATION ==================

@ledger_router.post("/reconciliation")
async def update_reconciliation(
    data: ReconciliationEntry,
    current_user: dict = Depends(get_erp_user)
):
    """Update reconciliation status for a ledger entry"""
    if current_user.get("role") not in ["super_admin", "admin", "finance", "accountant"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    entry = await db.party_ledger.find_one({"id": data.ledger_entry_id})
    if not entry:
        raise HTTPException(status_code=404, detail="Ledger entry not found")
    
    await db.party_ledger.update_one(
        {"id": data.ledger_entry_id},
        {
            "$set": {
                "reconciliation_status": data.status,
                "dispute_reason": data.dispute_reason,
                "adjustment_amount": data.adjustment_amount,
                "reconciliation_notes": data.notes,
                "reconciled_by": current_user.get("name"),
                "reconciled_at": datetime.now(timezone.utc).isoformat()
            }
        }
    )
    
    # Audit log
    await db.ledger_audit.insert_one({
        "id": str(uuid.uuid4()),
        "action": "reconciliation_update",
        "entry_id": data.ledger_entry_id,
        "status": data.status,
        "dispute_reason": data.dispute_reason,
        "adjustment_amount": data.adjustment_amount,
        "created_by": current_user.get("name"),
        "created_at": datetime.now(timezone.utc).isoformat()
    })
    
    return {"message": "Reconciliation updated", "status": data.status}


# ================== REPORTS ==================

@ledger_router.get("/reports/payment-receipt")
async def get_payment_receipt_ledger(
    start_date: str = None,
    end_date: str = None,
    mode: str = None,  # upi, bank, cash
    current_user: dict = Depends(get_erp_user)
):
    """Get payment and receipt ledger"""
    if current_user.get("role") not in ["super_admin", "admin", "finance", "accountant", "ca"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    # Query for payment received and payment made entries
    query = {"entry_type": {"$in": ["payment_received", "payment_made"]}}
    
    if start_date:
        query["transaction_date"] = {"$gte": start_date}
    if end_date:
        if "transaction_date" in query:
            query["transaction_date"]["$lte"] = end_date
        else:
            query["transaction_date"] = {"$lte": end_date}
    
    entries = await db.party_ledger.find(query, {"_id": 0}).sort("transaction_date", 1).to_list(10000)
    
    receipts = []
    payments = []
    total_receipts = 0
    total_payments = 0
    
    mode_summary = {"upi": 0, "bank": 0, "cash": 0, "other": 0}
    
    for entry in entries:
        if entry.get("entry_type") == "payment_received":
            receipts.append(entry)
            total_receipts += entry.get("total_amount", 0)
        else:
            payments.append(entry)
            total_payments += entry.get("total_amount", 0)
    
    return {
        "receipts": receipts,
        "payments": payments,
        "summary": {
            "total_receipts": round(total_receipts, 2),
            "total_payments": round(total_payments, 2),
            "net_flow": round(total_receipts - total_payments, 2),
            "receipt_count": len(receipts),
            "payment_count": len(payments)
        },
        "period": {
            "start_date": start_date,
            "end_date": end_date
        },
        "generated_at": datetime.now(timezone.utc).isoformat()
    }


@ledger_router.get("/audit-trail")
async def get_audit_trail(
    start_date: str = None,
    end_date: str = None,
    action: str = None,
    limit: int = 100,
    current_user: dict = Depends(get_erp_user)
):
    """Get ledger audit trail"""
    if current_user.get("role") not in ["super_admin", "admin", "finance", "accountant", "ca"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    query = {}
    if start_date:
        query["created_at"] = {"$gte": start_date}
    if end_date:
        if "created_at" in query:
            query["created_at"]["$lte"] = end_date + "T23:59:59"
        else:
            query["created_at"] = {"$lte": end_date + "T23:59:59"}
    if action:
        query["action"] = action
    
    logs = await db.ledger_audit.find(query, {"_id": 0}).sort("created_at", -1).limit(limit).to_list(limit)
    
    return {"audit_trail": logs, "count": len(logs)}
