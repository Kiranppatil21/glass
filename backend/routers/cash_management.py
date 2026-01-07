"""
Cash Management & Financial Reports Router
Tracks: Cash In, Cash Out, Cash Balance, Collections (Online/Cash)
Reports: Daily, Weekly, Monthly P&L Reports
Auto Email/WhatsApp: Daily (5 AM), Weekly (Monday 5 AM), Monthly (1st of month 5 AM)
"""
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from typing import Optional, List
from datetime import datetime, timezone, timedelta
from pydantic import BaseModel
from .base import get_erp_user, get_db
from .audit import log_action
import uuid
import aiosmtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
import asyncio
import logging
import calendar
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from twilio.rest import Client as TwilioClient

# Scheduler instance
scheduler = AsyncIOScheduler()
scheduler_started = False

# Twilio config
TWILIO_ACCOUNT_SID = os.environ.get('TWILIO_ACCOUNT_SID', '')
TWILIO_AUTH_TOKEN = os.environ.get('TWILIO_AUTH_TOKEN', '')
TWILIO_WHATSAPP = os.environ.get('TWILIO_WHATSAPP_NUMBER', 'whatsapp:+14155238886')

cash_router = APIRouter(prefix="/cash", tags=["Cash Management"])

# Transaction Types
TRANSACTION_TYPES = {
    "CASH_IN": "cash_in",
    "CASH_OUT": "cash_out",
    "ONLINE_IN": "online_in",
    "CASH_PURCHASE": "cash_purchase",
    "CASH_EXPENSE": "cash_expense",
    "CASH_SALARY": "cash_salary",
    "CASH_COLLECTION": "cash_collection",
    "ADVANCE_COLLECTION": "advance_collection",
    "REMAINING_COLLECTION": "remaining_collection",
    "TRANSPORT_COLLECTION": "transport_collection",
    "BANK_DEPOSIT": "bank_deposit",
    "BANK_WITHDRAWAL": "bank_withdrawal"
}


class CashTransaction(BaseModel):
    amount: float
    transaction_type: str
    description: str
    reference_id: Optional[str] = None  # Order ID, Invoice ID, etc.
    reference_type: Optional[str] = None  # order, invoice, expense, salary, purchase
    party_name: Optional[str] = None  # Customer/Vendor name
    payment_mode: Optional[str] = "cash"  # cash, online, cheque
    vehicle_type: Optional[str] = None
    notes: Optional[str] = None


class DailyReport(BaseModel):
    date: str
    opening_balance: float
    cash_in: float
    cash_out: float
    closing_balance: float
    transactions: List[dict]


# ================== CASH TRANSACTIONS ==================

@cash_router.post("/transaction")
async def record_cash_transaction(
    data: CashTransaction,
    current_user: dict = Depends(get_erp_user)
):
    """Record a cash transaction (in or out)"""
    allowed_roles = ['admin', 'super_admin', 'owner', 'hr', 'accountant', 'finance', 'manager', 'operator']
    if current_user.get("role") not in allowed_roles:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    now = datetime.now(timezone.utc)
    
    # Determine if cash in or out
    is_cash_in = data.transaction_type in ['cash_in', 'cash_collection', 'advance_collection', 
                                            'remaining_collection', 'transport_collection', 
                                            'online_in', 'bank_withdrawal']
    
    transaction = {
        "id": str(uuid.uuid4()),
        "amount": abs(data.amount),
        "transaction_type": data.transaction_type,
        "direction": "in" if is_cash_in else "out",
        "description": data.description,
        "reference_id": data.reference_id,
        "reference_type": data.reference_type,
        "party_name": data.party_name,
        "payment_mode": data.payment_mode,
        "vehicle_type": data.vehicle_type,
        "notes": data.notes,
        "recorded_by": current_user.get("id"),
        "recorded_by_name": current_user.get("name"),
        "recorded_by_role": current_user.get("role"),
        "timestamp": now.isoformat(),
        "date": now.strftime("%Y-%m-%d"),
        "month": now.strftime("%Y-%m"),
        "year": now.strftime("%Y")
    }
    
    await db.cash_transactions.insert_one(transaction)
    
    # Log to audit
    await log_action(
        user_id=current_user.get("id"),
        user_name=current_user.get("name"),
        user_role=current_user.get("role"),
        action="create",
        module="cash",
        details={
            "action": f"Cash {'In' if is_cash_in else 'Out'}",
            "amount": data.amount,
            "type": data.transaction_type,
            "party": data.party_name
        },
        record_id=transaction["id"]
    )
    
    return {
        "message": f"Cash {'received' if is_cash_in else 'paid'} recorded successfully",
        "transaction_id": transaction["id"],
        "amount": data.amount,
        "type": data.transaction_type
    }


@cash_router.get("/balance")
async def get_cash_balance(
    current_user: dict = Depends(get_erp_user)
):
    """Get current cash in hand balance"""
    allowed_roles = ['admin', 'super_admin', 'owner', 'hr', 'accountant', 'finance', 'manager']
    if current_user.get("role") not in allowed_roles:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    # Calculate total cash in and out
    pipeline = [
        {"$group": {
            "_id": "$direction",
            "total": {"$sum": "$amount"}
        }}
    ]
    
    results = await db.cash_transactions.aggregate(pipeline).to_list(10)
    
    cash_in = 0
    cash_out = 0
    for r in results:
        if r["_id"] == "in":
            cash_in = r["total"]
        else:
            cash_out = r["total"]
    
    balance = cash_in - cash_out
    
    # Today's transactions
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    today_transactions = await db.cash_transactions.find(
        {"date": today},
        {"_id": 0}
    ).sort("timestamp", -1).to_list(50)
    
    today_in = sum(t["amount"] for t in today_transactions if t["direction"] == "in")
    today_out = sum(t["amount"] for t in today_transactions if t["direction"] == "out")
    
    return {
        "current_balance": round(balance, 2),
        "total_cash_in": round(cash_in, 2),
        "total_cash_out": round(cash_out, 2),
        "today": {
            "date": today,
            "cash_in": round(today_in, 2),
            "cash_out": round(today_out, 2),
            "net": round(today_in - today_out, 2),
            "transactions_count": len(today_transactions)
        },
        "recent_transactions": today_transactions[:10]
    }


# ================== COLLECTION REPORTS ==================

@cash_router.get("/collections")
async def get_collections_report(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    payment_mode: Optional[str] = None,
    operator_id: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get collections report - Cash vs Online, by operator"""
    allowed_roles = ['admin', 'super_admin', 'owner', 'hr', 'accountant', 'finance', 'manager']
    if current_user.get("role") not in allowed_roles:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    query = {"direction": "in"}
    if start_date:
        query["date"] = {"$gte": start_date}
    if end_date:
        if "date" in query:
            query["date"]["$lte"] = end_date
        else:
            query["date"] = {"$lte": end_date}
    if payment_mode:
        query["payment_mode"] = payment_mode
    if operator_id:
        query["recorded_by"] = operator_id
    
    transactions = await db.cash_transactions.find(query, {"_id": 0}).to_list(5000)
    
    # Calculate totals
    total_cash = sum(t["amount"] for t in transactions if t.get("payment_mode") == "cash")
    total_online = sum(t["amount"] for t in transactions if t.get("payment_mode") == "online")
    total_cheque = sum(t["amount"] for t in transactions if t.get("payment_mode") == "cheque")
    
    # By operator
    by_operator = {}
    for t in transactions:
        op_id = t.get("recorded_by", "unknown")
        op_name = t.get("recorded_by_name", "Unknown")
        if op_id not in by_operator:
            by_operator[op_id] = {
                "operator_id": op_id,
                "operator_name": op_name,
                "cash": 0,
                "online": 0,
                "cheque": 0,
                "total": 0,
                "count": 0
            }
        mode = t.get("payment_mode", "cash")
        by_operator[op_id][mode] = by_operator[op_id].get(mode, 0) + t["amount"]
        by_operator[op_id]["total"] += t["amount"]
        by_operator[op_id]["count"] += 1
    
    # By transaction type
    by_type = {}
    for t in transactions:
        t_type = t.get("transaction_type", "other")
        if t_type not in by_type:
            by_type[t_type] = {"type": t_type, "total": 0, "count": 0}
        by_type[t_type]["total"] += t["amount"]
        by_type[t_type]["count"] += 1
    
    return {
        "period": {
            "start": start_date or "all time",
            "end": end_date or "today"
        },
        "summary": {
            "total_collection": round(total_cash + total_online + total_cheque, 2),
            "cash_collection": round(total_cash, 2),
            "online_collection": round(total_online, 2),
            "cheque_collection": round(total_cheque, 2),
            "total_transactions": len(transactions)
        },
        "by_operator": list(by_operator.values()),
        "by_type": list(by_type.values()),
        "transactions": transactions[:100]
    }


@cash_router.get("/expenses")
async def get_cash_expenses_report(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    category: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get cash expenses report - Where cash was spent"""
    allowed_roles = ['admin', 'super_admin', 'owner', 'hr', 'accountant', 'finance', 'manager']
    if current_user.get("role") not in allowed_roles:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    query = {"direction": "out"}
    if start_date:
        query["date"] = {"$gte": start_date}
    if end_date:
        if "date" in query:
            query["date"]["$lte"] = end_date
        else:
            query["date"] = {"$lte": end_date}
    if category:
        query["transaction_type"] = category
    
    transactions = await db.cash_transactions.find(query, {"_id": 0}).to_list(5000)
    
    # By category/type
    by_category = {}
    for t in transactions:
        cat = t.get("transaction_type", "other")
        if cat not in by_category:
            by_category[cat] = {"category": cat, "total": 0, "count": 0, "items": []}
        by_category[cat]["total"] += t["amount"]
        by_category[cat]["count"] += 1
        by_category[cat]["items"].append({
            "date": t["date"],
            "amount": t["amount"],
            "description": t.get("description"),
            "party": t.get("party_name"),
            "recorded_by": t.get("recorded_by_name")
        })
    
    # By date
    by_date = {}
    for t in transactions:
        date = t["date"]
        if date not in by_date:
            by_date[date] = {"date": date, "total": 0, "count": 0}
        by_date[date]["total"] += t["amount"]
        by_date[date]["count"] += 1
    
    total_expenses = sum(t["amount"] for t in transactions)
    
    return {
        "period": {
            "start": start_date or "all time",
            "end": end_date or "today"
        },
        "summary": {
            "total_expenses": round(total_expenses, 2),
            "total_transactions": len(transactions),
            "categories_count": len(by_category)
        },
        "by_category": [
            {"category": k, "total": round(v["total"], 2), "count": v["count"]}
            for k, v in sorted(by_category.items(), key=lambda x: x[1]["total"], reverse=True)
        ],
        "by_date": sorted(by_date.values(), key=lambda x: x["date"], reverse=True),
        "transactions": transactions[:100]
    }


# ================== DAILY, MONTHLY, YEARLY REPORTS ==================

@cash_router.get("/report/daily")
async def get_daily_cash_report(
    date: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get detailed daily cash report"""
    allowed_roles = ['admin', 'super_admin', 'owner', 'hr', 'accountant', 'finance', 'manager']
    if current_user.get("role") not in allowed_roles:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    target_date = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    # Get all transactions for the day
    transactions = await db.cash_transactions.find(
        {"date": target_date},
        {"_id": 0}
    ).sort("timestamp", 1).to_list(1000)
    
    # Calculate opening balance (sum of all transactions before this date)
    prev_pipeline = [
        {"$match": {"date": {"$lt": target_date}}},
        {"$group": {
            "_id": "$direction",
            "total": {"$sum": "$amount"}
        }}
    ]
    prev_results = await db.cash_transactions.aggregate(prev_pipeline).to_list(10)
    
    prev_in = sum(r["total"] for r in prev_results if r["_id"] == "in")
    prev_out = sum(r["total"] for r in prev_results if r["_id"] == "out")
    opening_balance = prev_in - prev_out
    
    # Today's totals
    today_in = sum(t["amount"] for t in transactions if t["direction"] == "in")
    today_out = sum(t["amount"] for t in transactions if t["direction"] == "out")
    closing_balance = opening_balance + today_in - today_out
    
    # Breakdown by type
    by_type = {}
    for t in transactions:
        t_type = t["transaction_type"]
        direction = t["direction"]
        key = f"{t_type}_{direction}"
        if key not in by_type:
            by_type[key] = {"type": t_type, "direction": direction, "total": 0, "count": 0}
        by_type[key]["total"] += t["amount"]
        by_type[key]["count"] += 1
    
    # By operator
    by_operator = {}
    for t in transactions:
        op = t.get("recorded_by_name", "Unknown")
        if op not in by_operator:
            by_operator[op] = {"operator": op, "cash_in": 0, "cash_out": 0, "count": 0}
        if t["direction"] == "in":
            by_operator[op]["cash_in"] += t["amount"]
        else:
            by_operator[op]["cash_out"] += t["amount"]
        by_operator[op]["count"] += 1
    
    # Get order collections for today
    orders = await db.orders.find(
        {"$or": [
            {"advance_payment_status": "paid", "created_at": {"$regex": f"^{target_date}"}},
            {"remaining_payment_status": {"$in": ["paid", "cash_received"]}, "cash_received_at": {"$regex": f"^{target_date}"}}
        ]},
        {"_id": 0, "order_number": 1, "customer_name": 1, "advance_amount": 1, 
         "remaining_amount": 1, "advance_payment_status": 1, "remaining_payment_status": 1,
         "remaining_payment_method": 1, "transport_charge": 1}
    ).to_list(100)
    
    order_collections = {
        "advance_online": sum(o.get("advance_amount", 0) for o in orders if o.get("advance_payment_status") == "paid"),
        "remaining_cash": sum(o.get("remaining_amount", 0) for o in orders if o.get("remaining_payment_status") == "cash_received"),
        "remaining_online": sum(o.get("remaining_amount", 0) for o in orders if o.get("remaining_payment_status") == "paid" and o.get("remaining_payment_method") != "cash"),
        "transport": sum(o.get("transport_charge", 0) for o in orders)
    }
    
    return {
        "date": target_date,
        "balances": {
            "opening": round(opening_balance, 2),
            "cash_in": round(today_in, 2),
            "cash_out": round(today_out, 2),
            "closing": round(closing_balance, 2)
        },
        "order_collections": order_collections,
        "by_type": list(by_type.values()),
        "by_operator": list(by_operator.values()),
        "transactions": transactions
    }


@cash_router.get("/report/monthly")
async def get_monthly_cash_report(
    month: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get monthly cash report with daily breakdown"""
    allowed_roles = ['admin', 'super_admin', 'owner', 'hr', 'accountant', 'finance', 'manager']
    if current_user.get("role") not in allowed_roles:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    target_month = month or datetime.now(timezone.utc).strftime("%Y-%m")
    
    # Get all transactions for the month
    transactions = await db.cash_transactions.find(
        {"month": target_month},
        {"_id": 0}
    ).to_list(10000)
    
    # Daily breakdown
    daily = {}
    for t in transactions:
        date = t["date"]
        if date not in daily:
            daily[date] = {"date": date, "cash_in": 0, "cash_out": 0, "count": 0}
        if t["direction"] == "in":
            daily[date]["cash_in"] += t["amount"]
        else:
            daily[date]["cash_out"] += t["amount"]
        daily[date]["count"] += 1
    
    # Calculate cumulative balance
    sorted_dates = sorted(daily.keys())
    cumulative = 0
    for date in sorted_dates:
        cumulative += daily[date]["cash_in"] - daily[date]["cash_out"]
        daily[date]["cumulative_balance"] = round(cumulative, 2)
    
    # Monthly totals
    total_in = sum(t["amount"] for t in transactions if t["direction"] == "in")
    total_out = sum(t["amount"] for t in transactions if t["direction"] == "out")
    
    # By category
    by_category = {}
    for t in transactions:
        cat = t["transaction_type"]
        if cat not in by_category:
            by_category[cat] = {"category": cat, "in": 0, "out": 0}
        if t["direction"] == "in":
            by_category[cat]["in"] += t["amount"]
        else:
            by_category[cat]["out"] += t["amount"]
    
    # By operator
    by_operator = {}
    for t in transactions:
        op = t.get("recorded_by_name", "Unknown")
        if op not in by_operator:
            by_operator[op] = {"operator": op, "cash_in": 0, "cash_out": 0}
        if t["direction"] == "in":
            by_operator[op]["cash_in"] += t["amount"]
        else:
            by_operator[op]["cash_out"] += t["amount"]
    
    return {
        "month": target_month,
        "summary": {
            "total_cash_in": round(total_in, 2),
            "total_cash_out": round(total_out, 2),
            "net_cash_flow": round(total_in - total_out, 2),
            "total_transactions": len(transactions),
            "working_days": len(daily)
        },
        "daily_breakdown": [daily[d] for d in sorted_dates],
        "by_category": list(by_category.values()),
        "by_operator": list(by_operator.values())
    }


@cash_router.get("/report/yearly")
async def get_yearly_cash_report(
    year: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get yearly cash report with monthly breakdown"""
    allowed_roles = ['admin', 'super_admin', 'owner', 'hr', 'accountant', 'finance']
    if current_user.get("role") not in allowed_roles:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    target_year = year or datetime.now(timezone.utc).strftime("%Y")
    
    # Monthly aggregation
    pipeline = [
        {"$match": {"year": target_year}},
        {"$group": {
            "_id": {"month": "$month", "direction": "$direction"},
            "total": {"$sum": "$amount"},
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id.month": 1}}
    ]
    
    results = await db.cash_transactions.aggregate(pipeline).to_list(100)
    
    # Process results
    monthly = {}
    for r in results:
        month = r["_id"]["month"]
        direction = r["_id"]["direction"]
        if month not in monthly:
            monthly[month] = {"month": month, "cash_in": 0, "cash_out": 0, "transactions": 0}
        if direction == "in":
            monthly[month]["cash_in"] = r["total"]
        else:
            monthly[month]["cash_out"] = r["total"]
        monthly[month]["transactions"] += r["count"]
    
    # Calculate net and cumulative
    sorted_months = sorted(monthly.keys())
    cumulative = 0
    for month in sorted_months:
        monthly[month]["net"] = round(monthly[month]["cash_in"] - monthly[month]["cash_out"], 2)
        cumulative += monthly[month]["net"]
        monthly[month]["cumulative"] = round(cumulative, 2)
    
    # Yearly totals
    total_in = sum(m["cash_in"] for m in monthly.values())
    total_out = sum(m["cash_out"] for m in monthly.values())
    
    return {
        "year": target_year,
        "summary": {
            "total_cash_in": round(total_in, 2),
            "total_cash_out": round(total_out, 2),
            "net_cash_flow": round(total_in - total_out, 2),
            "months_with_data": len(monthly)
        },
        "monthly_breakdown": [monthly[m] for m in sorted_months]
    }


@cash_router.get("/report/operator/{operator_id}")
async def get_operator_cash_report(
    operator_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get cash report for specific operator"""
    allowed_roles = ['admin', 'super_admin', 'owner', 'hr', 'accountant', 'finance', 'manager']
    if current_user.get("role") not in allowed_roles:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    query = {"recorded_by": operator_id}
    if start_date:
        query["date"] = {"$gte": start_date}
    if end_date:
        if "date" in query:
            query["date"]["$lte"] = end_date
        else:
            query["date"] = {"$lte": end_date}
    
    transactions = await db.cash_transactions.find(query, {"_id": 0}).sort("timestamp", -1).to_list(1000)
    
    if not transactions:
        return {"operator_id": operator_id, "message": "No transactions found"}
    
    operator_name = transactions[0].get("recorded_by_name", "Unknown")
    
    # Calculate totals
    total_in = sum(t["amount"] for t in transactions if t["direction"] == "in")
    total_out = sum(t["amount"] for t in transactions if t["direction"] == "out")
    
    # By date
    by_date = {}
    for t in transactions:
        date = t["date"]
        if date not in by_date:
            by_date[date] = {"date": date, "cash_in": 0, "cash_out": 0, "count": 0}
        if t["direction"] == "in":
            by_date[date]["cash_in"] += t["amount"]
        else:
            by_date[date]["cash_out"] += t["amount"]
        by_date[date]["count"] += 1
    
    # By type
    by_type = {}
    for t in transactions:
        t_type = t["transaction_type"]
        if t_type not in by_type:
            by_type[t_type] = {"type": t_type, "total": 0, "count": 0}
        by_type[t_type]["total"] += t["amount"]
        by_type[t_type]["count"] += 1
    
    return {
        "operator": {
            "id": operator_id,
            "name": operator_name
        },
        "period": {
            "start": start_date or transactions[-1]["date"],
            "end": end_date or transactions[0]["date"]
        },
        "summary": {
            "total_cash_in": round(total_in, 2),
            "total_cash_out": round(total_out, 2),
            "net_handled": round(total_in - total_out, 2),
            "total_transactions": len(transactions),
            "active_days": len(by_date)
        },
        "by_date": sorted(by_date.values(), key=lambda x: x["date"], reverse=True),
        "by_type": list(by_type.values()),
        "recent_transactions": transactions[:50]
    }


# ================== PURCHASE TRACKING ==================

@cash_router.post("/purchase")
async def record_cash_purchase(
    vendor_name: str,
    amount: float,
    description: str,
    items: Optional[str] = None,
    invoice_number: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Record a cash purchase (material purchase with cash)"""
    allowed_roles = ['admin', 'super_admin', 'owner', 'accountant', 'finance', 'store', 'manager']
    if current_user.get("role") not in allowed_roles:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    now = datetime.now(timezone.utc)
    
    purchase = {
        "id": str(uuid.uuid4()),
        "vendor_name": vendor_name,
        "amount": amount,
        "description": description,
        "items": items,
        "invoice_number": invoice_number,
        "payment_mode": "cash",
        "recorded_by": current_user.get("id"),
        "recorded_by_name": current_user.get("name"),
        "timestamp": now.isoformat(),
        "date": now.strftime("%Y-%m-%d"),
        "month": now.strftime("%Y-%m"),
        "year": now.strftime("%Y")
    }
    
    await db.cash_purchases.insert_one(purchase)
    
    # Also record as cash transaction
    await db.cash_transactions.insert_one({
        "id": str(uuid.uuid4()),
        "amount": amount,
        "transaction_type": "cash_purchase",
        "direction": "out",
        "description": f"Purchase from {vendor_name}: {description}",
        "reference_id": purchase["id"],
        "reference_type": "purchase",
        "party_name": vendor_name,
        "payment_mode": "cash",
        "recorded_by": current_user.get("id"),
        "recorded_by_name": current_user.get("name"),
        "recorded_by_role": current_user.get("role"),
        "timestamp": now.isoformat(),
        "date": now.strftime("%Y-%m-%d"),
        "month": now.strftime("%Y-%m"),
        "year": now.strftime("%Y")
    })
    
    # Log to audit
    await log_action(
        user_id=current_user.get("id"),
        user_name=current_user.get("name"),
        user_role=current_user.get("role"),
        action="create",
        module="purchase",
        details={
            "action": "Cash Purchase",
            "vendor": vendor_name,
            "amount": amount,
            "description": description
        },
        record_id=purchase["id"]
    )
    
    return {
        "message": "Cash purchase recorded",
        "purchase_id": purchase["id"],
        "amount": amount,
        "vendor": vendor_name
    }


@cash_router.get("/purchases")
async def get_cash_purchases(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    vendor: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get cash purchases report"""
    allowed_roles = ['admin', 'super_admin', 'owner', 'accountant', 'finance', 'store', 'manager']
    if current_user.get("role") not in allowed_roles:
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
    if vendor:
        query["vendor_name"] = {"$regex": vendor, "$options": "i"}
    
    purchases = await db.cash_purchases.find(query, {"_id": 0}).sort("timestamp", -1).to_list(500)
    
    # By vendor
    by_vendor = {}
    for p in purchases:
        v = p["vendor_name"]
        if v not in by_vendor:
            by_vendor[v] = {"vendor": v, "total": 0, "count": 0}
        by_vendor[v]["total"] += p["amount"]
        by_vendor[v]["count"] += 1
    
    # By date
    by_date = {}
    for p in purchases:
        d = p["date"]
        if d not in by_date:
            by_date[d] = {"date": d, "total": 0, "count": 0}
        by_date[d]["total"] += p["amount"]
        by_date[d]["count"] += 1
    
    total = sum(p["amount"] for p in purchases)
    
    return {
        "summary": {
            "total_purchases": round(total, 2),
            "total_count": len(purchases),
            "vendors_count": len(by_vendor)
        },
        "by_vendor": sorted(by_vendor.values(), key=lambda x: x["total"], reverse=True),
        "by_date": sorted(by_date.values(), key=lambda x: x["date"], reverse=True),
        "purchases": purchases[:100]
    }


# ================== P&L REPORT ==================

@cash_router.get("/pnl")
async def get_pnl_report(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get comprehensive P&L Report"""
    allowed_roles = ['admin', 'super_admin', 'owner', 'accountant', 'finance']
    if current_user.get("role") not in allowed_roles:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    # Default to current month
    if not start_date:
        start_date = datetime.now(timezone.utc).strftime("%Y-%m-01")
    if not end_date:
        end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    # Revenue from orders
    orders = await db.orders.find(
        {"payment_status": "completed", "created_at": {"$gte": start_date, "$lte": end_date + "T23:59:59"}},
        {"_id": 0, "total_price": 1, "transport_charge": 1, "advance_amount": 1, 
         "remaining_amount": 1, "remaining_payment_method": 1}
    ).to_list(5000)
    
    product_revenue = sum(o.get("total_price", 0) for o in orders)
    transport_revenue = sum(o.get("transport_charge", 0) for o in orders)
    online_collection = sum(o.get("advance_amount", 0) for o in orders)
    cash_collection = sum(o.get("remaining_amount", 0) for o in orders if o.get("remaining_payment_method") == "cash")
    
    # Expenses from cash transactions
    expenses_query = {"direction": "out", "date": {"$gte": start_date, "$lte": end_date}}
    expenses = await db.cash_transactions.find(expenses_query, {"_id": 0}).to_list(5000)
    
    # Group expenses by type
    expense_by_type = {}
    for e in expenses:
        t_type = e.get("transaction_type", "other")
        if t_type not in expense_by_type:
            expense_by_type[t_type] = 0
        expense_by_type[t_type] += e["amount"]
    
    total_expenses = sum(expense_by_type.values())
    
    # Get expenses from expenses collection too
    db_expenses = await db.expenses.find(
        {"date": {"$gte": start_date, "$lte": end_date}},
        {"_id": 0, "amount": 1, "category": 1}
    ).to_list(1000)
    
    for e in db_expenses:
        cat = e.get("category", "other")
        expense_by_type[cat] = expense_by_type.get(cat, 0) + e.get("amount", 0)
        total_expenses += e.get("amount", 0)
    
    # Calculate P&L
    total_revenue = product_revenue + transport_revenue
    gross_profit = total_revenue - total_expenses
    profit_margin = round((gross_profit / max(total_revenue, 1)) * 100, 2)
    
    return {
        "period": {
            "start": start_date,
            "end": end_date
        },
        "revenue": {
            "product_sales": round(product_revenue, 2),
            "transport_charges": round(transport_revenue, 2),
            "total_revenue": round(total_revenue, 2)
        },
        "collections": {
            "online": round(online_collection, 2),
            "cash": round(cash_collection, 2),
            "total": round(online_collection + cash_collection, 2)
        },
        "expenses": {
            "by_category": {k: round(v, 2) for k, v in sorted(expense_by_type.items(), key=lambda x: x[1], reverse=True)},
            "total": round(total_expenses, 2)
        },
        "profit_loss": {
            "gross_profit": round(gross_profit, 2),
            "profit_margin": profit_margin,
            "status": "profit" if gross_profit >= 0 else "loss"
        },
        "orders_count": len(orders)
    }


# ================== AUTO EMAIL/WHATSAPP REPORTS ==================

async def send_daily_pnl_email(recipients: List[str], report_data: dict):
    """Send daily P&L report via email"""
    smtp_host = os.environ.get("SMTP_HOST", "smtp.hostinger.com")
    smtp_port = int(os.environ.get("SMTP_PORT", 465))
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASS", "")
    
    if not smtp_user or not smtp_pass:
        return {"error": "SMTP not configured"}
    
    date = report_data.get("period", {}).get("end", "Today")
    
    html_content = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }}
            .container {{ max-width: 600px; margin: 0 auto; background: white; border-radius: 10px; overflow: hidden; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
            .header {{ background: linear-gradient(135deg, #0d9488, #0f766e); color: white; padding: 20px; text-align: center; }}
            .header h1 {{ margin: 0; font-size: 24px; }}
            .content {{ padding: 20px; }}
            .section {{ margin-bottom: 20px; padding: 15px; background: #f8fafc; border-radius: 8px; }}
            .section h3 {{ margin: 0 0 10px 0; color: #0d9488; font-size: 16px; }}
            .row {{ display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid #e2e8f0; }}
            .row:last-child {{ border-bottom: none; }}
            .label {{ color: #64748b; }}
            .value {{ font-weight: bold; color: #1e293b; }}
            .profit {{ color: #10b981; }}
            .loss {{ color: #ef4444; }}
            .total-row {{ background: #0d9488; color: white; padding: 15px; border-radius: 8px; margin-top: 15px; }}
            .total-row .value {{ font-size: 24px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>📊 Daily P&L Report</h1>
                <p style="margin: 5px 0 0 0; opacity: 0.9;">Lucumaa Glass - {date}</p>
            </div>
            <div class="content">
                <div class="section">
                    <h3>💰 Revenue</h3>
                    <div class="row">
                        <span class="label">Product Sales</span>
                        <span class="value">₹{report_data.get('revenue', {}).get('product_sales', 0):,.2f}</span>
                    </div>
                    <div class="row">
                        <span class="label">Transport Charges</span>
                        <span class="value">₹{report_data.get('revenue', {}).get('transport_charges', 0):,.2f}</span>
                    </div>
                    <div class="row">
                        <span class="label"><strong>Total Revenue</strong></span>
                        <span class="value" style="color: #10b981;">₹{report_data.get('revenue', {}).get('total_revenue', 0):,.2f}</span>
                    </div>
                </div>
                
                <div class="section">
                    <h3>💳 Collections</h3>
                    <div class="row">
                        <span class="label">Online Collection</span>
                        <span class="value">₹{report_data.get('collections', {}).get('online', 0):,.2f}</span>
                    </div>
                    <div class="row">
                        <span class="label">Cash Collection</span>
                        <span class="value">₹{report_data.get('collections', {}).get('cash', 0):,.2f}</span>
                    </div>
                </div>
                
                <div class="section">
                    <h3>📉 Expenses</h3>
                    <div class="row">
                        <span class="label">Total Expenses</span>
                        <span class="value" style="color: #ef4444;">₹{report_data.get('expenses', {}).get('total', 0):,.2f}</span>
                    </div>
                </div>
                
                <div class="total-row">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <span>{'📈 Gross Profit' if report_data.get('profit_loss', {}).get('gross_profit', 0) >= 0 else '📉 Loss'}</span>
                        <span class="value">₹{abs(report_data.get('profit_loss', {}).get('gross_profit', 0)):,.2f}</span>
                    </div>
                    <div style="text-align: right; margin-top: 5px; font-size: 14px; opacity: 0.9;">
                        Margin: {report_data.get('profit_loss', {}).get('profit_margin', 0)}%
                    </div>
                </div>
                
                <p style="text-align: center; color: #64748b; font-size: 12px; margin-top: 20px;">
                    This is an automated report from Lucumaa Glass ERP System
                </p>
            </div>
        </div>
    </body>
    </html>
    """
    
    for recipient in recipients:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"📊 Daily P&L Report - {date} | Lucumaa Glass"
        msg['From'] = smtp_user
        msg['To'] = recipient
        msg.attach(MIMEText(html_content, 'html'))
        
        try:
            await aiosmtplib.send(
                msg,
                hostname=smtp_host,
                port=smtp_port,
                username=smtp_user,
                password=smtp_pass,
                use_tls=True
            )
        except Exception as e:
            print(f"Failed to send email to {recipient}: {e}")
    
    return {"message": "Emails sent"}


@cash_router.post("/send-daily-report")
async def trigger_daily_report(
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_erp_user)
):
    """Manually trigger daily P&L report email"""
    if current_user.get("role") not in ["super_admin", "admin", "owner", "finance"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    # Get today's P&L
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    # Get P&L data (reuse the pnl endpoint logic)
    orders = await db.orders.find(
        {"payment_status": "completed", "created_at": {"$regex": f"^{today}"}},
        {"_id": 0}
    ).to_list(500)
    
    product_revenue = sum(o.get("total_price", 0) for o in orders)
    transport_revenue = sum(o.get("transport_charge", 0) for o in orders)
    online_collection = sum(o.get("advance_amount", 0) for o in orders)
    cash_collection = sum(o.get("remaining_amount", 0) for o in orders if o.get("remaining_payment_method") == "cash")
    
    expenses = await db.cash_transactions.find(
        {"direction": "out", "date": today},
        {"_id": 0}
    ).to_list(500)
    total_expenses = sum(e["amount"] for e in expenses)
    
    report_data = {
        "period": {"start": today, "end": today},
        "revenue": {
            "product_sales": product_revenue,
            "transport_charges": transport_revenue,
            "total_revenue": product_revenue + transport_revenue
        },
        "collections": {
            "online": online_collection,
            "cash": cash_collection,
            "total": online_collection + cash_collection
        },
        "expenses": {"total": total_expenses},
        "profit_loss": {
            "gross_profit": product_revenue + transport_revenue - total_expenses,
            "profit_margin": round((product_revenue + transport_revenue - total_expenses) / max(product_revenue + transport_revenue, 1) * 100, 2)
        }
    }
    
    # Get recipients (admin, super_admin, finance users)
    recipients_users = await db.users.find(
        {"role": {"$in": ["super_admin", "admin", "owner", "finance", "accountant"]}},
        {"_id": 0, "email": 1}
    ).to_list(20)
    
    recipients = [u["email"] for u in recipients_users if u.get("email")]
    
    if recipients:
        background_tasks.add_task(send_daily_pnl_email, recipients, report_data)
    
    return {
        "message": "Daily report triggered",
        "recipients": recipients,
        "report_date": today
    }


# ================== WHATSAPP NOTIFICATION ==================

async def send_whatsapp_pnl_report(phones: List[str], report_data: dict):
    """Send daily P&L report via WhatsApp"""
    if not TWILIO_ACCOUNT_SID or not TWILIO_AUTH_TOKEN:
        logging.warning("Twilio not configured - skipping WhatsApp")
        return
    
    try:
        twilio_client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    except Exception as e:
        logging.error(f"Twilio client init failed: {e}")
        return
    
    date = report_data.get("period", {}).get("end", "Today")
    revenue = report_data.get("revenue", {}).get("total_revenue", 0)
    expenses = report_data.get("expenses", {}).get("total", 0)
    profit = report_data.get("profit_loss", {}).get("gross_profit", 0)
    margin = report_data.get("profit_loss", {}).get("profit_margin", 0)
    cash_balance = report_data.get("cash_balance", 0)
    online = report_data.get("collections", {}).get("online", 0)
    cash = report_data.get("collections", {}).get("cash", 0)
    
    profit_emoji = "📈" if profit >= 0 else "📉"
    
    message = f"""📊 *Daily P&L Report - {date}*
_Lucumaa Glass ERP_

💰 *Revenue:* ₹{revenue:,.0f}
💳 *Collections:*
   • Online: ₹{online:,.0f}
   • Cash: ₹{cash:,.0f}
📉 *Expenses:* ₹{expenses:,.0f}

{profit_emoji} *{'Profit' if profit >= 0 else 'Loss'}:* ₹{abs(profit):,.0f} ({margin}%)
🏦 *Cash in Hand:* ₹{cash_balance:,.0f}

_Auto-generated at 5 AM IST_"""
    
    for phone in phones:
        try:
            formatted_phone = phone if phone.startswith("+") else f"+91{phone}"
            await asyncio.to_thread(
                lambda p=formatted_phone: twilio_client.messages.create(
                    body=message,
                    from_=TWILIO_WHATSAPP,
                    to=f"whatsapp:{p}"
                )
            )
            logging.info(f"WhatsApp sent to {formatted_phone}")
        except Exception as e:
            logging.error(f"WhatsApp failed to {phone}: {e}")


# ================== AUTO SCHEDULER (5 AM IST) ==================

async def scheduled_daily_report():
    """Auto-triggered daily report at 5 AM IST"""
    logging.info("Starting scheduled daily P&L report...")
    
    db = get_db()
    
    # Get yesterday's date (since report runs at 5 AM, report on previous day)
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    
    # Check if reports are enabled
    settings = await db.settings.find_one({"type": "daily_report"}, {"_id": 0})
    if settings and not settings.get("enabled", True):
        logging.info("Daily reports disabled in settings")
        return
    
    # Fetch yesterday's data
    orders = await db.orders.find(
        {"created_at": {"$regex": f"^{yesterday}"}},
        {"_id": 0}
    ).to_list(500)
    
    completed_orders = [o for o in orders if o.get("payment_status") in ["completed", "partial"]]
    
    product_revenue = sum(o.get("total_price", 0) for o in completed_orders)
    transport_revenue = sum(o.get("transport_charge", 0) for o in completed_orders)
    online_collection = sum(o.get("advance_amount", 0) for o in completed_orders if o.get("advance_payment_status") == "paid")
    cash_collection = sum(o.get("remaining_amount", 0) for o in completed_orders if o.get("remaining_payment_status") == "cash_received")
    
    # Get expenses
    expenses = await db.cash_transactions.find(
        {"direction": "out", "date": yesterday},
        {"_id": 0}
    ).to_list(500)
    total_expenses = sum(e["amount"] for e in expenses)
    
    # Get current cash balance
    cash_in_cursor = await db.cash_transactions.aggregate([
        {"$match": {"direction": "in"}},
        {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
    ]).to_list(1)
    cash_out_cursor = await db.cash_transactions.aggregate([
        {"$match": {"direction": "out"}},
        {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
    ]).to_list(1)
    
    cash_in = cash_in_cursor[0]["total"] if cash_in_cursor else 0
    cash_out = cash_out_cursor[0]["total"] if cash_out_cursor else 0
    cash_balance = cash_in - cash_out
    
    report_data = {
        "period": {"start": yesterday, "end": yesterday},
        "revenue": {
            "product_sales": product_revenue,
            "transport_charges": transport_revenue,
            "total_revenue": product_revenue + transport_revenue
        },
        "collections": {
            "online": online_collection,
            "cash": cash_collection,
            "total": online_collection + cash_collection
        },
        "expenses": {"total": total_expenses},
        "profit_loss": {
            "gross_profit": product_revenue + transport_revenue - total_expenses,
            "profit_margin": round((product_revenue + transport_revenue - total_expenses) / max(product_revenue + transport_revenue, 1) * 100, 2)
        },
        "cash_balance": cash_balance,
        "orders_count": len(completed_orders)
    }
    
    # Get recipients (admin, super_admin, finance users)
    recipients_users = await db.users.find(
        {"role": {"$in": ["super_admin", "admin", "owner", "finance", "accountant"]}},
        {"_id": 0, "email": 1, "phone": 1}
    ).to_list(20)
    
    emails = [u["email"] for u in recipients_users if u.get("email")]
    phones = [u["phone"] for u in recipients_users if u.get("phone")]
    
    # Send Email
    if emails:
        await send_daily_pnl_email(emails, report_data)
        logging.info(f"Email sent to {len(emails)} recipients")
    
    # Send WhatsApp
    if phones:
        await send_whatsapp_pnl_report(phones, report_data)
        logging.info(f"WhatsApp sent to {len(phones)} recipients")
    
    # Log the report send
    await db.report_logs.insert_one({
        "type": "daily_pnl",
        "date": yesterday,
        "sent_at": datetime.now(timezone.utc).isoformat(),
        "email_recipients": emails,
        "whatsapp_recipients": phones,
        "report_summary": {
            "revenue": product_revenue + transport_revenue,
            "expenses": total_expenses,
            "profit": product_revenue + transport_revenue - total_expenses
        }
    })
    
    logging.info(f"Daily report completed for {yesterday}")


# ================== WEEKLY P&L REPORT ==================

async def generate_period_report_data(start_date: str, end_date: str, report_type: str):
    """Generate P&L report data for a date range"""
    db = get_db()
    
    # Fetch orders in the period
    orders = await db.orders.find(
        {"created_at": {"$gte": start_date, "$lte": end_date + "T23:59:59"}},
        {"_id": 0}
    ).to_list(5000)
    
    completed_orders = [o for o in orders if o.get("payment_status") in ["completed", "partial"]]
    
    product_revenue = sum(o.get("total_price", 0) for o in completed_orders)
    transport_revenue = sum(o.get("transport_charge", 0) for o in completed_orders)
    online_collection = sum(o.get("advance_amount", 0) for o in completed_orders if o.get("advance_payment_status") == "paid")
    cash_collection = sum(o.get("remaining_amount", 0) for o in completed_orders if o.get("remaining_payment_status") == "cash_received")
    
    # Get expenses
    expenses = await db.cash_transactions.find(
        {"direction": "out", "date": {"$gte": start_date, "$lte": end_date}},
        {"_id": 0}
    ).to_list(5000)
    total_expenses = sum(e["amount"] for e in expenses)
    
    # Category-wise expenses
    expense_by_category = {}
    for e in expenses:
        cat = e.get("transaction_type", "other")
        expense_by_category[cat] = expense_by_category.get(cat, 0) + e["amount"]
    
    # Get current cash balance
    cash_in_cursor = await db.cash_transactions.aggregate([
        {"$match": {"direction": "in"}},
        {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
    ]).to_list(1)
    cash_out_cursor = await db.cash_transactions.aggregate([
        {"$match": {"direction": "out"}},
        {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
    ]).to_list(1)
    
    cash_in = cash_in_cursor[0]["total"] if cash_in_cursor else 0
    cash_out = cash_out_cursor[0]["total"] if cash_out_cursor else 0
    cash_balance = cash_in - cash_out
    
    return {
        "report_type": report_type,
        "period": {"start": start_date, "end": end_date},
        "revenue": {
            "product_sales": product_revenue,
            "transport_charges": transport_revenue,
            "total_revenue": product_revenue + transport_revenue
        },
        "collections": {
            "online": online_collection,
            "cash": cash_collection,
            "total": online_collection + cash_collection
        },
        "expenses": {
            "total": total_expenses,
            "by_category": expense_by_category
        },
        "profit_loss": {
            "gross_profit": product_revenue + transport_revenue - total_expenses,
            "profit_margin": round((product_revenue + transport_revenue - total_expenses) / max(product_revenue + transport_revenue, 1) * 100, 2)
        },
        "cash_balance": cash_balance,
        "orders_count": len(completed_orders),
        "total_orders": len(orders)
    }


async def send_period_pnl_email(recipients: List[str], report_data: dict):
    """Send weekly/monthly P&L report via email"""
    smtp_host = os.environ.get("SMTP_HOST", "smtp.hostinger.com")
    smtp_port = int(os.environ.get("SMTP_PORT", 465))
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASS", "")
    
    if not smtp_user or not smtp_pass:
        return {"error": "SMTP not configured"}
    
    report_type = report_data.get("report_type", "Weekly")
    start_date = report_data.get("period", {}).get("start", "")
    end_date = report_data.get("period", {}).get("end", "")
    
    emoji = "📅" if report_type == "Weekly" else "📆"
    
    # Format expense breakdown
    expense_breakdown = ""
    for cat, amount in report_data.get("expenses", {}).get("by_category", {}).items():
        expense_breakdown += f"""
                    <div class="row">
                        <span class="label">{cat.replace('_', ' ').title()}</span>
                        <span class="value">₹{amount:,.2f}</span>
                    </div>"""
    
    html_content = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }}
            .container {{ max-width: 600px; margin: 0 auto; background: white; border-radius: 10px; overflow: hidden; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
            .header {{ background: linear-gradient(135deg, {'#7c3aed' if report_type == 'Weekly' else '#dc2626'}, {'#6d28d9' if report_type == 'Weekly' else '#b91c1c'}); color: white; padding: 20px; text-align: center; }}
            .header h1 {{ margin: 0; font-size: 24px; }}
            .content {{ padding: 20px; }}
            .section {{ margin-bottom: 20px; padding: 15px; background: #f8fafc; border-radius: 8px; }}
            .section h3 {{ margin: 0 0 10px 0; color: {'#7c3aed' if report_type == 'Weekly' else '#dc2626'}; font-size: 16px; }}
            .row {{ display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid #e2e8f0; }}
            .row:last-child {{ border-bottom: none; }}
            .label {{ color: #64748b; }}
            .value {{ font-weight: bold; color: #1e293b; }}
            .profit {{ color: #10b981; }}
            .loss {{ color: #ef4444; }}
            .total-row {{ background: {'#7c3aed' if report_type == 'Weekly' else '#dc2626'}; color: white; padding: 15px; border-radius: 8px; margin-top: 15px; }}
            .total-row .value {{ font-size: 24px; }}
            .period-badge {{ background: rgba(255,255,255,0.2); padding: 5px 15px; border-radius: 20px; display: inline-block; margin-top: 10px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>{emoji} {report_type} P&L Report</h1>
                <p style="margin: 5px 0 0 0; opacity: 0.9;">Lucumaa Glass</p>
                <div class="period-badge">{start_date} to {end_date}</div>
            </div>
            <div class="content">
                <div class="section">
                    <h3>💰 Revenue Summary</h3>
                    <div class="row">
                        <span class="label">Product Sales</span>
                        <span class="value">₹{report_data.get('revenue', {}).get('product_sales', 0):,.2f}</span>
                    </div>
                    <div class="row">
                        <span class="label">Transport Charges</span>
                        <span class="value">₹{report_data.get('revenue', {}).get('transport_charges', 0):,.2f}</span>
                    </div>
                    <div class="row">
                        <span class="label"><strong>Total Revenue</strong></span>
                        <span class="value profit">₹{report_data.get('revenue', {}).get('total_revenue', 0):,.2f}</span>
                    </div>
                </div>
                
                <div class="section">
                    <h3>💳 Collections</h3>
                    <div class="row">
                        <span class="label">Online Collection</span>
                        <span class="value">₹{report_data.get('collections', {}).get('online', 0):,.2f}</span>
                    </div>
                    <div class="row">
                        <span class="label">Cash Collection</span>
                        <span class="value">₹{report_data.get('collections', {}).get('cash', 0):,.2f}</span>
                    </div>
                    <div class="row">
                        <span class="label"><strong>Total Collections</strong></span>
                        <span class="value">₹{report_data.get('collections', {}).get('total', 0):,.2f}</span>
                    </div>
                </div>
                
                <div class="section">
                    <h3>📉 Expenses Breakdown</h3>
                    {expense_breakdown if expense_breakdown else '<div class="row"><span class="label">No expenses recorded</span></div>'}
                    <div class="row" style="margin-top: 10px; border-top: 2px solid #e2e8f0; padding-top: 10px;">
                        <span class="label"><strong>Total Expenses</strong></span>
                        <span class="value loss">₹{report_data.get('expenses', {}).get('total', 0):,.2f}</span>
                    </div>
                </div>
                
                <div class="section">
                    <h3>📊 Orders Summary</h3>
                    <div class="row">
                        <span class="label">Total Orders</span>
                        <span class="value">{report_data.get('total_orders', 0)}</span>
                    </div>
                    <div class="row">
                        <span class="label">Completed Orders</span>
                        <span class="value">{report_data.get('orders_count', 0)}</span>
                    </div>
                </div>
                
                <div class="total-row">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <span>{'📈 Gross Profit' if report_data.get('profit_loss', {}).get('gross_profit', 0) >= 0 else '📉 Loss'}</span>
                        <span class="value">₹{abs(report_data.get('profit_loss', {}).get('gross_profit', 0)):,.2f}</span>
                    </div>
                    <div style="text-align: right; margin-top: 5px; font-size: 14px; opacity: 0.9;">
                        Margin: {report_data.get('profit_loss', {}).get('profit_margin', 0)}%
                    </div>
                </div>
                
                <div style="background: #f0fdf4; padding: 15px; border-radius: 8px; margin-top: 15px; border: 1px solid #86efac;">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <span style="color: #166534; font-weight: bold;">🏦 Current Cash-in-Hand</span>
                        <span style="color: #166534; font-size: 20px; font-weight: bold;">₹{report_data.get('cash_balance', 0):,.2f}</span>
                    </div>
                </div>
                
                <p style="text-align: center; color: #64748b; font-size: 12px; margin-top: 20px;">
                    This is an automated {report_type.lower()} report from Lucumaa Glass ERP System
                </p>
            </div>
        </div>
    </body>
    </html>
    """
    
    for recipient in recipients:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"{emoji} {report_type} P&L Report ({start_date} - {end_date}) | Lucumaa Glass"
        msg['From'] = smtp_user
        msg['To'] = recipient
        msg.attach(MIMEText(html_content, 'html'))
        
        try:
            await aiosmtplib.send(
                msg,
                hostname=smtp_host,
                port=smtp_port,
                username=smtp_user,
                password=smtp_pass,
                use_tls=True
            )
        except Exception as e:
            print(f"Failed to send email to {recipient}: {e}")
    
    return {"message": "Emails sent"}


async def send_period_whatsapp_report(phones: List[str], report_data: dict):
    """Send weekly/monthly P&L report via WhatsApp"""
    if not TWILIO_ACCOUNT_SID or not TWILIO_AUTH_TOKEN:
        logging.warning("Twilio not configured - skipping WhatsApp")
        return
    
    try:
        twilio_client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    except Exception as e:
        logging.error(f"Twilio client init failed: {e}")
        return
    
    report_type = report_data.get("report_type", "Weekly")
    start_date = report_data.get("period", {}).get("start", "")
    end_date = report_data.get("period", {}).get("end", "")
    revenue = report_data.get("revenue", {}).get("total_revenue", 0)
    expenses = report_data.get("expenses", {}).get("total", 0)
    profit = report_data.get("profit_loss", {}).get("gross_profit", 0)
    margin = report_data.get("profit_loss", {}).get("profit_margin", 0)
    cash_balance = report_data.get("cash_balance", 0)
    online = report_data.get("collections", {}).get("online", 0)
    cash = report_data.get("collections", {}).get("cash", 0)
    orders = report_data.get("orders_count", 0)
    
    emoji = "📅" if report_type == "Weekly" else "📆"
    profit_emoji = "📈" if profit >= 0 else "📉"
    
    message = f"""{emoji} *{report_type} P&L Report*
_{start_date} to {end_date}_
_Lucumaa Glass ERP_

📦 *Orders:* {orders} completed
💰 *Revenue:* ₹{revenue:,.0f}
💳 *Collections:*
   • Online: ₹{online:,.0f}
   • Cash: ₹{cash:,.0f}
📉 *Expenses:* ₹{expenses:,.0f}

{profit_emoji} *{'Profit' if profit >= 0 else 'Loss'}:* ₹{abs(profit):,.0f} ({margin}%)
🏦 *Cash in Hand:* ₹{cash_balance:,.0f}

_Auto-generated {report_type} report_"""
    
    for phone in phones:
        try:
            formatted_phone = phone if phone.startswith("+") else f"+91{phone}"
            await asyncio.to_thread(
                lambda p=formatted_phone: twilio_client.messages.create(
                    body=message,
                    from_=TWILIO_WHATSAPP,
                    to=f"whatsapp:{p}"
                )
            )
            logging.info(f"WhatsApp sent to {formatted_phone}")
        except Exception as e:
            logging.error(f"WhatsApp failed to {phone}: {e}")


async def scheduled_weekly_report():
    """Auto-triggered weekly report on Monday at 5 AM IST"""
    logging.info("Starting scheduled weekly P&L report...")
    
    db = get_db()
    
    # Check if weekly reports are enabled
    settings = await db.settings.find_one({"type": "daily_report"}, {"_id": 0})
    if settings and not settings.get("weekly_enabled", True):
        logging.info("Weekly reports disabled in settings")
        return
    
    # Calculate last week's date range (Monday to Sunday)
    today = datetime.now(timezone.utc)
    # Go back to last Monday
    last_monday = today - timedelta(days=today.weekday() + 7)
    last_sunday = last_monday + timedelta(days=6)
    
    start_date = last_monday.strftime("%Y-%m-%d")
    end_date = last_sunday.strftime("%Y-%m-%d")
    
    # Generate report data
    report_data = await generate_period_report_data(start_date, end_date, "Weekly")
    
    # Get recipients
    recipients_users = await db.users.find(
        {"role": {"$in": ["super_admin", "admin", "owner", "finance", "accountant"]}},
        {"_id": 0, "email": 1, "phone": 1}
    ).to_list(20)
    
    emails = [u["email"] for u in recipients_users if u.get("email")]
    phones = [u["phone"] for u in recipients_users if u.get("phone")]
    
    # Send reports
    if emails and settings.get("email_enabled", True):
        await send_period_pnl_email(emails, report_data)
        logging.info(f"Weekly email sent to {len(emails)} recipients")
    
    if phones and settings.get("whatsapp_enabled", True):
        await send_period_whatsapp_report(phones, report_data)
        logging.info(f"Weekly WhatsApp sent to {len(phones)} recipients")
    
    # Log the report
    await db.report_logs.insert_one({
        "type": "weekly_pnl",
        "period": {"start": start_date, "end": end_date},
        "sent_at": datetime.now(timezone.utc).isoformat(),
        "email_recipients": emails,
        "whatsapp_recipients": phones,
        "report_summary": {
            "revenue": report_data.get("revenue", {}).get("total_revenue", 0),
            "expenses": report_data.get("expenses", {}).get("total", 0),
            "profit": report_data.get("profit_loss", {}).get("gross_profit", 0),
            "orders": report_data.get("orders_count", 0)
        }
    })
    
    logging.info(f"Weekly report completed for {start_date} to {end_date}")


async def scheduled_monthly_report():
    """Auto-triggered monthly report on 1st of month at 5 AM IST"""
    logging.info("Starting scheduled monthly P&L report...")
    
    db = get_db()
    
    # Check if monthly reports are enabled
    settings = await db.settings.find_one({"type": "daily_report"}, {"_id": 0})
    if settings and not settings.get("monthly_enabled", True):
        logging.info("Monthly reports disabled in settings")
        return
    
    # Calculate last month's date range
    today = datetime.now(timezone.utc)
    first_of_this_month = today.replace(day=1)
    last_day_of_last_month = first_of_this_month - timedelta(days=1)
    first_of_last_month = last_day_of_last_month.replace(day=1)
    
    start_date = first_of_last_month.strftime("%Y-%m-%d")
    end_date = last_day_of_last_month.strftime("%Y-%m-%d")
    
    # Generate report data
    report_data = await generate_period_report_data(start_date, end_date, "Monthly")
    
    # Get recipients
    recipients_users = await db.users.find(
        {"role": {"$in": ["super_admin", "admin", "owner", "finance", "accountant"]}},
        {"_id": 0, "email": 1, "phone": 1}
    ).to_list(20)
    
    emails = [u["email"] for u in recipients_users if u.get("email")]
    phones = [u["phone"] for u in recipients_users if u.get("phone")]
    
    # Send reports
    if emails and settings.get("email_enabled", True):
        await send_period_pnl_email(emails, report_data)
        logging.info(f"Monthly email sent to {len(emails)} recipients")
    
    if phones and settings.get("whatsapp_enabled", True):
        await send_period_whatsapp_report(phones, report_data)
        logging.info(f"Monthly WhatsApp sent to {len(phones)} recipients")
    
    # Log the report
    await db.report_logs.insert_one({
        "type": "monthly_pnl",
        "period": {"start": start_date, "end": end_date},
        "sent_at": datetime.now(timezone.utc).isoformat(),
        "email_recipients": emails,
        "whatsapp_recipients": phones,
        "report_summary": {
            "revenue": report_data.get("revenue", {}).get("total_revenue", 0),
            "expenses": report_data.get("expenses", {}).get("total", 0),
            "profit": report_data.get("profit_loss", {}).get("gross_profit", 0),
            "orders": report_data.get("orders_count", 0)
        }
    })
    
    logging.info(f"Monthly report completed for {start_date} to {end_date}")


def start_report_scheduler():
    """Start the APScheduler for daily/weekly/monthly reports"""
    global scheduler_started
    if scheduler_started:
        return
    
    # Daily Report: 5 AM IST = 23:30 UTC previous day (IST is UTC+5:30)
    scheduler.add_job(
        scheduled_daily_report,
        CronTrigger(hour=23, minute=30),
        id="daily_pnl_report",
        replace_existing=True
    )
    
    # Weekly Report: Monday 5 AM IST = Sunday 23:30 UTC
    scheduler.add_job(
        scheduled_weekly_report,
        CronTrigger(day_of_week='sun', hour=23, minute=30),
        id="weekly_pnl_report",
        replace_existing=True
    )
    
    # Monthly Report: 1st of month 5 AM IST = Last day of prev month 23:30 UTC
    # Using day=1 will trigger on the 1st at the specified UTC time
    scheduler.add_job(
        scheduled_monthly_report,
        CronTrigger(day=1, hour=23, minute=30),
        id="monthly_pnl_report",
        replace_existing=True
    )
    
    scheduler.start()
    scheduler_started = True
    logging.info("Report scheduler started - Daily (5 AM), Weekly (Monday 5 AM), Monthly (1st 5 AM) IST")


# ================== REPORT SETTINGS API ==================

@cash_router.get("/report-settings")
async def get_report_settings(
    current_user: dict = Depends(get_erp_user)
):
    """Get report settings (daily, weekly, monthly)"""
    if current_user.get("role") not in ["super_admin", "admin", "owner"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    settings = await db.settings.find_one({"type": "daily_report"}, {"_id": 0})
    
    if not settings:
        settings = {
            "type": "daily_report",
            "enabled": True,
            "email_enabled": True,
            "whatsapp_enabled": True,
            "report_time": "05:00",
            "timezone": "Asia/Kolkata",
            "weekly_enabled": True,
            "monthly_enabled": True
        }
    else:
        # Ensure new fields have defaults
        settings.setdefault("weekly_enabled", True)
        settings.setdefault("monthly_enabled", True)
    
    return settings


@cash_router.put("/report-settings")
async def update_report_settings(
    data: dict,
    current_user: dict = Depends(get_erp_user)
):
    """Update report settings (daily, weekly, monthly)"""
    if current_user.get("role") not in ["super_admin", "admin", "owner"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    settings = {
        "type": "daily_report",
        "enabled": data.get("enabled", True),
        "email_enabled": data.get("email_enabled", True),
        "whatsapp_enabled": data.get("whatsapp_enabled", True),
        "report_time": data.get("report_time", "05:00"),
        "timezone": data.get("timezone", "Asia/Kolkata"),
        "weekly_enabled": data.get("weekly_enabled", True),
        "monthly_enabled": data.get("monthly_enabled", True),
        "updated_by": current_user.get("name"),
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.settings.update_one(
        {"type": "daily_report"},
        {"$set": settings},
        upsert=True
    )
    
    return {"message": "Report settings updated", "settings": settings}


@cash_router.post("/send-weekly-report")
async def trigger_weekly_report(
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_erp_user)
):
    """Manually trigger weekly P&L report"""
    if current_user.get("role") not in ["super_admin", "admin", "owner", "finance"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    # Calculate last 7 days
    today = datetime.now(timezone.utc)
    start_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    
    # Generate report data
    report_data = await generate_period_report_data(start_date, end_date, "Weekly")
    
    # Get recipients
    recipients_users = await db.users.find(
        {"role": {"$in": ["super_admin", "admin", "owner", "finance", "accountant"]}},
        {"_id": 0, "email": 1, "phone": 1}
    ).to_list(20)
    
    emails = [u["email"] for u in recipients_users if u.get("email")]
    phones = [u["phone"] for u in recipients_users if u.get("phone")]
    
    if emails:
        background_tasks.add_task(send_period_pnl_email, emails, report_data)
    if phones:
        background_tasks.add_task(send_period_whatsapp_report, phones, report_data)
    
    return {
        "message": "Weekly report triggered",
        "period": {"start": start_date, "end": end_date},
        "email_recipients": emails,
        "whatsapp_recipients": phones
    }


@cash_router.post("/send-monthly-report")
async def trigger_monthly_report(
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_erp_user)
):
    """Manually trigger monthly P&L report"""
    if current_user.get("role") not in ["super_admin", "admin", "owner", "finance"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    # Calculate last month
    today = datetime.now(timezone.utc)
    first_of_this_month = today.replace(day=1)
    last_day_of_last_month = first_of_this_month - timedelta(days=1)
    first_of_last_month = last_day_of_last_month.replace(day=1)
    
    start_date = first_of_last_month.strftime("%Y-%m-%d")
    end_date = last_day_of_last_month.strftime("%Y-%m-%d")
    
    # Generate report data
    report_data = await generate_period_report_data(start_date, end_date, "Monthly")
    
    # Get recipients
    recipients_users = await db.users.find(
        {"role": {"$in": ["super_admin", "admin", "owner", "finance", "accountant"]}},
        {"_id": 0, "email": 1, "phone": 1}
    ).to_list(20)
    
    emails = [u["email"] for u in recipients_users if u.get("email")]
    phones = [u["phone"] for u in recipients_users if u.get("phone")]
    
    if emails:
        background_tasks.add_task(send_period_pnl_email, emails, report_data)
    if phones:
        background_tasks.add_task(send_period_whatsapp_report, phones, report_data)
    
    return {
        "message": "Monthly report triggered",
        "period": {"start": start_date, "end": end_date},
        "email_recipients": emails,
        "whatsapp_recipients": phones
    }


@cash_router.get("/report-logs")
async def get_report_logs(
    limit: int = 30,
    current_user: dict = Depends(get_erp_user)
):
    """Get history of sent reports"""
    if current_user.get("role") not in ["super_admin", "admin", "owner", "finance"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    logs = await db.report_logs.find(
        {},
        {"_id": 0}
    ).sort("sent_at", -1).to_list(limit)
    
    return {"logs": logs}


# Auto-start scheduler when module loads
try:
    start_report_scheduler()
except Exception as e:
    logging.error(f"Failed to start scheduler: {e}")
