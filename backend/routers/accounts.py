"""
Accounts Router - Invoicing, Payments, P&L, GST Reports, and Exports
With Auto-posting to Party Ledger & GL
"""
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from fastapi.responses import StreamingResponse
from typing import Optional, Dict, Any
from datetime import datetime, timezone
import uuid
import io
from .base import get_erp_user, get_db
from .notifications import notify_new_invoice, notify_payment_received
from .ledger import auto_post_to_ledger

accounts_router = APIRouter(prefix="/accounts", tags=["Accounts"])


# =============== INVOICE MANAGEMENT ===============

@accounts_router.post("/invoices")
async def create_invoice(
    invoice_data: Dict[str, Any],
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_erp_user)
):
    """Create new sales invoice"""
    db = get_db()
    invoice_number = f"INV{datetime.now().strftime('%Y%m%d')}{uuid.uuid4().hex[:6].upper()}"
    
    items = invoice_data.get("items", [])
    subtotal = sum(item.get("quantity", 0) * item.get("unit_price", 0) for item in items)
    
    # GST calculation
    cgst = subtotal * 0.09
    sgst = subtotal * 0.09
    igst = 0  # For inter-state, use IGST instead
    if invoice_data.get("is_interstate", False):
        igst = subtotal * 0.18
        cgst = 0
        sgst = 0
    
    total_tax = cgst + sgst + igst
    total = subtotal + total_tax
    
    invoice = {
        "id": str(uuid.uuid4()),
        "invoice_number": invoice_number,
        "customer_id": invoice_data.get("customer_id", ""),
        "customer_name": invoice_data.get("customer_name"),
        "customer_gst": invoice_data.get("customer_gst", ""),
        "customer_address": invoice_data.get("customer_address", ""),
        "order_id": invoice_data.get("order_id", ""),
        "items": items,
        "subtotal": round(subtotal, 2),
        "cgst": round(cgst, 2),
        "sgst": round(sgst, 2),
        "igst": round(igst, 2),
        "total_tax": round(total_tax, 2),
        "total": round(total, 2),
        "payment_status": "pending",  # pending, partial, paid
        "amount_paid": 0,
        "due_date": invoice_data.get("due_date", ""),
        "notes": invoice_data.get("notes", ""),
        "created_by": current_user.get("id", "system"),
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.invoices.insert_one(invoice)
    
    # AUTO-POST TO PARTY LEDGER & GL
    # Sales Invoice → Debit Accounts Receivable, Credit Sales + GST
    try:
        result = await auto_post_to_ledger(
            db=db,
            entry_type="sales_invoice",
            reference_id=invoice["id"],
            reference_number=invoice_number,
            party_type="customer",
            party_id=invoice_data.get("customer_id", ""),
            party_name=invoice_data.get("customer_name", ""),
            amount=subtotal,
            gst_amount=total_tax,
            description=f"Sales Invoice {invoice_number}",
            transaction_date=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            created_by=current_user.get("name", "system")
        )
        print(f"[ACCOUNTS] Ledger auto-post result: {result}")
    except Exception as e:
        import traceback
        print(f"[ACCOUNTS] Ledger auto-post failed: {e}")
        print(traceback.format_exc())
    
    # Legacy ledger entry (for backward compatibility)
    ledger_entry = {
        "id": str(uuid.uuid4()),
        "date": datetime.now(timezone.utc).date().isoformat(),
        "type": "invoice",
        "reference": invoice_number,
        "description": f"Sales Invoice - {invoice_data.get('customer_name')}",
        "debit": round(total, 2),  # Accounts Receivable
        "credit": 0,
        "account": "Accounts Receivable",
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    await db.ledger.insert_one(ledger_entry)
    
    # Send invoice notification to customer (background task)
    customer_email = invoice_data.get("customer_email")
    if customer_email:
        background_tasks.add_task(notify_new_invoice, invoice, customer_email)
    
    return {
        "message": "Invoice created",
        "invoice_number": invoice_number,
        "invoice_id": invoice["id"],
        "total": total
    }


@accounts_router.get("/invoices")
async def get_invoices(
    status: Optional[str] = None,
    customer_id: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get all invoices"""
    db = get_db()
    query = {}
    if status:
        query["payment_status"] = status
    if customer_id:
        query["customer_id"] = customer_id
    
    invoices = await db.invoices.find(query, {"_id": 0}).sort("created_at", -1).to_list(500)
    return invoices


@accounts_router.get("/invoices/{invoice_id}")
async def get_invoice(invoice_id: str, current_user: dict = Depends(get_erp_user)):
    """Get single invoice"""
    db = get_db()
    invoice = await db.invoices.find_one({"id": invoice_id}, {"_id": 0})
    if not invoice:
        raise HTTPException(status_code=404, detail="Invoice not found")
    return invoice


@accounts_router.post("/invoices/{invoice_id}/payment")
async def record_payment(
    invoice_id: str,
    payment_data: Dict[str, Any],
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_erp_user)
):
    """Record payment against invoice"""
    db = get_db()
    invoice = await db.invoices.find_one({"id": invoice_id}, {"_id": 0})
    if not invoice:
        raise HTTPException(status_code=404, detail="Invoice not found")
    
    amount = float(payment_data.get("amount", 0))
    new_paid = invoice.get("amount_paid", 0) + amount
    
    # Determine payment status
    if new_paid >= invoice["total"]:
        payment_status = "paid"
    elif new_paid > 0:
        payment_status = "partial"
    else:
        payment_status = "pending"
    
    # Create payment record
    payment = {
        "id": str(uuid.uuid4()),
        "invoice_id": invoice_id,
        "invoice_number": invoice["invoice_number"],
        "amount": amount,
        "payment_method": payment_data.get("payment_method", "cash"),
        "reference": payment_data.get("reference", ""),
        "notes": payment_data.get("notes", ""),
        "received_by": current_user.get("id", "system"),
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    await db.payments.insert_one(payment)
    
    # Update invoice
    await db.invoices.update_one(
        {"id": invoice_id},
        {"$set": {
            "amount_paid": new_paid,
            "payment_status": payment_status,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }}
    )
    
    # AUTO-POST TO PARTY LEDGER & GL
    # Payment Received → Debit Cash/Bank, Credit Accounts Receivable
    try:
        await auto_post_to_ledger(
            db=db,
            entry_type="payment_received",
            reference_id=payment["id"],
            reference_number=f"RCP-{invoice['invoice_number']}",
            party_type="customer",
            party_id=invoice.get("customer_id", ""),
            party_name=invoice.get("customer_name", ""),
            amount=amount,
            gst_amount=0,
            description=f"Payment for Invoice {invoice['invoice_number']}",
            transaction_date=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            created_by=current_user.get("name", "system")
        )
    except Exception as e:
        print(f"Ledger auto-post failed: {e}")
    
    # Legacy ledger entry (for backward compatibility)
    ledger_entry = {
        "id": str(uuid.uuid4()),
        "date": datetime.now(timezone.utc).date().isoformat(),
        "type": "payment",
        "reference": payment["id"],
        "description": f"Payment received - {invoice['invoice_number']}",
        "debit": 0,
        "credit": amount,  # Cash/Bank
        "account": "Cash",
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    await db.ledger.insert_one(ledger_entry)
    
    # Send payment confirmation notification (background task)
    customer_email = payment_data.get("customer_email") or invoice.get("customer_email")
    if customer_email:
        background_tasks.add_task(notify_payment_received, payment, invoice, customer_email)
    
    return {
        "message": "Payment recorded",
        "payment_id": payment["id"],
        "new_balance": invoice["total"] - new_paid,
        "payment_status": payment_status
    }


# =============== LEDGER & REPORTS ===============

@accounts_router.get("/ledger")
async def get_ledger(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    account: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get ledger entries"""
    db = get_db()
    query = {}
    if start_date and end_date:
        query["date"] = {"$gte": start_date, "$lte": end_date}
    if account:
        query["account"] = account
    
    entries = await db.ledger.find(query, {"_id": 0}).sort("date", -1).to_list(1000)
    return entries


@accounts_router.get("/dashboard")
async def get_accounts_dashboard(current_user: dict = Depends(get_erp_user)):
    """Get accounting dashboard metrics"""
    db = get_db()
    today = datetime.now(timezone.utc).date()
    month_start = today.replace(day=1).isoformat()
    
    # Total receivables
    pending_invoices = await db.invoices.find(
        {"payment_status": {"$in": ["pending", "partial"]}},
        {"_id": 0}
    ).to_list(1000)
    total_receivables = sum(inv["total"] - inv.get("amount_paid", 0) for inv in pending_invoices)
    
    # Monthly sales
    monthly_invoices = await db.invoices.find(
        {"created_at": {"$gte": month_start}},
        {"_id": 0}
    ).to_list(1000)
    monthly_sales = sum(inv["total"] for inv in monthly_invoices)
    monthly_gst = sum(inv.get("total_tax", 0) for inv in monthly_invoices)
    
    # Monthly collections
    monthly_payments = await db.payments.find(
        {"created_at": {"$gte": month_start}},
        {"_id": 0}
    ).to_list(1000)
    monthly_collections = sum(p["amount"] for p in monthly_payments)
    
    # Overdue invoices
    overdue_invoices = await db.invoices.find({
        "payment_status": {"$in": ["pending", "partial"]},
        "due_date": {"$lt": today.isoformat(), "$ne": ""}
    }, {"_id": 0}).to_list(100)
    overdue_amount = sum(inv["total"] - inv.get("amount_paid", 0) for inv in overdue_invoices)
    
    return {
        "total_receivables": round(total_receivables, 2),
        "monthly_sales": round(monthly_sales, 2),
        "monthly_gst_collected": round(monthly_gst, 2),
        "monthly_collections": round(monthly_collections, 2),
        "overdue_count": len(overdue_invoices),
        "overdue_amount": round(overdue_amount, 2),
        "pending_invoices": len(pending_invoices)
    }


@accounts_router.get("/gst-report")
async def get_gst_report(
    month: str,  # Format: YYYY-MM
    current_user: dict = Depends(get_erp_user)
):
    """Get GST report for a month"""
    db = get_db()
    # Sales invoices for the month
    invoices = await db.invoices.find(
        {"created_at": {"$regex": f"^{month}"}},
        {"_id": 0}
    ).to_list(1000)
    
    total_taxable = sum(inv["subtotal"] for inv in invoices)
    total_cgst = sum(inv.get("cgst", 0) for inv in invoices)
    total_sgst = sum(inv.get("sgst", 0) for inv in invoices)
    total_igst = sum(inv.get("igst", 0) for inv in invoices)
    
    # Purchase orders for input credit
    pos = await db.purchase_orders.find(
        {"created_at": {"$regex": f"^{month}"}, "status": "received"},
        {"_id": 0}
    ).to_list(1000)
    
    input_gst = sum(po.get("gst", 0) for po in pos)
    
    return {
        "month": month,
        "output_gst": {
            "taxable_amount": round(total_taxable, 2),
            "cgst": round(total_cgst, 2),
            "sgst": round(total_sgst, 2),
            "igst": round(total_igst, 2),
            "total": round(total_cgst + total_sgst + total_igst, 2)
        },
        "input_gst": round(input_gst, 2),
        "net_gst_liability": round((total_cgst + total_sgst + total_igst) - input_gst, 2),
        "invoice_count": len(invoices),
        "purchase_count": len(pos)
    }


# =============== PROFIT & LOSS ===============

@accounts_router.get("/profit-loss")
async def get_profit_loss(
    start_date: str,
    end_date: str,
    current_user: dict = Depends(get_erp_user)
):
    """Get Profit & Loss statement for date range"""
    db = get_db()
    
    # Revenue (Sales Invoices)
    invoices = await db.invoices.find({
        "created_at": {"$gte": start_date, "$lte": end_date + "T23:59:59"}
    }, {"_id": 0}).to_list(5000)
    
    total_revenue = sum(inv.get("subtotal", 0) for inv in invoices)
    total_gst_collected = sum(inv.get("total_tax", 0) for inv in invoices)
    
    # Job Work Revenue (NEW)
    job_work_orders = await db.job_work_orders.find({
        "payment_status": "completed",
        "paid_at": {"$gte": start_date, "$lte": end_date + "T23:59:59"}
    }, {"_id": 0}).to_list(5000)
    
    job_work_revenue = sum(jw.get("summary", {}).get("labour_charges", 0) for jw in job_work_orders)
    job_work_gst = sum(jw.get("summary", {}).get("gst_amount", 0) for jw in job_work_orders)
    job_work_total = sum(jw.get("paid_amount", 0) for jw in job_work_orders)
    
    # Cost of Goods (Purchase Orders received)
    pos = await db.purchase_orders.find({
        "status": "received",
        "received_at": {"$gte": start_date, "$lte": end_date + "T23:59:59"}
    }, {"_id": 0}).to_list(5000)
    
    total_purchases = sum(po.get("subtotal", 0) for po in pos)
    total_gst_paid = sum(po.get("gst", 0) for po in pos)
    
    # Breakage/Wastage Costs
    breakages = await db.breakage_entries.find({
        "created_at": {"$gte": start_date, "$lte": end_date + "T23:59:59"}
    }, {"_id": 0}).to_list(5000)
    
    total_breakage_loss = sum(b.get("total_cost", 0) for b in breakages)
    
    # Salary Expenses
    salaries = await db.salary_payments.find({
        "created_at": {"$gte": start_date, "$lte": end_date + "T23:59:59"},
        "status": {"$in": ["approved", "paid"]}
    }, {"_id": 0}).to_list(5000)
    
    total_salaries = sum(s.get("net_salary", s.get("amount", 0)) for s in salaries)
    
    # Calculations (including Job Work)
    combined_revenue = total_revenue + job_work_revenue
    combined_gst_collected = total_gst_collected + job_work_gst
    gross_profit = combined_revenue - total_purchases
    operating_expenses = total_breakage_loss + total_salaries
    net_profit = gross_profit - operating_expenses
    gst_liability = combined_gst_collected - total_gst_paid
    
    return {
        "period": {"start_date": start_date, "end_date": end_date},
        "revenue": {
            "sales": round(total_revenue, 2),
            "job_work": round(job_work_revenue, 2),
            "total_sales": round(combined_revenue, 2),
            "invoice_count": len(invoices),
            "job_work_count": len(job_work_orders),
            "gst_collected": round(combined_gst_collected, 2)
        },
        "job_work_details": {
            "orders": len(job_work_orders),
            "labour_revenue": round(job_work_revenue, 2),
            "gst_collected": round(job_work_gst, 2),
            "total_collected": round(job_work_total, 2)
        },
        "cost_of_goods": {
            "total_purchases": round(total_purchases, 2),
            "po_count": len(pos),
            "gst_paid": round(total_gst_paid, 2)
        },
        "gross_profit": round(gross_profit, 2),
        "operating_expenses": {
            "breakage_loss": round(total_breakage_loss, 2),
            "salaries": round(total_salaries, 2),
            "total": round(operating_expenses, 2)
        },
        "net_profit": round(net_profit, 2),
        "profit_margin": round((net_profit / combined_revenue * 100) if combined_revenue > 0 else 0, 2),
        "gst_summary": {
            "collected": round(combined_gst_collected, 2),
            "paid": round(total_gst_paid, 2),
            "net_liability": round(gst_liability, 2)
        }
    }
