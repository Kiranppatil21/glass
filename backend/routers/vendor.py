"""
Vendor Management & PO-Based Payment Module
Supports advance, partial, or full payments with audit trail
Uses Razorpay Payouts (RazorpayX) for outgoing vendor payments
With Auto-posting to Party Ledger & GL
"""
from fastapi import APIRouter, Depends, HTTPException, Query, Request, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime, timezone
import uuid
import os
import razorpay
import hmac
import hashlib

from .base import get_db, get_erp_user
from .ledger import auto_post_to_ledger

vendor_router = APIRouter(prefix="/vendors", tags=["Vendors"])

# ================== MOCK MODE CONFIG ==================
# Set to False when RazorpayX credentials are available
MOCK_PAYOUT_MODE = os.environ.get("MOCK_PAYOUT_MODE", "true").lower() == "true"

# Mock payout statuses for simulation
MOCK_PAYOUT_STATUSES = ["queued", "pending", "processing", "processed", "reversed", "cancelled"]

# ================== MODELS ==================

class VendorCreate(BaseModel):
    name: str
    company_name: str
    email: Optional[str] = ""
    phone: str
    gst_number: Optional[str] = ""
    pan_number: Optional[str] = ""
    bank_name: Optional[str] = ""
    bank_account: Optional[str] = ""
    ifsc_code: Optional[str] = ""
    upi_id: Optional[str] = ""
    address: Optional[str] = ""
    category: str = "raw_material"  # raw_material, glass_processing, logistics, job_work, other
    credit_days: int = 30
    credit_limit: float = 100000
    notes: Optional[str] = ""

class VendorUpdate(BaseModel):
    name: Optional[str] = None
    company_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    gst_number: Optional[str] = None
    pan_number: Optional[str] = None
    bank_name: Optional[str] = None
    bank_account: Optional[str] = None
    ifsc_code: Optional[str] = None
    upi_id: Optional[str] = None
    address: Optional[str] = None
    category: Optional[str] = None
    credit_days: Optional[int] = None
    credit_limit: Optional[float] = None
    notes: Optional[str] = None
    is_active: Optional[bool] = None

class POCreate(BaseModel):
    vendor_id: str
    items: List[dict]  # [{name, description, quantity, unit, unit_price, gst_rate}]
    delivery_date: Optional[str] = None
    delivery_address: Optional[str] = ""
    payment_terms: str = "30_days"  # immediate, 15_days, 30_days, 45_days, 60_days
    notes: Optional[str] = ""
    
class POApproval(BaseModel):
    status: str  # approved, rejected
    notes: Optional[str] = ""

class VendorPaymentCreate(BaseModel):
    po_id: str
    payment_type: str  # advance, partial, full
    percentage: Optional[float] = None  # 25, 50, 75, 100 or custom
    amount: float
    payment_mode: str  # upi, net_banking, bank_transfer, razorpay
    notes: Optional[str] = ""

class PaymentVerify(BaseModel):
    razorpay_payment_id: Optional[str] = None
    razorpay_signature: Optional[str] = None
    transaction_ref: Optional[str] = None

class BulkPaymentCreate(BaseModel):
    vendor_id: str
    po_ids: List[str]  # List of PO IDs to pay
    payment_mode: str  # upi, net_banking, bank_transfer, razorpay, cash
    transaction_ref: Optional[str] = None
    notes: Optional[str] = ""


# ================== VENDOR CRUD ==================

@vendor_router.post("/")
async def create_vendor(
    data: VendorCreate,
    current_user: dict = Depends(get_erp_user)
):
    """Create a new vendor"""
    if current_user.get("role") not in ["super_admin", "admin", "finance", "accountant"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    # Check if vendor with same phone/email exists
    existing = await db.vendors.find_one({
        "$or": [
            {"phone": data.phone},
            {"email": data.email} if data.email else {"phone": data.phone}
        ]
    })
    if existing:
        raise HTTPException(status_code=400, detail="Vendor with this phone/email already exists")
    
    # Generate vendor code
    count = await db.vendors.count_documents({})
    vendor_code = f"VND-{str(count + 1).zfill(4)}"
    
    vendor = {
        "id": str(uuid.uuid4()),
        "vendor_code": vendor_code,
        **data.model_dump(),
        "opening_balance": 0,
        "current_balance": 0,  # Positive = we owe them, Negative = they owe us
        "total_po_value": 0,
        "total_paid": 0,
        "is_active": True,
        "created_by": current_user.get("name"),
        "created_by_id": current_user.get("id"),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.vendors.insert_one(vendor)
    del vendor["_id"]
    
    # Log audit
    await log_audit(db, "vendor_created", vendor["id"], current_user, f"Vendor {vendor_code} created")
    
    return {"message": "Vendor created successfully", "vendor": vendor}


@vendor_router.get("/")
async def get_vendors(
    category: str = None,
    is_active: bool = True,
    search: str = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get all vendors"""
    db = get_db()
    
    query = {}
    if category:
        query["category"] = category
    if is_active is not None:
        query["is_active"] = is_active
    if search:
        query["$or"] = [
            {"name": {"$regex": search, "$options": "i"}},
            {"company_name": {"$regex": search, "$options": "i"}},
            {"vendor_code": {"$regex": search, "$options": "i"}}
        ]
    
    cursor = db.vendors.find(query, {"_id": 0}).sort("created_at", -1)
    vendors = await cursor.to_list(length=500)
    
    return {"vendors": vendors, "count": len(vendors)}


@vendor_router.get("/{vendor_id}")
async def get_vendor(
    vendor_id: str,
    current_user: dict = Depends(get_erp_user)
):
    """Get vendor details with ledger summary"""
    db = get_db()
    
    vendor = await db.vendors.find_one({"id": vendor_id}, {"_id": 0})
    if not vendor:
        raise HTTPException(status_code=404, detail="Vendor not found")
    
    # Get PO count and payment summary
    po_count = await db.purchase_orders.count_documents({"vendor_id": vendor_id})
    
    pipeline = [
        {"$match": {"vendor_id": vendor_id}},
        {"$group": {
            "_id": None,
            "total_po_value": {"$sum": "$grand_total"},
            "total_paid": {"$sum": "$amount_paid"}
        }}
    ]
    summary = await db.purchase_orders.aggregate(pipeline).to_list(length=1)
    
    vendor["po_count"] = po_count
    vendor["summary"] = summary[0] if summary else {"total_po_value": 0, "total_paid": 0}
    
    return vendor


@vendor_router.put("/{vendor_id}")
async def update_vendor(
    vendor_id: str,
    data: VendorUpdate,
    current_user: dict = Depends(get_erp_user)
):
    """Update vendor"""
    if current_user.get("role") not in ["super_admin", "admin", "finance", "accountant"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    update_data = {k: v for k, v in data.model_dump().items() if v is not None}
    update_data["updated_at"] = datetime.now(timezone.utc).isoformat()
    
    result = await db.vendors.update_one(
        {"id": vendor_id},
        {"$set": update_data}
    )
    
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="Vendor not found")
    
    await log_audit(db, "vendor_updated", vendor_id, current_user, "Vendor updated")
    
    return {"message": "Vendor updated successfully"}


# ================== PURCHASE ORDER (PO) ==================

@vendor_router.post("/po")
async def create_purchase_order(
    data: POCreate,
    current_user: dict = Depends(get_erp_user)
):
    """Create a Purchase Order"""
    if current_user.get("role") not in ["super_admin", "admin", "finance", "accountant", "purchase"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    # Verify vendor exists
    vendor = await db.vendors.find_one({"id": data.vendor_id}, {"_id": 0})
    if not vendor:
        raise HTTPException(status_code=404, detail="Vendor not found")
    
    # Calculate totals
    subtotal = 0
    items_with_total = []
    
    for item in data.items:
        item_total = item.get("quantity", 0) * item.get("unit_price", 0)
        gst_amount = item_total * item.get("gst_rate", 18) / 100
        items_with_total.append({
            **item,
            "total": item_total,
            "gst_amount": gst_amount
        })
        subtotal += item_total
    
    # Calculate GST (assuming same rate for all items for simplicity)
    avg_gst_rate = sum(item.get("gst_rate", 18) for item in data.items) / len(data.items) if data.items else 18
    gst_amount = subtotal * avg_gst_rate / 100
    grand_total = subtotal + gst_amount
    
    # Generate PO number
    today = datetime.now()
    count = await db.purchase_orders.count_documents({
        "created_at": {"$regex": today.strftime("%Y-%m")}
    })
    po_number = f"PO-{today.strftime('%Y%m%d')}-{str(count + 1).zfill(4)}"
    
    po = {
        "id": str(uuid.uuid4()),
        "po_number": po_number,
        "vendor_id": data.vendor_id,
        "vendor_name": vendor.get("name"),
        "vendor_code": vendor.get("vendor_code"),
        "items": items_with_total,
        "subtotal": round(subtotal, 2),
        "gst_rate": avg_gst_rate,
        "gst_amount": round(gst_amount, 2),
        "grand_total": round(grand_total, 2),
        "delivery_date": data.delivery_date,
        "delivery_address": data.delivery_address,
        "payment_terms": data.payment_terms,
        "notes": data.notes,
        "status": "draft",  # draft, pending_approval, approved, rejected, completed, cancelled
        "payment_status": "unpaid",  # unpaid, partially_paid, fully_paid
        "amount_paid": 0,
        "outstanding_balance": round(grand_total, 2),
        "payment_history": [],
        "created_by": current_user.get("name"),
        "created_by_id": current_user.get("id"),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.purchase_orders.insert_one(po)
    del po["_id"]
    
    await log_audit(db, "po_created", po["id"], current_user, f"PO {po_number} created for {vendor.get('name')}")
    
    return {"message": "Purchase Order created", "po": po}


@vendor_router.get("/po/list")
async def get_purchase_orders(
    vendor_id: str = None,
    status: str = None,
    payment_status: str = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get all POs"""
    db = get_db()
    
    query = {}
    if vendor_id:
        query["vendor_id"] = vendor_id
    if status:
        query["status"] = status
    if payment_status:
        query["payment_status"] = payment_status
    
    cursor = db.purchase_orders.find(query, {"_id": 0}).sort("created_at", -1)
    pos = await cursor.to_list(length=500)
    
    return {"purchase_orders": pos, "count": len(pos)}


@vendor_router.get("/po/{po_id}")
async def get_purchase_order(
    po_id: str,
    current_user: dict = Depends(get_erp_user)
):
    """Get PO details"""
    db = get_db()
    
    po = await db.purchase_orders.find_one({"id": po_id}, {"_id": 0})
    if not po:
        raise HTTPException(status_code=404, detail="PO not found")
    
    # Get vendor details
    vendor = await db.vendors.find_one({"id": po.get("vendor_id")}, {"_id": 0})
    po["vendor"] = vendor
    
    return po


@vendor_router.post("/po/{po_id}/submit")
async def submit_po_for_approval(
    po_id: str,
    current_user: dict = Depends(get_erp_user)
):
    """Submit PO for approval"""
    db = get_db()
    
    po = await db.purchase_orders.find_one({"id": po_id})
    if not po:
        raise HTTPException(status_code=404, detail="PO not found")
    
    if po.get("status") != "draft":
        raise HTTPException(status_code=400, detail="Only draft POs can be submitted")
    
    await db.purchase_orders.update_one(
        {"id": po_id},
        {
            "$set": {
                "status": "pending_approval",
                "submitted_at": datetime.now(timezone.utc).isoformat(),
                "submitted_by": current_user.get("name")
            }
        }
    )
    
    await log_audit(db, "po_submitted", po_id, current_user, f"PO {po.get('po_number')} submitted for approval")
    
    return {"message": "PO submitted for approval"}


@vendor_router.post("/po/{po_id}/approve")
async def approve_po(
    po_id: str,
    data: POApproval,
    current_user: dict = Depends(get_erp_user)
):
    """Approve or reject PO"""
    if current_user.get("role") not in ["super_admin", "admin", "finance"]:
        raise HTTPException(status_code=403, detail="Only admins/finance can approve POs")
    
    db = get_db()
    
    po = await db.purchase_orders.find_one({"id": po_id})
    if not po:
        raise HTTPException(status_code=404, detail="PO not found")
    
    if po.get("status") != "pending_approval":
        raise HTTPException(status_code=400, detail="PO is not pending approval")
    
    new_status = "approved" if data.status == "approved" else "rejected"
    
    await db.purchase_orders.update_one(
        {"id": po_id},
        {
            "$set": {
                "status": new_status,
                "approved_at": datetime.now(timezone.utc).isoformat() if new_status == "approved" else None,
                "approved_by": current_user.get("name") if new_status == "approved" else None,
                "rejected_at": datetime.now(timezone.utc).isoformat() if new_status == "rejected" else None,
                "rejected_by": current_user.get("name") if new_status == "rejected" else None,
                "approval_notes": data.notes
            }
        }
    )
    
    # Update vendor ledger if approved
    if new_status == "approved":
        await db.vendors.update_one(
            {"id": po.get("vendor_id")},
            {
                "$inc": {
                    "total_po_value": po.get("grand_total"),
                    "current_balance": po.get("grand_total")
                }
            }
        )
        
        # AUTO-POST TO PARTY LEDGER & GL
        # Purchase Bill → Debit Purchases + GST Input, Credit Accounts Payable
        try:
            await auto_post_to_ledger(
                db=db,
                entry_type="purchase_bill",
                reference_id=po["id"],
                reference_number=po.get("po_number"),
                party_type="vendor",
                party_id=po.get("vendor_id"),
                party_name=po.get("vendor_name"),
                amount=po.get("subtotal", 0),
                gst_amount=po.get("gst_amount", 0),
                description=f"Purchase Order {po.get('po_number')} Approved",
                transaction_date=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                created_by=current_user.get("name", "system")
            )
        except Exception as e:
            print(f"Ledger auto-post failed: {e}")
    
    await log_audit(db, f"po_{new_status}", po_id, current_user, f"PO {po.get('po_number')} {new_status}")
    
    return {"message": f"PO {new_status}"}


# ================== VENDOR PAYMENTS (RAZORPAY PAYOUTS) ==================

async def create_mock_payout(db, payment_id: str, amount: float, vendor: dict, po: dict):
    """
    Simulate a payout in MOCK mode.
    In real mode, Razorpay webhook handles status updates.
    In mock mode, we simulate instant processing with a fake UTR.
    """
    import random
    import string
    
    # Generate mock payout ID and UTR
    mock_payout_id = f"pout_mock_{uuid.uuid4().hex[:12]}"
    mock_utr = f"UTR{''.join(random.choices(string.digits, k=12))}"
    
    # Simulate processing delay (in real world, this happens via webhook)
    # For mock, we'll mark it as processed immediately
    
    await db.vendor_payments.update_one(
        {"id": payment_id},
        {
            "$set": {
                "razorpay_payout_id": mock_payout_id,
                "payout_status": "processed",
                "mock_mode": True
            }
        }
    )
    
    return {
        "payout_id": mock_payout_id,
        "status": "processed",
        "utr": mock_utr,
        "mock": True
    }


@vendor_router.post("/po/{po_id}/initiate-payment")
async def initiate_vendor_payment(
    po_id: str,
    data: VendorPaymentCreate,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_erp_user)
):
    """
    Initiate OUTGOING payment to vendor.
    
    For Razorpay mode: Uses RazorpayX Payouts API to transfer money to vendor's bank.
    For Manual modes: Records payment with UTR tracking.
    
    MOCK_PAYOUT_MODE=true: Simulates payout without actual transfer.
    """
    if current_user.get("role") not in ["super_admin", "admin", "finance", "accountant"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    po = await db.purchase_orders.find_one({"id": po_id})
    if not po:
        raise HTTPException(status_code=404, detail="PO not found")
    
    if po.get("status") != "approved":
        raise HTTPException(status_code=400, detail="Payment allowed only for approved POs")
    
    if po.get("payment_status") == "fully_paid":
        raise HTTPException(status_code=400, detail="PO already fully paid")
    
    outstanding = po.get("outstanding_balance", po.get("grand_total"))
    
    # Validate amount
    if data.amount > outstanding:
        raise HTTPException(status_code=400, detail=f"Amount exceeds outstanding balance (₹{outstanding})")
    
    if data.amount <= 0:
        raise HTTPException(status_code=400, detail="Amount must be greater than 0")
    
    # Determine payment type
    if data.percentage:
        payment_type = f"{int(data.percentage)}% Payment"
    elif data.amount == outstanding:
        payment_type = "Full Payment"
    elif po.get("amount_paid") == 0:
        payment_type = "Advance Payment"
    else:
        payment_type = "Partial Payment"
    
    # Get vendor details
    vendor = await db.vendors.find_one({"id": po.get("vendor_id")}, {"_id": 0})
    if not vendor:
        raise HTTPException(status_code=404, detail="Vendor not found")
    
    # For online payout modes, require bank details
    if data.payment_mode in ["razorpay", "razorpay_payout"]:
        if not vendor.get("bank_account") or not vendor.get("ifsc_code"):
            raise HTTPException(
                status_code=400, 
                detail="Vendor bank details not configured. Please update vendor with Bank Account Number and IFSC Code."
            )
    
    # Create payment record
    payment_id = str(uuid.uuid4())
    reference_id = f"VP-{po.get('po_number')}-{uuid.uuid4().hex[:6]}"
    
    payment_record = {
        "id": payment_id,
        "reference_id": reference_id,
        "po_id": po_id,
        "po_number": po.get("po_number"),
        "vendor_id": po.get("vendor_id"),
        "vendor_name": po.get("vendor_name"),
        "vendor_bank": {
            "bank_name": vendor.get("bank_name"),
            "account_number": vendor.get("bank_account", "")[-4:] if vendor.get("bank_account") else "",  # Last 4 digits only
            "ifsc": vendor.get("ifsc_code"),
            "upi_id": vendor.get("upi_id")
        },
        "payment_type": payment_type,
        "percentage": data.percentage,
        "amount": data.amount,
        "payment_mode": data.payment_mode,
        "status": "initiated",
        "payout_status": None,
        "razorpay_payout_id": None,
        "razorpay_fund_account_id": None,
        "utr": None,
        "transaction_ref": None,
        "mock_mode": MOCK_PAYOUT_MODE,
        "notes": data.notes,
        "initiated_by": current_user.get("name"),
        "initiated_by_id": current_user.get("id"),
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    payout_result = None
    
    # Handle Razorpay Payout (outgoing transfer to vendor)
    if data.payment_mode in ["razorpay", "razorpay_payout"]:
        if MOCK_PAYOUT_MODE:
            # MOCK MODE: Simulate payout
            payment_record["status"] = "processing"
            payment_record["payout_status"] = "queued"
            await db.vendor_payments.insert_one(payment_record)
            
            # Simulate async payout processing
            payout_result = await create_mock_payout(db, payment_id, data.amount, vendor, po)
            
            await log_audit(db, "vendor_payout_initiated_mock", payment_id, current_user,
                           f"[MOCK] Payout ₹{data.amount} initiated for PO {po.get('po_number')}")
            
            return {
                "message": "Payout initiated (MOCK MODE)",
                "payment_id": payment_id,
                "reference_id": reference_id,
                "payout_id": payout_result.get("payout_id"),
                "payout_status": "processing",
                "amount": data.amount,
                "payment_type": payment_type,
                "po_number": po.get("po_number"),
                "vendor_name": po.get("vendor_name"),
                "vendor_bank": f"XXXX{vendor.get('bank_account', '')[-4:]}" if vendor.get('bank_account') else "N/A",
                "mock_mode": True,
                "next_step": "Poll /payment/{payment_id}/status or wait for webhook"
            }
        else:
            # LIVE MODE: Call RazorpayX Payouts API
            razorpay_key = os.environ.get("RAZORPAY_KEY_ID")
            razorpay_secret = os.environ.get("RAZORPAY_KEY_SECRET")
            razorpay_account = os.environ.get("RAZORPAYX_ACCOUNT_NUMBER")
            
            if not all([razorpay_key, razorpay_secret, razorpay_account]):
                raise HTTPException(
                    status_code=500, 
                    detail="RazorpayX credentials not configured. Please set RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET, and RAZORPAYX_ACCOUNT_NUMBER."
                )
            
            try:
                client = razorpay.Client(auth=(razorpay_key, razorpay_secret))
                
                # Step 1: Create or get contact
                contact_id = vendor.get("razorpay_contact_id")
                if not contact_id:
                    contact = client.contact.create({
                        "name": vendor.get("name"),
                        "email": vendor.get("email") or "",
                        "contact": vendor.get("phone") or "",
                        "type": "vendor",
                        "reference_id": vendor.get("id"),
                        "notes": {
                            "vendor_code": vendor.get("vendor_code"),
                            "gst": vendor.get("gst_number", "")
                        }
                    })
                    contact_id = contact.get("id")
                    await db.vendors.update_one(
                        {"id": vendor.get("id")},
                        {"$set": {"razorpay_contact_id": contact_id}}
                    )
                
                # Step 2: Create or get fund account
                fund_account_id = vendor.get("razorpay_fund_account_id")
                if not fund_account_id:
                    fund_account = client.fund_account.create({
                        "contact_id": contact_id,
                        "account_type": "bank_account",
                        "bank_account": {
                            "name": vendor.get("company_name") or vendor.get("name"),
                            "ifsc": vendor.get("ifsc_code"),
                            "account_number": vendor.get("bank_account")
                        }
                    })
                    fund_account_id = fund_account.get("id")
                    await db.vendors.update_one(
                        {"id": vendor.get("id")},
                        {"$set": {"razorpay_fund_account_id": fund_account_id}}
                    )
                
                # Step 3: Create payout
                payout = client.payout.create({
                    "account_number": razorpay_account,
                    "fund_account_id": fund_account_id,
                    "amount": int(data.amount * 100),  # Paise
                    "currency": "INR",
                    "mode": "NEFT",  # or RTGS, IMPS
                    "purpose": "vendor_bill",
                    "queue_if_low_balance": True,
                    "reference_id": reference_id,
                    "narration": f"Payment for PO {po.get('po_number')}",
                    "notes": {
                        "payment_id": payment_id,
                        "po_id": po_id,
                        "po_number": po.get("po_number"),
                        "vendor_id": po.get("vendor_id")
                    }
                })
                
                payment_record["razorpay_payout_id"] = payout.get("id")
                payment_record["razorpay_fund_account_id"] = fund_account_id
                payment_record["status"] = "processing"
                payment_record["payout_status"] = payout.get("status")
                
                await db.vendor_payments.insert_one(payment_record)
                
                await log_audit(db, "vendor_payout_initiated", payment_id, current_user,
                               f"Payout ₹{data.amount} initiated for PO {po.get('po_number')}, Payout ID: {payout.get('id')}")
                
                return {
                    "message": "Payout initiated successfully",
                    "payment_id": payment_id,
                    "reference_id": reference_id,
                    "payout_id": payout.get("id"),
                    "payout_status": payout.get("status"),
                    "amount": data.amount,
                    "payment_type": payment_type,
                    "po_number": po.get("po_number"),
                    "vendor_name": po.get("vendor_name"),
                    "vendor_bank": f"XXXX{vendor.get('bank_account', '')[-4:]}",
                    "mock_mode": False,
                    "next_step": "UTR will be updated automatically via webhook when payout is processed"
                }
                
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Payout failed: {str(e)}")
    
    else:
        # MANUAL PAYMENT (UPI, Bank Transfer, Cash, etc.)
        # User will provide UTR manually
        payment_record["status"] = "pending_verification"
        payment_record["payout_status"] = "manual"
        
        await db.vendor_payments.insert_one(payment_record)
        
        await log_audit(db, "vendor_payment_initiated", payment_id, current_user,
                       f"Manual payment ₹{data.amount} initiated for PO {po.get('po_number')}")
        
        return {
            "message": "Payment record created. Please verify with UTR/Transaction Reference.",
            "payment_id": payment_id,
            "reference_id": reference_id,
            "amount": data.amount,
            "payment_type": payment_type,
            "payment_mode": data.payment_mode,
            "po_number": po.get("po_number"),
            "vendor_name": po.get("vendor_name"),
            "mock_mode": False,
            "requires_verification": True,
            "next_step": "Call /payment/{payment_id}/record-manual with UTR to complete"
        }


@vendor_router.post("/payment/{payment_id}/verify")
async def verify_vendor_payment(
    payment_id: str,
    data: PaymentVerify = None,
    razorpay_payment_id: str = None,
    razorpay_signature: str = None,
    transaction_ref: str = None,
    current_user: dict = Depends(get_erp_user)
):
    """Verify and complete vendor payment"""
    
    # Support both body and query params
    if data:
        razorpay_payment_id = data.razorpay_payment_id or razorpay_payment_id
        razorpay_signature = data.razorpay_signature or razorpay_signature
        transaction_ref = data.transaction_ref or transaction_ref
    
    db = get_db()
    
    payment = await db.vendor_payments.find_one({"id": payment_id})
    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found")
    
    if payment.get("status") == "completed":
        raise HTTPException(status_code=400, detail="Payment already completed")
    
    # Verify Razorpay signature if provided
    if razorpay_payment_id and razorpay_signature:
        razorpay_secret = os.environ.get("RAZORPAY_KEY_SECRET")
        message = f"{payment.get('razorpay_order_id')}|{razorpay_payment_id}"
        expected_signature = hmac.new(
            razorpay_secret.encode(),
            message.encode(),
            hashlib.sha256
        ).hexdigest()
        
        if expected_signature != razorpay_signature:
            await db.vendor_payments.update_one(
                {"id": payment_id},
                {"$set": {"status": "failed", "failed_reason": "Signature mismatch"}}
            )
            raise HTTPException(status_code=400, detail="Payment verification failed")
    
    # Generate receipt number
    today = datetime.now()
    receipt_count = await db.vendor_payments.count_documents({
        "status": "completed",
        "completed_at": {"$regex": today.strftime("%Y-%m")}
    })
    receipt_number = f"VPR-{today.strftime('%Y%m%d')}-{str(receipt_count + 1).zfill(4)}"
    
    # Update payment record
    await db.vendor_payments.update_one(
        {"id": payment_id},
        {
            "$set": {
                "status": "completed",
                "receipt_number": receipt_number,
                "razorpay_payment_id": razorpay_payment_id,
                "transaction_ref": transaction_ref or razorpay_payment_id,
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "verified_by": current_user.get("name")
            }
        }
    )
    
    # Update PO
    po_id = payment.get("po_id")
    po = await db.purchase_orders.find_one({"id": po_id})
    
    new_amount_paid = po.get("amount_paid", 0) + payment.get("amount")
    new_outstanding = po.get("grand_total") - new_amount_paid
    
    new_payment_status = "fully_paid" if new_outstanding <= 0 else "partially_paid"
    
    payment_history_entry = {
        "payment_id": payment_id,
        "receipt_number": receipt_number,
        "amount": payment.get("amount"),
        "payment_type": payment.get("payment_type"),
        "payment_mode": payment.get("payment_mode"),
        "transaction_ref": transaction_ref or razorpay_payment_id,
        "date": datetime.now(timezone.utc).isoformat(),
        "by": current_user.get("name")
    }
    
    await db.purchase_orders.update_one(
        {"id": po_id},
        {
            "$set": {
                "amount_paid": new_amount_paid,
                "outstanding_balance": max(0, new_outstanding),
                "payment_status": new_payment_status,
                "updated_at": datetime.now(timezone.utc).isoformat()
            },
            "$push": {"payment_history": payment_history_entry}
        }
    )
    
    # Update vendor ledger
    await db.vendors.update_one(
        {"id": payment.get("vendor_id")},
        {
            "$inc": {
                "total_paid": payment.get("amount"),
                "current_balance": -payment.get("amount")
            }
        }
    )
    
    # Add to vendor ledger entries (legacy)
    ledger_entry = {
        "id": str(uuid.uuid4()),
        "vendor_id": payment.get("vendor_id"),
        "type": "payment",
        "reference": receipt_number,
        "po_number": payment.get("po_number"),
        "description": f"Payment for PO {payment.get('po_number')} - {payment.get('payment_type')}",
        "debit": payment.get("amount"),
        "credit": 0,
        "balance_after": None,  # Will be calculated
        "date": datetime.now(timezone.utc).isoformat(),
        "created_by": current_user.get("name")
    }
    await db.vendor_ledger.insert_one(ledger_entry)
    
    # AUTO-POST TO PARTY LEDGER & GL
    # Payment Made → Debit Accounts Payable, Credit Cash/Bank
    try:
        await auto_post_to_ledger(
            db=db,
            entry_type="payment_made",
            reference_id=payment.get("id"),
            reference_number=receipt_number,
            party_type="vendor",
            party_id=payment.get("vendor_id"),
            party_name=payment.get("vendor_name"),
            amount=payment.get("amount"),
            gst_amount=0,
            description=f"Payment for PO {payment.get('po_number')} - {payment.get('payment_type')}",
            transaction_date=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            created_by=current_user.get("name", "system")
        )
    except Exception as e:
        print(f"Ledger auto-post failed: {e}")
    
    await log_audit(db, "vendor_payment_completed", payment_id, current_user,
                   f"Payment ₹{payment.get('amount')} completed for PO {payment.get('po_number')}, Receipt: {receipt_number}")
    
    return {
        "message": "Payment verified successfully",
        "receipt_number": receipt_number,
        "amount_paid": payment.get("amount"),
        "total_paid": new_amount_paid,
        "outstanding_balance": max(0, new_outstanding),
        "payment_status": new_payment_status
    }


@vendor_router.post("/payment/{payment_id}/record-manual")
async def record_manual_payment(
    payment_id: str,
    transaction_ref: str,
    payment_date: str = None,
    current_user: dict = Depends(get_erp_user)
):
    """Record manual payment (bank transfer, UPI, etc.) with UTR"""
    if current_user.get("role") not in ["super_admin", "admin", "finance", "accountant"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    # Create PaymentVerify object for manual payments
    verify_data = PaymentVerify(transaction_ref=transaction_ref)
    return await verify_vendor_payment(payment_id, verify_data, None, None, None, current_user)


@vendor_router.get("/payment/{payment_id}/status")
async def get_payment_status(
    payment_id: str,
    current_user: dict = Depends(get_erp_user)
):
    """
    Get current payout status.
    For MOCK mode: Also simulates status progression.
    For LIVE mode: Fetches real status from RazorpayX.
    """
    db = get_db()
    
    payment = await db.vendor_payments.find_one({"id": payment_id}, {"_id": 0})
    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found")
    
    # For mock mode, simulate completion if still processing
    if payment.get("mock_mode") and payment.get("status") == "processing":
        import random
        import string
        
        # Auto-complete mock payout
        mock_utr = f"UTR{''.join(random.choices(string.digits, k=12))}"
        today = datetime.now()
        receipt_count = await db.vendor_payments.count_documents({
            "status": "completed",
            "completed_at": {"$regex": today.strftime("%Y-%m")}
        })
        receipt_number = f"VPR-{today.strftime('%Y%m%d')}-{str(receipt_count + 1).zfill(4)}"
        
        await db.vendor_payments.update_one(
            {"id": payment_id},
            {
                "$set": {
                    "status": "completed",
                    "payout_status": "processed",
                    "utr": mock_utr,
                    "transaction_ref": mock_utr,
                    "receipt_number": receipt_number,
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                    "auto_verified": True
                }
            }
        )
        
        # Update PO
        po = await db.purchase_orders.find_one({"id": payment.get("po_id")})
        if po:
            new_amount_paid = po.get("amount_paid", 0) + payment.get("amount")
            new_outstanding = po.get("grand_total") - new_amount_paid
            new_payment_status = "fully_paid" if new_outstanding <= 0 else "partially_paid"
            
            await db.purchase_orders.update_one(
                {"id": payment.get("po_id")},
                {
                    "$set": {
                        "amount_paid": new_amount_paid,
                        "outstanding_balance": max(0, new_outstanding),
                        "payment_status": new_payment_status,
                        "updated_at": datetime.now(timezone.utc).isoformat()
                    },
                    "$push": {"payment_history": {
                        "payment_id": payment_id,
                        "receipt_number": receipt_number,
                        "amount": payment.get("amount"),
                        "utr": mock_utr,
                        "payment_mode": "razorpay_payout_mock",
                        "date": datetime.now(timezone.utc).isoformat(),
                        "auto_verified": True
                    }}
                }
            )
            
            # Update vendor balance
            await db.vendors.update_one(
                {"id": payment.get("vendor_id")},
                {
                    "$inc": {
                        "total_paid": payment.get("amount"),
                        "current_balance": -payment.get("amount")
                    }
                }
            )
        
        return {
            "payment_id": payment_id,
            "status": "completed",
            "payout_status": "processed",
            "utr": mock_utr,
            "receipt_number": receipt_number,
            "amount": payment.get("amount"),
            "mock_mode": True,
            "message": "Mock payout completed successfully"
        }
    
    # For live mode or already completed
    return {
        "payment_id": payment_id,
        "status": payment.get("status"),
        "payout_status": payment.get("payout_status"),
        "utr": payment.get("utr"),
        "transaction_ref": payment.get("transaction_ref"),
        "receipt_number": payment.get("receipt_number"),
        "amount": payment.get("amount"),
        "mock_mode": payment.get("mock_mode", False),
        "created_at": payment.get("created_at"),
        "completed_at": payment.get("completed_at")
    }


@vendor_router.get("/payments/history")
async def get_vendor_payments_history(
    vendor_id: str = None,
    status: str = None,
    limit: int = 50,
    current_user: dict = Depends(get_erp_user)
):
    """Get payment history with payout status"""
    db = get_db()
    
    query = {}
    if vendor_id:
        query["vendor_id"] = vendor_id
    if status:
        query["status"] = status
    
    payments = await db.vendor_payments.find(query, {"_id": 0}).sort("created_at", -1).limit(limit).to_list(limit)
    
    return {
        "payments": payments,
        "count": len(payments)
    }


@vendor_router.post("/webhook/razorpay-payout")
async def razorpay_payout_webhook(request_data: dict):
    """
    Webhook endpoint for Razorpay Payout status updates.
    Auto-captures UTR when payout is processed.
    
    Configure this URL in RazorpayX dashboard:
    https://yourdomain.com/api/erp/vendors/webhook/razorpay-payout
    """
    
    db = get_db()
    
    # event = request_data.get("event")  # Available for logging if needed
    payload = request_data.get("payload", {}).get("payout", {}).get("entity", {})
    
    if not payload:
        return {"status": "ignored", "reason": "No payout data"}
    
    payout_id = payload.get("id")
    status = payload.get("status")
    utr = payload.get("utr")
    reference_id = payload.get("reference_id")
    
    # Find the payment record by payout_id
    payment = await db.vendor_payments.find_one({"razorpay_payout_id": payout_id})
    
    if not payment:
        # Try finding by reference_id
        if reference_id:
            payment = await db.vendor_payments.find_one({"id": reference_id})
    
    if not payment:
        return {"status": "ignored", "reason": "Payment not found"}
    
    # Update based on payout status
    if status == "processed":
        # Payout successful - UTR received
        today = datetime.now()
        receipt_count = await db.vendor_payments.count_documents({
            "status": "completed",
            "completed_at": {"$regex": today.strftime("%Y-%m")}
        })
        receipt_number = f"VPR-{today.strftime('%Y%m%d')}-{str(receipt_count + 1).zfill(4)}"
        
        await db.vendor_payments.update_one(
            {"id": payment.get("id")},
            {
                "$set": {
                    "status": "completed",
                    "receipt_number": receipt_number,
                    "transaction_ref": utr,
                    "utr": utr,
                    "payout_status": status,
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                    "auto_verified": True
                }
            }
        )
        
        # Update PO
        po = await db.purchase_orders.find_one({"id": payment.get("po_id")})
        if po:
            new_amount_paid = po.get("amount_paid", 0) + payment.get("amount")
            new_outstanding = po.get("grand_total") - new_amount_paid
            new_payment_status = "fully_paid" if new_outstanding <= 0 else "partially_paid"
            
            await db.purchase_orders.update_one(
                {"id": payment.get("po_id")},
                {
                    "$set": {
                        "amount_paid": new_amount_paid,
                        "outstanding_balance": max(0, new_outstanding),
                        "payment_status": new_payment_status,
                        "updated_at": datetime.now(timezone.utc).isoformat()
                    },
                    "$push": {"payment_history": {
                        "payment_id": payment.get("id"),
                        "receipt_number": receipt_number,
                        "amount": payment.get("amount"),
                        "utr": utr,
                        "payment_mode": "razorpay_payout",
                        "date": datetime.now(timezone.utc).isoformat(),
                        "auto_verified": True
                    }}
                }
            )
            
            # Update vendor balance
            await db.vendors.update_one(
                {"id": payment.get("vendor_id")},
                {
                    "$inc": {
                        "total_paid": payment.get("amount"),
                        "current_balance": -payment.get("amount")
                    }
                }
            )
        
        return {
            "status": "success",
            "message": "Payment completed",
            "payment_id": payment.get("id"),
            "utr": utr,
            "receipt_number": receipt_number
        }
    
    elif status == "reversed":
        # Payout reversed
        await db.vendor_payments.update_one(
            {"id": payment.get("id")},
            {
                "$set": {
                    "status": "reversed",
                    "payout_status": status,
                    "reversed_at": datetime.now(timezone.utc).isoformat()
                }
            }
        )
        return {"status": "reversed", "payment_id": payment.get("id")}
    
    elif status in ["failed", "cancelled", "rejected"]:
        # Payout failed
        failure_reason = payload.get("failure_reason", "Unknown")
        await db.vendor_payments.update_one(
            {"id": payment.get("id")},
            {
                "$set": {
                    "status": "failed",
                    "payout_status": status,
                    "failed_reason": failure_reason,
                    "failed_at": datetime.now(timezone.utc).isoformat()
                }
            }
        )
        return {"status": "failed", "payment_id": payment.get("id"), "reason": failure_reason}
    
    else:
        # Update status for other events (queued, pending, processing)
        await db.vendor_payments.update_one(
            {"id": payment.get("id")},
            {"$set": {"payout_status": status}}
        )
        return {"status": "updated", "payout_status": status}


@vendor_router.get("/payment/{payment_id}/check-status")
async def check_payout_status(
    payment_id: str,
    current_user: dict = Depends(get_erp_user)
):
    """Check payout status from Razorpay and update if needed"""
    db = get_db()
    
    payment = await db.vendor_payments.find_one({"id": payment_id}, {"_id": 0})
    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found")
    
    payout_id = payment.get("razorpay_payout_id")
    if not payout_id:
        return {"message": "Not a Razorpay payout", "status": payment.get("status")}
    
    # Check status from Razorpay
    razorpay_key = os.environ.get("RAZORPAY_KEY_ID")
    razorpay_secret = os.environ.get("RAZORPAY_KEY_SECRET")
    
    if razorpay_key and razorpay_secret:
        try:
            client = razorpay.Client(auth=(razorpay_key, razorpay_secret))
            payout = client.payout.fetch(payout_id)
            
            status = payout.get("status")
            utr = payout.get("utr")
            
            # If processed, auto-complete
            if status == "processed" and payment.get("status") != "completed":
                # Trigger webhook logic
                await razorpay_payout_webhook({
                    "event": "payout.processed",
                    "payload": {"payout": {"entity": payout}}
                })
                
                return {
                    "message": "Payment completed automatically",
                    "status": "completed",
                    "utr": utr
                }
            
            return {
                "payout_status": status,
                "utr": utr,
                "payment_status": payment.get("status")
            }
        except Exception as e:
            return {"error": str(e), "payment_status": payment.get("status")}
    
    return {"message": "Razorpay not configured", "status": payment.get("status")}


@vendor_router.post("/bulk-payment")
async def create_bulk_payment(
    data: BulkPaymentCreate,
    current_user: dict = Depends(get_erp_user)
):
    """Create bulk payment for multiple POs of a vendor"""
    if current_user.get("role") not in ["super_admin", "admin", "finance", "accountant"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    # Verify vendor exists
    vendor = await db.vendors.find_one({"id": data.vendor_id}, {"_id": 0})
    if not vendor:
        raise HTTPException(status_code=404, detail="Vendor not found")
    
    # Validate all POs belong to this vendor and are approved
    valid_pos = []
    total_amount = 0
    
    for po_id in data.po_ids:
        po = await db.purchase_orders.find_one({"id": po_id, "vendor_id": data.vendor_id}, {"_id": 0})
        if not po:
            raise HTTPException(status_code=404, detail=f"PO {po_id} not found or doesn't belong to this vendor")
        if po.get("status") not in ["approved", "completed"]:
            raise HTTPException(status_code=400, detail=f"PO {po.get('po_number')} is not approved")
        if po.get("payment_status") == "fully_paid":
            raise HTTPException(status_code=400, detail=f"PO {po.get('po_number')} is already fully paid")
        
        outstanding = po.get("outstanding_balance", po.get("grand_total", 0))
        valid_pos.append({"po": po, "outstanding": outstanding})
        total_amount += outstanding
    
    if total_amount <= 0:
        raise HTTPException(status_code=400, detail="No outstanding amount to pay")
    
    # Generate bulk payment ID
    bulk_payment_id = str(uuid.uuid4())
    
    # Create Razorpay order if payment mode is razorpay
    razorpay_order_id = None
    if data.payment_mode == "razorpay":
        try:
            razorpay_key = os.environ.get("RAZORPAY_KEY_ID")
            razorpay_secret = os.environ.get("RAZORPAY_KEY_SECRET")
            client = razorpay.Client(auth=(razorpay_key, razorpay_secret))
            
            razorpay_order = client.order.create({
                "amount": int(total_amount * 100),
                "currency": "INR",
                "receipt": f"BULK-{bulk_payment_id[:8]}",
                "notes": {
                    "vendor_id": data.vendor_id,
                    "bulk_payment_id": bulk_payment_id,
                    "po_count": len(data.po_ids)
                }
            })
            razorpay_order_id = razorpay_order["id"]
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Razorpay error: {str(e)}")
    
    # Create individual payment records for each PO
    payment_records = []
    for item in valid_pos:
        po = item["po"]
        outstanding = item["outstanding"]
        
        payment_record = {
            "id": str(uuid.uuid4()),
            "bulk_payment_id": bulk_payment_id,
            "vendor_id": data.vendor_id,
            "vendor_name": vendor.get("name"),
            "po_id": po.get("id"),
            "po_number": po.get("po_number"),
            "payment_type": "full",
            "amount": outstanding,
            "payment_mode": data.payment_mode,
            "razorpay_order_id": razorpay_order_id,
            "transaction_ref": data.transaction_ref,
            "notes": data.notes or f"Bulk payment - {len(data.po_ids)} POs",
            "status": "pending",
            "initiated_by": current_user.get("name"),
            "initiated_at": datetime.now(timezone.utc).isoformat(),
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        payment_records.append(payment_record)
    
    # Insert all payment records
    if payment_records:
        await db.vendor_payments.insert_many(payment_records)
    
    # Create bulk payment summary record
    bulk_record = {
        "id": bulk_payment_id,
        "vendor_id": data.vendor_id,
        "vendor_name": vendor.get("name"),
        "vendor_code": vendor.get("vendor_code"),
        "po_ids": data.po_ids,
        "po_count": len(data.po_ids),
        "total_amount": total_amount,
        "payment_mode": data.payment_mode,
        "razorpay_order_id": razorpay_order_id,
        "transaction_ref": data.transaction_ref,
        "status": "pending",
        "payment_ids": [p["id"] for p in payment_records],
        "initiated_by": current_user.get("name"),
        "initiated_at": datetime.now(timezone.utc).isoformat(),
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    await db.bulk_payments.insert_one(bulk_record)
    
    await log_audit(db, "bulk_payment_initiated", bulk_payment_id, current_user,
                   f"Bulk payment initiated for {len(data.po_ids)} POs, Total: ₹{total_amount}")
    
    return {
        "message": "Bulk payment initiated",
        "bulk_payment_id": bulk_payment_id,
        "payment_ids": [p["id"] for p in payment_records],
        "razorpay_order_id": razorpay_order_id,
        "total_amount": total_amount,
        "po_count": len(data.po_ids),
        "vendor_name": vendor.get("name")
    }


@vendor_router.post("/bulk-payment/{bulk_payment_id}/complete")
async def complete_bulk_payment(
    bulk_payment_id: str,
    transaction_ref: str = None,
    razorpay_payment_id: str = None,
    razorpay_signature: str = None,
    current_user: dict = Depends(get_erp_user)
):
    """Complete bulk payment - verify and update all POs"""
    if current_user.get("role") not in ["super_admin", "admin", "finance", "accountant"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    bulk_payment = await db.bulk_payments.find_one({"id": bulk_payment_id})
    if not bulk_payment:
        raise HTTPException(status_code=404, detail="Bulk payment not found")
    
    if bulk_payment.get("status") == "completed":
        raise HTTPException(status_code=400, detail="Bulk payment already completed")
    
    # Verify Razorpay signature if provided
    if razorpay_payment_id and razorpay_signature:
        import hmac
        import hashlib
        razorpay_secret = os.environ.get("RAZORPAY_KEY_SECRET")
        message = f"{bulk_payment.get('razorpay_order_id')}|{razorpay_payment_id}"
        expected_signature = hmac.new(
            razorpay_secret.encode(),
            message.encode(),
            hashlib.sha256
        ).hexdigest()
        
        if expected_signature != razorpay_signature:
            await db.bulk_payments.update_one(
                {"id": bulk_payment_id},
                {"$set": {"status": "failed", "failed_reason": "Signature mismatch"}}
            )
            raise HTTPException(status_code=400, detail="Payment verification failed")
    
    # Generate bulk receipt number
    today = datetime.now()
    receipt_count = await db.bulk_payments.count_documents({
        "status": "completed",
        "completed_at": {"$regex": today.strftime("%Y-%m")}
    })
    bulk_receipt_number = f"BULK-{today.strftime('%Y%m%d')}-{str(receipt_count + 1).zfill(4)}"
    
    # Update each payment record and PO
    results = []
    for payment_id in bulk_payment.get("payment_ids", []):
        payment = await db.vendor_payments.find_one({"id": payment_id})
        if not payment:
            continue
        
        # Generate individual receipt
        receipt_count_ind = await db.vendor_payments.count_documents({
            "status": "completed",
            "completed_at": {"$regex": today.strftime("%Y-%m")}
        })
        receipt_number = f"VPR-{today.strftime('%Y%m%d')}-{str(receipt_count_ind + 1).zfill(4)}"
        
        # Update payment
        await db.vendor_payments.update_one(
            {"id": payment_id},
            {
                "$set": {
                    "status": "completed",
                    "receipt_number": receipt_number,
                    "bulk_receipt_number": bulk_receipt_number,
                    "razorpay_payment_id": razorpay_payment_id,
                    "transaction_ref": transaction_ref or razorpay_payment_id,
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                    "verified_by": current_user.get("name")
                }
            }
        )
        
        # Update PO
        po_id = payment.get("po_id")
        po = await db.purchase_orders.find_one({"id": po_id})
        if po:
            new_amount_paid = po.get("amount_paid", 0) + payment.get("amount")
            new_outstanding = po.get("grand_total") - new_amount_paid
            new_payment_status = "fully_paid" if new_outstanding <= 0 else "partially_paid"
            
            await db.purchase_orders.update_one(
                {"id": po_id},
                {
                    "$set": {
                        "amount_paid": new_amount_paid,
                        "outstanding_balance": max(0, new_outstanding),
                        "payment_status": new_payment_status,
                        "updated_at": datetime.now(timezone.utc).isoformat()
                    },
                    "$push": {"payment_history": {
                        "payment_id": payment_id,
                        "receipt_number": receipt_number,
                        "amount": payment.get("amount"),
                        "payment_type": "bulk",
                        "payment_mode": payment.get("payment_mode"),
                        "transaction_ref": transaction_ref or razorpay_payment_id,
                        "date": datetime.now(timezone.utc).isoformat(),
                        "by": current_user.get("name")
                    }}
                }
            )
        
        results.append({
            "po_number": payment.get("po_number"),
            "receipt_number": receipt_number,
            "amount": payment.get("amount")
        })
    
    # Update vendor balance
    vendor_id = bulk_payment.get("vendor_id")
    total_amount = bulk_payment.get("total_amount", 0)
    await db.vendors.update_one(
        {"id": vendor_id},
        {
            "$inc": {
                "total_paid": total_amount,
                "current_balance": -total_amount
            }
        }
    )
    
    # Update bulk payment record
    await db.bulk_payments.update_one(
        {"id": bulk_payment_id},
        {
            "$set": {
                "status": "completed",
                "bulk_receipt_number": bulk_receipt_number,
                "razorpay_payment_id": razorpay_payment_id,
                "transaction_ref": transaction_ref or razorpay_payment_id,
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "verified_by": current_user.get("name")
            }
        }
    )
    
    await log_audit(db, "bulk_payment_completed", bulk_payment_id, current_user,
                   f"Bulk payment completed: {len(results)} POs, Total: ₹{total_amount}, Receipt: {bulk_receipt_number}")
    
    return {
        "message": "Bulk payment completed successfully",
        "bulk_receipt_number": bulk_receipt_number,
        "total_amount": total_amount,
        "po_count": len(results),
        "payments": results
    }


# ================== VENDOR LEDGER & REPORTS ==================

@vendor_router.get("/{vendor_id}/ledger")
async def get_vendor_ledger(
    vendor_id: str,
    start_date: str = None,
    end_date: str = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get vendor ledger with all transactions"""
    db = get_db()
    
    vendor = await db.vendors.find_one({"id": vendor_id}, {"_id": 0})
    if not vendor:
        raise HTTPException(status_code=404, detail="Vendor not found")
    
    # Get all POs for this vendor
    po_query = {"vendor_id": vendor_id, "status": {"$in": ["approved", "completed"]}}
    if start_date:
        po_query["created_at"] = {"$gte": start_date}
    if end_date:
        if "created_at" in po_query:
            po_query["created_at"]["$lte"] = end_date
        else:
            po_query["created_at"] = {"$lte": end_date}
    
    pos = await db.purchase_orders.find(po_query, {"_id": 0}).sort("created_at", 1).to_list(500)
    
    # Get all payments
    payment_query = {"vendor_id": vendor_id, "status": "completed"}
    payments = await db.vendor_payments.find(payment_query, {"_id": 0}).sort("completed_at", 1).to_list(500)
    
    # Build ledger
    ledger_entries = []
    running_balance = vendor.get("opening_balance", 0)
    
    # Add PO entries
    for po in pos:
        running_balance += po.get("grand_total", 0)
        ledger_entries.append({
            "date": po.get("created_at"),
            "type": "PO",
            "reference": po.get("po_number"),
            "description": f"Purchase Order - {len(po.get('items', []))} items",
            "debit": 0,
            "credit": po.get("grand_total"),
            "balance": running_balance
        })
    
    # Add payment entries
    for payment in payments:
        running_balance -= payment.get("amount", 0)
        ledger_entries.append({
            "date": payment.get("completed_at"),
            "type": "Payment",
            "reference": payment.get("receipt_number"),
            "description": f"{payment.get('payment_type')} - {payment.get('payment_mode')}",
            "debit": payment.get("amount"),
            "credit": 0,
            "balance": running_balance
        })
    
    # Sort by date
    ledger_entries.sort(key=lambda x: x.get("date", ""))
    
    # Recalculate running balance
    balance = vendor.get("opening_balance", 0)
    for entry in ledger_entries:
        balance = balance + entry.get("credit", 0) - entry.get("debit", 0)
        entry["balance"] = round(balance, 2)
    
    return {
        "vendor": vendor,
        "opening_balance": vendor.get("opening_balance", 0),
        "closing_balance": round(balance, 2),
        "total_po_value": sum(po.get("grand_total", 0) for po in pos),
        "total_payments": sum(p.get("amount", 0) for p in payments),
        "entries": ledger_entries
    }


@vendor_router.get("/{vendor_id}/balance-sheet")
async def get_vendor_balance_sheet(
    vendor_id: str,
    financial_year: str = None,  # Format: 2025-26
    current_user: dict = Depends(get_erp_user)
):
    """Get detailed vendor balance sheet with monthly breakdown"""
    db = get_db()
    
    vendor = await db.vendors.find_one({"id": vendor_id}, {"_id": 0})
    if not vendor:
        raise HTTPException(status_code=404, detail="Vendor not found")
    
    # Determine date range for financial year (April to March)
    from datetime import datetime
    today = datetime.now()
    
    if financial_year:
        try:
            start_year = int(financial_year.split("-")[0])
            start_date = f"{start_year}-04-01"
            end_date = f"{start_year + 1}-03-31"
        except ValueError:
            start_date = f"{today.year}-04-01" if today.month >= 4 else f"{today.year - 1}-04-01"
            end_date = f"{today.year + 1}-03-31" if today.month >= 4 else f"{today.year}-03-31"
    else:
        # Current financial year
        start_date = f"{today.year}-04-01" if today.month >= 4 else f"{today.year - 1}-04-01"
        end_date = f"{today.year + 1}-03-31" if today.month >= 4 else f"{today.year}-03-31"
    
    # Get all POs in date range
    pos = await db.purchase_orders.find({
        "vendor_id": vendor_id,
        "status": {"$in": ["approved", "completed"]},
        "created_at": {"$gte": start_date, "$lte": end_date}
    }, {"_id": 0}).to_list(1000)
    
    # Get all payments in date range
    payments = await db.vendor_payments.find({
        "vendor_id": vendor_id,
        "status": "completed",
        "completed_at": {"$gte": start_date, "$lte": end_date}
    }, {"_id": 0}).to_list(1000)
    
    # Calculate opening balance (balance before start_date)
    prev_pos = await db.purchase_orders.find({
        "vendor_id": vendor_id,
        "status": {"$in": ["approved", "completed"]},
        "created_at": {"$lt": start_date}
    }, {"_id": 0}).to_list(1000)
    
    prev_payments = await db.vendor_payments.find({
        "vendor_id": vendor_id,
        "status": "completed",
        "completed_at": {"$lt": start_date}
    }, {"_id": 0}).to_list(1000)
    
    base_opening = vendor.get("opening_balance", 0)
    prev_po_total = sum(p.get("grand_total", 0) for p in prev_pos)
    prev_payment_total = sum(p.get("amount", 0) for p in prev_payments)
    opening_balance = base_opening + prev_po_total - prev_payment_total
    
    # Monthly breakdown
    monthly_data = {}
    months = ["04", "05", "06", "07", "08", "09", "10", "11", "12", "01", "02", "03"]
    
    for month in months:
        year = int(start_date[:4]) if int(month) >= 4 else int(start_date[:4]) + 1
        month_key = f"{year}-{month}"
        monthly_data[month_key] = {
            "month": month_key,
            "po_count": 0,
            "po_value": 0,
            "payment_count": 0,
            "payment_value": 0,
            "net": 0
        }
    
    # Fill PO data
    for po in pos:
        created = po.get("created_at", "")[:7]  # YYYY-MM
        if created in monthly_data:
            monthly_data[created]["po_count"] += 1
            monthly_data[created]["po_value"] += po.get("grand_total", 0)
    
    # Fill payment data
    for payment in payments:
        completed = payment.get("completed_at", "")[:7]
        if completed in monthly_data:
            monthly_data[completed]["payment_count"] += 1
            monthly_data[completed]["payment_value"] += payment.get("amount", 0)
    
    # Calculate net and running balance
    running_balance = opening_balance
    monthly_summary = []
    for month_key in sorted(monthly_data.keys()):
        data = monthly_data[month_key]
        data["net"] = data["po_value"] - data["payment_value"]
        running_balance += data["net"]
        data["closing_balance"] = round(running_balance, 2)
        monthly_summary.append(data)
    
    # Calculate totals
    total_po_value = sum(p.get("grand_total", 0) for p in pos)
    total_payments = sum(p.get("amount", 0) for p in payments)
    closing_balance = opening_balance + total_po_value - total_payments
    
    # Get top POs
    top_pos = sorted(pos, key=lambda x: x.get("grand_total", 0), reverse=True)[:5]
    
    return {
        "vendor": {
            "id": vendor.get("id"),
            "name": vendor.get("name"),
            "company_name": vendor.get("company_name"),
            "vendor_code": vendor.get("vendor_code"),
            "gst_number": vendor.get("gst_number"),
            "category": vendor.get("category")
        },
        "financial_year": financial_year or f"{start_date[:4]}-{int(start_date[:4])+1}",
        "period": {"start": start_date, "end": end_date},
        "summary": {
            "opening_balance": round(opening_balance, 2),
            "total_po_value": round(total_po_value, 2),
            "total_payments": round(total_payments, 2),
            "closing_balance": round(closing_balance, 2),
            "po_count": len(pos),
            "payment_count": len(payments)
        },
        "monthly_breakdown": monthly_summary,
        "top_purchases": [{
            "po_number": p.get("po_number"),
            "date": p.get("created_at", "")[:10],
            "amount": p.get("grand_total"),
            "status": p.get("payment_status")
        } for p in top_pos]
    }


@vendor_router.put("/{vendor_id}/opening-balance")
async def update_vendor_opening_balance(
    vendor_id: str,
    opening_balance: float,
    notes: str = "",
    current_user: dict = Depends(get_erp_user)
):
    """Update vendor opening balance"""
    if current_user.get("role") not in ["super_admin", "admin", "finance", "accountant"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    vendor = await db.vendors.find_one({"id": vendor_id})
    if not vendor:
        raise HTTPException(status_code=404, detail="Vendor not found")
    
    old_balance = vendor.get("opening_balance", 0)
    
    await db.vendors.update_one(
        {"id": vendor_id},
        {
            "$set": {
                "opening_balance": opening_balance,
                "opening_balance_updated_at": datetime.now(timezone.utc).isoformat(),
                "opening_balance_updated_by": current_user.get("name")
            }
        }
    )
    
    await log_audit(db, "vendor_opening_balance_updated", vendor_id, current_user,
                   f"Opening balance changed from ₹{old_balance} to ₹{opening_balance}. Notes: {notes}")
    
    return {
        "message": "Opening balance updated",
        "vendor_id": vendor_id,
        "old_balance": old_balance,
        "new_balance": opening_balance
    }


@vendor_router.get("/reports/outstanding")
async def get_outstanding_payables(
    current_user: dict = Depends(get_erp_user)
):
    """Get outstanding vendor payables report"""
    if current_user.get("role") not in ["super_admin", "admin", "finance", "accountant"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    # Get all vendors with outstanding balance
    pipeline = [
        {"$match": {"is_active": True}},
        {"$lookup": {
            "from": "purchase_orders",
            "localField": "id",
            "foreignField": "vendor_id",
            "as": "pos"
        }},
        {"$project": {
            "_id": 0,
            "id": 1,
            "vendor_code": 1,
            "name": 1,
            "company_name": 1,
            "phone": 1,
            "current_balance": 1,
            "total_po_value": 1,
            "total_paid": 1,
            "outstanding": {"$subtract": ["$total_po_value", "$total_paid"]},
            "po_count": {"$size": "$pos"}
        }},
        {"$match": {"outstanding": {"$gt": 0}}},
        {"$sort": {"outstanding": -1}}
    ]
    
    results = await db.vendors.aggregate(pipeline).to_list(500)
    
    total_outstanding = sum(r.get("outstanding", 0) for r in results)
    
    return {
        "vendors": results,
        "total_outstanding": round(total_outstanding, 2),
        "vendor_count": len(results)
    }


@vendor_router.get("/reports/payments")
async def get_payment_report(
    start_date: str = None,
    end_date: str = None,
    vendor_id: str = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get vendor payment report"""
    if current_user.get("role") not in ["super_admin", "admin", "finance", "accountant"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    query = {"status": "completed"}
    if vendor_id:
        query["vendor_id"] = vendor_id
    if start_date:
        query["completed_at"] = {"$gte": start_date}
    if end_date:
        if "completed_at" in query:
            query["completed_at"]["$lte"] = end_date
        else:
            query["completed_at"] = {"$lte": end_date}
    
    payments = await db.vendor_payments.find(query, {"_id": 0}).sort("completed_at", -1).to_list(1000)
    
    # Group by payment mode
    by_mode = {}
    for p in payments:
        mode = p.get("payment_mode", "other")
        if mode not in by_mode:
            by_mode[mode] = {"count": 0, "amount": 0}
        by_mode[mode]["count"] += 1
        by_mode[mode]["amount"] += p.get("amount", 0)
    
    return {
        "payments": payments,
        "total_amount": sum(p.get("amount", 0) for p in payments),
        "payment_count": len(payments),
        "by_mode": by_mode
    }


# ================== AUDIT HELPER ==================

async def log_audit(db, action: str, entity_id: str, user: dict, description: str):
    """Log audit trail entry"""
    entry = {
        "id": str(uuid.uuid4()),
        "action": action,
        "entity_id": entity_id,
        "description": description,
        "user_name": user.get("name"),
        "user_id": user.get("id"),
        "user_role": user.get("role"),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    await db.audit_logs.insert_one(entry)


@vendor_router.get("/audit/logs")
async def get_audit_logs(
    entity_id: str = None,
    action: str = None,
    limit: int = 100,
    current_user: dict = Depends(get_erp_user)
):
    """Get audit logs"""
    if current_user.get("role") not in ["super_admin", "admin"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    query = {}
    if entity_id:
        query["entity_id"] = entity_id
    if action:
        query["action"] = {"$regex": action, "$options": "i"}
    
    logs = await db.audit_logs.find(query, {"_id": 0}).sort("timestamp", -1).limit(limit).to_list(limit)
    
    return {"logs": logs, "count": len(logs)}
