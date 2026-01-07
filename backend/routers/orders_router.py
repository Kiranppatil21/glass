"""
Orders Router - Order creation, payment, dispatch management
With Auto-posting to Party Ledger & GL
"""
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, UploadFile, File
from pydantic import BaseModel, EmailStr
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone, timedelta
import os
import uuid
import razorpay
import hmac
import hashlib

orders_router = APIRouter(prefix="/orders", tags=["Orders"])

# Database reference
_db = None

def init_orders_router(database):
    global _db
    _db = database

def get_db():
    return _db

# Import auth dependency
from .auth_router import get_current_user

# Import ledger auto-post function
async def auto_post_to_ledger_orders(db, **kwargs):
    """Safe import and call ledger auto-post"""
    try:
        from .ledger import auto_post_to_ledger
        await auto_post_to_ledger(db, **kwargs)
    except Exception as e:
        print(f"Ledger auto-post failed: {e}")

# Razorpay client
RAZORPAY_KEY_ID = os.environ.get('RAZORPAY_KEY_ID', '')
RAZORPAY_KEY_SECRET = os.environ.get('RAZORPAY_KEY_SECRET', '')
razorpay_client = None
if RAZORPAY_KEY_ID and RAZORPAY_KEY_SECRET:
    razorpay_client = razorpay.Client(auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET))


# =============== MODELS ===============

class GlassItem(BaseModel):
    product_id: str
    product_name: str
    thickness: float
    width: float
    height: float
    quantity: int
    unit_price: float
    total_price: float
    edging: Optional[str] = None
    tempering: bool = False
    lamination: bool = False
    notes: Optional[str] = None


class DeliveryAddress(BaseModel):
    full_name: str
    phone: str
    address_line1: str
    address_line2: Optional[str] = None
    city: str
    state: str
    pincode: str
    landmark: Optional[str] = None


class OrderCreate(BaseModel):
    # Customer identification - can use profile_id OR manual entry
    customer_profile_id: Optional[str] = None  # If provided, auto-populate from Customer Master
    customer_name: Optional[str] = None  # Required if profile_id not provided
    customer_email: Optional[EmailStr] = None
    customer_phone: Optional[str] = None
    glass_items: List[GlassItem]
    delivery_address: Optional[DeliveryAddress] = None
    shipping_address_id: Optional[str] = None  # Use specific shipping address from profile
    delivery_type: str = "standard"
    notes: Optional[str] = None
    advance_percent: Optional[int] = None
    is_credit_customer: bool = False
    gst_number: Optional[str] = None
    company_name: Optional[str] = None
    # B2B fields from Customer Master
    place_of_supply: Optional[str] = None
    billing_address: Optional[Dict[str, Any]] = None


class RemainingPaymentVerify(BaseModel):
    razorpay_order_id: str
    razorpay_payment_id: str
    razorpay_signature: str


# =============== HELPERS ===============

async def generate_order_number() -> str:
    db = get_db()
    today = datetime.now(timezone.utc)
    prefix = f"LG{today.strftime('%y%m%d')}"
    
    count = await db.orders.count_documents({
        "order_number": {"$regex": f"^{prefix}"}
    })
    
    return f"{prefix}{str(count + 1).zfill(4)}"


async def get_advance_settings():
    db = get_db()
    settings = await db.settings.find_one({"type": "advance_settings"}, {"_id": 0})
    if not settings:
        settings = {
            "normal_customer_min": 50,
            "normal_customer_options": [50, 75, 100],
            "credit_customer_min": 0,
            "credit_customer_options": [0, 25, 50, 75, 100],
            "admin_override_allowed": True,
            "max_credit_limit": 100000
        }
    return settings


async def validate_advance_percent(total_amount: float, requested_percent: int, is_credit: bool, user_role: str):
    settings = await get_advance_settings()
    
    if is_credit:
        min_percent = settings.get("credit_customer_min", 0)
        allowed_options = settings.get("credit_customer_options", [0, 25, 50, 75, 100])
    else:
        min_percent = settings.get("normal_customer_min", 50)
        allowed_options = settings.get("normal_customer_options", [50, 75, 100])
    
    # Admin can override
    if user_role in ["admin", "super_admin", "owner"] and settings.get("admin_override_allowed", True):
        if requested_percent < 0 or requested_percent > 100:
            return False, f"Invalid advance percentage: {requested_percent}%"
        return True, None
    
    if requested_percent < min_percent:
        return False, f"Minimum advance required is {min_percent}%"
    
    if requested_percent not in allowed_options:
        return False, f"Advance must be one of: {allowed_options}%"
    
    return True, None


# =============== ENDPOINTS ===============

@orders_router.post("")
async def create_order(
    order_data: OrderCreate,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_user)
):
    """Create a new order with auto-population from Customer Master"""
    db = get_db()
    
    # Variables for customer data
    customer_profile = None
    customer_name = order_data.customer_name
    customer_email = order_data.customer_email
    customer_phone = order_data.customer_phone
    company_name = order_data.company_name
    gst_number = order_data.gst_number
    billing_address = order_data.billing_address
    delivery_address = order_data.delivery_address
    is_credit_customer = order_data.is_credit_customer
    credit_limit = 0
    credit_days = 0
    place_of_supply = order_data.place_of_supply
    invoice_type = "B2C"
    
    # Auto-populate from Customer Master if profile_id provided
    if order_data.customer_profile_id:
        customer_profile = await db.customer_profiles.find_one(
            {"id": order_data.customer_profile_id, "status": "active"},
            {"_id": 0}
        )
        if not customer_profile:
            raise HTTPException(status_code=404, detail="Customer profile not found or inactive")
        
        # Auto-populate from profile
        customer_name = customer_profile.get("display_name") or order_data.customer_name
        customer_email = customer_profile.get("email") or order_data.customer_email
        customer_phone = customer_profile.get("mobile") or order_data.customer_phone
        company_name = customer_profile.get("company_name") or order_data.company_name
        gst_number = customer_profile.get("gstin") or order_data.gst_number
        billing_address = customer_profile.get("billing_address") or order_data.billing_address
        is_credit_customer = customer_profile.get("credit_type") == "credit_allowed"
        credit_limit = customer_profile.get("credit_limit", 0)
        credit_days = customer_profile.get("credit_days", 0)
        place_of_supply = customer_profile.get("place_of_supply") or order_data.place_of_supply
        invoice_type = customer_profile.get("invoice_type", "B2C")
        
        # If shipping_address_id provided, use that specific address
        if order_data.shipping_address_id and not order_data.delivery_address:
            shipping_addresses = customer_profile.get("shipping_addresses", [])
            for addr in shipping_addresses:
                if addr.get("id") == order_data.shipping_address_id:
                    delivery_address = DeliveryAddress(
                        full_name=addr.get("contact_person") or customer_name,
                        phone=addr.get("contact_phone") or customer_phone,
                        address_line1=addr.get("address_line1", ""),
                        address_line2=addr.get("address_line2"),
                        city=addr.get("city", ""),
                        state=addr.get("state", ""),
                        pincode=addr.get("pin_code", ""),
                        landmark=addr.get("site_name")
                    )
                    break
        # If no shipping address specified, use billing as delivery
        if not delivery_address and billing_address:
            delivery_address = DeliveryAddress(
                full_name=customer_name,
                phone=customer_phone,
                address_line1=billing_address.get("address_line1", ""),
                address_line2=billing_address.get("address_line2"),
                city=billing_address.get("city", ""),
                state=billing_address.get("state", ""),
                pincode=billing_address.get("pin_code", ""),
                landmark=None
            )
    
    # Validate required fields
    if not customer_name:
        raise HTTPException(status_code=400, detail="Customer name is required")
    if not customer_phone:
        raise HTTPException(status_code=400, detail="Customer phone is required")
    
    # Calculate totals
    subtotal = sum(item.total_price for item in order_data.glass_items)
    total_sqft = sum((item.width * item.height * item.quantity) / 144 for item in order_data.glass_items)
    
    # Tax calculation (18% GST)
    tax_rate = 0.18
    tax_amount = round(subtotal * tax_rate, 2)
    total_price = round(subtotal + tax_amount, 2)
    
    # Advance calculation
    advance_percent = order_data.advance_percent
    if advance_percent is None:
        # Auto-determine based on credit status
        if is_credit_customer:
            advance_percent = 0  # Credit customers can have 0% advance
        else:
            advance_percent = 50  # Normal customers default 50%
    
    is_valid, error = await validate_advance_percent(
        total_price, 
        advance_percent, 
        is_credit_customer,
        current_user.get("role", "customer")
    )
    
    if not is_valid:
        raise HTTPException(status_code=400, detail=error)
    
    # Check credit limit for credit customers
    if is_credit_customer and customer_profile:
        # Get current outstanding
        outstanding = 0
        existing_orders = await db.orders.find({
            "customer_profile_id": order_data.customer_profile_id,
            "payment_status": {"$ne": "completed"}
        }, {"remaining_amount": 1}).to_list(1000)
        
        for o in existing_orders:
            outstanding += o.get("remaining_amount", 0)
        
        if outstanding + total_price > credit_limit:
            raise HTTPException(
                status_code=400, 
                detail=f"Order exceeds credit limit. Current outstanding: ₹{outstanding:,.2f}, Limit: ₹{credit_limit:,.2f}"
            )
    
    advance_amount = round(total_price * advance_percent / 100, 2)
    remaining_amount = round(total_price - advance_amount, 2)
    
    order_id = str(uuid.uuid4())
    order_number = await generate_order_number()
    
    # Create Razorpay order for advance payment
    razorpay_order_id = None
    if advance_amount > 0 and razorpay_client:
        try:
            rz_order = razorpay_client.order.create({
                "amount": int(advance_amount * 100),
                "currency": "INR",
                "receipt": order_number,
                "notes": {
                    "order_id": order_id,
                    "type": "advance"
                }
            })
            razorpay_order_id = rz_order["id"]
        except Exception as e:
            print(f"Razorpay order creation failed: {e}")
    
    order = {
        "id": order_id,
        "order_number": order_number,
        "customer_id": current_user.get("id"),
        "customer_profile_id": order_data.customer_profile_id,  # Link to Customer Master
        "customer_name": customer_name,
        "customer_email": customer_email,
        "customer_phone": customer_phone,
        "company_name": company_name,
        "gst_number": gst_number,
        "invoice_type": invoice_type,
        "place_of_supply": place_of_supply,
        "billing_address": billing_address,
        "glass_items": [item.dict() for item in order_data.glass_items],
        "total_sqft": round(total_sqft, 2),
        "subtotal": subtotal,
        "tax_rate": tax_rate,
        "tax_amount": tax_amount,
        "total_price": total_price,
        "advance_percent": advance_percent,
        "advance_amount": advance_amount,
        "remaining_amount": remaining_amount,
        "is_credit_customer": is_credit_customer,
        "credit_limit": credit_limit,
        "credit_days": credit_days,
        "payment_status": "pending",
        "razorpay_order_id": razorpay_order_id,
        "status": "pending",
        "production_stage": None,
        "delivery_address": delivery_address.dict() if delivery_address else None,
        "delivery_type": order_data.delivery_type,
        "notes": order_data.notes,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.orders.insert_one(order)
    
    # AUTO-POST TO PARTY LEDGER & GL
    # Sales Invoice → Debit Accounts Receivable, Credit Sales + GST
    await auto_post_to_ledger_orders(
        db,
        entry_type="sales_invoice",
        reference_id=order["id"],
        reference_number=order_number,
        party_type="customer",
        party_id=order_data.customer_profile_id or order.get("customer_id", ""),
        party_name=customer_name,
        amount=subtotal,
        gst_amount=tax_amount,
        description=f"Sales Order {order_number}",
        transaction_date=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        created_by="system"
    )
    
    return {
        "message": "Order created successfully",
        "order": {k: v for k, v in order.items() if k != "_id"},
        "razorpay_order_id": razorpay_order_id,
        "razorpay_key": RAZORPAY_KEY_ID,
        "customer_profile": customer_profile  # Return profile for frontend reference
    }


@orders_router.post("/{order_id}/payment")
async def verify_payment(
    order_id: str,
    payment_data: dict,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_user)
):
    """Verify Razorpay payment for order"""
    db = get_db()
    
    order = await db.orders.find_one({"id": order_id})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    razorpay_payment_id = payment_data.get("razorpay_payment_id")
    razorpay_signature = payment_data.get("razorpay_signature")
    razorpay_order_id = payment_data.get("razorpay_order_id")
    
    # Verify signature
    if razorpay_client:
        message = f"{razorpay_order_id}|{razorpay_payment_id}"
        expected_signature = hmac.new(
            RAZORPAY_KEY_SECRET.encode(),
            message.encode(),
            hashlib.sha256
        ).hexdigest()
        
        if expected_signature != razorpay_signature:
            raise HTTPException(status_code=400, detail="Payment verification failed")
    
    # Update order
    payment_status = "partial" if order.get("remaining_amount", 0) > 0 else "completed"
    
    await db.orders.update_one(
        {"id": order_id},
        {
            "$set": {
                "razorpay_payment_id": razorpay_payment_id,
                "payment_status": payment_status,
                "status": "processing" if payment_status in ["partial", "completed"] else "pending",
                "advance_paid_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
        }
    )
    
    # AUTO-POST TO PARTY LEDGER & GL
    # Payment Received → Debit Cash/Bank, Credit Accounts Receivable
    payment_amount = order.get("advance_amount", 0)
    await auto_post_to_ledger_orders(
        db,
        entry_type="payment_received",
        reference_id=f"{order_id}-advance",
        reference_number=f"RCP-{order.get('order_number')}-ADV",
        party_type="customer",
        party_id=order.get("user_id", ""),
        party_name=order.get("customer_name", ""),
        amount=payment_amount,
        gst_amount=0,
        description=f"Advance Payment for Order {order.get('order_number')}",
        transaction_date=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        created_by="system"
    )
    
    return {
        "message": "Payment verified successfully",
        "order_id": order_id,
        "payment_status": payment_status
    }


@orders_router.post("/{order_id}/initiate-remaining-payment")
async def initiate_remaining_payment(
    order_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Initiate payment for remaining amount"""
    db = get_db()
    
    order = await db.orders.find_one({"id": order_id}, {"_id": 0})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    remaining = order.get("remaining_amount", 0)
    if remaining <= 0:
        raise HTTPException(status_code=400, detail="No remaining amount to pay")
    
    # Create Razorpay order
    razorpay_order_id = None
    if razorpay_client:
        try:
            rz_order = razorpay_client.order.create({
                "amount": int(remaining * 100),
                "currency": "INR",
                "receipt": f"{order.get('order_number')}-REM",
                "notes": {
                    "order_id": order_id,
                    "type": "remaining"
                }
            })
            razorpay_order_id = rz_order["id"]
            
            await db.orders.update_one(
                {"id": order_id},
                {"$set": {"remaining_razorpay_order_id": razorpay_order_id}}
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to create payment: {str(e)}")
    
    return {
        "razorpay_order_id": razorpay_order_id,
        "razorpay_key": RAZORPAY_KEY_ID,
        "amount": remaining,
        "order_number": order.get("order_number")
    }


@orders_router.post("/{order_id}/verify-remaining-payment")
async def verify_remaining_payment(
    order_id: str,
    payment_data: RemainingPaymentVerify,
    current_user: dict = Depends(get_current_user)
):
    """Verify remaining payment"""
    db = get_db()
    
    order = await db.orders.find_one({"id": order_id})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    # Verify signature
    if razorpay_client:
        message = f"{payment_data.razorpay_order_id}|{payment_data.razorpay_payment_id}"
        expected_signature = hmac.new(
            RAZORPAY_KEY_SECRET.encode(),
            message.encode(),
            hashlib.sha256
        ).hexdigest()
        
        if expected_signature != payment_data.razorpay_signature:
            raise HTTPException(status_code=400, detail="Payment verification failed")
    
    # Update order
    await db.orders.update_one(
        {"id": order_id},
        {
            "$set": {
                "remaining_razorpay_payment_id": payment_data.razorpay_payment_id,
                "remaining_amount": 0,
                "payment_status": "completed",
                "remaining_paid_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
        }
    )
    
    return {
        "message": "Remaining payment verified successfully",
        "order_id": order_id,
        "payment_status": "completed"
    }


@orders_router.post("/{order_id}/mark-cash-received")
async def mark_cash_received(
    order_id: str,
    data: dict,
    current_user: dict = Depends(get_current_user)
):
    """Mark cash payment received (admin only)"""
    if current_user.get("role") not in ["admin", "super_admin", "owner", "accountant"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    db = get_db()
    
    order = await db.orders.find_one({"id": order_id})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    payment_type = data.get("payment_type", "advance")  # advance or remaining
    amount = data.get("amount", 0)
    
    update_data = {
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    
    if payment_type == "advance":
        update_data["payment_status"] = "partial" if order.get("remaining_amount", 0) > 0 else "completed"
        update_data["status"] = "processing"
        update_data["advance_paid_at"] = datetime.now(timezone.utc).isoformat()
        update_data["advance_payment_method"] = "cash"
    else:
        update_data["remaining_amount"] = 0
        update_data["payment_status"] = "completed"
        update_data["remaining_paid_at"] = datetime.now(timezone.utc).isoformat()
        update_data["remaining_payment_method"] = "cash"
    
    await db.orders.update_one({"id": order_id}, {"$set": update_data})
    
    # Record cash payment
    await db.cash_payments.insert_one({
        "id": str(uuid.uuid4()),
        "order_id": order_id,
        "order_number": order.get("order_number"),
        "amount": amount,
        "payment_type": payment_type,
        "received_by": current_user.get("name"),
        "received_at": datetime.now(timezone.utc).isoformat()
    })
    
    return {"message": "Cash payment recorded", "payment_status": update_data.get("payment_status")}


@orders_router.get("/my-orders")
async def get_my_orders(current_user: dict = Depends(get_current_user)):
    """Get orders for current user"""
    db = get_db()
    
    orders = await db.orders.find(
        {"customer_id": current_user.get("id")},
        {"_id": 0}
    ).sort("created_at", -1).to_list(100)
    
    return orders


@orders_router.get("/track/{order_id}")
async def track_order(order_id: str):
    """Track order status (public endpoint)"""
    db = get_db()
    
    order = await db.orders.find_one(
        {"$or": [{"id": order_id}, {"order_number": order_id}]},
        {"_id": 0, "customer_email": 0}
    )
    
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    return order
