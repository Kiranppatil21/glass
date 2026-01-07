"""
Customer Profile / Master Module
Comprehensive customer data management for GST, billing, credit control, and CRM

Features:
- Basic Identity (Individual/Business)
- GST & Tax Details with GSTIN validation
- Billing & Multiple Shipping Addresses
- Credit Control (Category, Limit, Days)
- Bank Details (Internal use only)
- Invoice Preferences
- Compliance & Declaration
- CRM & Sales Tracking
- Auto-linked sections (read-only)
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field, EmailStr, field_validator
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
from enum import Enum
import uuid
import re
from .base import get_erp_user, get_db

customer_master_router = APIRouter(prefix="/customer-master", tags=["Customer Master"])


# =============== ENUMS ===============

class CustomerType(str, Enum):
    INDIVIDUAL = "individual"
    PROPRIETOR = "proprietor"
    PARTNERSHIP = "partnership"
    PVT_LTD = "pvt_ltd"
    LTD = "ltd"
    BUILDER = "builder"
    DEALER = "dealer"
    ARCHITECT = "architect"


class GSTType(str, Enum):
    REGULAR = "regular"
    COMPOSITION = "composition"
    UNREGISTERED = "unregistered"


class CustomerCategory(str, Enum):
    RETAIL = "retail"
    BUILDER = "builder"
    DEALER = "dealer"
    PROJECT = "project"


class CreditType(str, Enum):
    ADVANCE_ONLY = "advance_only"
    CREDIT_ALLOWED = "credit_allowed"


class KYCStatus(str, Enum):
    PENDING = "pending"
    VERIFIED = "verified"
    REJECTED = "rejected"


class InvoiceLanguage(str, Enum):
    ENGLISH = "english"
    HINDI = "hindi"


# =============== PYDANTIC MODELS ===============

class ShippingAddress(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    site_name: Optional[str] = None  # Project / Site Name
    address_line1: str
    address_line2: Optional[str] = None
    city: str
    state: str
    state_code: Optional[str] = None
    pin_code: str
    contact_person: Optional[str] = None
    contact_phone: Optional[str] = None
    is_default: bool = False


class BillingAddress(BaseModel):
    address_line1: str
    address_line2: Optional[str] = None
    city: str
    state: str
    state_code: Optional[str] = None
    pin_code: str
    country: str = "India"


class BankDetails(BaseModel):
    account_holder_name: Optional[str] = None
    bank_name: Optional[str] = None
    account_number: Optional[str] = None
    ifsc_code: Optional[str] = None
    upi_id: Optional[str] = None


class InvoicePreferences(BaseModel):
    language: InvoiceLanguage = InvoiceLanguage.ENGLISH
    email_invoice: bool = True
    whatsapp_invoice: bool = False
    po_mandatory: bool = False


class Compliance(BaseModel):
    gst_declaration: bool = False
    data_consent: bool = False
    terms_accepted: bool = False
    kyc_status: KYCStatus = KYCStatus.PENDING
    kyc_verified_at: Optional[str] = None
    kyc_verified_by: Optional[str] = None


class CRMDetails(BaseModel):
    sales_person_id: Optional[str] = None
    sales_person_name: Optional[str] = None
    source: Optional[str] = None  # Reference / Google / Dealer
    notes: Optional[str] = None
    special_pricing: Optional[Dict[str, Any]] = None  # Product-wise special prices


class CustomerProfileCreate(BaseModel):
    # 1️⃣ Basic Identity
    customer_type: CustomerType
    company_name: Optional[str] = None  # Required for Business, optional for Retail
    individual_name: Optional[str] = None  # Required for Retail
    contact_person: Optional[str] = None
    mobile: str
    email: Optional[EmailStr] = None
    
    # 2️⃣ GST & Tax Details
    needs_gst_invoice: bool = False  # "Do you have GSTIN / Do you need GST Invoice?"
    gstin: Optional[str] = None
    pan: Optional[str] = None
    place_of_supply: Optional[str] = None
    gst_type: GSTType = GSTType.UNREGISTERED
    
    # 3️⃣ Billing Address (Mandatory)
    billing_address: BillingAddress
    
    # 4️⃣ Shipping Addresses (Optional - Multiple)
    shipping_addresses: List[ShippingAddress] = []
    
    # 5️⃣ Business & Credit Control
    customer_category: CustomerCategory = CustomerCategory.RETAIL
    credit_type: CreditType = CreditType.ADVANCE_ONLY
    credit_limit: float = 0
    credit_days: int = 0  # 15 / 30 / 45 days
    
    # 6️⃣ Bank Details (Internal)
    bank_details: Optional[BankDetails] = None
    
    # 7️⃣ Invoice Preferences
    invoice_preferences: InvoicePreferences = Field(default_factory=InvoicePreferences)
    
    # 8️⃣ Compliance
    compliance: Compliance = Field(default_factory=Compliance)
    
    # 9️⃣ CRM Details
    crm_details: Optional[CRMDetails] = None
    
    @field_validator('gstin')
    @classmethod
    def validate_gstin(cls, v):
        if v:
            # GSTIN format: 2 digits state code + 10 char PAN + 1 entity code + Z + 1 checksum
            pattern = r'^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z]{1}[1-9A-Z]{1}Z[0-9A-Z]{1}$'
            if not re.match(pattern, v.upper()):
                raise ValueError('Invalid GSTIN format. Expected: 22AAAAA0000A1Z5')
        return v.upper() if v else v
    
    @field_validator('pan')
    @classmethod
    def validate_pan(cls, v):
        if v:
            pattern = r'^[A-Z]{5}[0-9]{4}[A-Z]{1}$'
            if not re.match(pattern, v.upper()):
                raise ValueError('Invalid PAN format. Expected: AAAAA0000A')
        return v.upper() if v else v
    
    @field_validator('mobile')
    @classmethod
    def validate_mobile(cls, v):
        if v:
            # Remove spaces and dashes
            v = re.sub(r'[\s\-]', '', v)
            # Check if it's a valid 10-digit Indian mobile
            if not re.match(r'^[6-9]\d{9}$', v):
                raise ValueError('Invalid mobile number. Expected 10 digits starting with 6-9')
        return v


class CustomerProfileUpdate(BaseModel):
    # All fields optional for partial updates
    customer_type: Optional[CustomerType] = None
    company_name: Optional[str] = None
    individual_name: Optional[str] = None
    contact_person: Optional[str] = None
    mobile: Optional[str] = None
    email: Optional[EmailStr] = None
    needs_gst_invoice: Optional[bool] = None  # GST checkbox
    gstin: Optional[str] = None
    pan: Optional[str] = None
    place_of_supply: Optional[str] = None
    gst_type: Optional[GSTType] = None
    billing_address: Optional[BillingAddress] = None
    shipping_addresses: Optional[List[ShippingAddress]] = None  # For updating multiple addresses
    customer_category: Optional[CustomerCategory] = None
    credit_type: Optional[CreditType] = None
    credit_limit: Optional[float] = None
    credit_days: Optional[int] = None
    bank_details: Optional[BankDetails] = None
    invoice_preferences: Optional[InvoicePreferences] = None
    compliance: Optional[Compliance] = None
    crm_details: Optional[CRMDetails] = None


class ShippingAddressCreate(BaseModel):
    site_name: Optional[str] = None
    address_line1: str
    address_line2: Optional[str] = None
    city: str
    state: str
    state_code: Optional[str] = None
    pin_code: str
    contact_person: Optional[str] = None
    contact_phone: Optional[str] = None
    is_default: bool = False


# =============== STATE CODES ===============

INDIAN_STATES = {
    "01": "Jammu & Kashmir",
    "02": "Himachal Pradesh",
    "03": "Punjab",
    "04": "Chandigarh",
    "05": "Uttarakhand",
    "06": "Haryana",
    "07": "Delhi",
    "08": "Rajasthan",
    "09": "Uttar Pradesh",
    "10": "Bihar",
    "11": "Sikkim",
    "12": "Arunachal Pradesh",
    "13": "Nagaland",
    "14": "Manipur",
    "15": "Mizoram",
    "16": "Tripura",
    "17": "Meghalaya",
    "18": "Assam",
    "19": "West Bengal",
    "20": "Jharkhand",
    "21": "Odisha",
    "22": "Chhattisgarh",
    "23": "Madhya Pradesh",
    "24": "Gujarat",
    "26": "Dadra & Nagar Haveli and Daman & Diu",
    "27": "Maharashtra",
    "28": "Andhra Pradesh (Old)",
    "29": "Karnataka",
    "30": "Goa",
    "31": "Lakshadweep",
    "32": "Kerala",
    "33": "Tamil Nadu",
    "34": "Puducherry",
    "35": "Andaman & Nicobar Islands",
    "36": "Telangana",
    "37": "Andhra Pradesh",
    "38": "Ladakh"
}


# =============== HELPER FUNCTIONS ===============

def extract_state_from_gstin(gstin: str) -> tuple:
    """Extract state code and name from GSTIN"""
    if gstin and len(gstin) >= 2:
        state_code = gstin[:2]
        state_name = INDIAN_STATES.get(state_code, "Unknown")
        return state_code, state_name
    return None, None


def get_display_name(profile: dict) -> str:
    """Get display name based on customer type and GST requirement"""
    customer_type = profile.get("customer_type", "individual")
    needs_gst = profile.get("needs_gst_invoice", False)
    has_gstin = bool(profile.get("gstin"))
    
    # Company types always use company name
    is_company_type = customer_type in ["pvt_ltd", "ltd", "llp"]
    
    # B2B - Use company name if:
    # 1. Company type (Pvt Ltd, Ltd, LLP)
    # 2. Has GSTIN
    # 3. Needs GST Invoice checkbox is checked
    if is_company_type or has_gstin or needs_gst:
        return profile.get("company_name") or profile.get("individual_name") or "Unknown"
    
    # B2C - Use individual name
    return profile.get("individual_name") or profile.get("company_name") or "Unknown"


def generate_customer_code() -> str:
    """Generate unique customer code: CUS-YYYYMMDD-XXXXX"""
    date_str = datetime.now().strftime("%Y%m%d")
    random_part = uuid.uuid4().hex[:5].upper()
    return f"CUS-{date_str}-{random_part}"


# =============== API ENDPOINTS ===============

@customer_master_router.get("/states")
async def get_indian_states():
    """Get list of Indian states with codes"""
    return [
        {"code": code, "name": name}
        for code, name in sorted(INDIAN_STATES.items(), key=lambda x: x[1])
    ]


@customer_master_router.post("/")
async def create_customer_profile(
    data: CustomerProfileCreate,
    current_user: dict = Depends(get_erp_user)
):
    """Create a new customer profile"""
    db = get_db()
    
    # Check permissions
    if current_user.get("role") not in ["admin", "owner", "super_admin", "accounts", "sales"]:
        raise HTTPException(status_code=403, detail="Not authorized to create customers")
    
    # Auto-switch logic: If GSTIN provided, require company name
    if data.gstin and not data.company_name:
        raise HTTPException(
            status_code=400, 
            detail="Company name is required when GSTIN is provided"
        )
    
    # If Retail/Individual without GSTIN, require individual name
    if data.customer_type == CustomerType.INDIVIDUAL and not data.gstin and not data.individual_name:
        raise HTTPException(
            status_code=400,
            detail="Individual name is required for Retail customers without GSTIN"
        )
    
    # Check for duplicate GSTIN
    if data.gstin:
        existing = await db.customer_profiles.find_one({"gstin": data.gstin})
        if existing:
            raise HTTPException(status_code=400, detail="Customer with this GSTIN already exists")
    
    # Check for duplicate mobile
    existing_mobile = await db.customer_profiles.find_one({"mobile": data.mobile})
    if existing_mobile:
        raise HTTPException(status_code=400, detail="Customer with this mobile number already exists")
    
    # Extract state code from GSTIN if provided
    state_code, state_name = None, None
    if data.gstin:
        state_code, state_name = extract_state_from_gstin(data.gstin)
    
    # Determine invoice type based on needs_gst_invoice, gstin, or customer_type
    # Company types (pvt_ltd, ltd) always B2B
    is_company_type = data.customer_type in [CustomerType.PVT_LTD, CustomerType.LTD]
    needs_b2b = is_company_type or data.needs_gst_invoice or data.gstin
    invoice_type = "B2B" if needs_b2b else "B2C"
    
    # Build profile document
    profile = {
        "id": str(uuid.uuid4()),
        "customer_code": generate_customer_code(),
        
        # Basic Identity
        "customer_type": data.customer_type.value,
        "company_name": data.company_name,
        "individual_name": data.individual_name,
        "contact_person": data.contact_person,
        "mobile": data.mobile,
        "email": data.email,
        
        # GST & Tax
        "needs_gst_invoice": data.needs_gst_invoice or is_company_type,
        "gstin": data.gstin,
        "pan": data.pan,
        "state_code": state_code,
        "state_name": state_name,
        "place_of_supply": data.place_of_supply or state_name,
        "gst_type": data.gst_type.value if data.needs_gst_invoice or data.gstin else GSTType.UNREGISTERED.value,
        
        # Billing Address
        "billing_address": data.billing_address.model_dump() if data.billing_address else None,
        
        # Shipping Addresses
        "shipping_addresses": [addr.model_dump() for addr in data.shipping_addresses],
        
        # Credit Control
        "customer_category": data.customer_category.value,
        "credit_type": data.credit_type.value,
        "credit_limit": data.credit_limit,
        "credit_days": data.credit_days,
        
        # Bank Details (Internal)
        "bank_details": data.bank_details.model_dump() if data.bank_details else None,
        
        # Invoice Preferences
        "invoice_preferences": data.invoice_preferences.model_dump(),
        
        # Compliance
        "compliance": data.compliance.model_dump(),
        
        # CRM
        "crm_details": data.crm_details.model_dump() if data.crm_details else None,
        
        # Display Name (computed)
        "display_name": get_display_name({
            "customer_type": data.customer_type.value,
            "company_name": data.company_name,
            "individual_name": data.individual_name,
            "gstin": data.gstin,
            "needs_gst_invoice": data.needs_gst_invoice
        }),
        
        # Invoice Type
        "invoice_type": invoice_type,
        
        # Metadata
        "status": "active",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "created_by": current_user.get("id"),
        "updated_at": None,
        "updated_by": None
    }
    
    await db.customer_profiles.insert_one(profile)
    
    # Create audit log
    await db.audit_logs.insert_one({
        "id": str(uuid.uuid4()),
        "action": "customer_created",
        "entity_type": "customer_profile",
        "entity_id": profile["id"],
        "user_id": current_user.get("id"),
        "user_name": current_user.get("name"),
        "details": {"customer_code": profile["customer_code"], "display_name": profile["display_name"]},
        "created_at": datetime.now(timezone.utc).isoformat()
    })
    
    # Return without _id
    profile.pop("_id", None)
    return profile


@customer_master_router.get("/")
async def list_customer_profiles(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    search: Optional[str] = None,
    customer_type: Optional[str] = None,
    category: Optional[str] = None,
    status: str = "active",
    current_user: dict = Depends(get_erp_user)
):
    """List all customer profiles with pagination and filters"""
    db = get_db()
    
    # Build query
    query = {}
    
    if status:
        query["status"] = status
    
    if customer_type:
        query["customer_type"] = customer_type
    
    if category:
        query["customer_category"] = category
    
    if search:
        query["$or"] = [
            {"display_name": {"$regex": search, "$options": "i"}},
            {"company_name": {"$regex": search, "$options": "i"}},
            {"individual_name": {"$regex": search, "$options": "i"}},
            {"mobile": {"$regex": search, "$options": "i"}},
            {"gstin": {"$regex": search, "$options": "i"}},
            {"customer_code": {"$regex": search, "$options": "i"}}
        ]
    
    # Get total count
    total = await db.customer_profiles.count_documents(query)
    
    # Get paginated results
    skip = (page - 1) * limit
    profiles = await db.customer_profiles.find(
        query, {"_id": 0}
    ).sort("created_at", -1).skip(skip).limit(limit).to_list(limit)
    
    return {
        "profiles": profiles,
        "total": total,
        "page": page,
        "limit": limit,
        "total_pages": (total + limit - 1) // limit
    }


@customer_master_router.get("/stats")
async def get_customer_stats(current_user: dict = Depends(get_erp_user)):
    """Get customer statistics for dashboard"""
    db = get_db()
    
    # Total active customers
    total_active = await db.customer_profiles.count_documents({"status": "active"})
    
    # By category
    retail_count = await db.customer_profiles.count_documents({"status": "active", "customer_category": "retail"})
    builder_count = await db.customer_profiles.count_documents({"status": "active", "customer_category": "builder"})
    dealer_count = await db.customer_profiles.count_documents({"status": "active", "customer_category": "dealer"})
    project_count = await db.customer_profiles.count_documents({"status": "active", "customer_category": "project"})
    
    # Credit customers
    credit_customers = await db.customer_profiles.count_documents({
        "status": "active", 
        "credit_type": "credit_allowed"
    })
    
    # B2B vs B2C
    b2b_count = await db.customer_profiles.count_documents({"status": "active", "invoice_type": "B2B"})
    b2c_count = await db.customer_profiles.count_documents({"status": "active", "invoice_type": "B2C"})
    
    # KYC Status
    kyc_verified = await db.customer_profiles.count_documents({
        "status": "active",
        "compliance.kyc_status": "verified"
    })
    kyc_pending = await db.customer_profiles.count_documents({
        "status": "active",
        "compliance.kyc_status": "pending"
    })
    
    return {
        "total_active": total_active,
        "by_category": {
            "retail": retail_count,
            "builder": builder_count,
            "dealer": dealer_count,
            "project": project_count
        },
        "credit_customers": credit_customers,
        "invoice_type": {
            "b2b": b2b_count,
            "b2c": b2c_count
        },
        "kyc": {
            "verified": kyc_verified,
            "pending": kyc_pending
        }
    }


@customer_master_router.get("/{customer_id}")
async def get_customer_profile(
    customer_id: str,
    current_user: dict = Depends(get_erp_user)
):
    """Get a single customer profile with linked data"""
    db = get_db()
    
    # Try to find by id or customer_code
    profile = await db.customer_profiles.find_one(
        {"$or": [{"id": customer_id}, {"customer_code": customer_id}]},
        {"_id": 0}
    )
    
    if not profile:
        raise HTTPException(status_code=404, detail="Customer not found")
    
    # Get linked data (system generated - read only)
    
    # 1. Orders
    orders = await db.orders.find(
        {"$or": [
            {"user_id": profile["id"]},
            {"customer_phone": profile["mobile"]}
        ]},
        {"_id": 0, "id": 1, "order_number": 1, "total_price": 1, "status": 1, "payment_status": 1, "created_at": 1}
    ).sort("created_at", -1).limit(10).to_list(10)
    
    # 2. Invoices
    invoices = await db.invoices.find(
        {"$or": [
            {"customer_id": profile["id"]},
            {"customer_phone": profile["mobile"]}
        ]},
        {"_id": 0, "id": 1, "invoice_number": 1, "total_amount": 1, "status": 1, "created_at": 1}
    ).sort("created_at", -1).limit(10).to_list(10)
    
    # 3. Payments
    payments = await db.payments.find(
        {"$or": [
            {"customer_id": profile["id"]},
            {"order_id": {"$in": [o["id"] for o in orders]}}
        ]},
        {"_id": 0, "id": 1, "amount": 1, "payment_method": 1, "status": 1, "created_at": 1}
    ).sort("created_at", -1).limit(10).to_list(10)
    
    # 4. Party Ledger
    ledger_entries = await db.party_ledger.find(
        {"party_id": profile["id"]},
        {"_id": 0}
    ).sort("date", -1).limit(20).to_list(20)
    
    # 5. Calculate outstanding
    total_orders_value = sum(o.get("total_price", 0) for o in orders)
    total_paid = sum(p.get("amount", 0) for p in payments if p.get("status") == "completed")
    outstanding_balance = total_orders_value - total_paid
    
    # 6. Ageing
    today = datetime.now(timezone.utc)
    ageing = {"0_30": 0, "31_60": 0, "61_90": 0, "over_90": 0}
    for order in orders:
        if order.get("payment_status") not in ["paid", "completed"]:
            order_date = datetime.fromisoformat(order["created_at"].replace("Z", "+00:00")) if order.get("created_at") else today
            days_old = (today - order_date).days
            amount = order.get("total_price", 0) - sum(
                p.get("amount", 0) for p in payments 
                if p.get("order_id") == order.get("id") and p.get("status") == "completed"
            )
            if days_old <= 30:
                ageing["0_30"] += amount
            elif days_old <= 60:
                ageing["31_60"] += amount
            elif days_old <= 90:
                ageing["61_90"] += amount
            else:
                ageing["over_90"] += amount
    
    return {
        **profile,
        "linked_data": {
            "orders": orders,
            "invoices": invoices,
            "payments": payments,
            "ledger_entries": ledger_entries,
            "outstanding_balance": round(outstanding_balance, 2),
            "ageing": ageing,
            "total_orders": len(orders),
            "total_spent": round(total_paid, 2)
        }
    }


@customer_master_router.put("/{customer_id}")
async def update_customer_profile(
    customer_id: str,
    data: CustomerProfileUpdate,
    current_user: dict = Depends(get_erp_user)
):
    """Update customer profile"""
    db = get_db()
    
    # Find customer first
    profile = await db.customer_profiles.find_one(
        {"$or": [{"id": customer_id}, {"customer_code": customer_id}]}
    )
    
    if not profile:
        raise HTTPException(status_code=404, detail="Customer not found")
    
    # Check permissions - allow customers to update their own profile
    is_own_profile = (
        profile.get("mobile") == current_user.get("phone") or
        profile.get("email") == current_user.get("email") or
        profile.get("migrated_from_user_id") == current_user.get("id")
    )
    
    is_admin_role = current_user.get("role") in ["admin", "owner", "super_admin", "accounts", "sales"]
    
    if not is_own_profile and not is_admin_role:
        raise HTTPException(status_code=403, detail="Not authorized to update this customer profile")
    
    # Build update
    update_data = {}
    
    for field, value in data.model_dump(exclude_unset=True).items():
        if value is not None:
            if isinstance(value, BaseModel):
                update_data[field] = value.model_dump()
            elif hasattr(value, "value"):  # Enum
                update_data[field] = value.value
            else:
                update_data[field] = value
    
    # Re-validate GSTIN if changed
    if "gstin" in update_data and update_data["gstin"]:
        # Check for duplicate
        existing = await db.customer_profiles.find_one({
            "gstin": update_data["gstin"],
            "id": {"$ne": profile["id"]}
        })
        if existing:
            raise HTTPException(status_code=400, detail="Customer with this GSTIN already exists")
        
        # Extract state
        state_code, state_name = extract_state_from_gstin(update_data["gstin"])
        update_data["state_code"] = state_code
        update_data["state_name"] = state_name
        update_data["invoice_type"] = "B2B"
        update_data["needs_gst_invoice"] = True
    
    # Handle needs_gst_invoice checkbox
    if "needs_gst_invoice" in update_data:
        if not update_data.get("needs_gst_invoice"):
            # B2C - Clear GST fields
            update_data["invoice_type"] = "B2C"
            # Don't clear GSTIN if already set, just mark as B2C
        else:
            # B2B - Ensure GST fields are provided
            update_data["invoice_type"] = "B2B"
    
    # Update display name if relevant fields changed
    if any(f in update_data for f in ["customer_type", "company_name", "individual_name", "gstin", "needs_gst_invoice"]):
        merged = {**profile, **update_data}
        update_data["display_name"] = get_display_name(merged)
        # Determine invoice type based on needs_gst_invoice or gstin
        if merged.get("needs_gst_invoice") or merged.get("gstin"):
            update_data["invoice_type"] = "B2B"
        else:
            update_data["invoice_type"] = "B2C"
    
    update_data["updated_at"] = datetime.now(timezone.utc).isoformat()
    update_data["updated_by"] = current_user.get("id")
    
    await db.customer_profiles.update_one(
        {"id": profile["id"]},
        {"$set": update_data}
    )
    
    # Create audit log
    await db.audit_logs.insert_one({
        "id": str(uuid.uuid4()),
        "action": "customer_updated",
        "entity_type": "customer_profile",
        "entity_id": profile["id"],
        "user_id": current_user.get("id"),
        "user_name": current_user.get("name"),
        "details": {"fields_updated": list(update_data.keys())},
        "created_at": datetime.now(timezone.utc).isoformat()
    })
    
    # Return updated profile
    updated = await db.customer_profiles.find_one({"id": profile["id"]}, {"_id": 0})
    return updated


@customer_master_router.patch("/{customer_id}/deactivate")
async def deactivate_customer(
    customer_id: str,
    current_user: dict = Depends(get_erp_user)
):
    """Deactivate a customer (soft delete - no hard delete allowed)"""
    db = get_db()
    
    # Only admin can deactivate
    if current_user.get("role") not in ["admin", "owner", "super_admin"]:
        raise HTTPException(status_code=403, detail="Only admin can deactivate customers")
    
    # Find customer
    profile = await db.customer_profiles.find_one(
        {"$or": [{"id": customer_id}, {"customer_code": customer_id}]}
    )
    
    if not profile:
        raise HTTPException(status_code=404, detail="Customer not found")
    
    # Check for outstanding balance
    total_outstanding = 0
    orders = await db.orders.find(
        {"$or": [{"user_id": profile["id"]}, {"customer_phone": profile["mobile"]}]},
        {"total_price": 1, "payment_status": 1}
    ).to_list(1000)
    
    for order in orders:
        if order.get("payment_status") not in ["paid", "completed"]:
            total_outstanding += order.get("total_price", 0)
    
    if total_outstanding > 0:
        raise HTTPException(
            status_code=400, 
            detail=f"Cannot deactivate customer with outstanding balance: ₹{total_outstanding:,.2f}"
        )
    
    # Deactivate
    await db.customer_profiles.update_one(
        {"id": profile["id"]},
        {"$set": {
            "status": "inactive",
            "deactivated_at": datetime.now(timezone.utc).isoformat(),
            "deactivated_by": current_user.get("id")
        }}
    )
    
    # Create audit log
    await db.audit_logs.insert_one({
        "id": str(uuid.uuid4()),
        "action": "customer_deactivated",
        "entity_type": "customer_profile",
        "entity_id": profile["id"],
        "user_id": current_user.get("id"),
        "user_name": current_user.get("name"),
        "details": {"customer_code": profile["customer_code"]},
        "created_at": datetime.now(timezone.utc).isoformat()
    })
    
    return {"message": "Customer deactivated successfully"}


@customer_master_router.patch("/{customer_id}/reactivate")
async def reactivate_customer(
    customer_id: str,
    current_user: dict = Depends(get_erp_user)
):
    """Reactivate a deactivated customer"""
    db = get_db()
    
    if current_user.get("role") not in ["admin", "owner", "super_admin"]:
        raise HTTPException(status_code=403, detail="Only admin can reactivate customers")
    
    profile = await db.customer_profiles.find_one(
        {"$or": [{"id": customer_id}, {"customer_code": customer_id}]}
    )
    
    if not profile:
        raise HTTPException(status_code=404, detail="Customer not found")
    
    if profile.get("status") == "active":
        raise HTTPException(status_code=400, detail="Customer is already active")
    
    await db.customer_profiles.update_one(
        {"id": profile["id"]},
        {"$set": {
            "status": "active",
            "reactivated_at": datetime.now(timezone.utc).isoformat(),
            "reactivated_by": current_user.get("id")
        }}
    )
    
    return {"message": "Customer reactivated successfully"}


# =============== SHIPPING ADDRESSES ===============

@customer_master_router.post("/{customer_id}/shipping-addresses")
async def add_shipping_address(
    customer_id: str,
    address: ShippingAddressCreate,
    current_user: dict = Depends(get_erp_user)
):
    """Add a new shipping address to customer"""
    db = get_db()
    
    profile = await db.customer_profiles.find_one(
        {"$or": [{"id": customer_id}, {"customer_code": customer_id}]}
    )
    
    if not profile:
        raise HTTPException(status_code=404, detail="Customer not found")
    
    new_address = {
        "id": str(uuid.uuid4()),
        **address.model_dump()
    }
    
    # If this is default, unset other defaults
    if new_address["is_default"]:
        shipping_addresses = profile.get("shipping_addresses", [])
        for addr in shipping_addresses:
            addr["is_default"] = False
        await db.customer_profiles.update_one(
            {"id": profile["id"]},
            {"$set": {"shipping_addresses": shipping_addresses}}
        )
    
    await db.customer_profiles.update_one(
        {"id": profile["id"]},
        {"$push": {"shipping_addresses": new_address}}
    )
    
    return {"message": "Shipping address added", "address_id": new_address["id"]}


@customer_master_router.put("/{customer_id}/shipping-addresses/{address_id}")
async def update_shipping_address(
    customer_id: str,
    address_id: str,
    address: ShippingAddressCreate,
    current_user: dict = Depends(get_erp_user)
):
    """Update a shipping address"""
    db = get_db()
    
    profile = await db.customer_profiles.find_one(
        {"$or": [{"id": customer_id}, {"customer_code": customer_id}]}
    )
    
    if not profile:
        raise HTTPException(status_code=404, detail="Customer not found")
    
    shipping_addresses = profile.get("shipping_addresses", [])
    found = False
    
    for i, addr in enumerate(shipping_addresses):
        if addr["id"] == address_id:
            shipping_addresses[i] = {"id": address_id, **address.model_dump()}
            found = True
        elif address.is_default:
            shipping_addresses[i]["is_default"] = False
    
    if not found:
        raise HTTPException(status_code=404, detail="Shipping address not found")
    
    await db.customer_profiles.update_one(
        {"id": profile["id"]},
        {"$set": {"shipping_addresses": shipping_addresses}}
    )
    
    return {"message": "Shipping address updated"}


@customer_master_router.delete("/{customer_id}/shipping-addresses/{address_id}")
async def delete_shipping_address(
    customer_id: str,
    address_id: str,
    current_user: dict = Depends(get_erp_user)
):
    """Delete a shipping address"""
    db = get_db()
    
    profile = await db.customer_profiles.find_one(
        {"$or": [{"id": customer_id}, {"customer_code": customer_id}]}
    )
    
    if not profile:
        raise HTTPException(status_code=404, detail="Customer not found")
    
    await db.customer_profiles.update_one(
        {"id": profile["id"]},
        {"$pull": {"shipping_addresses": {"id": address_id}}}
    )
    
    return {"message": "Shipping address deleted"}


# =============== KYC ===============

@customer_master_router.patch("/{customer_id}/kyc")
async def update_kyc_status(
    customer_id: str,
    status: KYCStatus,
    notes: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Update customer KYC status (CA / Admin only)"""
    db = get_db()
    
    if current_user.get("role") not in ["admin", "owner", "super_admin", "ca"]:
        raise HTTPException(status_code=403, detail="Only admin or CA can update KYC status")
    
    profile = await db.customer_profiles.find_one(
        {"$or": [{"id": customer_id}, {"customer_code": customer_id}]}
    )
    
    if not profile:
        raise HTTPException(status_code=404, detail="Customer not found")
    
    update = {
        "compliance.kyc_status": status.value,
        "compliance.kyc_verified_at": datetime.now(timezone.utc).isoformat(),
        "compliance.kyc_verified_by": current_user.get("id")
    }
    
    if notes:
        update["compliance.kyc_notes"] = notes
    
    await db.customer_profiles.update_one(
        {"id": profile["id"]},
        {"$set": update}
    )
    
    return {"message": f"KYC status updated to {status.value}"}


# =============== MIGRATION ===============

@customer_master_router.post("/migrate-existing")
async def migrate_existing_customers(
    current_user: dict = Depends(get_erp_user)
):
    """
    Migrate existing customer data from users collection to customer_profiles
    One-time migration endpoint
    """
    db = get_db()
    
    # Only super admin can run migration
    if current_user.get("role") not in ["super_admin", "admin"]:
        raise HTTPException(status_code=403, detail="Only super admin can run migration")
    
    # Get all existing users with customer-like roles
    users = await db.users.find({
        "role": {"$in": ["customer", "builder", "dealer"]}
    }, {"_id": 0, "password_hash": 0}).to_list(10000)
    
    migrated = 0
    skipped = 0
    errors = []
    
    for user in users:
        try:
            # Check if already migrated
            existing = await db.customer_profiles.find_one({
                "$or": [
                    {"mobile": user.get("phone")},
                    {"email": user.get("email")}
                ]
            })
            
            if existing:
                skipped += 1
                continue
            
            # Determine customer type based on existing data
            customer_type = CustomerType.INDIVIDUAL
            if user.get("role") == "builder":
                customer_type = CustomerType.BUILDER
            elif user.get("role") == "dealer":
                customer_type = CustomerType.DEALER
            elif user.get("gst_number") or user.get("company_name"):
                customer_type = CustomerType.PVT_LTD
            
            # Build profile
            profile = {
                "id": user.get("id", str(uuid.uuid4())),
                "customer_code": generate_customer_code(),
                "customer_type": customer_type.value,
                "company_name": user.get("company_name"),
                "individual_name": user.get("name"),
                "contact_person": user.get("name"),
                "mobile": user.get("phone", ""),
                "email": user.get("email"),
                "gstin": user.get("gst_number"),
                "pan": user.get("pan"),
                "state_code": None,
                "state_name": None,
                "place_of_supply": user.get("state"),
                "gst_type": GSTType.REGULAR.value if user.get("gst_number") else GSTType.UNREGISTERED.value,
                "billing_address": {
                    "address_line1": user.get("address", ""),
                    "address_line2": "",
                    "city": user.get("city", ""),
                    "state": user.get("state", ""),
                    "state_code": "",
                    "pin_code": user.get("pincode", ""),
                    "country": "India"
                } if user.get("address") else None,
                "shipping_addresses": [],
                "customer_category": CustomerCategory.RETAIL.value,
                "credit_type": CreditType.ADVANCE_ONLY.value,
                "credit_limit": 0,
                "credit_days": 0,
                "bank_details": None,
                "invoice_preferences": InvoicePreferences().model_dump(),
                "compliance": Compliance().model_dump(),
                "crm_details": None,
                "display_name": user.get("company_name") or user.get("name") or user.get("email", "Unknown"),
                "invoice_type": "B2B" if user.get("gst_number") else "B2C",
                "status": "active",
                "created_at": user.get("created_at", datetime.now(timezone.utc).isoformat()),
                "created_by": "migration",
                "migrated_from_user_id": user.get("id"),
                "updated_at": None,
                "updated_by": None
            }
            
            # Extract state from GSTIN if available
            if profile["gstin"]:
                state_code, state_name = extract_state_from_gstin(profile["gstin"])
                profile["state_code"] = state_code
                profile["state_name"] = state_name
            
            await db.customer_profiles.insert_one(profile)
            migrated += 1
            
        except Exception as e:
            errors.append({"user_id": user.get("id"), "error": str(e)})
    
    return {
        "message": "Migration completed",
        "migrated": migrated,
        "skipped": skipped,
        "errors": errors
    }


# =============== SEARCH FOR INVOICING ===============

@customer_master_router.get("/search/for-invoice")
async def search_customers_for_invoice(
    q: str = Query(..., min_length=2),
    current_user: dict = Depends(get_erp_user)
):
    """
    Quick search for customers when creating invoices/orders
    Returns minimal data needed for selection
    """
    db = get_db()
    
    profiles = await db.customer_profiles.find(
        {
            "status": "active",
            "$or": [
                {"display_name": {"$regex": q, "$options": "i"}},
                {"company_name": {"$regex": q, "$options": "i"}},
                {"individual_name": {"$regex": q, "$options": "i"}},
                {"mobile": {"$regex": q, "$options": "i"}},
                {"gstin": {"$regex": q, "$options": "i"}},
                {"customer_code": {"$regex": q, "$options": "i"}}
            ]
        },
        {
            "_id": 0,
            "id": 1,
            "customer_code": 1,
            "display_name": 1,
            "company_name": 1,
            "individual_name": 1,
            "mobile": 1,
            "email": 1,
            "gstin": 1,
            "gst_type": 1,
            "invoice_type": 1,
            "billing_address": 1,
            "credit_type": 1,
            "credit_limit": 1,
            "credit_days": 1
        }
    ).limit(10).to_list(10)
    
    return profiles
