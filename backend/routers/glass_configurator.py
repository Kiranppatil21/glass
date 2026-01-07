"""
3D Glass Configurator Backend
- Admin-configurable pricing (glass, thickness, color, holes/cut-outs)
- Quotation generation with validity (7/15 days)
- Role-based access
- Complete audit trail
- PDF Export for Production
"""
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone, timedelta
from bson import ObjectId
import uuid
import os
import io

# Import database and auth
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from routers.base import get_db, get_erp_user

# PDF generation
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
from reportlab.graphics.shapes import Drawing, Line, Rect, Circle, String
from reportlab.graphics import renderPDF

router = APIRouter(prefix="/glass-config", tags=["3D Glass Configurator"])

# ============== PRICING MODELS ==============

class GlassTypePricing(BaseModel):
    glass_type: str  # toughened, laminated, frosted, etc.
    base_price_per_sqft: float
    display_name: str
    description: Optional[str] = None
    active: bool = True

class ThicknessPricing(BaseModel):
    thickness_mm: int
    price_multiplier: float = 1.0  # Multiplier on base price
    display_name: str
    active: bool = True

class ColorPricing(BaseModel):
    color_id: str
    color_name: str
    hex_code: str
    price_percentage: float = 0  # Additional percentage (e.g., 10 for +10%)
    active: bool = True

class ApplicationPricing(BaseModel):
    application_id: str
    application_name: str
    price_multiplier: float = 1.0
    active: bool = True

class HoleCutoutPricing(BaseModel):
    """Shape + Size combo pricing for holes/cut-outs"""
    shape: str  # circle, square, rectangle
    size_slabs: List[Dict[str, Any]]  # [{min_mm: 0, max_mm: 20, price: 30}, ...]
    base_price: float  # Base price for this shape
    active: bool = True

class PricingConfig(BaseModel):
    glass_types: List[GlassTypePricing] = []
    thickness_options: List[ThicknessPricing] = []
    colors: List[ColorPricing] = []
    applications: List[ApplicationPricing] = []
    hole_cutout_pricing: List[HoleCutoutPricing] = []
    transport_base_charge: float = 500
    transport_per_km_rate: float = 15
    gst_rate: float = 18

# ============== QUOTATION MODELS ==============

class HoleCutoutSpec(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    shape: str  # circle, square, rectangle
    diameter_mm: Optional[float] = None  # For circle
    width_mm: Optional[float] = None  # For square/rectangle
    height_mm: Optional[float] = None  # For square/rectangle
    position_x_mm: float  # X position from left edge
    position_y_mm: float  # Y position from bottom edge
    # Calculated distances
    distance_left_mm: float = 0
    distance_right_mm: float = 0
    distance_top_mm: float = 0
    distance_bottom_mm: float = 0

class GlassConfigSpec(BaseModel):
    glass_type: str
    thickness_mm: int
    color_id: str
    color_name: str
    application: str
    width_mm: float
    height_mm: float
    holes_cutouts: List[HoleCutoutSpec] = []
    quantity: int = 1
    needs_transport: bool = False
    transport_address: Optional[str] = None
    transport_distance_km: Optional[float] = None

class QuotationCreate(BaseModel):
    config: GlassConfigSpec
    customer_profile_id: Optional[str] = None
    customer_name: str
    customer_mobile: str
    customer_email: Optional[str] = None
    validity_days: int = 7  # 7 or 15 days
    notes: Optional[str] = None
    # For job work
    is_job_work: bool = False
    job_work_type: Optional[str] = None  # toughening, lamination, etc.

class QuotationResponse(BaseModel):
    quotation_id: str
    quotation_number: str
    config: Dict[str, Any]
    price_breakdown: Dict[str, Any]
    total_amount: float
    gst_amount: float
    grand_total: float
    validity_days: int
    valid_until: str
    status: str
    created_by: str
    created_at: str

# ============== PRICING ENDPOINTS ==============

@router.get("/pricing")
async def get_pricing_config(db=Depends(get_db)):
    """Get current pricing configuration - Public access for customers"""
    config = await db.glass_pricing_config.find_one({"active": True})
    
    if not config:
        # Return default config if none exists
        return {
            "glass_types": [
                {"glass_type": "toughened", "base_price_per_sqft": 85, "display_name": "Toughened Glass", "active": True},
                {"glass_type": "laminated", "base_price_per_sqft": 120, "display_name": "Laminated Glass", "active": True},
                {"glass_type": "frosted", "base_price_per_sqft": 95, "display_name": "Frosted Glass", "active": True},
            ],
            "thickness_options": [
                {"thickness_mm": 5, "price_multiplier": 1.0, "display_name": "5mm", "active": True},
                {"thickness_mm": 6, "price_multiplier": 1.1, "display_name": "6mm", "active": True},
                {"thickness_mm": 8, "price_multiplier": 1.25, "display_name": "8mm", "active": True},
                {"thickness_mm": 10, "price_multiplier": 1.4, "display_name": "10mm", "active": True},
                {"thickness_mm": 12, "price_multiplier": 1.6, "display_name": "12mm", "active": True},
            ],
            "colors": [
                {"color_id": "clear", "color_name": "Clear", "hex_code": "#E8E8E8", "price_percentage": 0, "active": True},
                {"color_id": "grey", "color_name": "Grey", "hex_code": "#808080", "price_percentage": 10, "active": True},
                {"color_id": "bronze", "color_name": "Bronze", "hex_code": "#CD7F32", "price_percentage": 15, "active": True},
                {"color_id": "blue", "color_name": "Blue", "hex_code": "#4169E1", "price_percentage": 15, "active": True},
                {"color_id": "green", "color_name": "Green", "hex_code": "#228B22", "price_percentage": 10, "active": True},
            ],
            "applications": [
                {"application_id": "window", "application_name": "Window", "price_multiplier": 1.0, "active": True},
                {"application_id": "door", "application_name": "Door", "price_multiplier": 1.0, "active": True},
                {"application_id": "partition", "application_name": "Partition", "price_multiplier": 1.0, "active": True},
                {"application_id": "railing", "application_name": "Railing", "price_multiplier": 1.1, "active": True},
                {"application_id": "shower", "application_name": "Shower Enclosure", "price_multiplier": 1.15, "active": True},
                {"application_id": "other", "application_name": "Other", "price_multiplier": 1.0, "active": True},
            ],
            "hole_cutout_pricing": [
                {
                    "shape": "circle",
                    "base_price": 30,
                    "size_slabs": [
                        {"min_mm": 0, "max_mm": 20, "price": 30},
                        {"min_mm": 21, "max_mm": 50, "price": 50},
                        {"min_mm": 51, "max_mm": 100, "price": 80},
                        {"min_mm": 101, "max_mm": 500, "price": 120},
                    ],
                    "active": True
                },
                {
                    "shape": "square",
                    "base_price": 50,
                    "size_slabs": [
                        {"min_mm": 0, "max_mm": 50, "price": 50},
                        {"min_mm": 51, "max_mm": 100, "price": 80},
                        {"min_mm": 101, "max_mm": 200, "price": 120},
                        {"min_mm": 201, "max_mm": 500, "price": 180},
                    ],
                    "active": True
                },
                {
                    "shape": "rectangle",
                    "base_price": 60,
                    "size_slabs": [
                        {"min_mm": 0, "max_mm": 50, "price": 60},
                        {"min_mm": 51, "max_mm": 100, "price": 100},
                        {"min_mm": 101, "max_mm": 200, "price": 150},
                        {"min_mm": 201, "max_mm": 500, "price": 220},
                    ],
                    "active": True
                },
            ],
            "transport_base_charge": 500,
            "transport_per_km_rate": 15,
            "gst_rate": 18
        }
    
    config.pop("_id", None)
    return config


@router.put("/pricing")
async def update_pricing_config(config: PricingConfig, db=Depends(get_db), user=Depends(get_erp_user)):
    """Update pricing configuration - Admin/Super Admin only"""
    if user.get("role") not in ["admin", "super_admin"]:
        raise HTTPException(status_code=403, detail="Only Admin/Super Admin can update pricing")
    
    # Deactivate old config
    await db.glass_pricing_config.update_many({"active": True}, {"$set": {"active": False}})
    
    # Create new config
    config_dict = config.dict()
    config_dict["active"] = True
    config_dict["created_at"] = datetime.now(timezone.utc).isoformat()
    config_dict["created_by"] = user.get("email")
    config_dict["version"] = str(uuid.uuid4())[:8]
    
    await db.glass_pricing_config.insert_one(config_dict)
    
    # Audit log
    await db.glass_config_audit.insert_one({
        "action": "pricing_updated",
        "user_email": user.get("email"),
        "user_role": user.get("role"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "details": "Pricing configuration updated"
    })
    
    return {"message": "Pricing configuration updated successfully", "version": config_dict["version"]}


# ============== PRICE CALCULATION ==============

@router.post("/calculate-price")
async def calculate_price(config: GlassConfigSpec, db=Depends(get_db)):
    """Calculate live price based on configuration - Public access"""
    pricing = await get_pricing_config(db)
    
    # Find glass type pricing
    glass_type_config = next((g for g in pricing["glass_types"] if g["glass_type"] == config.glass_type), None)
    if not glass_type_config:
        raise HTTPException(status_code=400, detail=f"Invalid glass type: {config.glass_type}")
    
    base_price_per_sqft = glass_type_config["base_price_per_sqft"]
    
    # Find thickness multiplier
    thickness_config = next((t for t in pricing["thickness_options"] if t["thickness_mm"] == config.thickness_mm), None)
    thickness_multiplier = thickness_config["price_multiplier"] if thickness_config else 1.0
    
    # Find color percentage
    color_config = next((c for c in pricing["colors"] if c["color_id"] == config.color_id), None)
    color_percentage = color_config["price_percentage"] if color_config else 0
    
    # Find application multiplier
    app_config = next((a for a in pricing["applications"] if a["application_id"] == config.application), None)
    app_multiplier = app_config["price_multiplier"] if app_config else 1.0
    
    # Calculate glass area in sq ft (from mm)
    area_sqmm = config.width_mm * config.height_mm
    area_sqft = area_sqmm / 92903.04  # 1 sq ft = 92903.04 sq mm
    
    # Base glass price
    glass_price = base_price_per_sqft * area_sqft * thickness_multiplier * app_multiplier
    
    # Apply color percentage
    glass_price = glass_price * (1 + color_percentage / 100)
    
    # Calculate holes/cut-outs price
    holes_price = 0
    hole_details = []
    
    for hole in config.holes_cutouts:
        hole_pricing = next((h for h in pricing["hole_cutout_pricing"] if h["shape"] == hole.shape), None)
        if hole_pricing:
            # Determine size for slab pricing
            if hole.shape == "circle":
                size_mm = hole.diameter_mm or 0
            else:
                size_mm = max(hole.width_mm or 0, hole.height_mm or 0)
            
            # Find appropriate slab
            slab_price = hole_pricing["base_price"]
            for slab in hole_pricing["size_slabs"]:
                if slab["min_mm"] <= size_mm <= slab["max_mm"]:
                    slab_price = slab["price"]
                    break
            
            holes_price += slab_price
            hole_details.append({
                "id": hole.id,
                "shape": hole.shape,
                "size_mm": size_mm,
                "price": slab_price
            })
    
    # Quantity multiplier
    subtotal = (glass_price + holes_price) * config.quantity
    
    # Transport charges
    transport_charges = 0
    if config.needs_transport and config.transport_distance_km:
        transport_charges = pricing["transport_base_charge"] + (pricing["transport_per_km_rate"] * config.transport_distance_km)
    
    # GST
    taxable_amount = subtotal + transport_charges
    gst_amount = taxable_amount * (pricing["gst_rate"] / 100)
    grand_total = taxable_amount + gst_amount
    
    return {
        "price_breakdown": {
            "glass_type": config.glass_type,
            "base_price_per_sqft": base_price_per_sqft,
            "area_sqft": round(area_sqft, 2),
            "thickness_multiplier": thickness_multiplier,
            "color_percentage": color_percentage,
            "application_multiplier": app_multiplier,
            "glass_price": round(glass_price, 2),
            "holes_cutouts": hole_details,
            "holes_total": round(holes_price, 2),
            "quantity": config.quantity,
            "subtotal": round(subtotal, 2),
            "transport_charges": round(transport_charges, 2),
            "taxable_amount": round(taxable_amount, 2),
            "gst_rate": pricing["gst_rate"],
            "gst_amount": round(gst_amount, 2),
            "grand_total": round(grand_total, 2)
        },
        "total": round(grand_total, 2)
    }


# ============== QUOTATION ENDPOINTS ==============

@router.post("/quotation")
async def create_quotation(data: QuotationCreate, db=Depends(get_db), user=Depends(get_erp_user)):
    """Create a new quotation"""
    # Check role permissions
    allowed_roles = ["admin", "super_admin", "sales", "sales_manager", "marketing", "accounts", "customer"]
    if user.get("role") not in allowed_roles:
        raise HTTPException(status_code=403, detail="Not authorized to create quotations")
    
    # Calculate price
    price_result = await calculate_price(data.config, db)
    
    # Generate quotation number
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    count = await db.glass_quotations.count_documents({"quotation_number": {"$regex": f"^QT-{today}"}})
    quotation_number = f"QT-{today}-{str(count + 1).zfill(4)}"
    
    # Calculate validity
    valid_until = datetime.now(timezone.utc) + timedelta(days=data.validity_days)
    
    quotation = {
        "quotation_id": str(uuid.uuid4()),
        "quotation_number": quotation_number,
        "config": data.config.dict(),
        "price_breakdown": price_result["price_breakdown"],
        "total_amount": price_result["price_breakdown"]["subtotal"],
        "gst_amount": price_result["price_breakdown"]["gst_amount"],
        "grand_total": price_result["total"],
        "customer_profile_id": data.customer_profile_id,
        "customer_name": data.customer_name,
        "customer_mobile": data.customer_mobile,
        "customer_email": data.customer_email,
        "validity_days": data.validity_days,
        "valid_until": valid_until.isoformat(),
        "status": "draft",  # draft, sent, viewed, approved, rejected, expired, converted
        "is_job_work": data.is_job_work,
        "job_work_type": data.job_work_type,
        "notes": data.notes,
        "created_by": user.get("email"),
        "created_by_role": user.get("role"),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "version_history": []
    }
    
    await db.glass_quotations.insert_one(quotation)
    
    # Audit log
    await db.glass_config_audit.insert_one({
        "action": "quotation_created",
        "quotation_id": quotation["quotation_id"],
        "quotation_number": quotation_number,
        "user_email": user.get("email"),
        "user_role": user.get("role"),
        "customer_name": data.customer_name,
        "grand_total": price_result["total"],
        "timestamp": datetime.now(timezone.utc).isoformat()
    })
    
    quotation.pop("_id", None)
    return quotation


@router.get("/quotations")
async def list_quotations(
    status: Optional[str] = None,
    is_job_work: Optional[bool] = None,
    customer_mobile: Optional[str] = None,
    skip: int = 0,
    limit: int = 50,
    db=Depends(get_db),
    user=Depends(get_erp_user)
):
    """List quotations based on role"""
    query = {}
    
    # Role-based filtering
    if user.get("role") == "customer":
        # Customers can only see their own quotations
        query["$or"] = [
            {"customer_mobile": user.get("phone")},
            {"customer_email": user.get("email")},
            {"created_by": user.get("email")}
        ]
    
    if status:
        query["status"] = status
    if is_job_work is not None:
        query["is_job_work"] = is_job_work
    if customer_mobile:
        query["customer_mobile"] = customer_mobile
    
    cursor = db.glass_quotations.find(query, {"_id": 0}).sort("created_at", -1).skip(skip).limit(limit)
    quotations = await cursor.to_list(length=limit)
    total = await db.glass_quotations.count_documents(query)
    
    return {"quotations": quotations, "total": total}


@router.get("/quotation/{quotation_id}")
async def get_quotation(quotation_id: str, db=Depends(get_db), user=Depends(get_erp_user)):
    """Get quotation details"""
    quotation = await db.glass_quotations.find_one({"quotation_id": quotation_id}, {"_id": 0})
    
    if not quotation:
        raise HTTPException(status_code=404, detail="Quotation not found")
    
    # Update status to viewed if customer is viewing
    if user.get("role") == "customer" and quotation["status"] == "sent":
        await db.glass_quotations.update_one(
            {"quotation_id": quotation_id},
            {"$set": {"status": "viewed", "viewed_at": datetime.now(timezone.utc).isoformat()}}
        )
        quotation["status"] = "viewed"
    
    return quotation


@router.post("/quotation/{quotation_id}/send")
async def send_quotation(quotation_id: str, db=Depends(get_db), user=Depends(get_erp_user)):
    """Mark quotation as sent"""
    result = await db.glass_quotations.update_one(
        {"quotation_id": quotation_id},
        {"$set": {"status": "sent", "sent_at": datetime.now(timezone.utc).isoformat(), "sent_by": user.get("email")}}
    )
    
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="Quotation not found")
    
    # Audit log
    await db.glass_config_audit.insert_one({
        "action": "quotation_sent",
        "quotation_id": quotation_id,
        "user_email": user.get("email"),
        "timestamp": datetime.now(timezone.utc).isoformat()
    })
    
    return {"message": "Quotation marked as sent"}


@router.post("/quotation/{quotation_id}/approve")
async def approve_quotation(quotation_id: str, db=Depends(get_db), user=Depends(get_erp_user)):
    """Customer approves quotation"""
    quotation = await db.glass_quotations.find_one({"quotation_id": quotation_id})
    
    if not quotation:
        raise HTTPException(status_code=404, detail="Quotation not found")
    
    # Check if expired
    valid_until = datetime.fromisoformat(quotation["valid_until"].replace("Z", "+00:00"))
    if datetime.now(timezone.utc) > valid_until:
        await db.glass_quotations.update_one({"quotation_id": quotation_id}, {"$set": {"status": "expired"}})
        raise HTTPException(status_code=400, detail="Quotation has expired")
    
    await db.glass_quotations.update_one(
        {"quotation_id": quotation_id},
        {"$set": {
            "status": "approved",
            "approved_at": datetime.now(timezone.utc).isoformat(),
            "approved_by": user.get("email")
        }}
    )
    
    # Audit log
    await db.glass_config_audit.insert_one({
        "action": "quotation_approved",
        "quotation_id": quotation_id,
        "user_email": user.get("email"),
        "timestamp": datetime.now(timezone.utc).isoformat()
    })
    
    return {"message": "Quotation approved successfully"}


@router.post("/quotation/{quotation_id}/reject")
async def reject_quotation(quotation_id: str, reason: Optional[str] = None, db=Depends(get_db), user=Depends(get_erp_user)):
    """Reject quotation with optional reason"""
    result = await db.glass_quotations.update_one(
        {"quotation_id": quotation_id},
        {"$set": {
            "status": "rejected",
            "rejected_at": datetime.now(timezone.utc).isoformat(),
            "rejected_by": user.get("email"),
            "rejection_reason": reason
        }}
    )
    
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="Quotation not found")
    
    # Audit log
    await db.glass_config_audit.insert_one({
        "action": "quotation_rejected",
        "quotation_id": quotation_id,
        "user_email": user.get("email"),
        "reason": reason,
        "timestamp": datetime.now(timezone.utc).isoformat()
    })
    
    return {"message": "Quotation rejected"}


@router.post("/quotation/{quotation_id}/convert-to-order")
async def convert_to_order(quotation_id: str, db=Depends(get_db), user=Depends(get_erp_user)):
    """Convert approved quotation to order"""
    quotation = await db.glass_quotations.find_one({"quotation_id": quotation_id})
    
    if not quotation:
        raise HTTPException(status_code=404, detail="Quotation not found")
    
    if quotation["status"] != "approved":
        raise HTTPException(status_code=400, detail="Only approved quotations can be converted to orders")
    
    # Generate order number
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    count = await db.orders.count_documents({"order_number": {"$regex": f"^ORD-{today}"}})
    order_number = f"ORD-{today}-{str(count + 1).zfill(4)}"
    
    # Create order from quotation
    order = {
        "order_id": str(uuid.uuid4()),
        "order_number": order_number,
        "quotation_id": quotation_id,
        "quotation_number": quotation["quotation_number"],
        "config": quotation["config"],
        "price_breakdown": quotation["price_breakdown"],
        "total_amount": quotation["total_amount"],
        "gst_amount": quotation["gst_amount"],
        "grand_total": quotation["grand_total"],
        "customer_profile_id": quotation.get("customer_profile_id"),
        "customer_name": quotation["customer_name"],
        "customer_mobile": quotation["customer_mobile"],
        "customer_email": quotation.get("customer_email"),
        "is_job_work": quotation.get("is_job_work", False),
        "job_work_type": quotation.get("job_work_type"),
        "status": "pending_payment",
        "payment_status": "pending",
        "created_from": "quotation",
        "created_by": user.get("email"),
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.glass_orders.insert_one(order)
    
    # Update quotation status
    await db.glass_quotations.update_one(
        {"quotation_id": quotation_id},
        {"$set": {
            "status": "converted",
            "converted_to_order": order_number,
            "converted_at": datetime.now(timezone.utc).isoformat(),
            "converted_by": user.get("email")
        }}
    )
    
    # Audit log
    await db.glass_config_audit.insert_one({
        "action": "quotation_converted_to_order",
        "quotation_id": quotation_id,
        "order_number": order_number,
        "user_email": user.get("email"),
        "timestamp": datetime.now(timezone.utc).isoformat()
    })
    
    order.pop("_id", None)
    return {"message": "Order created successfully", "order": order}


# ============== AUDIT TRAIL ==============

@router.get("/audit-trail")
async def get_audit_trail(
    action: Optional[str] = None,
    user_email: Optional[str] = None,
    skip: int = 0,
    limit: int = 100,
    db=Depends(get_db),
    user=Depends(get_erp_user)
):
    """Get audit trail - Admin/Super Admin only"""
    if user.get("role") not in ["admin", "super_admin", "accounts"]:
        raise HTTPException(status_code=403, detail="Not authorized to view audit trail")
    
    query = {}
    if action:
        query["action"] = action
    if user_email:
        query["user_email"] = user_email
    
    cursor = db.glass_config_audit.find(query, {"_id": 0}).sort("timestamp", -1).skip(skip).limit(limit)
    logs = await cursor.to_list(length=limit)
    total = await db.glass_config_audit.count_documents(query)
    
    return {"logs": logs, "total": total}


# ============== JOB WORK SPECIFIC ==============

@router.get("/job-work/labour-rates")
async def get_job_work_labour_rates(db=Depends(get_db)):
    """Get job work labour rates by thickness"""
    rates = await db.job_work_rates.find_one({"active": True}, {"_id": 0})
    
    if not rates:
        return {
            "rates": {
                "4": 8, "5": 10, "6": 12, "8": 15,
                "10": 18, "12": 22, "15": 28, "19": 35
            },
            "hole_rates": {
                "circle": {"base": 20, "per_mm": 0.5},
                "square": {"base": 30, "per_mm": 0.6},
                "rectangle": {"base": 35, "per_mm": 0.6}
            },
            "transport_pickup_base": 300,
            "transport_drop_base": 300,
            "transport_per_km": 12
        }
    
    return rates


@router.put("/job-work/labour-rates")
async def update_job_work_labour_rates(rates: Dict[str, Any], db=Depends(get_db), user=Depends(get_erp_user)):
    """Update job work labour rates - Admin only"""
    if user.get("role") not in ["admin", "super_admin"]:
        raise HTTPException(status_code=403, detail="Only Admin can update labour rates")
    
    await db.job_work_rates.update_many({"active": True}, {"$set": {"active": False}})
    
    rates["active"] = True
    rates["updated_at"] = datetime.now(timezone.utc).isoformat()
    rates["updated_by"] = user.get("email")
    
    await db.job_work_rates.insert_one(rates)
    
    return {"message": "Job work rates updated"}



# ============== PDF EXPORT ==============

class CutoutExportSpec(BaseModel):
    number: str  # e.g., "H1", "R2"
    type: str  # Shape name
    diameter: Optional[float] = None
    width: Optional[float] = None
    height: Optional[float] = None
    x: float
    y: float
    rotation: float = 0
    left_edge: float
    right_edge: float
    top_edge: float
    bottom_edge: float

class GlassExportSpec(BaseModel):
    width_mm: float
    height_mm: float
    thickness_mm: int
    glass_type: str
    color_name: str
    application: str

class PDFExportRequest(BaseModel):
    glass_config: GlassExportSpec
    cutouts: List[CutoutExportSpec]
    quantity: int = 1

@router.post("/export-pdf")
async def export_pdf(data: PDFExportRequest, user=Depends(get_erp_user)):
    """Generate a PDF specification sheet for the glass configuration"""
    try:
        buffer = io.BytesIO()
        
        # Create PDF document
        doc = SimpleDocTemplate(
            buffer, 
            pagesize=A4,
            rightMargin=20*mm,
            leftMargin=20*mm,
            topMargin=20*mm,
            bottomMargin=20*mm
        )
        
        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=18,
            alignment=1,  # Center
            spaceAfter=10*mm
        )
        subtitle_style = ParagraphStyle(
            'CustomSubtitle',
            parent=styles['Normal'],
            fontSize=10,
            textColor=colors.gray,
            alignment=1,
            spaceAfter=5*mm
        )
        
        elements = []
        
        # Title
        elements.append(Paragraph("Glass Specification Sheet", title_style))
        elements.append(Paragraph(f"Generated on {datetime.now().strftime('%d %b %Y, %H:%M')}", subtitle_style))
        elements.append(Spacer(1, 5*mm))
        
        # Glass Configuration Table
        glass_data = [
            ["Glass Properties", "Value"],
            ["Dimensions (W × H)", f"{data.glass_config.width_mm} × {data.glass_config.height_mm} mm"],
            ["Thickness", f"{data.glass_config.thickness_mm} mm"],
            ["Glass Type", data.glass_config.glass_type.title()],
            ["Color", data.glass_config.color_name],
            ["Application", data.glass_config.application.title()],
            ["Quantity", str(data.quantity)],
        ]
        
        glass_table = Table(glass_data, colWidths=[80*mm, 80*mm])
        glass_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#3B82F6')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 11),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 10),
            ('TOPPADDING', (0, 0), (-1, 0), 10),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#F8FAFC')),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#E2E8F0')),
            ('FONTSIZE', (0, 1), (-1, -1), 10),
            ('TOPPADDING', (0, 1), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 1), (-1, -1), 6),
        ]))
        elements.append(glass_table)
        elements.append(Spacer(1, 8*mm))
        
        # 2D Technical Drawing
        drawing_width = 160*mm
        drawing_height = 100*mm
        
        # Calculate scale for drawing
        glass_w = data.glass_config.width_mm
        glass_h = data.glass_config.height_mm
        margin = 20*mm
        usable_width = drawing_width - 2*margin
        usable_height = drawing_height - 2*margin
        scale = min(usable_width / glass_w, usable_height / glass_h)
        
        # Center the drawing
        offset_x = margin + (usable_width - glass_w * scale) / 2
        offset_y = margin + (usable_height - glass_h * scale) / 2
        
        drawing = Drawing(drawing_width, drawing_height)
        
        # Background
        drawing.add(Rect(0, 0, drawing_width, drawing_height, fillColor=colors.white, strokeColor=colors.HexColor('#E2E8F0')))
        
        # Glass rectangle
        drawing.add(Rect(
            offset_x, offset_y, 
            glass_w * scale, glass_h * scale,
            fillColor=colors.HexColor('#E8F4F8'),
            strokeColor=colors.HexColor('#3B82F6'),
            strokeWidth=2
        ))
        
        # Cutout colors
        cutout_colors = {
            'Hole': colors.HexColor('#3B82F6'),
            'Rectangle': colors.HexColor('#22C55E'),
            'Triangle': colors.HexColor('#F59E0B'),
            'Hexagon': colors.HexColor('#8B5CF6'),
            'Heart': colors.HexColor('#EC4899'),
        }
        
        # Draw cutouts
        for cutout in data.cutouts:
            cx = offset_x + cutout.x * scale
            cy = offset_y + cutout.y * scale
            cutout_color = cutout_colors.get(cutout.type, colors.HexColor('#3B82F6'))
            
            if cutout.diameter:
                # Circular cutout
                radius = (cutout.diameter / 2) * scale
                drawing.add(Circle(cx, cy, radius, fillColor=cutout_color, strokeColor=colors.black, strokeWidth=1))
            else:
                # Rectangular cutout
                w = cutout.width * scale
                h = cutout.height * scale
                drawing.add(Rect(cx - w/2, cy - h/2, w, h, fillColor=cutout_color, strokeColor=colors.black, strokeWidth=1))
            
            # Center mark
            mark_size = 3*mm
            drawing.add(Line(cx - mark_size, cy, cx + mark_size, cy, strokeColor=colors.red, strokeWidth=0.5))
            drawing.add(Line(cx, cy - mark_size, cx, cy + mark_size, strokeColor=colors.red, strokeWidth=0.5))
            
            # Label
            drawing.add(String(cx, cy + 4*mm, cutout.number, fontSize=7, fillColor=colors.white, textAnchor='middle'))
        
        # Dimension lines
        # Width dimension (bottom)
        dim_offset = 8*mm
        drawing.add(Line(offset_x, offset_y - dim_offset, offset_x + glass_w * scale, offset_y - dim_offset, strokeColor=colors.black, strokeWidth=0.5))
        drawing.add(Line(offset_x, offset_y - dim_offset - 2*mm, offset_x, offset_y - dim_offset + 2*mm, strokeColor=colors.black, strokeWidth=0.5))
        drawing.add(Line(offset_x + glass_w * scale, offset_y - dim_offset - 2*mm, offset_x + glass_w * scale, offset_y - dim_offset + 2*mm, strokeColor=colors.black, strokeWidth=0.5))
        drawing.add(String(offset_x + (glass_w * scale) / 2, offset_y - dim_offset - 4*mm, f"{glass_w} mm", fontSize=8, textAnchor='middle'))
        
        # Height dimension (left)
        drawing.add(Line(offset_x - dim_offset, offset_y, offset_x - dim_offset, offset_y + glass_h * scale, strokeColor=colors.black, strokeWidth=0.5))
        drawing.add(Line(offset_x - dim_offset - 2*mm, offset_y, offset_x - dim_offset + 2*mm, offset_y, strokeColor=colors.black, strokeWidth=0.5))
        drawing.add(Line(offset_x - dim_offset - 2*mm, offset_y + glass_h * scale, offset_x - dim_offset + 2*mm, offset_y + glass_h * scale, strokeColor=colors.black, strokeWidth=0.5))
        
        elements.append(drawing)
        elements.append(Spacer(1, 5*mm))
        
        # Cutout Details Table
        if data.cutouts:
            elements.append(Paragraph("<b>Cutout Specifications</b>", styles['Heading2']))
            elements.append(Spacer(1, 3*mm))
            
            cutout_headers = ["#", "Type", "Size (mm)", "Position (X,Y)", "Edges: L/R/T/B"]
            cutout_rows = [cutout_headers]
            
            for cutout in data.cutouts:
                size = f"Ø{cutout.diameter:.0f}" if cutout.diameter else f"{cutout.width:.0f}×{cutout.height:.0f}"
                edges = f"{cutout.left_edge:.0f}/{cutout.right_edge:.0f}/{cutout.top_edge:.0f}/{cutout.bottom_edge:.0f}"
                cutout_rows.append([
                    cutout.number,
                    cutout.type,
                    size,
                    f"({cutout.x:.0f}, {cutout.y:.0f})",
                    edges
                ])
            
            cutout_table = Table(cutout_rows, colWidths=[20*mm, 35*mm, 35*mm, 35*mm, 45*mm])
            cutout_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1E293B')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 9),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
                ('TOPPADDING', (0, 0), (-1, 0), 8),
                ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#CBD5E1')),
                ('FONTSIZE', (0, 1), (-1, -1), 8),
                ('TOPPADDING', (0, 1), (-1, -1), 5),
                ('BOTTOMPADDING', (0, 1), (-1, -1), 5),
            ]))
            elements.append(cutout_table)
        
        elements.append(Spacer(1, 10*mm))
        
        # Footer
        footer_text = f"Document generated by Glass Configurator | User: {user.get('email', 'N/A')}"
        elements.append(Paragraph(f"<para align='center'><font size='8' color='gray'>{footer_text}</font></para>", styles['Normal']))
        
        # Build PDF
        doc.build(elements)
        buffer.seek(0)
        
        return StreamingResponse(
            buffer,
            media_type="application/pdf",
            headers={"Content-Disposition": f"attachment; filename=glass_specification_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"}
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate PDF: {str(e)}")


# ============== EMAIL QUOTATION ==============

class EmailQuotationRequest(BaseModel):
    quotation_id: str
    cc_sales_team: bool = True

@router.post("/quotation/{quotation_id}/send-email")
async def send_quotation_email(quotation_id: str, db=Depends(get_db), user=Depends(get_erp_user)):
    """Send quotation via email with PDF attachment"""
    import aiosmtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from email.mime.base import MIMEBase
    from email import encoders
    
    # Get quotation
    quotation = await db.glass_quotations.find_one({"quotation_id": quotation_id})
    if not quotation:
        raise HTTPException(status_code=404, detail="Quotation not found")
    
    customer_email = quotation.get("customer_email")
    if not customer_email:
        raise HTTPException(status_code=400, detail="Customer email not available")
    
    # Generate PDF
    pdf_buffer = await generate_quotation_pdf(quotation, db)
    
    # Email configuration
    SMTP_HOST = os.environ.get('SMTP_HOST', 'smtp.hostinger.com')
    SMTP_PORT = int(os.environ.get('SMTP_PORT', 465))
    SMTP_USER = os.environ.get('SMTP_USER', 'info@lucumaaGlass.in')
    SMTP_PASSWORD = os.environ.get('SMTP_PASSWORD', '')
    SENDER_EMAIL = os.environ.get('SENDER_EMAIL', 'info@lucumaaGlass.in')
    SALES_TEAM_EMAIL = os.environ.get('SALES_TEAM_EMAIL', 'sales@lucumaaglass.in')
    
    # Create email
    msg = MIMEMultipart()
    msg['From'] = f"Lucumaa Glass <{SENDER_EMAIL}>"
    msg['To'] = customer_email
    msg['Subject'] = f"Your Glass Quotation #{quotation['quotation_number']} - Lucumaa Glass"
    
    # CC sales team
    cc_list = [SALES_TEAM_EMAIL] if SALES_TEAM_EMAIL else []
    if cc_list:
        msg['Cc'] = ', '.join(cc_list)
    
    # Email body
    price_breakdown = quotation.get('price_breakdown', {})
    html_body = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
            .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
            .header {{ background: linear-gradient(135deg, #1e3a8a, #3b82f6); color: white; padding: 30px; text-align: center; border-radius: 10px 10px 0 0; }}
            .content {{ background: #f8fafc; padding: 30px; border-radius: 0 0 10px 10px; }}
            .price-box {{ background: white; border: 2px solid #3b82f6; border-radius: 10px; padding: 20px; margin: 20px 0; }}
            .total {{ font-size: 28px; color: #1e3a8a; font-weight: bold; }}
            .details {{ background: white; padding: 15px; border-radius: 8px; margin: 15px 0; }}
            .footer {{ text-align: center; color: #64748b; font-size: 12px; margin-top: 20px; }}
            .btn {{ display: inline-block; background: #3b82f6; color: white; padding: 12px 30px; text-decoration: none; border-radius: 6px; margin-top: 15px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>Your Glass Quotation</h1>
                <p>Quotation #{quotation['quotation_number']}</p>
            </div>
            <div class="content">
                <p>Dear <strong>{quotation.get('customer_name', 'Valued Customer')}</strong>,</p>
                <p>Thank you for your interest in Lucumaa Glass! Please find your quotation details below:</p>
                
                <div class="price-box">
                    <p style="margin: 0; color: #64748b;">Total Amount</p>
                    <p class="total">₹{quotation.get('grand_total', 0):,.2f}</p>
                    <p style="margin: 0; color: #64748b; font-size: 12px;">Including GST @ {price_breakdown.get('gst_rate', 18)}%</p>
                </div>
                
                <div class="details">
                    <h3 style="margin-top: 0;">Configuration Details</h3>
                    <p><strong>Glass Type:</strong> {quotation.get('config', {}).get('glass_type', 'N/A').title()}</p>
                    <p><strong>Thickness:</strong> {quotation.get('config', {}).get('thickness_mm', 'N/A')}mm</p>
                    <p><strong>Size:</strong> {quotation.get('config', {}).get('width_mm', 0)} × {quotation.get('config', {}).get('height_mm', 0)} mm</p>
                    <p><strong>Quantity:</strong> {quotation.get('config', {}).get('quantity', 1)} pcs</p>
                    <p><strong>Valid Until:</strong> {quotation.get('valid_until', 'N/A')[:10]}</p>
                </div>
                
                <p>The detailed specification sheet is attached as a PDF for your reference.</p>
                
                <p>If you have any questions or would like to proceed with this order, please don't hesitate to contact us.</p>
                
                <p>Best regards,<br><strong>Lucumaa Glass Team</strong></p>
            </div>
            <div class="footer">
                <p>Lucumaa Glass | Premium Glass Manufacturing</p>
                <p>📞 +91 92847 01985 | ✉️ info@lucumaaglass.in</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    msg.attach(MIMEText(html_body, 'html'))
    
    # Attach PDF
    pdf_attachment = MIMEBase('application', 'pdf')
    pdf_attachment.set_payload(pdf_buffer.getvalue())
    encoders.encode_base64(pdf_attachment)
    pdf_attachment.add_header('Content-Disposition', f'attachment; filename="Quotation_{quotation["quotation_number"]}.pdf"')
    msg.attach(pdf_attachment)
    
    try:
        # Send email
        recipients = [customer_email] + cc_list
        await aiosmtplib.send(
            msg,
            hostname=SMTP_HOST,
            port=SMTP_PORT,
            username=SMTP_USER,
            password=SMTP_PASSWORD,
            use_tls=True
        )
        
        # Update quotation with sent timestamp
        await db.glass_quotations.update_one(
            {"quotation_id": quotation_id},
            {"$set": {
                "status": "sent",
                "email_sent_at": datetime.now(timezone.utc).isoformat(),
                "email_sent_to": customer_email,
                "email_cc": cc_list,
                "email_sent_by": user.get("email")
            }}
        )
        
        # Audit log
        await db.glass_config_audit.insert_one({
            "action": "quotation_email_sent",
            "quotation_id": quotation_id,
            "recipient": customer_email,
            "cc": cc_list,
            "user_email": user.get("email"),
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        
        return {"message": "Quotation sent successfully", "sent_to": customer_email, "cc": cc_list}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to send email: {str(e)}")


async def generate_quotation_pdf(quotation: dict, db) -> io.BytesIO:
    """Generate PDF for quotation (reusable helper)"""
    buffer = io.BytesIO()
    
    doc = SimpleDocTemplate(
        buffer, pagesize=A4,
        rightMargin=20*mm, leftMargin=20*mm,
        topMargin=20*mm, bottomMargin=20*mm
    )
    
    styles = getSampleStyleSheet()
    elements = []
    
    # Title
    title_style = ParagraphStyle('Title', parent=styles['Heading1'], fontSize=18, alignment=1, spaceAfter=5*mm)
    elements.append(Paragraph(f"Quotation #{quotation.get('quotation_number', 'N/A')}", title_style))
    elements.append(Paragraph(f"<para align='center'><font size='10' color='gray'>Valid until: {quotation.get('valid_until', 'N/A')[:10]}</font></para>", styles['Normal']))
    elements.append(Spacer(1, 10*mm))
    
    # Customer & Config info
    config = quotation.get('config', {})
    info_data = [
        ["Customer", quotation.get('customer_name', 'N/A'), "Glass Type", config.get('glass_type', 'N/A').title()],
        ["Mobile", quotation.get('customer_mobile', 'N/A'), "Thickness", f"{config.get('thickness_mm', 'N/A')}mm"],
        ["Email", quotation.get('customer_email', 'N/A'), "Size", f"{config.get('width_mm', 0)} × {config.get('height_mm', 0)} mm"],
    ]
    info_table = Table(info_data, colWidths=[30*mm, 55*mm, 30*mm, 55*mm])
    info_table.setStyle(TableStyle([
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTNAME', (2, 0), (2, -1), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
    ]))
    elements.append(info_table)
    elements.append(Spacer(1, 10*mm))
    
    # Price breakdown
    price = quotation.get('price_breakdown', {})
    price_data = [
        ["Description", "Amount"],
        ["Glass Price", f"₹{price.get('glass_price', 0):,.2f}"],
        ["Holes/Cutouts", f"₹{price.get('holes_total', 0):,.2f}"],
        ["Subtotal", f"₹{price.get('subtotal', 0):,.2f}"],
        [f"GST ({price.get('gst_rate', 18)}%)", f"₹{price.get('gst_amount', 0):,.2f}"],
        ["Grand Total", f"₹{quotation.get('grand_total', 0):,.2f}"],
    ]
    price_table = Table(price_data, colWidths=[100*mm, 70*mm])
    price_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#3B82F6')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, -1), (-1, -1), 12),
        ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor('#EFF6FF')),
        ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#E2E8F0')),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
    ]))
    elements.append(price_table)
    
    doc.build(elements)
    buffer.seek(0)
    return buffer


# ============== SHARE CONFIGURATION ==============

class ShareConfigRequest(BaseModel):
    glass_config: Dict[str, Any]
    cutouts: List[Dict[str, Any]]
    title: Optional[str] = None

class SharedConfigResponse(BaseModel):
    share_id: str
    share_url: str
    expires_at: str

@router.post("/share", response_model=SharedConfigResponse)
async def create_shared_config(data: ShareConfigRequest, db=Depends(get_db)):
    """Create a shareable configuration link"""
    share_id = str(uuid.uuid4())[:8]
    expires_at = datetime.now(timezone.utc) + timedelta(days=7)
    
    shared_config = {
        "share_id": share_id,
        "glass_config": data.glass_config,
        "cutouts": data.cutouts,
        "title": data.title or "Shared Glass Configuration",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "expires_at": expires_at.isoformat(),
        "view_count": 0,
        "order_count": 0
    }
    
    await db.shared_glass_configs.insert_one(shared_config)
    
    app_url = os.environ.get('APP_URL', 'https://lucumaaglass.in')
    share_url = f"{app_url}/share/{share_id}"
    
    return SharedConfigResponse(
        share_id=share_id,
        share_url=share_url,
        expires_at=expires_at.isoformat()
    )


@router.get("/share/{share_id}")
async def get_shared_config(share_id: str, db=Depends(get_db)):
    """Get shared configuration by ID"""
    config = await db.shared_glass_configs.find_one({"share_id": share_id}, {"_id": 0})
    
    if not config:
        raise HTTPException(status_code=404, detail="Shared configuration not found")
    
    # Check expiry
    expires_at = datetime.fromisoformat(config["expires_at"].replace("Z", "+00:00"))
    if datetime.now(timezone.utc) > expires_at:
        raise HTTPException(status_code=410, detail="This shared link has expired")
    
    # Increment view count
    await db.shared_glass_configs.update_one(
        {"share_id": share_id},
        {"$inc": {"view_count": 1}}
    )
    
    return config


@router.post("/share/{share_id}/order")
async def order_from_shared_config(share_id: str, db=Depends(get_db), user=Depends(get_erp_user)):
    """Create order from shared configuration"""
    config = await db.shared_glass_configs.find_one({"share_id": share_id})
    
    if not config:
        raise HTTPException(status_code=404, detail="Shared configuration not found")
    
    # Increment order count
    await db.shared_glass_configs.update_one(
        {"share_id": share_id},
        {"$inc": {"order_count": 1}}
    )
    
    return {
        "message": "Configuration loaded for ordering",
        "glass_config": config["glass_config"],
        "cutouts": config["cutouts"],
        "share_id": share_id
    }


# ============== MULTI-PAGE PDF EXPORT ==============

@router.post("/export-pdf-multipage")
async def export_pdf_multipage(data: PDFExportRequest, user=Depends(get_erp_user)):
    """Generate a multi-page PDF specification sheet for orders with many cutouts"""
    try:
        buffer = io.BytesIO()
        
        doc = SimpleDocTemplate(
            buffer, pagesize=A4,
            rightMargin=15*mm, leftMargin=15*mm,
            topMargin=15*mm, bottomMargin=15*mm
        )
        
        styles = getSampleStyleSheet()
        elements = []
        
        # Page 1: Title and 2D Drawing
        title_style = ParagraphStyle('Title', parent=styles['Heading1'], fontSize=16, alignment=1, spaceAfter=5*mm)
        elements.append(Paragraph("Glass Specification Sheet", title_style))
        elements.append(Paragraph(f"<para align='center'><font size='9' color='gray'>Generated: {datetime.now().strftime('%d %b %Y, %H:%M')} | Page 1</font></para>", styles['Normal']))
        elements.append(Spacer(1, 5*mm))
        
        # Glass info table
        glass_data = [
            ["Dimensions", f"{data.glass_config.width_mm} × {data.glass_config.height_mm} mm"],
            ["Thickness", f"{data.glass_config.thickness_mm} mm"],
            ["Type", data.glass_config.glass_type.title()],
            ["Color", data.glass_config.color_name],
            ["Quantity", str(data.quantity)],
            ["Total Cutouts", str(len(data.cutouts))],
        ]
        glass_table = Table(glass_data, colWidths=[50*mm, 130*mm])
        glass_table.setStyle(TableStyle([
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#E2E8F0')),
        ]))
        elements.append(glass_table)
        elements.append(Spacer(1, 5*mm))
        
        # 2D Drawing
        drawing_width = 180*mm
        drawing_height = 120*mm
        
        glass_w = data.glass_config.width_mm
        glass_h = data.glass_config.height_mm
        margin = 15*mm
        usable_width = drawing_width - 2*margin
        usable_height = drawing_height - 2*margin
        scale = min(usable_width / glass_w, usable_height / glass_h)
        
        offset_x = margin + (usable_width - glass_w * scale) / 2
        offset_y = margin + (usable_height - glass_h * scale) / 2
        
        drawing = Drawing(drawing_width, drawing_height)
        drawing.add(Rect(0, 0, drawing_width, drawing_height, fillColor=colors.white, strokeColor=colors.HexColor('#E2E8F0')))
        drawing.add(Rect(offset_x, offset_y, glass_w * scale, glass_h * scale, fillColor=colors.HexColor('#E8F4F8'), strokeColor=colors.HexColor('#3B82F6'), strokeWidth=2))
        
        cutout_colors_map = {
            'Hole': colors.HexColor('#3B82F6'),
            'Rectangle': colors.HexColor('#22C55E'),
            'Triangle': colors.HexColor('#F59E0B'),
            'Hexagon': colors.HexColor('#8B5CF6'),
            'Heart': colors.HexColor('#EC4899'),
        }
        
        for cutout in data.cutouts:
            cx = offset_x + cutout.x * scale
            cy = offset_y + cutout.y * scale
            cutout_color = cutout_colors_map.get(cutout.type, colors.HexColor('#3B82F6'))
            
            if cutout.diameter:
                radius = (cutout.diameter / 2) * scale
                drawing.add(Circle(cx, cy, radius, fillColor=cutout_color, strokeColor=colors.black, strokeWidth=1))
            else:
                w = cutout.width * scale
                h = cutout.height * scale
                drawing.add(Rect(cx - w/2, cy - h/2, w, h, fillColor=cutout_color, strokeColor=colors.black, strokeWidth=1))
            
            # Label
            drawing.add(String(cx, cy + 3*mm, cutout.number, fontSize=6, fillColor=colors.white, textAnchor='middle'))
        
        elements.append(drawing)
        
        # Cutout table with pagination
        if data.cutouts:
            elements.append(Spacer(1, 5*mm))
            elements.append(Paragraph("<b>Cutout Specifications</b>", styles['Heading3']))
            
            cutout_headers = ["#", "Type", "Size (mm)", "Position (X,Y)", "L", "R", "T", "B"]
            cutouts_per_page = 15
            total_pages = (len(data.cutouts) + cutouts_per_page - 1) // cutouts_per_page
            
            for page_num in range(total_pages):
                start_idx = page_num * cutouts_per_page
                end_idx = min(start_idx + cutouts_per_page, len(data.cutouts))
                page_cutouts = data.cutouts[start_idx:end_idx]
                
                if page_num > 0:
                    from reportlab.platypus import PageBreak
                    elements.append(PageBreak())
                    elements.append(Paragraph(f"<b>Cutout Specifications (Page {page_num + 1} of {total_pages})</b>", styles['Heading3']))
                    elements.append(Spacer(1, 3*mm))
                
                cutout_rows = [cutout_headers]
                for cutout in page_cutouts:
                    size = f"Ø{cutout.diameter:.0f}" if cutout.diameter else f"{cutout.width:.0f}×{cutout.height:.0f}"
                    cutout_rows.append([
                        cutout.number,
                        cutout.type,
                        size,
                        f"({cutout.x:.0f}, {cutout.y:.0f})",
                        f"{cutout.left_edge:.0f}",
                        f"{cutout.right_edge:.0f}",
                        f"{cutout.top_edge:.0f}",
                        f"{cutout.bottom_edge:.0f}"
                    ])
                
                cutout_table = Table(cutout_rows, colWidths=[15*mm, 25*mm, 25*mm, 30*mm, 18*mm, 18*mm, 18*mm, 18*mm])
                cutout_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1E293B')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, -1), 8),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#CBD5E1')),
                    ('TOPPADDING', (0, 0), (-1, -1), 4),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
                ]))
                elements.append(cutout_table)
        
        # Footer
        elements.append(Spacer(1, 10*mm))
        footer_text = f"Generated by Lucumaa Glass Configurator | {user.get('email', 'N/A')}"
        elements.append(Paragraph(f"<para align='center'><font size='8' color='gray'>{footer_text}</font></para>", styles['Normal']))
        
        doc.build(elements)
        buffer.seek(0)
        
        return StreamingResponse(
            buffer,
            media_type="application/pdf",
            headers={"Content-Disposition": f"attachment; filename=glass_specification_multipage_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"}
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate PDF: {str(e)}")

