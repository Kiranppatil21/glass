"""
Asset Management Router
Handles company-owned assets, rented assets, depreciation, and employee handover
"""
from fastapi import APIRouter, Depends, HTTPException
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import uuid
from .base import get_erp_user, get_db

asset_router = APIRouter(prefix="/assets", tags=["Asset Management"])

# Asset Types
ASSET_TYPES = [
    {"id": "machine", "name": "Machine/Equipment", "icon": "🏭"},
    {"id": "vehicle", "name": "Vehicle", "icon": "🚗"},
    {"id": "tool", "name": "Tools", "icon": "🔧"},
    {"id": "it_asset", "name": "IT Asset", "icon": "💻"},
    {"id": "furniture", "name": "Furniture", "icon": "🪑"},
    {"id": "building", "name": "Building/Premises", "icon": "🏢"},
    {"id": "other", "name": "Other", "icon": "📦"},
]

DEPRECIATION_METHODS = ["straight_line", "wdv"]  # Written Down Value

ASSET_CONDITIONS = ["excellent", "good", "fair", "poor", "damaged", "scrapped"]


# =============== COMPANY OWNED ASSETS ===============

@asset_router.get("/types")
async def get_asset_types(current_user: dict = Depends(get_erp_user)):
    """Get all asset types"""
    return ASSET_TYPES


@asset_router.post("/owned")
async def create_owned_asset(
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Create new company-owned asset"""
    if current_user.get("role") not in ["admin", "owner", "accountant", "store"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    db = get_db()
    
    asset = {
        "id": str(uuid.uuid4()),
        "asset_code": f"AST{datetime.now().strftime('%Y%m%d')}{str(uuid.uuid4())[:4].upper()}",
        "name": data["name"],
        "asset_type": data.get("asset_type", "other"),
        "description": data.get("description", ""),
        "category": data.get("category", "Fixed Asset"),
        
        # Purchase Details
        "purchase_date": data.get("purchase_date"),
        "purchase_price": float(data.get("purchase_price", 0)),
        "vendor_name": data.get("vendor_name", ""),
        "invoice_number": data.get("invoice_number", ""),
        "warranty_expiry": data.get("warranty_expiry"),
        
        # Location & Assignment
        "location": data.get("location", ""),
        "department": data.get("department", ""),
        "assigned_to": data.get("assigned_to"),  # Employee ID
        "assigned_to_name": data.get("assigned_to_name"),
        
        # Depreciation
        "depreciation_method": data.get("depreciation_method", "straight_line"),
        "useful_life_years": int(data.get("useful_life_years", 5)),
        "salvage_value": float(data.get("salvage_value", 0)),
        "depreciation_rate": float(data.get("depreciation_rate", 20)),  # For WDV method
        
        # Status
        "condition": data.get("condition", "good"),
        "status": "active",  # active, under_maintenance, disposed, scrapped
        "is_insured": data.get("is_insured", False),
        "insurance_expiry": data.get("insurance_expiry"),
        
        # Tracking
        "serial_number": data.get("serial_number", ""),
        "model_number": data.get("model_number", ""),
        "manufacturer": data.get("manufacturer", ""),
        
        # Metadata
        "created_by": current_user["id"],
        "created_at": datetime.now(timezone.utc).isoformat(),
        "maintenance_history": [],
        "handover_history": []
    }
    
    await db.owned_assets.insert_one(asset)
    
    return {"message": "Asset created", "asset": {k: v for k, v in asset.items() if k != "_id"}}


@asset_router.get("/owned")
async def get_owned_assets(
    asset_type: Optional[str] = None,
    department: Optional[str] = None,
    status: Optional[str] = None,
    assigned_to: Optional[str] = None,
    limit: int = 100,
    current_user: dict = Depends(get_erp_user)
):
    """Get all company-owned assets"""
    db = get_db()
    
    query = {}
    if asset_type:
        query["asset_type"] = asset_type
    if department:
        query["department"] = department
    if status:
        query["status"] = status
    if assigned_to:
        query["assigned_to"] = assigned_to
    
    assets = await db.owned_assets.find(query, {"_id": 0}).sort("created_at", -1).to_list(limit)
    
    # Calculate current book value for each asset
    for asset in assets:
        asset["book_value"] = calculate_book_value(asset)
        asset["accumulated_depreciation"] = asset["purchase_price"] - asset["book_value"]
    
    return assets


@asset_router.get("/owned/{asset_id}")
async def get_owned_asset(asset_id: str, current_user: dict = Depends(get_erp_user)):
    """Get single owned asset details"""
    db = get_db()
    asset = await db.owned_assets.find_one({"id": asset_id}, {"_id": 0})
    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")
    
    asset["book_value"] = calculate_book_value(asset)
    asset["accumulated_depreciation"] = asset["purchase_price"] - asset["book_value"]
    asset["annual_depreciation"] = calculate_annual_depreciation(asset)
    
    return asset


@asset_router.put("/owned/{asset_id}")
async def update_owned_asset(
    asset_id: str,
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Update owned asset"""
    if current_user.get("role") not in ["admin", "owner", "accountant", "store"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    db = get_db()
    
    allowed_fields = [
        "name", "description", "location", "department", "condition",
        "status", "warranty_expiry", "insurance_expiry", "is_insured",
        "serial_number", "model_number"
    ]
    
    update_data = {k: v for k, v in data.items() if k in allowed_fields}
    update_data["updated_at"] = datetime.now(timezone.utc).isoformat()
    
    await db.owned_assets.update_one({"id": asset_id}, {"$set": update_data})
    
    return {"message": "Asset updated"}


def calculate_book_value(asset: dict) -> float:
    """Calculate current book value of an asset"""
    purchase_price = asset.get("purchase_price", 0)
    salvage_value = asset.get("salvage_value", 0)
    purchase_date = asset.get("purchase_date")
    
    if not purchase_date:
        return purchase_price
    
    # Calculate years since purchase - handle both naive and aware datetimes
    try:
        if isinstance(purchase_date, str):
            # Handle ISO format with or without timezone
            if "+" in purchase_date or "Z" in purchase_date:
                purchase_dt = datetime.fromisoformat(purchase_date.replace("Z", "+00:00"))
            else:
                # Naive datetime string - assume UTC
                purchase_dt = datetime.fromisoformat(purchase_date).replace(tzinfo=timezone.utc)
        else:
            purchase_dt = purchase_date
            if purchase_dt.tzinfo is None:
                purchase_dt = purchase_dt.replace(tzinfo=timezone.utc)
    except Exception:
        return purchase_price
    
    years_used = (datetime.now(timezone.utc) - purchase_dt).days / 365.25
    
    method = asset.get("depreciation_method", "straight_line")
    
    if method == "straight_line":
        useful_life = asset.get("useful_life_years", 5)
        annual_depreciation = (purchase_price - salvage_value) / useful_life
        total_depreciation = annual_depreciation * min(years_used, useful_life)
    else:  # WDV
        rate = asset.get("depreciation_rate", 20) / 100
        book_value = purchase_price * ((1 - rate) ** years_used)
        return max(book_value, salvage_value)
    
    book_value = purchase_price - total_depreciation
    return max(book_value, salvage_value)


def calculate_annual_depreciation(asset: dict) -> float:
    """Calculate annual depreciation amount"""
    purchase_price = asset.get("purchase_price", 0)
    salvage_value = asset.get("salvage_value", 0)
    method = asset.get("depreciation_method", "straight_line")
    
    if method == "straight_line":
        useful_life = asset.get("useful_life_years", 5)
        return (purchase_price - salvage_value) / useful_life
    else:  # WDV
        rate = asset.get("depreciation_rate", 20) / 100
        book_value = calculate_book_value(asset)
        return book_value * rate


# =============== ASSET MAINTENANCE ===============

@asset_router.post("/owned/{asset_id}/maintenance")
async def add_maintenance_record(
    asset_id: str,
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Add maintenance/service record to asset"""
    db = get_db()
    
    maintenance = {
        "id": str(uuid.uuid4()),
        "date": data.get("date", datetime.now(timezone.utc).date().isoformat()),
        "type": data.get("type", "routine"),  # routine, repair, breakdown
        "description": data["description"],
        "cost": float(data.get("cost", 0)),
        "vendor": data.get("vendor", ""),
        "next_service_date": data.get("next_service_date"),
        "recorded_by": current_user["id"],
        "recorded_at": datetime.now(timezone.utc).isoformat()
    }
    
    result = await db.owned_assets.update_one(
        {"id": asset_id},
        {
            "$push": {"maintenance_history": maintenance},
            "$set": {"last_maintenance_date": maintenance["date"]}
        }
    )
    
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="Asset not found")
    
    return {"message": "Maintenance record added", "record": maintenance}


@asset_router.post("/owned/{asset_id}/dispose")
async def dispose_asset(
    asset_id: str,
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Dispose/scrap an asset"""
    if current_user.get("role") not in ["admin", "owner", "accountant"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    db = get_db()
    
    disposal_record = {
        "disposal_date": data.get("date", datetime.now(timezone.utc).date().isoformat()),
        "disposal_type": data.get("type", "sold"),  # sold, scrapped, donated
        "disposal_value": float(data.get("value", 0)),
        "reason": data.get("reason", ""),
        "buyer": data.get("buyer", ""),
        "disposed_by": current_user["id"],
        "disposed_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.owned_assets.update_one(
        {"id": asset_id},
        {"$set": {"status": "disposed", "disposal_details": disposal_record}}
    )
    
    return {"message": "Asset disposed", "disposal": disposal_record}


# =============== RENTED ASSETS ===============

@asset_router.post("/rented")
async def create_rented_asset(
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Add new rented asset"""
    if current_user.get("role") not in ["admin", "owner", "accountant", "store"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    db = get_db()
    
    asset = {
        "id": str(uuid.uuid4()),
        "asset_code": f"RNT{datetime.now().strftime('%Y%m%d')}{str(uuid.uuid4())[:4].upper()}",
        "name": data["name"],
        "asset_type": data.get("asset_type", "machine"),
        "description": data.get("description", ""),
        
        # Vendor/Owner Details
        "vendor_id": data.get("vendor_id"),
        "vendor_name": data["vendor_name"],
        "vendor_contact": data.get("vendor_contact", ""),
        "vendor_address": data.get("vendor_address", ""),
        
        # Rental Terms
        "rent_type": data.get("rent_type", "monthly"),  # monthly, hourly, daily, unit_based
        "rent_amount": float(data["rent_amount"]),
        "rent_start_date": data["rent_start_date"],
        "rent_end_date": data.get("rent_end_date"),
        "security_deposit": float(data.get("security_deposit", 0)),
        "deposit_paid": data.get("deposit_paid", False),
        
        # Assignment
        "department": data.get("department", ""),
        "location": data.get("location", ""),
        "used_for": data.get("used_for", ""),  # Description of usage
        
        # Status
        "status": "active",  # active, expired, returned, renewed
        "condition_at_start": data.get("condition", "good"),
        "condition_current": data.get("condition", "good"),
        
        # Tracking
        "renewal_reminder_days": data.get("renewal_reminder_days", 30),
        "auto_renew": data.get("auto_renew", False),
        
        # Metadata
        "created_by": current_user["id"],
        "created_at": datetime.now(timezone.utc).isoformat(),
        "rent_payments": [],
        "damage_records": []
    }
    
    await db.rented_assets.insert_one(asset)
    
    return {"message": "Rented asset added", "asset": {k: v for k, v in asset.items() if k != "_id"}}


@asset_router.get("/rented")
async def get_rented_assets(
    status: Optional[str] = None,
    vendor_name: Optional[str] = None,
    department: Optional[str] = None,
    expiring_soon: Optional[bool] = None,
    limit: int = 100,
    current_user: dict = Depends(get_erp_user)
):
    """Get all rented assets"""
    db = get_db()
    
    query = {}
    if status:
        query["status"] = status
    if vendor_name:
        query["vendor_name"] = {"$regex": vendor_name, "$options": "i"}
    if department:
        query["department"] = department
    if expiring_soon:
        future_date = (datetime.now(timezone.utc) + timedelta(days=30)).date().isoformat()
        query["rent_end_date"] = {"$lte": future_date}
        query["status"] = "active"
    
    assets = await db.rented_assets.find(query, {"_id": 0}).sort("rent_end_date", 1).to_list(limit)
    
    # Calculate total rent liability
    for asset in assets:
        asset["days_remaining"] = calculate_days_remaining(asset.get("rent_end_date"))
    
    return assets


@asset_router.get("/rented/{asset_id}")
async def get_rented_asset(asset_id: str, current_user: dict = Depends(get_erp_user)):
    """Get single rented asset"""
    db = get_db()
    asset = await db.rented_assets.find_one({"id": asset_id}, {"_id": 0})
    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")
    return asset


@asset_router.post("/rented/{asset_id}/payment")
async def record_rent_payment(
    asset_id: str,
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Record rent payment for rented asset"""
    db = get_db()
    
    payment = {
        "id": str(uuid.uuid4()),
        "date": data.get("date", datetime.now(timezone.utc).date().isoformat()),
        "amount": float(data["amount"]),
        "period": data.get("period", ""),  # e.g., "January 2026"
        "payment_mode": data.get("payment_mode", "bank"),
        "reference": data.get("reference", ""),
        "recorded_by": current_user["id"],
        "recorded_at": datetime.now(timezone.utc).isoformat()
    }
    
    result = await db.rented_assets.update_one(
        {"id": asset_id},
        {"$push": {"rent_payments": payment}}
    )
    
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="Asset not found")
    
    # Post to expenses
    settings = await db.expense_settings.find_one({"id": "default"}, {"_id": 0})
    if settings and settings.get("auto_post_to_ledger"):
        asset = await db.rented_assets.find_one({"id": asset_id}, {"_id": 0})
        ledger_entry = {
            "id": str(uuid.uuid4()),
            "date": payment["date"],
            "type": "expense",
            "category": "Rent",
            "description": f"Rent payment for {asset['name']}",
            "debit": payment["amount"],
            "credit": 0,
            "reference_type": "rent_payment",
            "reference_id": payment["id"],
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        await db.ledger.insert_one(ledger_entry)
    
    return {"message": "Payment recorded", "payment": payment}


@asset_router.post("/rented/{asset_id}/return")
async def return_rented_asset(
    asset_id: str,
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Mark rented asset as returned"""
    db = get_db()
    
    return_record = {
        "return_date": data.get("date", datetime.now(timezone.utc).date().isoformat()),
        "condition": data.get("condition", "good"),
        "damage_charges": float(data.get("damage_charges", 0)),
        "deposit_refunded": float(data.get("deposit_refunded", 0)),
        "notes": data.get("notes", ""),
        "returned_by": current_user["id"]
    }
    
    await db.rented_assets.update_one(
        {"id": asset_id},
        {"$set": {"status": "returned", "return_details": return_record}}
    )
    
    return {"message": "Asset marked as returned", "return": return_record}


def calculate_days_remaining(end_date: str) -> int:
    """Calculate days remaining until rent end date"""
    if not end_date:
        return -1
    try:
        end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
        return (end_dt - datetime.now(timezone.utc)).days
    except:
        return -1


# =============== EMPLOYEE ASSET HANDOVER ===============

@asset_router.post("/handover/request")
async def create_handover_request(
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Create asset handover request"""
    db = get_db()
    
    request = {
        "id": str(uuid.uuid4()),
        "request_number": f"HO{datetime.now().strftime('%Y%m%d')}{str(uuid.uuid4())[:4].upper()}",
        "asset_id": data["asset_id"],
        "asset_name": data.get("asset_name", ""),
        "asset_code": data.get("asset_code", ""),
        "employee_id": data["employee_id"],
        "employee_name": data.get("employee_name", ""),
        "department": data.get("department", ""),
        "purpose": data.get("purpose", ""),
        "expected_return_date": data.get("expected_return_date"),
        "status": "pending",  # pending, approved, issued, returned, rejected
        "requested_by": current_user["id"],
        "requested_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.asset_handovers.insert_one(request)
    
    return {"message": "Handover request created", "request": {k: v for k, v in request.items() if k != "_id"}}


@asset_router.get("/handover/requests")
async def get_handover_requests(
    status: Optional[str] = None,
    employee_id: Optional[str] = None,
    asset_id: Optional[str] = None,
    limit: int = 100,
    current_user: dict = Depends(get_erp_user)
):
    """Get asset handover requests"""
    db = get_db()
    
    query = {}
    if status:
        query["status"] = status
    if employee_id:
        query["employee_id"] = employee_id
    if asset_id:
        query["asset_id"] = asset_id
    
    requests = await db.asset_handovers.find(query, {"_id": 0}).sort("requested_at", -1).to_list(limit)
    return requests


@asset_router.post("/handover/{request_id}/approve")
async def approve_handover(
    request_id: str,
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Approve/reject handover request"""
    if current_user.get("role") not in ["admin", "owner", "hr", "store"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    db = get_db()
    action = data.get("action", "approve")
    
    update_data = {
        "status": "approved" if action == "approve" else "rejected",
        "approved_by": current_user["id"],
        "approved_at": datetime.now(timezone.utc).isoformat(),
        "approval_note": data.get("note", "")
    }
    
    await db.asset_handovers.update_one({"id": request_id}, {"$set": update_data})
    
    return {"message": f"Request {action}d"}


@asset_router.post("/handover/{request_id}/issue")
async def issue_asset(
    request_id: str,
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Issue asset to employee (after approval)"""
    db = get_db()
    
    request = await db.asset_handovers.find_one({"id": request_id}, {"_id": 0})
    if not request:
        raise HTTPException(status_code=404, detail="Request not found")
    
    if request["status"] != "approved":
        raise HTTPException(status_code=400, detail="Request must be approved first")
    
    issue_data = {
        "status": "issued",
        "issue_date": data.get("date", datetime.now(timezone.utc).date().isoformat()),
        "condition_at_issue": data.get("condition", "good"),
        "issued_by": current_user["id"],
        "issued_at": datetime.now(timezone.utc).isoformat(),
        "acknowledgment": data.get("acknowledgment", False),
        "acknowledgment_note": data.get("acknowledgment_note", "")
    }
    
    await db.asset_handovers.update_one({"id": request_id}, {"$set": issue_data})
    
    # Update asset assignment
    await db.owned_assets.update_one(
        {"id": request["asset_id"]},
        {
            "$set": {
                "assigned_to": request["employee_id"],
                "assigned_to_name": request["employee_name"]
            },
            "$push": {
                "handover_history": {
                    "type": "issued",
                    "employee_id": request["employee_id"],
                    "employee_name": request["employee_name"],
                    "date": issue_data["issue_date"],
                    "condition": issue_data["condition_at_issue"],
                    "request_id": request_id
                }
            }
        }
    )
    
    return {"message": "Asset issued to employee"}


@asset_router.post("/handover/{request_id}/return")
async def return_asset(
    request_id: str,
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Return asset from employee"""
    db = get_db()
    
    request = await db.asset_handovers.find_one({"id": request_id}, {"_id": 0})
    if not request:
        raise HTTPException(status_code=404, detail="Request not found")
    
    return_data = {
        "status": "returned",
        "return_date": data.get("date", datetime.now(timezone.utc).date().isoformat()),
        "condition_at_return": data.get("condition", "good"),
        "damage_reported": data.get("damage_reported", False),
        "damage_description": data.get("damage_description", ""),
        "damage_recovery_amount": float(data.get("damage_recovery_amount", 0)),
        "returned_to": current_user["id"],
        "returned_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.asset_handovers.update_one({"id": request_id}, {"$set": return_data})
    
    # Clear asset assignment
    await db.owned_assets.update_one(
        {"id": request["asset_id"]},
        {
            "$set": {"assigned_to": None, "assigned_to_name": None},
            "$push": {
                "handover_history": {
                    "type": "returned",
                    "employee_id": request["employee_id"],
                    "employee_name": request["employee_name"],
                    "date": return_data["return_date"],
                    "condition": return_data["condition_at_return"],
                    "damage": return_data["damage_reported"],
                    "request_id": request_id
                }
            }
        }
    )
    
    return {"message": "Asset returned successfully"}


@asset_router.get("/employee/{employee_id}/holdings")
async def get_employee_asset_holdings(
    employee_id: str,
    current_user: dict = Depends(get_erp_user)
):
    """Get all assets currently held by an employee"""
    db = get_db()
    
    # Get from owned assets
    owned = await db.owned_assets.find(
        {"assigned_to": employee_id},
        {"_id": 0}
    ).to_list(100)
    
    # Get active handovers
    handovers = await db.asset_handovers.find(
        {"employee_id": employee_id, "status": "issued"},
        {"_id": 0}
    ).to_list(100)
    
    return {
        "employee_id": employee_id,
        "owned_assets": owned,
        "handover_records": handovers,
        "total_assets": len(owned)
    }


# =============== ASSET REPORTS ===============

@asset_router.get("/reports/register")
async def get_asset_register(current_user: dict = Depends(get_erp_user)):
    """Get complete asset register"""
    db = get_db()
    
    assets = await db.owned_assets.find({"status": {"$ne": "disposed"}}, {"_id": 0}).to_list(500)
    
    # Enrich with calculated values
    total_purchase = 0
    total_book_value = 0
    total_depreciation = 0
    
    for asset in assets:
        asset["book_value"] = calculate_book_value(asset)
        asset["accumulated_depreciation"] = asset["purchase_price"] - asset["book_value"]
        total_purchase += asset["purchase_price"]
        total_book_value += asset["book_value"]
        total_depreciation += asset["accumulated_depreciation"]
    
    return {
        "assets": assets,
        "summary": {
            "total_assets": len(assets),
            "total_purchase_value": total_purchase,
            "total_book_value": round(total_book_value, 2),
            "total_accumulated_depreciation": round(total_depreciation, 2)
        }
    }


@asset_router.get("/reports/depreciation")
async def get_depreciation_report(
    year: Optional[int] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get depreciation report for all assets"""
    db = get_db()
    
    if not year:
        year = datetime.now().year
    
    assets = await db.owned_assets.find({"status": {"$ne": "disposed"}}, {"_id": 0}).to_list(500)
    
    report = []
    total_depreciation = 0
    
    for asset in assets:
        annual_dep = calculate_annual_depreciation(asset)
        total_depreciation += annual_dep
        
        report.append({
            "asset_code": asset["asset_code"],
            "name": asset["name"],
            "purchase_price": asset["purchase_price"],
            "method": asset.get("depreciation_method", "straight_line"),
            "useful_life": asset.get("useful_life_years", 5),
            "annual_depreciation": round(annual_dep, 2),
            "book_value": round(calculate_book_value(asset), 2)
        })
    
    return {
        "year": year,
        "assets": report,
        "total_annual_depreciation": round(total_depreciation, 2)
    }


@asset_router.get("/reports/rent-liability")
async def get_rent_liability_report(current_user: dict = Depends(get_erp_user)):
    """Get monthly rent liability for all rented assets"""
    db = get_db()
    
    assets = await db.rented_assets.find({"status": "active"}, {"_id": 0}).to_list(100)
    
    total_monthly = 0
    vendor_totals = {}
    
    for asset in assets:
        rent = asset.get("rent_amount", 0)
        rent_type = asset.get("rent_type", "monthly")
        
        # Convert to monthly
        if rent_type == "daily":
            monthly = rent * 30
        elif rent_type == "hourly":
            monthly = rent * 8 * 30  # 8 hours/day
        else:
            monthly = rent
        
        total_monthly += monthly
        
        vendor = asset.get("vendor_name", "Unknown")
        vendor_totals[vendor] = vendor_totals.get(vendor, 0) + monthly
    
    return {
        "active_rentals": len(assets),
        "total_monthly_liability": round(total_monthly, 2),
        "by_vendor": [{"vendor": k, "monthly_rent": round(v, 2)} for k, v in vendor_totals.items()],
        "assets": assets
    }
