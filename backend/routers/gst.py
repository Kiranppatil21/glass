"""
GST Management System
- GST Settings & Configuration
- HSN Code Management
- CGST/SGST/IGST Calculation
- GST Verification API
"""
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime, timezone
import uuid
import httpx
import re

from routers.base import get_db, get_erp_user

gst_router = APIRouter(prefix="/gst", tags=["GST Management"])

# Indian States for GST
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

# Default HSN Codes for Glass Products
DEFAULT_HSN_CODES = [
    {"code": "7003", "description": "Cast glass and rolled glass", "gst_rate": 18},
    {"code": "7004", "description": "Drawn glass and blown glass", "gst_rate": 18},
    {"code": "7005", "description": "Float glass and surface ground glass", "gst_rate": 18},
    {"code": "7006", "description": "Glass bent, edge-worked, engraved", "gst_rate": 18},
    {"code": "7007", "description": "Safety glass (toughened/laminated)", "gst_rate": 18},
    {"code": "7008", "description": "Insulating glass units", "gst_rate": 18},
    {"code": "7009", "description": "Glass mirrors", "gst_rate": 18},
]

# ============ PYDANTIC MODELS ============

class GSTSettings(BaseModel):
    company_name: str = "Lucumaa Glass"
    company_gstin: str = ""
    company_state_code: str = "27"  # Maharashtra default
    company_address: str = ""
    default_gst_rate: float = 18.0
    gst_api_key: Optional[str] = None
    gst_api_enabled: bool = False
    invoice_prefix: str = "INV"
    invoice_series: int = 1
    hsn_codes: List[dict] = DEFAULT_HSN_CODES
    active: bool = True

class HSNCode(BaseModel):
    code: str
    description: str
    gst_rate: float = 18.0
    active: bool = True

class GSTVerifyRequest(BaseModel):
    gstin: str

class GSTCalculationRequest(BaseModel):
    amount: float
    delivery_state_code: str
    hsn_code: Optional[str] = "7007"  # Default: Safety glass

class CustomerGSTInfo(BaseModel):
    gstin: Optional[str] = None
    legal_name: Optional[str] = None
    trade_name: Optional[str] = None
    state_code: str
    state_name: str
    address: Optional[str] = None

# ============ GST SETTINGS ============

@gst_router.get("/settings")
async def get_gst_settings(current_user: dict = Depends(get_erp_user)):
    """Get GST settings"""
    db = get_db()
    settings = await db.gst_settings.find_one({}, {"_id": 0})
    if not settings:
        settings = GSTSettings().model_dump()
        # Don't expose API key to non-admin
    
    # Hide API key for non-super-admin
    if current_user.get("role") not in ["super_admin"]:
        settings["gst_api_key"] = "***" if settings.get("gst_api_key") else None
    
    return settings

@gst_router.put("/settings")
async def update_gst_settings(
    settings: GSTSettings,
    current_user: dict = Depends(get_erp_user)
):
    """Update GST settings (Super Admin only)"""
    if current_user.get("role") != "super_admin":
        raise HTTPException(status_code=403, detail="Super Admin access required")
    
    db = get_db()
    doc = settings.model_dump()
    doc["updated_by"] = current_user.get("name", "")
    doc["updated_at"] = datetime.now(timezone.utc).isoformat()
    
    await db.gst_settings.update_one({}, {"$set": doc}, upsert=True)
    return {"message": "GST settings updated", "settings": {**doc, "gst_api_key": "***" if doc.get("gst_api_key") else None}}

# ============ HSN CODES ============

@gst_router.get("/hsn-codes")
async def get_hsn_codes():
    """Get all HSN codes"""
    db = get_db()
    settings = await db.gst_settings.find_one({}, {"_id": 0, "hsn_codes": 1})
    return {"hsn_codes": settings.get("hsn_codes", DEFAULT_HSN_CODES) if settings else DEFAULT_HSN_CODES}

@gst_router.post("/hsn-codes")
async def add_hsn_code(
    hsn: HSNCode,
    current_user: dict = Depends(get_erp_user)
):
    """Add new HSN code"""
    if current_user.get("role") not in ["super_admin", "admin"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    db = get_db()
    
    # Get current settings
    settings = await db.gst_settings.find_one({}, {"_id": 0})
    hsn_codes = settings.get("hsn_codes", DEFAULT_HSN_CODES) if settings else DEFAULT_HSN_CODES
    
    # Check duplicate
    if any(h["code"] == hsn.code for h in hsn_codes):
        raise HTTPException(status_code=400, detail="HSN code already exists")
    
    hsn_codes.append(hsn.model_dump())
    
    await db.gst_settings.update_one({}, {"$set": {"hsn_codes": hsn_codes}}, upsert=True)
    return {"message": "HSN code added", "hsn_code": hsn.model_dump()}

@gst_router.delete("/hsn-codes/{code}")
async def delete_hsn_code(code: str, current_user: dict = Depends(get_erp_user)):
    """Delete HSN code"""
    if current_user.get("role") not in ["super_admin", "admin"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    db = get_db()
    settings = await db.gst_settings.find_one({}, {"_id": 0})
    hsn_codes = settings.get("hsn_codes", []) if settings else []
    
    hsn_codes = [h for h in hsn_codes if h["code"] != code]
    
    await db.gst_settings.update_one({}, {"$set": {"hsn_codes": hsn_codes}})
    return {"message": "HSN code deleted"}

# ============ STATES LIST ============

@gst_router.get("/states")
async def get_indian_states():
    """Get list of Indian states with codes"""
    states = [{"code": k, "name": v} for k, v in INDIAN_STATES.items()]
    return {"states": sorted(states, key=lambda x: x["name"])}

# ============ GST CALCULATION ============

@gst_router.post("/calculate")
async def calculate_gst(request: GSTCalculationRequest):
    """Calculate GST based on delivery state"""
    db = get_db()
    
    # Get company settings
    settings = await db.gst_settings.find_one({}, {"_id": 0})
    if not settings:
        settings = GSTSettings().model_dump()
    
    company_state = settings.get("company_state_code", "27")
    delivery_state = request.delivery_state_code
    
    # Get GST rate from HSN code
    hsn_codes = settings.get("hsn_codes", DEFAULT_HSN_CODES)
    hsn_info = next((h for h in hsn_codes if h["code"] == request.hsn_code), None)
    gst_rate = hsn_info["gst_rate"] if hsn_info else settings.get("default_gst_rate", 18)
    
    base_amount = request.amount
    
    # Calculate GST
    if company_state == delivery_state:
        # Intra-state: CGST + SGST
        cgst_rate = gst_rate / 2
        sgst_rate = gst_rate / 2
        cgst_amount = round(base_amount * cgst_rate / 100, 2)
        sgst_amount = round(base_amount * sgst_rate / 100, 2)
        igst_amount = 0
        igst_rate = 0
        gst_type = "intra_state"
    else:
        # Inter-state: IGST
        igst_rate = gst_rate
        igst_amount = round(base_amount * igst_rate / 100, 2)
        cgst_amount = 0
        sgst_amount = 0
        cgst_rate = 0
        sgst_rate = 0
        gst_type = "inter_state"
    
    total_gst = cgst_amount + sgst_amount + igst_amount
    total_amount = round(base_amount + total_gst, 2)
    
    return {
        "base_amount": base_amount,
        "gst_type": gst_type,
        "company_state": {"code": company_state, "name": INDIAN_STATES.get(company_state, "Unknown")},
        "delivery_state": {"code": delivery_state, "name": INDIAN_STATES.get(delivery_state, "Unknown")},
        "hsn_code": request.hsn_code,
        "gst_rate": gst_rate,
        "breakdown": {
            "cgst_rate": cgst_rate,
            "cgst_amount": cgst_amount,
            "sgst_rate": sgst_rate,
            "sgst_amount": sgst_amount,
            "igst_rate": igst_rate,
            "igst_amount": igst_amount,
            "total_gst": total_gst
        },
        "total_amount": total_amount
    }

# ============ GST VERIFICATION ============

@gst_router.post("/verify")
async def verify_gstin(
    request: GSTVerifyRequest,
    current_user: dict = Depends(get_erp_user)
):
    """Verify GSTIN using GST API"""
    db = get_db()
    
    # Validate GSTIN format
    gstin_pattern = r'^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z]{1}[1-9A-Z]{1}Z[0-9A-Z]{1}$'
    if not re.match(gstin_pattern, request.gstin.upper()):
        raise HTTPException(status_code=400, detail="Invalid GSTIN format")
    
    gstin = request.gstin.upper()
    
    # Get API settings
    settings = await db.gst_settings.find_one({}, {"_id": 0})
    
    # Check if API is enabled
    if not settings or not settings.get("gst_api_enabled") or not settings.get("gst_api_key"):
        # Return basic info from GSTIN (without API verification)
        state_code = gstin[:2]
        return {
            "gstin": gstin,
            "valid": True,
            "verified_via_api": False,
            "state_code": state_code,
            "state_name": INDIAN_STATES.get(state_code, "Unknown"),
            "message": "GST API not configured. Basic validation passed."
        }
    
    # Call GST API for verification
    try:
        api_key = settings.get("gst_api_key")
        # Using a generic GST API endpoint - can be replaced with actual API
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"https://api.gst.gov.in/commonapi/v1.0/search?gstin={gstin}",
                headers={"Authorization": f"Bearer {api_key}"}
            )
            
            if response.status_code == 200:
                data = response.json()
                return {
                    "gstin": gstin,
                    "valid": True,
                    "verified_via_api": True,
                    "legal_name": data.get("lgnm", ""),
                    "trade_name": data.get("tradeNam", ""),
                    "state_code": data.get("stj", gstin[:2]),
                    "state_name": INDIAN_STATES.get(gstin[:2], "Unknown"),
                    "status": data.get("sts", ""),
                    "registration_date": data.get("rgdt", ""),
                    "business_type": data.get("ctb", ""),
                    "address": data.get("pradr", {}).get("adr", "")
                }
            else:
                raise HTTPException(status_code=400, detail="GST verification failed")
                
    except httpx.TimeoutException:
        # Fallback to basic validation
        state_code = gstin[:2]
        return {
            "gstin": gstin,
            "valid": True,
            "verified_via_api": False,
            "state_code": state_code,
            "state_name": INDIAN_STATES.get(state_code, "Unknown"),
            "message": "GST API timeout. Basic validation passed."
        }
    except Exception as e:
        # Fallback to basic validation
        state_code = gstin[:2]
        return {
            "gstin": gstin,
            "valid": True,
            "verified_via_api": False,
            "state_code": state_code,
            "state_name": INDIAN_STATES.get(state_code, "Unknown"),
            "message": f"API error: {str(e)}. Basic validation passed."
        }

# ============ PUBLIC ENDPOINTS ============

@gst_router.get("/company-info")
async def get_company_gst_info():
    """Get company GST info (public)"""
    db = get_db()
    settings = await db.gst_settings.find_one({}, {"_id": 0})
    if not settings:
        settings = GSTSettings().model_dump()
    
    return {
        "company_name": settings.get("company_name", "Lucumaa Glass"),
        "gstin": settings.get("company_gstin", ""),
        "state_code": settings.get("company_state_code", "27"),
        "state_name": INDIAN_STATES.get(settings.get("company_state_code", "27"), "Maharashtra"),
        "address": settings.get("company_address", "")
    }
