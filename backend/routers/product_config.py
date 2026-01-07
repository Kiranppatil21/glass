"""
Product Configuration - Glass Types, Thickness, Colors
"""
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime, timezone
import uuid
from routers.base import get_db, get_erp_user

config_router = APIRouter(prefix="/config", tags=["Product Configuration"])

class ThicknessOption(BaseModel):
    value: float
    label: str
    active: bool = True

class GlassType(BaseModel):
    id: Optional[str] = None
    name: str
    code: str
    description: Optional[str] = ""
    base_price_per_sqft: float = 0
    thickness_options: List[float] = [4, 5, 6, 8, 10, 12]
    active: bool = True

class ColorType(BaseModel):
    id: Optional[str] = None
    name: str
    code: str
    hex_code: Optional[str] = "#000000"
    price_multiplier: float = 1.0
    active: bool = True

# ============ THICKNESS OPTIONS ============

@config_router.get("/thickness")
async def get_thickness_options():
    """Get all thickness options"""
    db = get_db()
    options = await db.config_thickness.find({}, {"_id": 0}).sort("value", 1).to_list(50)
    if not options:
        # Default options
        options = [
            {"value": 3.5, "label": "3.5mm", "active": True},
            {"value": 4, "label": "4mm", "active": True},
            {"value": 5, "label": "5mm", "active": True},
            {"value": 6, "label": "6mm", "active": True},
            {"value": 8, "label": "8mm", "active": True},
            {"value": 10, "label": "10mm", "active": True},
            {"value": 12, "label": "12mm", "active": True},
            {"value": 14, "label": "14mm", "active": True},
            {"value": 16, "label": "16mm", "active": True},
        ]
    return {"thickness_options": options}

@config_router.post("/thickness")
async def add_thickness_option(option: ThicknessOption, current_user: dict = Depends(get_erp_user)):
    """Add new thickness option"""
    if current_user.get("role") not in ["super_admin", "admin", "owner"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    db = get_db()
    doc = option.model_dump()
    doc["created_at"] = datetime.now(timezone.utc).isoformat()
    await db.config_thickness.update_one(
        {"value": option.value},
        {"$set": doc},
        upsert=True
    )
    return {"message": "Thickness option saved"}

@config_router.delete("/thickness/{value}")
async def delete_thickness_option(value: float, current_user: dict = Depends(get_erp_user)):
    """Delete thickness option"""
    if current_user.get("role") not in ["super_admin", "admin"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    db = get_db()
    await db.config_thickness.delete_one({"value": value})
    return {"message": "Thickness option deleted"}

# ============ GLASS TYPES ============

@config_router.get("/glass-types")
async def get_glass_types():
    """Get all glass types"""
    db = get_db()
    types = await db.config_glass_types.find({}, {"_id": 0}).sort("name", 1).to_list(50)
    return {"glass_types": types}

@config_router.post("/glass-types")
async def add_glass_type(glass_type: GlassType, current_user: dict = Depends(get_erp_user)):
    """Add/update glass type"""
    if current_user.get("role") not in ["super_admin", "admin", "owner"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    db = get_db()
    doc = glass_type.model_dump()
    if not doc.get("id"):
        doc["id"] = str(uuid.uuid4())
    doc["updated_at"] = datetime.now(timezone.utc).isoformat()
    
    await db.config_glass_types.update_one(
        {"id": doc["id"]},
        {"$set": doc},
        upsert=True
    )
    return {"message": "Glass type saved", "id": doc["id"]}

@config_router.delete("/glass-types/{type_id}")
async def delete_glass_type(type_id: str, current_user: dict = Depends(get_erp_user)):
    """Delete glass type"""
    if current_user.get("role") not in ["super_admin", "admin"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    db = get_db()
    await db.config_glass_types.delete_one({"id": type_id})
    return {"message": "Glass type deleted"}

# ============ COLOR TYPES ============

@config_router.get("/colors")
async def get_color_types():
    """Get all color options"""
    db = get_db()
    colors = await db.config_colors.find({}, {"_id": 0}).sort("name", 1).to_list(50)
    if not colors:
        colors = [
            {"id": "clear", "name": "Clear", "code": "CLR", "hex_code": "#E8E8E8", "price_multiplier": 1.0, "active": True},
            {"id": "grey", "name": "Grey", "code": "GRY", "hex_code": "#808080", "price_multiplier": 1.1, "active": True},
            {"id": "bronze", "name": "Bronze", "code": "BRZ", "hex_code": "#CD7F32", "price_multiplier": 1.15, "active": True},
            {"id": "green", "name": "Green", "code": "GRN", "hex_code": "#228B22", "price_multiplier": 1.1, "active": True},
            {"id": "blue", "name": "Blue", "code": "BLU", "hex_code": "#4169E1", "price_multiplier": 1.15, "active": True},
            {"id": "black", "name": "Black", "code": "BLK", "hex_code": "#1C1C1C", "price_multiplier": 1.2, "active": True},
        ]
    return {"colors": colors}

@config_router.post("/colors")
async def add_color_type(color: ColorType, current_user: dict = Depends(get_erp_user)):
    """Add/update color type"""
    if current_user.get("role") not in ["super_admin", "admin", "owner"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    db = get_db()
    doc = color.model_dump()
    if not doc.get("id"):
        doc["id"] = str(uuid.uuid4())
    doc["updated_at"] = datetime.now(timezone.utc).isoformat()
    
    await db.config_colors.update_one(
        {"id": doc["id"]},
        {"$set": doc},
        upsert=True
    )
    return {"message": "Color saved", "id": doc["id"]}

@config_router.delete("/colors/{color_id}")
async def delete_color_type(color_id: str, current_user: dict = Depends(get_erp_user)):
    """Delete color type"""
    if current_user.get("role") not in ["super_admin", "admin"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    db = get_db()
    await db.config_colors.delete_one({"id": color_id})
    return {"message": "Color deleted"}

# ============ GET ALL CONFIG ============

@config_router.get("/all")
async def get_all_config():
    """Get all product configuration options"""
    db = get_db()
    
    thickness = await db.config_thickness.find({}, {"_id": 0}).sort("value", 1).to_list(50)
    if not thickness:
        thickness = [
            {"value": 3.5, "label": "3.5mm", "active": True},
            {"value": 4, "label": "4mm", "active": True},
            {"value": 5, "label": "5mm", "active": True},
            {"value": 6, "label": "6mm", "active": True},
            {"value": 8, "label": "8mm", "active": True},
            {"value": 10, "label": "10mm", "active": True},
            {"value": 12, "label": "12mm", "active": True},
            {"value": 14, "label": "14mm", "active": True},
            {"value": 16, "label": "16mm", "active": True},
        ]
    
    glass_types = await db.config_glass_types.find({}, {"_id": 0}).to_list(50)
    colors = await db.config_colors.find({}, {"_id": 0}).to_list(50)
    
    if not colors:
        colors = [
            {"id": "clear", "name": "Clear", "code": "CLR", "hex_code": "#E8E8E8", "price_multiplier": 1.0, "active": True},
            {"id": "grey", "name": "Grey", "code": "GRY", "hex_code": "#808080", "price_multiplier": 1.1, "active": True},
            {"id": "bronze", "name": "Bronze", "code": "BRZ", "hex_code": "#CD7F32", "price_multiplier": 1.15, "active": True},
            {"id": "green", "name": "Green", "code": "GRN", "hex_code": "#228B22", "price_multiplier": 1.1, "active": True},
            {"id": "blue", "name": "Blue", "code": "BLU", "hex_code": "#4169E1", "price_multiplier": 1.15, "active": True},
        ]
    
    return {
        "thickness_options": thickness,
        "glass_types": glass_types,
        "colors": colors
    }
