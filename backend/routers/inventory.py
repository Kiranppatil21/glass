"""
Inventory Router - Materials management, stock transactions, low-stock alerts
"""
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from typing import Optional, Dict, Any
from datetime import datetime, timezone
import uuid
from .base import get_erp_user, get_db
from .notifications import notify_low_stock

inventory_router = APIRouter(prefix="/inventory", tags=["Inventory"])


@inventory_router.post("/materials")
async def create_material(material_data: Dict[str, Any], current_user: dict = Depends(get_erp_user)):
    """Create new raw material"""
    db = get_db()
    material = {
        "id": str(uuid.uuid4()),
        "name": material_data.get("name"),
        "category": material_data.get("category", "glass"),  # glass, chemical, packing, spare
        "unit": material_data.get("unit", "pcs"),  # pcs, kg, ltr, sqft
        "current_stock": float(material_data.get("current_stock", 0)),
        "minimum_stock": float(material_data.get("minimum_stock", 10)),
        "unit_price": float(material_data.get("unit_price", 0)),
        "location": material_data.get("location", "Main Store"),
        "supplier_id": material_data.get("supplier_id", ""),
        "last_restocked": None,
        "status": "active",
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    await db.raw_materials.insert_one(material)
    return {"message": "Material created", "material_id": material["id"]}


@inventory_router.get("/materials")
async def get_materials(
    category: Optional[str] = None,
    low_stock: Optional[bool] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get all materials with optional filters"""
    db = get_db()
    query = {"status": "active"}
    if category:
        query["category"] = category
    
    materials = await db.raw_materials.find(query, {"_id": 0}).to_list(500)
    
    # Filter low stock if requested
    if low_stock:
        materials = [m for m in materials if m.get("current_stock", 0) <= m.get("minimum_stock", 10)]
    
    return materials


@inventory_router.get("/materials/{material_id}")
async def get_material(material_id: str, current_user: dict = Depends(get_erp_user)):
    """Get single material details"""
    db = get_db()
    material = await db.raw_materials.find_one({"id": material_id}, {"_id": 0})
    if not material:
        raise HTTPException(status_code=404, detail="Material not found")
    return material


@inventory_router.patch("/materials/{material_id}")
async def update_material(
    material_id: str,
    update_data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Update material details"""
    db = get_db()
    material = await db.raw_materials.find_one({"id": material_id}, {"_id": 0})
    if not material:
        raise HTTPException(status_code=404, detail="Material not found")
    
    update_fields = {}
    allowed_fields = ["name", "category", "unit", "minimum_stock", "unit_price", "location", "supplier_id"]
    for field in allowed_fields:
        if field in update_data:
            update_fields[field] = update_data[field]
    
    update_fields["updated_at"] = datetime.now(timezone.utc).isoformat()
    
    await db.raw_materials.update_one({"id": material_id}, {"$set": update_fields})
    return {"message": "Material updated"}


@inventory_router.post("/transactions")
async def create_inventory_transaction(
    txn_data: Dict[str, Any],
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_erp_user)
):
    """Create stock transaction (IN/OUT)"""
    db = get_db()
    material = await db.raw_materials.find_one({"id": txn_data.get("material_id")}, {"_id": 0})
    if not material:
        raise HTTPException(status_code=404, detail="Material not found")
    
    txn_type = txn_data.get("type", "IN")  # IN, OUT, ADJUST
    quantity = float(txn_data.get("quantity", 0))
    
    # Calculate new stock
    current_stock = material.get("current_stock", 0)
    if txn_type == "IN":
        new_stock = current_stock + quantity
    elif txn_type == "OUT":
        if quantity > current_stock:
            raise HTTPException(status_code=400, detail="Insufficient stock")
        new_stock = current_stock - quantity
    else:  # ADJUST
        new_stock = quantity
    
    transaction = {
        "id": str(uuid.uuid4()),
        "material_id": txn_data.get("material_id"),
        "material_name": material.get("name"),
        "type": txn_type,
        "quantity": quantity,
        "previous_stock": current_stock,
        "new_stock": new_stock,
        "reference": txn_data.get("reference", ""),  # PO number, job card, etc.
        "notes": txn_data.get("notes", ""),
        "created_by": current_user.get("id", "system"),
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.inventory_transactions.insert_one(transaction)
    
    # Update material stock
    update_fields = {"current_stock": new_stock, "updated_at": datetime.now(timezone.utc).isoformat()}
    if txn_type == "IN":
        update_fields["last_restocked"] = datetime.now(timezone.utc).isoformat()
    
    await db.raw_materials.update_one({"id": txn_data.get("material_id")}, {"$set": update_fields})
    
    # Check for low stock and send alert if needed
    if new_stock <= material.get("minimum_stock", 10):
        # Get all low stock items for a comprehensive alert
        all_materials = await db.raw_materials.find({"status": "active"}, {"_id": 0}).to_list(500)
        low_stock_items = [m for m in all_materials if m.get("current_stock", 0) <= m.get("minimum_stock", 10)]
        if low_stock_items:
            background_tasks.add_task(notify_low_stock, low_stock_items)
    
    return {"message": "Transaction recorded", "transaction_id": transaction["id"], "new_stock": new_stock}


@inventory_router.get("/transactions")
async def get_inventory_transactions(
    material_id: Optional[str] = None,
    txn_type: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get inventory transactions"""
    db = get_db()
    query = {}
    if material_id:
        query["material_id"] = material_id
    if txn_type:
        query["type"] = txn_type
    
    transactions = await db.inventory_transactions.find(query, {"_id": 0}).sort("created_at", -1).to_list(200)
    return transactions


@inventory_router.get("/low-stock")
async def get_low_stock_items(current_user: dict = Depends(get_erp_user)):
    """Get items below minimum stock level"""
    db = get_db()
    materials = await db.raw_materials.find({"status": "active"}, {"_id": 0}).to_list(500)
    low_stock = [m for m in materials if m.get("current_stock", 0) <= m.get("minimum_stock", 10)]
    return low_stock
