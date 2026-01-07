"""
Purchase Router - Suppliers and purchase order management
"""
from fastapi import APIRouter, Depends, HTTPException
from typing import Optional, Dict, Any
from datetime import datetime, timezone
import uuid
from .base import get_erp_user, get_db

purchase_router = APIRouter(prefix="/purchase", tags=["Purchase"])


@purchase_router.post("/suppliers")
async def create_supplier(supplier_data: Dict[str, Any], current_user: dict = Depends(get_erp_user)):
    """Create new supplier"""
    db = get_db()
    supplier = {
        "id": str(uuid.uuid4()),
        "name": supplier_data.get("name"),
        "contact_person": supplier_data.get("contact_person", ""),
        "email": supplier_data.get("email", ""),
        "phone": supplier_data.get("phone"),
        "address": supplier_data.get("address", ""),
        "gst_number": supplier_data.get("gst_number", ""),
        "payment_terms": supplier_data.get("payment_terms", "Net 30"),
        "status": "active",
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    await db.suppliers.insert_one(supplier)
    return {"message": "Supplier created", "supplier_id": supplier["id"]}


@purchase_router.get("/suppliers")
async def get_suppliers(current_user: dict = Depends(get_erp_user)):
    """Get all suppliers"""
    db = get_db()
    suppliers = await db.suppliers.find({"status": "active"}, {"_id": 0}).to_list(200)
    return suppliers


@purchase_router.post("/orders")
async def create_purchase_order(po_data: Dict[str, Any], current_user: dict = Depends(get_erp_user)):
    """Create new purchase order"""
    db = get_db()
    po_number = f"PO{datetime.now().strftime('%Y%m%d')}{uuid.uuid4().hex[:6].upper()}"
    
    # Calculate totals
    items = po_data.get("items", [])
    subtotal = sum(item.get("quantity", 0) * item.get("unit_price", 0) for item in items)
    gst = subtotal * 0.18
    total = subtotal + gst
    
    purchase_order = {
        "id": str(uuid.uuid4()),
        "po_number": po_number,
        "supplier_id": po_data.get("supplier_id"),
        "supplier_name": po_data.get("supplier_name", ""),
        "items": items,
        "subtotal": round(subtotal, 2),
        "gst": round(gst, 2),
        "total": round(total, 2),
        "status": "pending",  # pending, approved, ordered, received, cancelled
        "expected_delivery": po_data.get("expected_delivery", ""),
        "notes": po_data.get("notes", ""),
        "created_by": current_user.get("id", "system"),
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.purchase_orders.insert_one(purchase_order)
    return {"message": "Purchase order created", "po_number": po_number, "po_id": purchase_order["id"]}


@purchase_router.get("/orders")
async def get_purchase_orders(
    status: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get purchase orders"""
    db = get_db()
    query = {}
    if status:
        query["status"] = status
    
    pos = await db.purchase_orders.find(query, {"_id": 0}).sort("created_at", -1).to_list(200)
    return pos


@purchase_router.get("/orders/{po_id}")
async def get_purchase_order(po_id: str, current_user: dict = Depends(get_erp_user)):
    """Get single purchase order"""
    db = get_db()
    po = await db.purchase_orders.find_one({"id": po_id}, {"_id": 0})
    if not po:
        raise HTTPException(status_code=404, detail="Purchase order not found")
    return po


@purchase_router.patch("/orders/{po_id}/status")
async def update_po_status(
    po_id: str,
    status_data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Update PO status"""
    db = get_db()
    new_status = status_data.get("status")
    valid_statuses = ["pending", "approved", "ordered", "received", "cancelled"]
    
    if new_status not in valid_statuses:
        raise HTTPException(status_code=400, detail=f"Invalid status. Must be one of: {valid_statuses}")
    
    po = await db.purchase_orders.find_one({"id": po_id}, {"_id": 0})
    if not po:
        raise HTTPException(status_code=404, detail="Purchase order not found")
    
    update_fields = {
        "status": new_status,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    
    # If received, update inventory
    if new_status == "received" and po.get("status") != "received":
        for item in po.get("items", []):
            material = await db.raw_materials.find_one({"id": item.get("material_id")}, {"_id": 0})
            if material:
                new_stock = material.get("current_stock", 0) + item.get("quantity", 0)
                await db.raw_materials.update_one(
                    {"id": item.get("material_id")},
                    {"$set": {"current_stock": new_stock, "last_restocked": datetime.now(timezone.utc).isoformat()}}
                )
                
                # Create inventory transaction
                txn = {
                    "id": str(uuid.uuid4()),
                    "material_id": item.get("material_id"),
                    "material_name": item.get("material_name", material.get("name")),
                    "type": "IN",
                    "quantity": item.get("quantity"),
                    "previous_stock": material.get("current_stock", 0),
                    "new_stock": new_stock,
                    "reference": po.get("po_number"),
                    "notes": f"Received from PO {po.get('po_number')}",
                    "created_by": current_user.get("id", "system"),
                    "created_at": datetime.now(timezone.utc).isoformat()
                }
                await db.inventory_transactions.insert_one(txn)
        
        update_fields["received_at"] = datetime.now(timezone.utc).isoformat()
        update_fields["received_by"] = current_user.get("id", "system")
    
    await db.purchase_orders.update_one({"id": po_id}, {"$set": update_fields})
    return {"message": f"PO status updated to {new_status}"}
