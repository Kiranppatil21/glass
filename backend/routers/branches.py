"""
Multi-Branch Support for Glass Factory ERP
"""
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime, timezone
import uuid
from routers.base import get_db, get_erp_user

branch_router = APIRouter(prefix="/branches", tags=["Branches"])

class BranchCreate(BaseModel):
    name: str
    code: str
    address: str
    city: str
    state: str
    pincode: str
    phone: Optional[str] = None
    email: Optional[str] = None
    manager_id: Optional[str] = None
    is_warehouse: bool = False
    is_active: bool = True

class BranchUpdate(BaseModel):
    name: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    pincode: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    manager_id: Optional[str] = None
    is_warehouse: Optional[bool] = None
    is_active: Optional[bool] = None

@branch_router.get("")
async def get_branches(
    is_active: Optional[bool] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get all branches"""
    db = get_db()
    query = {}
    if is_active is not None:
        query["is_active"] = is_active
    
    branches = await db.branches.find(query, {"_id": 0}).sort("name", 1).to_list(100)
    return {"branches": branches, "total": len(branches)}

@branch_router.get("/{branch_id}")
async def get_branch(branch_id: str, current_user: dict = Depends(get_erp_user)):
    """Get single branch"""
    db = get_db()
    branch = await db.branches.find_one({"id": branch_id}, {"_id": 0})
    if not branch:
        raise HTTPException(status_code=404, detail="Branch not found")
    return branch

@branch_router.post("")
async def create_branch(branch: BranchCreate, current_user: dict = Depends(get_erp_user)):
    """Create new branch"""
    if current_user.get("role") not in ["super_admin", "admin", "owner"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    db = get_db()
    
    # Check duplicate code
    existing = await db.branches.find_one({"code": branch.code.upper()})
    if existing:
        raise HTTPException(status_code=400, detail="Branch code already exists")
    
    branch_doc = {
        "id": str(uuid.uuid4()),
        "code": branch.code.upper(),
        **branch.model_dump(exclude={"code"}),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "created_by": current_user.get("id")
    }
    
    await db.branches.insert_one(branch_doc)
    branch_doc.pop("_id", None)
    return {"message": "Branch created", "branch": branch_doc}

@branch_router.put("/{branch_id}")
async def update_branch(branch_id: str, branch: BranchUpdate, current_user: dict = Depends(get_erp_user)):
    """Update branch"""
    if current_user.get("role") not in ["super_admin", "admin", "owner"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    db = get_db()
    update_data = {k: v for k, v in branch.model_dump().items() if v is not None}
    update_data["updated_at"] = datetime.now(timezone.utc).isoformat()
    
    result = await db.branches.update_one({"id": branch_id}, {"$set": update_data})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Branch not found")
    
    return {"message": "Branch updated"}

@branch_router.delete("/{branch_id}")
async def delete_branch(branch_id: str, current_user: dict = Depends(get_erp_user)):
    """Delete branch"""
    if current_user.get("role") not in ["super_admin", "admin"]:
        raise HTTPException(status_code=403, detail="Super admin access required")
    
    db = get_db()
    result = await db.branches.delete_one({"id": branch_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Branch not found")
    
    return {"message": "Branch deleted"}

@branch_router.get("/{branch_id}/stats")
async def get_branch_stats(branch_id: str, current_user: dict = Depends(get_erp_user)):
    """Get branch statistics"""
    db = get_db()
    
    branch = await db.branches.find_one({"id": branch_id}, {"_id": 0})
    if not branch:
        raise HTTPException(status_code=404, detail="Branch not found")
    
    # Get orders for this branch
    orders_count = await db.orders.count_documents({"branch_id": branch_id})
    pending_orders = await db.orders.count_documents({"branch_id": branch_id, "status": {"$in": ["pending", "confirmed", "processing"]}})
    
    # Get inventory for this branch
    inventory_items = await db.inventory.count_documents({"branch_id": branch_id})
    
    # Get employees for this branch
    employees = await db.employees.count_documents({"branch_id": branch_id, "status": "active"})
    
    return {
        "branch": branch,
        "stats": {
            "total_orders": orders_count,
            "pending_orders": pending_orders,
            "inventory_items": inventory_items,
            "employees": employees
        }
    }

@branch_router.post("/{branch_id}/transfer-inventory")
async def transfer_inventory(
    branch_id: str,
    target_branch_id: str,
    product_id: str,
    quantity: int,
    current_user: dict = Depends(get_erp_user)
):
    """Transfer inventory between branches"""
    if current_user.get("role") not in ["super_admin", "admin", "owner", "store"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    # Check source inventory
    source_inv = await db.inventory.find_one({"branch_id": branch_id, "product_id": product_id})
    if not source_inv or source_inv.get("quantity", 0) < quantity:
        raise HTTPException(status_code=400, detail="Insufficient inventory")
    
    # Deduct from source
    await db.inventory.update_one(
        {"branch_id": branch_id, "product_id": product_id},
        {"$inc": {"quantity": -quantity}}
    )
    
    # Add to target
    await db.inventory.update_one(
        {"branch_id": target_branch_id, "product_id": product_id},
        {"$inc": {"quantity": quantity}},
        upsert=True
    )
    
    # Log transfer
    transfer_log = {
        "id": str(uuid.uuid4()),
        "from_branch": branch_id,
        "to_branch": target_branch_id,
        "product_id": product_id,
        "quantity": quantity,
        "transferred_by": current_user.get("id"),
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    await db.inventory_transfers.insert_one(transfer_log)
    
    return {"message": "Inventory transferred", "transfer_id": transfer_log["id"]}
