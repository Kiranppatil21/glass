"""
Production Router - Job cards, stage management, breakage tracking
"""
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from typing import Optional, Dict, Any
from datetime import datetime, timezone
import uuid
from .base import get_erp_user, get_db
from .notifications import notify_new_order

production_router = APIRouter(prefix="/production", tags=["Production"])


@production_router.post("/orders")
async def create_production_order(
    order_data: Dict[str, Any],
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_erp_user)
):
    """Create new production order with job card"""
    db = get_db()
    job_card_number = f"JC{datetime.now().strftime('%Y%m%d')}{uuid.uuid4().hex[:6].upper()}"
    
    order = {
        "id": str(uuid.uuid4()),
        "job_card_number": job_card_number,
        "customer_order_id": order_data.get("customer_order_id", ""),
        "glass_type": order_data.get("glass_type"),
        "thickness": float(order_data.get("thickness")),
        "width": float(order_data.get("width")),
        "height": float(order_data.get("height")),
        "quantity": int(order_data.get("quantity")),
        "current_stage": "pending",
        "priority": int(order_data.get("priority", 1)),
        "stage_timestamps": {},
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.production_orders.insert_one(order)
    
    # Notify admin about new order (background task)
    background_tasks.add_task(notify_new_order, order)
    
    return {
        "message": "Production order created",
        "job_card": job_card_number,
        "order_id": order["id"]
    }


@production_router.get("/orders")
async def get_production_orders(
    stage: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get production orders with optional stage filter"""
    db = get_db()
    query = {}
    if stage:
        query['current_stage'] = stage
    
    orders = await db.production_orders.find(query, {"_id": 0}).sort("priority", -1).to_list(100)
    return orders


@production_router.patch("/orders/{order_id}/stage")
async def update_production_stage(
    order_id: str,
    stage_data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Update production order stage"""
    db = get_db()
    new_stage = stage_data.get("stage")
    
    order = await db.production_orders.find_one({"id": order_id}, {"_id": 0})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    stage_timestamps = order.get("stage_timestamps", {})
    stage_timestamps[new_stage] = datetime.now(timezone.utc).isoformat()
    
    await db.production_orders.update_one(
        {"id": order_id},
        {"$set": {
            "current_stage": new_stage,
            "stage_timestamps": stage_timestamps,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }}
    )
    
    return {"message": "Stage updated successfully"}


@production_router.post("/breakage")
async def create_breakage_entry(breakage_data: Dict[str, Any], current_user: dict = Depends(get_erp_user)):
    """Record breakage entry"""
    db = get_db()
    breakage = {
        "id": str(uuid.uuid4()),
        "production_order_id": breakage_data.get("production_order_id"),
        "job_card_number": breakage_data.get("job_card_number"),
        "stage": breakage_data.get("stage"),
        "operator_id": breakage_data.get("operator_id"),
        "machine_id": breakage_data.get("machine_id", ""),
        "quantity_broken": int(breakage_data.get("quantity_broken")),
        "glass_type": breakage_data.get("glass_type"),
        "size": breakage_data.get("size"),
        "reason": breakage_data.get("reason"),
        "cost_per_unit": float(breakage_data.get("cost_per_unit", 0)),
        "total_loss": float(breakage_data.get("quantity_broken", 0)) * float(breakage_data.get("cost_per_unit", 0)),
        "approval_status": "pending",
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.breakage_entries.insert_one(breakage)
    return {"message": "Breakage entry created", "breakage_id": breakage["id"]}


@production_router.get("/breakage/analytics")
async def get_breakage_analytics(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get breakage analytics"""
    db = get_db()
    query = {}
    if start_date and end_date:
        query['created_at'] = {
            "$gte": start_date,
            "$lte": end_date
        }
    
    # By stage
    stage_pipeline = [
        {"$match": query},
        {"$group": {
            "_id": "$stage",
            "total_quantity": {"$sum": "$quantity_broken"},
            "total_loss": {"$sum": "$total_loss"}
        }}
    ]
    by_stage = await db.breakage_entries.aggregate(stage_pipeline).to_list(10)
    
    # By operator
    operator_pipeline = [
        {"$match": query},
        {"$group": {
            "_id": "$operator_id",
            "total_quantity": {"$sum": "$quantity_broken"},
            "total_loss": {"$sum": "$total_loss"}
        }},
        {"$sort": {"total_loss": -1}},
        {"$limit": 10}
    ]
    by_operator = await db.breakage_entries.aggregate(operator_pipeline).to_list(10)
    
    return {
        "by_stage": by_stage,
        "by_operator": by_operator
    }
