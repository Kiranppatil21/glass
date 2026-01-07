"""
Customer Portal Router
Order tracking, history, invoices, and profile management
"""
from fastapi import APIRouter, Depends, HTTPException
from typing import Optional, Dict, Any
from datetime import datetime, timezone
import uuid
from .base import get_erp_user, get_db

customer_router = APIRouter(prefix="/customer", tags=["Customer Portal"])


@customer_router.get("/dashboard")
async def get_customer_dashboard(current_user: dict = Depends(get_erp_user)):
    """Get customer dashboard summary"""
    db = get_db()
    user_id = current_user["id"]
    
    # Get orders
    orders = await db.orders.find({"user_id": user_id}, {"_id": 0}).sort("created_at", -1).to_list(100)
    
    # Get wallet
    wallet = await db.wallets.find_one({"user_id": user_id}, {"_id": 0})
    
    # Calculate stats
    total_orders = len(orders)
    total_spent = sum(o.get("total_price", 0) for o in orders if o.get("payment_status") == "paid")
    pending_orders = len([o for o in orders if o.get("status") not in ["delivered", "cancelled"]])
    
    # Recent orders (last 5)
    recent_orders = orders[:5]
    
    return {
        "stats": {
            "total_orders": total_orders,
            "total_spent": round(total_spent, 2),
            "pending_orders": pending_orders,
            "wallet_balance": wallet.get("balance", 0) if wallet else 0
        },
        "recent_orders": recent_orders,
        "referral_code": wallet.get("referral_code") if wallet else None,
        "referral_count": wallet.get("referral_count", 0) if wallet else 0
    }


@customer_router.get("/orders")
async def get_customer_orders(
    status: Optional[str] = None,
    limit: int = 50,
    current_user: dict = Depends(get_erp_user)
):
    """Get customer's orders"""
    db = get_db()
    
    query = {"user_id": current_user["id"]}
    if status:
        query["status"] = status
    
    orders = await db.orders.find(query, {"_id": 0}).sort("created_at", -1).to_list(limit)
    
    return orders


@customer_router.get("/orders/{order_id}")
async def get_order_details(
    order_id: str,
    current_user: dict = Depends(get_erp_user)
):
    """Get detailed order information"""
    db = get_db()
    
    order = await db.orders.find_one({"id": order_id}, {"_id": 0})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    # Verify ownership
    if order.get("user_id") != current_user["id"] and current_user.get("role") not in ["admin", "owner"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    # Get production order if linked
    production_order = None
    if order.get("job_card_number"):
        production_order = await db.production_orders.find_one(
            {"job_card_number": order.get("job_card_number")},
            {"_id": 0}
        )
    
    # Get invoice if exists
    invoice = await db.invoices.find_one({"order_id": order_id}, {"_id": 0})
    
    # Build timeline
    timeline = [
        {
            "status": "ordered",
            "label": "Order Placed",
            "timestamp": order.get("created_at"),
            "completed": True
        }
    ]
    
    if order.get("payment_status") == "paid":
        timeline.append({
            "status": "paid",
            "label": "Payment Confirmed",
            "timestamp": order.get("paid_at", order.get("created_at")),
            "completed": True
        })
    
    if production_order:
        stages = ["cutting", "polishing", "grinding", "toughening", "quality_check", "packing", "dispatched"]
        current_stage = production_order.get("current_stage", "pending")
        stage_timestamps = production_order.get("stage_timestamps", {})
        
        for stage in stages:
            completed = stage in stage_timestamps or stages.index(stage) < stages.index(current_stage) if current_stage in stages else False
            timeline.append({
                "status": stage,
                "label": stage.replace("_", " ").title(),
                "timestamp": stage_timestamps.get(stage),
                "completed": completed,
                "current": stage == current_stage
            })
    
    if order.get("status") == "delivered":
        timeline.append({
            "status": "delivered",
            "label": "Delivered",
            "timestamp": order.get("delivered_at"),
            "completed": True
        })
    
    return {
        "order": order,
        "production": production_order,
        "invoice": invoice,
        "timeline": timeline
    }


@customer_router.get("/orders/{order_id}/track")
async def track_order_public(order_id: str):
    """Public order tracking (no auth required)"""
    db = get_db()
    
    order = await db.orders.find_one({"id": order_id}, {"_id": 0})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    # Get production status
    production_order = None
    if order.get("job_card_number"):
        production_order = await db.production_orders.find_one(
            {"job_card_number": order.get("job_card_number")},
            {"_id": 0, "current_stage": 1, "stage_timestamps": 1}
        )
    
    return {
        "order_id": order_id,
        "status": order.get("status"),
        "payment_status": order.get("payment_status"),
        "product_name": order.get("product_name"),
        "quantity": order.get("quantity", 1),
        "unit": order.get("unit", "pcs"),
        "total_price": order.get("total_price", 0),
        "created_at": order.get("created_at"),
        "current_stage": production_order.get("current_stage") if production_order else order.get("status"),
        "estimated_delivery": order.get("estimated_delivery")
    }


@customer_router.get("/invoices")
async def get_customer_invoices(
    limit: int = 50,
    current_user: dict = Depends(get_erp_user)
):
    """Get customer's invoices"""
    db = get_db()
    
    # Get customer's orders
    orders = await db.orders.find({"user_id": current_user["id"]}, {"_id": 0, "id": 1}).to_list(500)
    order_ids = [o["id"] for o in orders]
    
    # Get invoices for these orders
    invoices = await db.invoices.find(
        {"$or": [
            {"order_id": {"$in": order_ids}},
            {"customer_id": current_user["id"]}
        ]},
        {"_id": 0}
    ).sort("created_at", -1).to_list(limit)
    
    return invoices


@customer_router.get("/profile")
async def get_customer_profile(current_user: dict = Depends(get_erp_user)):
    """Get customer profile"""
    db = get_db()
    
    user = await db.users.find_one({"id": current_user["id"]}, {"_id": 0, "password_hash": 0})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Get wallet info
    wallet = await db.wallets.find_one({"user_id": current_user["id"]}, {"_id": 0})
    
    # Get saved addresses
    addresses = await db.addresses.find({"user_id": current_user["id"]}, {"_id": 0}).to_list(10)
    
    return {
        "user": user,
        "wallet": {
            "balance": wallet.get("balance", 0) if wallet else 0,
            "referral_code": wallet.get("referral_code") if wallet else None,
            "referral_count": wallet.get("referral_count", 0) if wallet else 0
        },
        "addresses": addresses
    }


@customer_router.put("/profile")
async def update_customer_profile(
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Update customer profile"""
    db = get_db()
    
    allowed_fields = ["name", "phone", "address", "city", "state", "pincode"]
    update_data = {k: v for k, v in data.items() if k in allowed_fields}
    update_data["updated_at"] = datetime.now(timezone.utc).isoformat()
    
    await db.users.update_one(
        {"id": current_user["id"]},
        {"$set": update_data}
    )
    
    return {"message": "Profile updated"}


@customer_router.post("/addresses")
async def add_address(
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Add a new delivery address"""
    db = get_db()
    
    address = {
        "id": str(uuid.uuid4()),
        "user_id": current_user["id"],
        "label": data.get("label", "Home"),
        "address_line1": data.get("address_line1"),
        "address_line2": data.get("address_line2", ""),
        "city": data.get("city"),
        "state": data.get("state"),
        "pincode": data.get("pincode"),
        "phone": data.get("phone", ""),
        "is_default": data.get("is_default", False),
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    # If this is default, unset other defaults
    if address["is_default"]:
        await db.addresses.update_many(
            {"user_id": current_user["id"]},
            {"$set": {"is_default": False}}
        )
    
    await db.addresses.insert_one(address)
    
    return {"message": "Address added", "address_id": address["id"]}


@customer_router.delete("/addresses/{address_id}")
async def delete_address(
    address_id: str,
    current_user: dict = Depends(get_erp_user)
):
    """Delete an address"""
    db = get_db()
    
    result = await db.addresses.delete_one({
        "id": address_id,
        "user_id": current_user["id"]
    })
    
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Address not found")
    
    return {"message": "Address deleted"}


# =============== SUPPORT TICKETS ===============

@customer_router.post("/support/ticket")
async def create_support_ticket(
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Create a support ticket"""
    db = get_db()
    
    ticket = {
        "id": str(uuid.uuid4()),
        "user_id": current_user["id"],
        "order_id": data.get("order_id"),
        "subject": data.get("subject"),
        "description": data.get("description"),
        "category": data.get("category", "general"),  # general, order, payment, delivery
        "priority": data.get("priority", "normal"),  # low, normal, high
        "status": "open",
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.support_tickets.insert_one(ticket)
    
    return {"message": "Ticket created", "ticket_id": ticket["id"]}


@customer_router.get("/support/tickets")
async def get_support_tickets(
    status: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get customer's support tickets"""
    db = get_db()
    
    query = {"user_id": current_user["id"]}
    if status:
        query["status"] = status
    
    tickets = await db.support_tickets.find(query, {"_id": 0}).sort("created_at", -1).to_list(50)
    
    return tickets
