"""
CRM Router - Lead management and sales pipeline
"""
from fastapi import APIRouter, Depends, HTTPException
from typing import Optional, Dict, Any
from datetime import datetime, timezone
import uuid
from .base import get_erp_user, get_db

crm_router = APIRouter(prefix="/crm", tags=["CRM"])


@crm_router.post("/leads")
async def create_lead(lead_data: Dict[str, Any], current_user: dict = Depends(get_erp_user)):
    """Create new lead"""
    db = get_db()
    lead = {
        "id": str(uuid.uuid4()),
        "name": lead_data.get("name"),
        "email": lead_data.get("email"),
        "phone": lead_data.get("phone"),
        "company": lead_data.get("company", ""),
        "customer_type": lead_data.get("customer_type", "retail"),
        "source": lead_data.get("source", "website"),
        "status": "new",
        "enquiry_details": lead_data.get("enquiry_details", ""),
        "expected_value": float(lead_data.get("expected_value", 0)),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    await db.leads.insert_one(lead)
    return {"message": "Lead created", "lead_id": lead["id"]}


@crm_router.get("/leads")
async def get_leads(
    status: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get all leads with optional status filter"""
    db = get_db()
    query = {}
    if status:
        query['status'] = status
    
    leads = await db.leads.find(query, {"_id": 0}).sort("created_at", -1).to_list(100)
    return leads


@crm_router.patch("/leads/{lead_id}/status")
async def update_lead_status(
    lead_id: str,
    status_data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Update lead status"""
    db = get_db()
    new_status = status_data.get("status")
    valid_statuses = ["new", "contacted", "quoted", "negotiation", "won", "lost"]
    
    if new_status not in valid_statuses:
        raise HTTPException(status_code=400, detail=f"Invalid status. Must be one of: {valid_statuses}")
    
    lead = await db.leads.find_one({"id": lead_id}, {"_id": 0})
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    
    await db.leads.update_one(
        {"id": lead_id},
        {"$set": {
            "status": new_status,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }}
    )
    
    return {"message": f"Lead status updated to {new_status}"}
