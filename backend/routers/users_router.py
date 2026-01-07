"""
Users Router - User profile, contact, inquiry endpoints
"""
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, EmailStr
from typing import Optional
from datetime import datetime, timezone
import uuid

users_router = APIRouter(tags=["Users"])

# Database reference
_db = None

def init_users_router(database):
    global _db
    _db = database

def get_db():
    return _db

# Import auth dependency
from .auth_router import get_current_user


# =============== MODELS ===============

class ContactInquiry(BaseModel):
    name: str
    email: EmailStr
    phone: str
    subject: str
    message: str


class InquiryRequest(BaseModel):
    name: str
    email: EmailStr
    phone: str
    inquiry_type: str = "general"
    message: str
    product_interest: Optional[str] = None
    quantity_needed: Optional[str] = None
    expected_delivery: Optional[str] = None


class ProfileUpdateRequest(BaseModel):
    name: Optional[str] = None
    phone: Optional[str] = None
    company_name: Optional[str] = None
    gst_number: Optional[str] = None
    address: Optional[str] = None


# =============== ENDPOINTS ===============

@users_router.get("/users/profile")
async def get_profile(current_user: dict = Depends(get_current_user)):
    """Get user profile"""
    db = get_db()
    user = await db.users.find_one({"id": current_user.get("id")}, {"_id": 0, "password_hash": 0})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user


@users_router.put("/users/profile")
async def update_profile(
    profile_data: ProfileUpdateRequest,
    current_user: dict = Depends(get_current_user)
):
    """Update user profile"""
    db = get_db()
    
    update_data = {k: v for k, v in profile_data.dict().items() if v is not None}
    update_data["updated_at"] = datetime.now(timezone.utc).isoformat()
    
    await db.users.update_one(
        {"id": current_user.get("id")},
        {"$set": update_data}
    )
    
    return {"message": "Profile updated successfully"}


@users_router.post("/contact")
async def submit_contact(
    inquiry: ContactInquiry,
    background_tasks: BackgroundTasks = None
):
    """Submit contact inquiry"""
    db = get_db()
    
    contact = {
        "id": str(uuid.uuid4()),
        "name": inquiry.name,
        "email": inquiry.email,
        "phone": inquiry.phone,
        "subject": inquiry.subject,
        "message": inquiry.message,
        "status": "new",
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.contact_inquiries.insert_one(contact)
    
    return {
        "message": "Thank you for contacting us! We will get back to you soon.",
        "inquiry_id": contact["id"]
    }


@users_router.post("/inquiry")
async def submit_inquiry(
    inquiry: InquiryRequest,
    background_tasks: BackgroundTasks = None
):
    """Submit product inquiry"""
    db = get_db()
    
    inquiry_doc = {
        "id": str(uuid.uuid4()),
        "name": inquiry.name,
        "email": inquiry.email,
        "phone": inquiry.phone,
        "inquiry_type": inquiry.inquiry_type,
        "message": inquiry.message,
        "product_interest": inquiry.product_interest,
        "quantity_needed": inquiry.quantity_needed,
        "expected_delivery": inquiry.expected_delivery,
        "status": "new",
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.inquiries.insert_one(inquiry_doc)
    
    return {
        "message": "Inquiry submitted successfully!",
        "inquiry_id": inquiry_doc["id"]
    }
