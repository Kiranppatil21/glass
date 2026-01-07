"""
SMS & WhatsApp Notifications Router
Uses Twilio for sending SMS and WhatsApp messages
"""
from fastapi import APIRouter, Depends, HTTPException
from typing import Optional, Dict, Any
from datetime import datetime, timezone
import uuid
import os
import json
import logging
from .base import get_erp_user, get_db

sms_router = APIRouter(prefix="/notifications", tags=["SMS & WhatsApp"])

# Twilio Configuration
TWILIO_ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "")
TWILIO_PHONE_NUMBER = os.environ.get("TWILIO_PHONE_NUMBER", "")
TWILIO_WHATSAPP_NUMBER = os.environ.get("TWILIO_WHATSAPP_NUMBER", "")
TWILIO_CONTENT_SID = os.environ.get("TWILIO_CONTENT_SID", "")

# Initialize Twilio client if credentials available
twilio_client = None
try:
    if TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN:
        from twilio.rest import Client
        twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        logging.info("✅ Twilio client initialized successfully")
except ImportError:
    logging.warning("Twilio library not installed. SMS/WhatsApp notifications disabled.")
except Exception as e:
    logging.error(f"Twilio init error: {str(e)}")


async def send_sms(phone: str, message: str, db=None) -> dict:
    """Send SMS via Twilio"""
    if not twilio_client:
        logging.warning("Twilio not configured. SMS not sent.")
        return {"success": False, "error": "Twilio not configured"}
    
    try:
        # Ensure phone is in E.164 format
        if not phone.startswith("+"):
            phone = f"+91{phone}"  # Default to India
        
        msg = twilio_client.messages.create(
            body=message,
            from_=TWILIO_PHONE_NUMBER,
            to=phone
        )
        
        # Log notification
        if db is not None:
            await db.notification_logs.insert_one({
                "id": str(uuid.uuid4()),
                "type": "sms",
                "phone": phone,
                "message": message,
                "status": msg.status,
                "sid": msg.sid,
                "created_at": datetime.now(timezone.utc).isoformat()
            })
        
        return {"success": True, "sid": msg.sid, "status": msg.status}
    except Exception as e:
        logging.error(f"SMS failed: {str(e)}")
        return {"success": False, "error": str(e)}


async def send_whatsapp(phone: str, message: str, db=None) -> dict:
    """Send WhatsApp message via Twilio"""
    if not twilio_client:
        logging.warning("Twilio not configured. WhatsApp not sent.")
        return {"success": False, "error": "Twilio not configured"}
    
    try:
        # Ensure phone is in E.164 format
        if not phone.startswith("+"):
            phone = f"+91{phone}"
        
        msg = twilio_client.messages.create(
            body=message,
            from_=f"whatsapp:+{TWILIO_WHATSAPP_NUMBER.replace('+', '')}",
            to=f"whatsapp:{phone}"
        )
        
        # Log notification
        if db is not None:
            await db.notification_logs.insert_one({
                "id": str(uuid.uuid4()),
                "type": "whatsapp",
                "phone": phone,
                "message": message,
                "status": msg.status,
                "sid": msg.sid,
                "created_at": datetime.now(timezone.utc).isoformat()
            })
        
        return {"success": True, "sid": msg.sid, "status": msg.status}
    except Exception as e:
        logging.error(f"WhatsApp failed: {str(e)}")
        return {"success": False, "error": str(e)}


async def send_whatsapp_template(phone: str, content_sid: str, content_variables: dict, db=None) -> dict:
    """Send WhatsApp message using pre-approved content template"""
    if not twilio_client:
        logging.warning("Twilio not configured. WhatsApp not sent.")
        return {"success": False, "error": "Twilio not configured"}
    
    try:
        # Ensure phone is in E.164 format
        if not phone.startswith("+"):
            phone = f"+91{phone}"
        
        msg = twilio_client.messages.create(
            from_=f"whatsapp:+{TWILIO_WHATSAPP_NUMBER.replace('+', '')}",
            content_sid=content_sid,
            content_variables=json.dumps(content_variables),
            to=f"whatsapp:{phone}"
        )
        
        # Log notification
        if db is not None:
            await db.notification_logs.insert_one({
                "id": str(uuid.uuid4()),
                "type": "whatsapp_template",
                "phone": phone,
                "content_sid": content_sid,
                "content_variables": content_variables,
                "status": msg.status,
                "sid": msg.sid,
                "created_at": datetime.now(timezone.utc).isoformat()
            })
        
        return {"success": True, "sid": msg.sid, "status": msg.status}
    except Exception as e:
        logging.error(f"WhatsApp template failed: {str(e)}")
        return {"success": False, "error": str(e)}


# =============== NOTIFICATION TEMPLATES ===============

def get_order_confirmation_message(order_id: str, amount: float, product: str) -> str:
    return f"""🎉 Order Confirmed! 

Order ID: {order_id[:8].upper()}
Product: {product}
Amount: ₹{amount:,.2f}

Track your order at:
https://glassmesh.preview.emergentagent.com/track-order?id={order_id}

Thank you for choosing Lucumaa Glass! 🪟"""


def get_payment_received_message(order_id: str, amount: float) -> str:
    return f"""✅ Payment Received!

Order ID: {order_id[:8].upper()}
Amount: ₹{amount:,.2f}

Your order is being processed. We'll update you on the progress.

- Team Lucumaa Glass"""


def get_order_status_message(order_id: str, status: str, stage: str = "") -> str:
    status_emoji = {
        "processing": "🔄",
        "cutting": "✂️",
        "polishing": "✨",
        "toughening": "🔥",
        "quality_check": "🔍",
        "packing": "📦",
        "dispatched": "🚚",
        "delivered": "✅"
    }
    emoji = status_emoji.get(stage or status, "📋")
    
    return f"""{emoji} Order Update

Order ID: {order_id[:8].upper()}
Status: {(stage or status).replace('_', ' ').title()}

Track: https://glassmesh.preview.emergentagent.com/track-order?id={order_id}"""


def get_referral_bonus_message(amount: float, balance: float) -> str:
    return f"""🎁 Referral Bonus Credited!

You earned ₹{amount:,.0f} from a referral!
New wallet balance: ₹{balance:,.0f}

Keep referring & earning! Share your code with friends.

- Lucumaa Glass"""


def get_wallet_credit_message(amount: float, reason: str, balance: float) -> str:
    return f"""💰 Wallet Credited!

Amount: ₹{amount:,.0f}
Reason: {reason}
New Balance: ₹{balance:,.0f}

Use your wallet balance on your next order!"""


# =============== API ENDPOINTS ===============

async def send_with_sms_fallback(phone: str, message: str, db=None, prefer_whatsapp: bool = True) -> dict:
    """
    Send notification with automatic fallback.
    Priority: WhatsApp -> SMS
    If WhatsApp fails, automatically falls back to SMS.
    """
    result = {
        "whatsapp": {"attempted": False, "success": False},
        "sms": {"attempted": False, "success": False},
        "channel_used": None,
        "success": False
    }
    
    if prefer_whatsapp:
        # Try WhatsApp first
        result["whatsapp"]["attempted"] = True
        wa_result = await send_whatsapp(phone, message, db)
        result["whatsapp"]["success"] = wa_result.get("success", False)
        
        if result["whatsapp"]["success"]:
            result["success"] = True
            result["channel_used"] = "whatsapp"
            return result
    
    # Fallback to SMS
    result["sms"]["attempted"] = True
    sms_result = await send_sms(phone, message, db)
    result["sms"]["success"] = sms_result.get("success", False)
    
    if result["sms"]["success"]:
        result["success"] = True
        result["channel_used"] = "sms"
        return result
    
    # If SMS also failed and we haven't tried WhatsApp yet
    if not prefer_whatsapp and not result["whatsapp"]["attempted"]:
        result["whatsapp"]["attempted"] = True
        wa_result = await send_whatsapp(phone, message, db)
        result["whatsapp"]["success"] = wa_result.get("success", False)
        
        if result["whatsapp"]["success"]:
            result["success"] = True
            result["channel_used"] = "whatsapp"
    
    return result


@sms_router.post("/send-sms")
async def api_send_sms(
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Send SMS (Admin only)"""
    if current_user.get("role") not in ["admin", "owner"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    db = get_db()
    phone = data.get("phone")
    message = data.get("message")
    
    if not phone or not message:
        raise HTTPException(status_code=400, detail="Phone and message required")
    
    result = await send_sms(phone, message, db)
    return result


@sms_router.post("/send-whatsapp")
async def api_send_whatsapp(
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Send WhatsApp message (Admin only)"""
    if current_user.get("role") not in ["admin", "owner"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    db = get_db()
    phone = data.get("phone")
    message = data.get("message")
    
    if not phone or not message:
        raise HTTPException(status_code=400, detail="Phone and message required")
    
    result = await send_whatsapp(phone, message, db)
    return result


@sms_router.post("/send-with-fallback")
async def api_send_with_fallback(
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """
    Send notification with automatic SMS fallback.
    If WhatsApp fails, automatically falls back to SMS.
    """
    if current_user.get("role") not in ["admin", "owner", "super_admin"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    db = get_db()
    phone = data.get("phone")
    message = data.get("message")
    prefer_whatsapp = data.get("prefer_whatsapp", True)
    
    if not phone or not message:
        raise HTTPException(status_code=400, detail="Phone and message required")
    
    result = await send_with_sms_fallback(phone, message, db, prefer_whatsapp)
    return result


@sms_router.post("/order-confirmation")
async def send_order_confirmation(
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Send order confirmation notification"""
    db = get_db()
    order_id = data.get("order_id")
    
    order = await db.orders.find_one({"id": order_id}, {"_id": 0})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    user = await db.users.find_one({"id": order.get("user_id")}, {"_id": 0})
    if not user or not user.get("phone"):
        return {"success": False, "error": "User phone not found"}
    
    message = get_order_confirmation_message(
        order_id,
        order.get("total_price", 0),
        order.get("product_name", "Glass")
    )
    
    # Send both SMS and WhatsApp
    sms_result = await send_sms(user["phone"], message, db)
    wa_result = await send_whatsapp(user["phone"], message, db)
    
    return {
        "sms": sms_result,
        "whatsapp": wa_result
    }


@sms_router.post("/payment-received")
async def send_payment_notification(
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Send payment received notification"""
    db = get_db()
    order_id = data.get("order_id")
    amount = float(data.get("amount", 0))
    phone = data.get("phone")
    
    if not phone:
        order = await db.orders.find_one({"id": order_id}, {"_id": 0})
        if order:
            user = await db.users.find_one({"id": order.get("user_id")}, {"_id": 0})
            phone = user.get("phone") if user else None
    
    if not phone:
        return {"success": False, "error": "Phone not found"}
    
    message = get_payment_received_message(order_id, amount)
    
    result = await send_sms(phone, message, db)
    return result


@sms_router.post("/order-status-update")
async def send_status_update(
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Send order status update notification"""
    db = get_db()
    order_id = data.get("order_id")
    status = data.get("status")
    stage = data.get("stage", "")
    phone = data.get("phone")
    
    if not phone:
        order = await db.orders.find_one({"id": order_id}, {"_id": 0})
        if order:
            user = await db.users.find_one({"id": order.get("user_id")}, {"_id": 0})
            phone = user.get("phone") if user else None
    
    if not phone:
        return {"success": False, "error": "Phone not found"}
    
    message = get_order_status_message(order_id, status, stage)
    
    # Send via WhatsApp (preferred for status updates)
    result = await send_whatsapp(phone, message, db)
    return result


@sms_router.get("/logs")
async def get_notification_logs(
    limit: int = 100,
    notification_type: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get notification logs (Admin)"""
    if current_user.get("role") not in ["admin", "owner"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    db = get_db()
    query = {}
    if notification_type:
        query["type"] = notification_type
    
    logs = await db.notification_logs.find(query, {"_id": 0}).sort("created_at", -1).to_list(limit)
    return logs


@sms_router.get("/settings")
async def get_notification_settings(current_user: dict = Depends(get_erp_user)):
    """Get notification settings"""
    db = get_db()
    
    settings = await db.notification_settings.find_one({"id": "default"}, {"_id": 0})
    
    if not settings:
        settings = {
            "id": "default",
            "sms_enabled": True,
            "whatsapp_enabled": True,
            "order_confirmation": True,
            "payment_received": True,
            "status_updates": True,
            "referral_bonus": True,
            "wallet_credits": True,
            "promotional": False,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        await db.notification_settings.insert_one(settings)
    
    return settings


@sms_router.put("/settings")
async def update_notification_settings(
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Update notification settings (Admin)"""
    if current_user.get("role") not in ["admin", "owner"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    db = get_db()
    
    allowed_fields = [
        "sms_enabled", "whatsapp_enabled", "order_confirmation",
        "payment_received", "status_updates", "referral_bonus",
        "wallet_credits", "promotional"
    ]
    
    update_data = {k: v for k, v in data.items() if k in allowed_fields}
    update_data["updated_at"] = datetime.now(timezone.utc).isoformat()
    
    await db.notification_settings.update_one(
        {"id": "default"},
        {"$set": update_data},
        upsert=True
    )
    
    return {"message": "Settings updated"}


# =============== TWILIO STATUS ===============

@sms_router.get("/status")
async def get_twilio_status(current_user: dict = Depends(get_erp_user)):
    """Check Twilio configuration status"""
    return {
        "twilio_configured": twilio_client is not None,
        "sms_enabled": bool(TWILIO_PHONE_NUMBER),
        "whatsapp_enabled": bool(TWILIO_WHATSAPP_NUMBER),
        "account_sid_set": bool(TWILIO_ACCOUNT_SID),
        "auth_token_set": bool(TWILIO_AUTH_TOKEN),
        "content_sid_set": bool(TWILIO_CONTENT_SID),
        "whatsapp_number": f"+{TWILIO_WHATSAPP_NUMBER.replace('+', '')}" if TWILIO_WHATSAPP_NUMBER else None
    }


@sms_router.post("/test-whatsapp")
async def test_whatsapp_message(
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Test WhatsApp message sending"""
    if current_user.get("role") not in ["admin", "owner"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    db = get_db()
    phone = data.get("phone")
    
    if not phone:
        raise HTTPException(status_code=400, detail="Phone number required")
    
    # Use the content template if provided, otherwise send a test message
    content_sid = data.get("content_sid", TWILIO_CONTENT_SID)
    content_variables = data.get("content_variables", {"1": "12/1", "2": "3pm"})
    
    if content_sid:
        result = await send_whatsapp_template(phone, content_sid, content_variables, db)
    else:
        message = data.get("message", "🔔 Test notification from Lucumaa Glass ERP System!")
        result = await send_whatsapp(phone, message, db)
    
    return result


@sms_router.post("/send-whatsapp-template")
async def api_send_whatsapp_template(
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Send WhatsApp message using content template (Admin only)"""
    if current_user.get("role") not in ["admin", "owner"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    db = get_db()
    phone = data.get("phone")
    content_sid = data.get("content_sid", TWILIO_CONTENT_SID)
    content_variables = data.get("content_variables", {})
    
    if not phone:
        raise HTTPException(status_code=400, detail="Phone required")
    
    if not content_sid:
        raise HTTPException(status_code=400, detail="Content SID required")
    
    result = await send_whatsapp_template(phone, content_sid, content_variables, db)
    return result
