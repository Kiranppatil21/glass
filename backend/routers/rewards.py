"""
Customer Referral, Rewards & Credit System
- Referral codes & tracking
- Reward points on orders
- Store credit balance
"""
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime, timezone
import uuid
import random
import string

from routers.base import get_db, get_erp_user

rewards_router = APIRouter(prefix="/rewards", tags=["Rewards & Referrals"])

# ============ PYDANTIC MODELS ============

class ReferralSettings(BaseModel):
    referrer_reward_percent: float = 5  # % of first order value as credit
    referee_discount_percent: float = 10  # % discount on first order
    min_order_for_referral: float = 1000  # Min order value to qualify
    max_referral_reward: float = 500  # Max reward per referral
    reward_points_per_rupee: float = 1  # Points earned per rupee spent
    points_to_rupee_ratio: float = 10  # 10 points = ₹1
    active: bool = True

class CreditTransaction(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    amount: float
    type: str  # credit, debit, referral_bonus, order_reward, admin_adjustment
    description: str
    reference_id: Optional[str] = None  # Order ID or Referral ID
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    created_by: Optional[str] = None

class RedeemRequest(BaseModel):
    amount: float
    order_id: Optional[str] = None

class AdminCreditAdjust(BaseModel):
    user_id: str
    amount: float
    type: str  # credit or debit
    reason: str

# ============ HELPER FUNCTIONS ============

def generate_referral_code(name: str) -> str:
    """Generate unique referral code from user name"""
    prefix = ''.join(c for c in name[:4].upper() if c.isalpha())
    suffix = ''.join(random.choices(string.digits, k=4))
    return f"{prefix}{suffix}"

async def get_user_credit_balance(db, user_id: str) -> float:
    """Calculate user's current credit balance"""
    pipeline = [
        {"$match": {"user_id": user_id}},
        {"$group": {
            "_id": None,
            "total_credit": {
                "$sum": {
                    "$cond": [{"$in": ["$type", ["credit", "referral_bonus", "order_reward", "admin_credit"]]}, "$amount", 0]
                }
            },
            "total_debit": {
                "$sum": {
                    "$cond": [{"$in": ["$type", ["debit", "redemption", "admin_debit"]]}, "$amount", 0]
                }
            }
        }}
    ]
    result = await db.credit_transactions.aggregate(pipeline).to_list(1)
    if result:
        return round(result[0]["total_credit"] - result[0]["total_debit"], 2)
    return 0.0

async def get_user_points_balance(db, user_id: str) -> int:
    """Calculate user's current reward points"""
    pipeline = [
        {"$match": {"user_id": user_id}},
        {"$group": {
            "_id": None,
            "total_earned": {
                "$sum": {
                    "$cond": [{"$eq": ["$type", "earned"]}, "$points", 0]
                }
            },
            "total_redeemed": {
                "$sum": {
                    "$cond": [{"$eq": ["$type", "redeemed"]}, "$points", 0]
                }
            }
        }}
    ]
    result = await db.reward_points.aggregate(pipeline).to_list(1)
    if result:
        return int(result[0]["total_earned"] - result[0]["total_redeemed"])
    return 0

# ============ REFERRAL SETTINGS ============

@rewards_router.get("/settings")
async def get_rewards_settings():
    """Get referral & rewards settings"""
    db = get_db()
    settings = await db.rewards_settings.find_one({}, {"_id": 0})
    if not settings:
        settings = ReferralSettings().model_dump()
    return settings

@rewards_router.put("/settings")
async def update_rewards_settings(
    settings: ReferralSettings,
    current_user: dict = Depends(get_erp_user)
):
    """Update referral & rewards settings (Admin only)"""
    if current_user.get("role") not in ["super_admin", "admin", "owner"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    db = get_db()
    doc = settings.model_dump()
    doc["updated_by"] = current_user.get("name", "")
    doc["updated_at"] = datetime.now(timezone.utc).isoformat()
    
    await db.rewards_settings.update_one({}, {"$set": doc}, upsert=True)
    return {"message": "Settings updated", "settings": doc}

# ============ REFERRAL CODE ============

@rewards_router.get("/my-referral-code")
async def get_my_referral_code(current_user: dict = Depends(get_erp_user)):
    """Get or generate referral code for current user"""
    db = get_db()
    user_id = current_user.get("user_id") or current_user.get("id")
    
    # Check if user already has a referral code
    user = await db.users.find_one({"id": user_id}, {"_id": 0, "referral_code": 1, "name": 1})
    
    if user and user.get("referral_code"):
        code = user["referral_code"]
    else:
        # Generate new code
        name = current_user.get("name", "USER")
        code = generate_referral_code(name)
        
        # Ensure uniqueness
        while await db.users.find_one({"referral_code": code}):
            code = generate_referral_code(name)
        
        await db.users.update_one(
            {"id": user_id},
            {"$set": {"referral_code": code}}
        )
    
    # Get referral stats
    referral_count = await db.referrals.count_documents({"referrer_id": user_id, "status": "completed"})
    total_earned = await db.credit_transactions.aggregate([
        {"$match": {"user_id": user_id, "type": "referral_bonus"}},
        {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
    ]).to_list(1)
    
    return {
        "referral_code": code,
        "referral_link": f"https://lucumaaglass.in/register?ref={code}",
        "total_referrals": referral_count,
        "total_earned": total_earned[0]["total"] if total_earned else 0
    }

@rewards_router.post("/apply-referral")
async def apply_referral_code(
    referral_code: str,
    current_user: dict = Depends(get_erp_user)
):
    """Apply referral code for new user"""
    db = get_db()
    user_id = current_user.get("user_id") or current_user.get("id")
    
    # Check if user already used a referral
    existing = await db.referrals.find_one({"referee_id": user_id})
    if existing:
        raise HTTPException(status_code=400, detail="You have already used a referral code")
    
    # Find referrer
    referrer = await db.users.find_one({"referral_code": referral_code.upper()}, {"_id": 0})
    if not referrer:
        raise HTTPException(status_code=404, detail="Invalid referral code")
    
    if referrer.get("id") == user_id:
        raise HTTPException(status_code=400, detail="Cannot use your own referral code")
    
    # Get settings
    settings = await db.rewards_settings.find_one({}, {"_id": 0})
    if not settings:
        settings = ReferralSettings().model_dump()
    
    # Create referral record
    referral = {
        "id": str(uuid.uuid4()),
        "referrer_id": referrer.get("id"),
        "referrer_name": referrer.get("name"),
        "referee_id": user_id,
        "referee_name": current_user.get("name"),
        "referral_code": referral_code.upper(),
        "discount_percent": settings.get("referee_discount_percent", 10),
        "status": "pending",  # Will become 'completed' after first order
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.referrals.insert_one(referral)
    
    # Update user with referred_by
    await db.users.update_one(
        {"id": user_id},
        {"$set": {"referred_by": referrer.get("id"), "referral_discount": settings.get("referee_discount_percent", 10)}}
    )
    
    return {
        "message": f"Referral code applied! You'll get {settings.get('referee_discount_percent', 10)}% off on your first order.",
        "discount_percent": settings.get("referee_discount_percent", 10),
        "referred_by": referrer.get("name")
    }

# ============ CREDIT BALANCE ============

@rewards_router.get("/my-balance")
async def get_my_balance(current_user: dict = Depends(get_erp_user)):
    """Get current user's credit balance and points"""
    db = get_db()
    user_id = current_user.get("user_id") or current_user.get("id")
    
    credit_balance = await get_user_credit_balance(db, user_id)
    points_balance = await get_user_points_balance(db, user_id)
    
    # Get settings for conversion
    settings = await db.rewards_settings.find_one({}, {"_id": 0})
    points_value = points_balance / (settings.get("points_to_rupee_ratio", 10) if settings else 10)
    
    # Recent transactions
    transactions = await db.credit_transactions.find(
        {"user_id": user_id},
        {"_id": 0}
    ).sort("created_at", -1).limit(10).to_list(10)
    
    return {
        "credit_balance": credit_balance,
        "points_balance": points_balance,
        "points_value": round(points_value, 2),
        "total_available": round(credit_balance + points_value, 2),
        "recent_transactions": transactions
    }

@rewards_router.get("/transactions")
async def get_credit_transactions(
    limit: int = 50,
    current_user: dict = Depends(get_erp_user)
):
    """Get user's credit transaction history"""
    db = get_db()
    user_id = current_user.get("user_id") or current_user.get("id")
    
    transactions = await db.credit_transactions.find(
        {"user_id": user_id},
        {"_id": 0}
    ).sort("created_at", -1).limit(limit).to_list(limit)
    
    return {"transactions": transactions, "count": len(transactions)}

@rewards_router.post("/redeem")
async def redeem_credit(
    request: RedeemRequest,
    current_user: dict = Depends(get_erp_user)
):
    """Redeem credit balance against an order"""
    db = get_db()
    user_id = current_user.get("user_id") or current_user.get("id")
    
    # Check balance
    balance = await get_user_credit_balance(db, user_id)
    if request.amount > balance:
        raise HTTPException(status_code=400, detail=f"Insufficient balance. Available: ₹{balance}")
    
    if request.amount <= 0:
        raise HTTPException(status_code=400, detail="Amount must be positive")
    
    # Create debit transaction
    transaction = {
        "id": str(uuid.uuid4()),
        "user_id": user_id,
        "amount": request.amount,
        "type": "redemption",
        "description": f"Credit redeemed for order {request.order_id}" if request.order_id else "Credit redeemed",
        "reference_id": request.order_id,
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.credit_transactions.insert_one(transaction)
    
    new_balance = await get_user_credit_balance(db, user_id)
    
    return {
        "message": f"₹{request.amount} redeemed successfully",
        "redeemed_amount": request.amount,
        "new_balance": new_balance
    }

# ============ ADMIN ENDPOINTS ============

@rewards_router.get("/admin/user/{user_id}/balance")
async def admin_get_user_balance(
    user_id: str,
    current_user: dict = Depends(get_erp_user)
):
    """Admin: Get any user's balance"""
    if current_user.get("role") not in ["super_admin", "admin", "owner", "manager"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    user = await db.users.find_one({"id": user_id}, {"_id": 0, "name": 1, "email": 1})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    credit_balance = await get_user_credit_balance(db, user_id)
    points_balance = await get_user_points_balance(db, user_id)
    
    transactions = await db.credit_transactions.find(
        {"user_id": user_id},
        {"_id": 0}
    ).sort("created_at", -1).limit(20).to_list(20)
    
    return {
        "user": user,
        "credit_balance": credit_balance,
        "points_balance": points_balance,
        "transactions": transactions
    }

@rewards_router.post("/admin/adjust-credit")
async def admin_adjust_credit(
    adjustment: AdminCreditAdjust,
    current_user: dict = Depends(get_erp_user)
):
    """Admin: Add or deduct credit from user account"""
    if current_user.get("role") not in ["super_admin", "admin", "owner"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    db = get_db()
    
    # Verify user exists
    user = await db.users.find_one({"id": adjustment.user_id}, {"_id": 0})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    trans_type = "admin_credit" if adjustment.type == "credit" else "admin_debit"
    
    transaction = {
        "id": str(uuid.uuid4()),
        "user_id": adjustment.user_id,
        "amount": abs(adjustment.amount),
        "type": trans_type,
        "description": f"Admin adjustment: {adjustment.reason}",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "created_by": current_user.get("name", "")
    }
    
    await db.credit_transactions.insert_one(transaction)
    
    new_balance = await get_user_credit_balance(db, adjustment.user_id)
    
    return {
        "message": f"Credit {'added' if adjustment.type == 'credit' else 'deducted'} successfully",
        "amount": adjustment.amount,
        "new_balance": new_balance,
        "user_name": user.get("name")
    }

@rewards_router.get("/admin/referrals")
async def admin_get_all_referrals(
    status: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Admin: Get all referrals"""
    if current_user.get("role") not in ["super_admin", "admin", "owner", "manager"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    query = {}
    if status:
        query["status"] = status
    
    referrals = await db.referrals.find(query, {"_id": 0}).sort("created_at", -1).limit(100).to_list(100)
    
    # Summary stats
    total = await db.referrals.count_documents({})
    completed = await db.referrals.count_documents({"status": "completed"})
    pending = await db.referrals.count_documents({"status": "pending"})
    
    return {
        "referrals": referrals,
        "summary": {
            "total": total,
            "completed": completed,
            "pending": pending
        }
    }

@rewards_router.get("/admin/dashboard")
async def admin_rewards_dashboard(current_user: dict = Depends(get_erp_user)):
    """Admin: Rewards program dashboard"""
    if current_user.get("role") not in ["super_admin", "admin", "owner", "manager"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    # Total credit issued
    credit_issued = await db.credit_transactions.aggregate([
        {"$match": {"type": {"$in": ["credit", "referral_bonus", "order_reward", "admin_credit"]}}},
        {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
    ]).to_list(1)
    
    # Total credit redeemed
    credit_redeemed = await db.credit_transactions.aggregate([
        {"$match": {"type": {"$in": ["debit", "redemption", "admin_debit"]}}},
        {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
    ]).to_list(1)
    
    # Total referrals
    total_referrals = await db.referrals.count_documents({})
    completed_referrals = await db.referrals.count_documents({"status": "completed"})
    
    # Top referrers
    top_referrers = await db.referrals.aggregate([
        {"$match": {"status": "completed"}},
        {"$group": {"_id": "$referrer_id", "count": {"$sum": 1}, "name": {"$first": "$referrer_name"}}},
        {"$sort": {"count": -1}},
        {"$limit": 5}
    ]).to_list(5)
    
    return {
        "credit_stats": {
            "total_issued": credit_issued[0]["total"] if credit_issued else 0,
            "total_redeemed": credit_redeemed[0]["total"] if credit_redeemed else 0,
            "outstanding": (credit_issued[0]["total"] if credit_issued else 0) - (credit_redeemed[0]["total"] if credit_redeemed else 0)
        },
        "referral_stats": {
            "total": total_referrals,
            "completed": completed_referrals,
            "conversion_rate": round(completed_referrals / total_referrals * 100, 1) if total_referrals > 0 else 0
        },
        "top_referrers": top_referrers
    }

# ============ ORDER INTEGRATION HELPERS ============

async def process_referral_on_order(db, order: dict, settings: dict = None):
    """Process referral rewards when order is placed"""
    if not settings:
        settings = await db.rewards_settings.find_one({}, {"_id": 0})
        if not settings:
            settings = ReferralSettings().model_dump()
    
    user_id = order.get("user_id")
    order_total = order.get("total_price", 0)
    
    # Check if user was referred and this is their first order
    referral = await db.referrals.find_one({
        "referee_id": user_id,
        "status": "pending"
    })
    
    if referral and order_total >= settings.get("min_order_for_referral", 1000):
        # Calculate referrer reward
        reward_percent = settings.get("referrer_reward_percent", 5)
        max_reward = settings.get("max_referral_reward", 500)
        reward_amount = min(order_total * reward_percent / 100, max_reward)
        
        # Add credit to referrer
        credit_transaction = {
            "id": str(uuid.uuid4()),
            "user_id": referral.get("referrer_id"),
            "amount": round(reward_amount, 2),
            "type": "referral_bonus",
            "description": f"Referral bonus for {referral.get('referee_name')}'s first order",
            "reference_id": order.get("id"),
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        await db.credit_transactions.insert_one(credit_transaction)
        
        # Update referral status
        await db.referrals.update_one(
            {"id": referral.get("id")},
            {"$set": {
                "status": "completed",
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "order_id": order.get("id"),
                "reward_amount": reward_amount
            }}
        )
        
        return {"referrer_rewarded": True, "reward_amount": reward_amount}
    
    return {"referrer_rewarded": False}

async def add_order_reward_points(db, user_id: str, order_total: float, order_id: str, settings: dict = None):
    """Add reward points for order"""
    if not settings:
        settings = await db.rewards_settings.find_one({}, {"_id": 0})
        if not settings:
            settings = ReferralSettings().model_dump()
    
    points_per_rupee = settings.get("reward_points_per_rupee", 1)
    points_earned = int(order_total * points_per_rupee)
    
    if points_earned > 0:
        points_record = {
            "id": str(uuid.uuid4()),
            "user_id": user_id,
            "points": points_earned,
            "type": "earned",
            "description": f"Points for order",
            "reference_id": order_id,
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        await db.reward_points.insert_one(points_record)
    
    return {"points_earned": points_earned}
