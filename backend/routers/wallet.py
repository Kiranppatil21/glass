"""
Wallet & Referral System Router
Handles customer wallet, referral codes, rewards, and admin settings
"""
from fastapi import APIRouter, Depends, HTTPException
from typing import Optional, Dict, Any
from datetime import datetime, timezone
import uuid
import random
import string
from .base import get_erp_user, get_db

wallet_router = APIRouter(prefix="/wallet", tags=["Wallet & Referral"])


def generate_referral_code(length: int = 8) -> str:
    """Generate a unique referral code"""
    chars = string.ascii_uppercase + string.digits
    return ''.join(random.choices(chars, k=length))


# =============== ADMIN SETTINGS ===============

@wallet_router.get("/settings")
async def get_wallet_settings(current_user: dict = Depends(get_erp_user)):
    """Get wallet and referral settings"""
    db = get_db()
    
    settings = await db.wallet_settings.find_one({"id": "default"}, {"_id": 0})
    
    if not settings:
        # Create default settings
        settings = {
            "id": "default",
            # Referral Settings
            "referral_enabled": True,
            "referral_bonus_type": "both",  # flat, percentage, both
            "referral_flat_bonus": 100,  # ₹100 flat bonus
            "referral_percentage_bonus": 5,  # 5% of first order
            "referral_bonus_cap": 500,  # Max bonus if percentage
            "referee_bonus_enabled": True,  # Bonus for person who was referred
            "referee_bonus_amount": 50,  # ₹50 for new user
            # Wallet Settings
            "wallet_enabled": True,
            "wallet_usage_type": "percentage",  # percentage, fixed
            "wallet_max_percentage": 25,  # Max 25% of order can be paid via wallet
            "wallet_max_fixed": 500,  # Or max ₹500 per order
            "min_order_for_wallet": 500,  # Min order value to use wallet
            # Rewards Settings
            "cashback_enabled": True,
            "cashback_percentage": 2,  # 2% cashback on orders
            "cashback_cap": 200,  # Max ₹200 cashback per order
            "min_order_for_cashback": 1000,  # Min order for cashback
            # Expiry
            "wallet_credit_expiry_days": 365,  # Credits expire after 1 year
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        await db.wallet_settings.insert_one(settings)
    
    return settings


@wallet_router.put("/settings")
async def update_wallet_settings(
    settings_data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Update wallet and referral settings (Admin only)"""
    db = get_db()
    
    if current_user.get("role") not in ["admin", "owner"]:
        raise HTTPException(status_code=403, detail="Only admin can update settings")
    
    allowed_fields = [
        "referral_enabled", "referral_bonus_type", "referral_flat_bonus",
        "referral_percentage_bonus", "referral_bonus_cap", "referee_bonus_enabled",
        "referee_bonus_amount", "wallet_enabled", "wallet_usage_type",
        "wallet_max_percentage", "wallet_max_fixed", "min_order_for_wallet",
        "cashback_enabled", "cashback_percentage", "cashback_cap",
        "min_order_for_cashback", "wallet_credit_expiry_days"
    ]
    
    update_data = {k: v for k, v in settings_data.items() if k in allowed_fields}
    update_data["updated_at"] = datetime.now(timezone.utc).isoformat()
    
    await db.wallet_settings.update_one(
        {"id": "default"},
        {"$set": update_data},
        upsert=True
    )
    
    return {"message": "Settings updated successfully"}


# =============== CUSTOMER WALLET ===============

@wallet_router.get("/balance")
async def get_wallet_balance(current_user: dict = Depends(get_erp_user)):
    """Get current user's wallet balance"""
    db = get_db()
    
    wallet = await db.wallets.find_one({"user_id": current_user["id"]}, {"_id": 0})
    
    if not wallet:
        # Create wallet for user
        wallet = {
            "id": str(uuid.uuid4()),
            "user_id": current_user["id"],
            "balance": 0,
            "total_earned": 0,
            "total_spent": 0,
            "referral_code": generate_referral_code(),
            "referred_by": None,
            "referral_count": 0,
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        await db.wallets.insert_one(wallet)
    
    return {
        "balance": wallet.get("balance", 0),
        "total_earned": wallet.get("total_earned", 0),
        "total_spent": wallet.get("total_spent", 0),
        "referral_code": wallet.get("referral_code"),
        "referral_count": wallet.get("referral_count", 0)
    }


@wallet_router.get("/transactions")
async def get_wallet_transactions(
    limit: int = 50,
    current_user: dict = Depends(get_erp_user)
):
    """Get wallet transaction history"""
    db = get_db()
    
    transactions = await db.wallet_transactions.find(
        {"user_id": current_user["id"]},
        {"_id": 0}
    ).sort("created_at", -1).to_list(limit)
    
    return transactions


@wallet_router.post("/apply-referral")
async def apply_referral_code(
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Apply a referral code (for new users)"""
    db = get_db()
    referral_code = data.get("referral_code", "").upper()
    
    # Get user's wallet
    wallet = await db.wallets.find_one({"user_id": current_user["id"]}, {"_id": 0})
    
    if wallet and wallet.get("referred_by"):
        raise HTTPException(status_code=400, detail="You have already used a referral code")
    
    # Find referrer
    referrer_wallet = await db.wallets.find_one({"referral_code": referral_code}, {"_id": 0})
    if not referrer_wallet:
        raise HTTPException(status_code=404, detail="Invalid referral code")
    
    if referrer_wallet["user_id"] == current_user["id"]:
        raise HTTPException(status_code=400, detail="Cannot use your own referral code")
    
    # Get settings
    settings = await get_wallet_settings(current_user)
    
    if not settings.get("referral_enabled"):
        raise HTTPException(status_code=400, detail="Referral program is currently disabled")
    
    # Update user's wallet with referrer
    if not wallet:
        wallet = {
            "id": str(uuid.uuid4()),
            "user_id": current_user["id"],
            "balance": 0,
            "total_earned": 0,
            "total_spent": 0,
            "referral_code": generate_referral_code(),
            "referred_by": referrer_wallet["user_id"],
            "referral_count": 0,
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        await db.wallets.insert_one(wallet)
    else:
        await db.wallets.update_one(
            {"user_id": current_user["id"]},
            {"$set": {"referred_by": referrer_wallet["user_id"]}}
        )
    
    # Give referee bonus if enabled
    if settings.get("referee_bonus_enabled"):
        referee_bonus = settings.get("referee_bonus_amount", 50)
        await credit_wallet(
            db, current_user["id"], referee_bonus,
            "referral_bonus", f"Welcome bonus for using referral code {referral_code}"
        )
    
    return {"message": "Referral code applied successfully"}


async def credit_wallet(db, user_id: str, amount: float, txn_type: str, description: str):
    """Credit amount to user's wallet"""
    # Get or create wallet
    wallet = await db.wallets.find_one({"user_id": user_id}, {"_id": 0})
    if not wallet:
        wallet = {
            "id": str(uuid.uuid4()),
            "user_id": user_id,
            "balance": 0,
            "total_earned": 0,
            "total_spent": 0,
            "referral_code": generate_referral_code(),
            "referred_by": None,
            "referral_count": 0,
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        await db.wallets.insert_one(wallet)
    
    new_balance = wallet.get("balance", 0) + amount
    new_earned = wallet.get("total_earned", 0) + amount
    
    await db.wallets.update_one(
        {"user_id": user_id},
        {"$set": {"balance": new_balance, "total_earned": new_earned}}
    )
    
    # Create transaction
    transaction = {
        "id": str(uuid.uuid4()),
        "user_id": user_id,
        "type": "credit",
        "category": txn_type,
        "amount": amount,
        "balance_after": new_balance,
        "description": description,
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    await db.wallet_transactions.insert_one(transaction)
    
    return new_balance


async def debit_wallet(db, user_id: str, amount: float, txn_type: str, description: str, reference: str = ""):
    """Debit amount from user's wallet"""
    wallet = await db.wallets.find_one({"user_id": user_id}, {"_id": 0})
    if not wallet or wallet.get("balance", 0) < amount:
        raise HTTPException(status_code=400, detail="Insufficient wallet balance")
    
    new_balance = wallet.get("balance", 0) - amount
    new_spent = wallet.get("total_spent", 0) + amount
    
    await db.wallets.update_one(
        {"user_id": user_id},
        {"$set": {"balance": new_balance, "total_spent": new_spent}}
    )
    
    # Create transaction
    transaction = {
        "id": str(uuid.uuid4()),
        "user_id": user_id,
        "type": "debit",
        "category": txn_type,
        "amount": amount,
        "balance_after": new_balance,
        "description": description,
        "reference": reference,
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    await db.wallet_transactions.insert_one(transaction)
    
    return new_balance


@wallet_router.post("/calculate-usage")
async def calculate_wallet_usage(
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Calculate how much wallet balance can be used for an order"""
    db = get_db()
    order_amount = float(data.get("order_amount", 0))
    
    settings = await get_wallet_settings(current_user)
    wallet = await db.wallets.find_one({"user_id": current_user["id"]}, {"_id": 0})
    
    if not settings.get("wallet_enabled"):
        return {"usable_amount": 0, "reason": "Wallet usage is disabled"}
    
    if order_amount < settings.get("min_order_for_wallet", 0):
        return {"usable_amount": 0, "reason": f"Minimum order ₹{settings.get('min_order_for_wallet')} required"}
    
    balance = wallet.get("balance", 0) if wallet else 0
    
    if balance <= 0:
        return {"usable_amount": 0, "reason": "No wallet balance"}
    
    # Calculate max usable
    if settings.get("wallet_usage_type") == "percentage":
        max_usable = order_amount * (settings.get("wallet_max_percentage", 25) / 100)
    else:
        max_usable = settings.get("wallet_max_fixed", 500)
    
    usable_amount = min(balance, max_usable)
    
    return {
        "usable_amount": round(usable_amount, 2),
        "wallet_balance": balance,
        "max_allowed": round(max_usable, 2),
        "usage_type": settings.get("wallet_usage_type"),
        "usage_limit": settings.get("wallet_max_percentage") if settings.get("wallet_usage_type") == "percentage" else settings.get("wallet_max_fixed")
    }


@wallet_router.post("/use")
async def use_wallet_balance(
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Use wallet balance for an order"""
    db = get_db()
    order_id = data.get("order_id")
    amount = float(data.get("amount", 0))
    
    if amount <= 0:
        raise HTTPException(status_code=400, detail="Invalid amount")
    
    # Verify order
    order = await db.orders.find_one({"id": order_id}, {"_id": 0})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    # Debit wallet
    new_balance = await debit_wallet(
        db, current_user["id"], amount,
        "order_payment", f"Payment for order {order_id[:8]}",
        order_id
    )
    
    return {
        "message": "Wallet balance applied",
        "amount_used": amount,
        "new_balance": new_balance
    }


# =============== REFERRAL REWARDS ===============

@wallet_router.post("/process-referral-bonus")
async def process_referral_bonus(
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Process referral bonus after first order (internal use)"""
    db = get_db()
    order_id = data.get("order_id")
    order_amount = float(data.get("order_amount", 0))
    user_id = data.get("user_id", current_user["id"])
    
    # Get user's wallet to check referrer
    wallet = await db.wallets.find_one({"user_id": user_id}, {"_id": 0})
    if not wallet or not wallet.get("referred_by"):
        return {"message": "No referrer found"}
    
    # Check if bonus already given for this referral
    existing_bonus = await db.wallet_transactions.find_one({
        "user_id": wallet["referred_by"],
        "category": "referral_bonus",
        "description": {"$regex": user_id}
    }, {"_id": 0})
    
    if existing_bonus:
        return {"message": "Referral bonus already processed"}
    
    # Get settings
    settings = await get_wallet_settings(current_user)
    
    if not settings.get("referral_enabled"):
        return {"message": "Referral program disabled"}
    
    # Calculate bonus
    bonus_type = settings.get("referral_bonus_type", "both")
    bonus = 0
    
    if bonus_type in ["flat", "both"]:
        bonus = settings.get("referral_flat_bonus", 100)
    
    if bonus_type in ["percentage", "both"]:
        percentage_bonus = order_amount * (settings.get("referral_percentage_bonus", 5) / 100)
        percentage_bonus = min(percentage_bonus, settings.get("referral_bonus_cap", 500))
        if bonus_type == "both":
            bonus = max(bonus, percentage_bonus)  # Give higher of the two
        else:
            bonus = percentage_bonus
    
    # Credit referrer
    await credit_wallet(
        db, wallet["referred_by"], bonus,
        "referral_bonus", f"Referral bonus for user {user_id[:8]}"
    )
    
    # Update referrer's referral count
    await db.wallets.update_one(
        {"user_id": wallet["referred_by"]},
        {"$inc": {"referral_count": 1}}
    )
    
    return {"message": "Referral bonus processed", "bonus_amount": bonus}


@wallet_router.post("/process-cashback")
async def process_cashback(
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Process cashback for an order"""
    db = get_db()
    order_id = data.get("order_id")
    order_amount = float(data.get("order_amount", 0))
    user_id = data.get("user_id", current_user["id"])
    
    settings = await get_wallet_settings(current_user)
    
    if not settings.get("cashback_enabled"):
        return {"message": "Cashback disabled"}
    
    if order_amount < settings.get("min_order_for_cashback", 0):
        return {"message": "Order below minimum for cashback"}
    
    # Calculate cashback
    cashback = order_amount * (settings.get("cashback_percentage", 2) / 100)
    cashback = min(cashback, settings.get("cashback_cap", 200))
    
    # Credit cashback
    await credit_wallet(
        db, user_id, cashback,
        "cashback", f"Cashback for order {order_id[:8]}"
    )
    
    return {"message": "Cashback credited", "cashback_amount": cashback}


# =============== ADMIN REPORTS ===============

@wallet_router.get("/admin/stats")
async def get_wallet_stats(current_user: dict = Depends(get_erp_user)):
    """Get wallet system statistics (Admin)"""
    db = get_db()
    
    if current_user.get("role") not in ["admin", "owner"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    # Total wallets
    total_wallets = await db.wallets.count_documents({})
    
    # Total balance across all wallets
    pipeline = [
        {"$group": {"_id": None, "total_balance": {"$sum": "$balance"}, "total_earned": {"$sum": "$total_earned"}}}
    ]
    totals = await db.wallets.aggregate(pipeline).to_list(1)
    
    # Recent transactions
    recent_txns = await db.wallet_transactions.find({}, {"_id": 0}).sort("created_at", -1).to_list(20)
    
    # Referral stats
    referral_pipeline = [
        {"$match": {"referred_by": {"$ne": None}}},
        {"$count": "total_referrals"}
    ]
    referral_result = await db.wallets.aggregate(referral_pipeline).to_list(1)
    
    return {
        "total_wallets": total_wallets,
        "total_balance_outstanding": totals[0]["total_balance"] if totals else 0,
        "total_rewards_given": totals[0]["total_earned"] if totals else 0,
        "total_referrals": referral_result[0]["total_referrals"] if referral_result else 0,
        "recent_transactions": recent_txns
    }


@wallet_router.get("/admin/users")
async def get_all_user_wallets(
    limit: int = 100,
    current_user: dict = Depends(get_erp_user)
):
    """Get all user wallets (Admin)"""
    db = get_db()
    
    if current_user.get("role") not in ["admin", "owner"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    wallets = await db.wallets.find({}, {"_id": 0}).sort("balance", -1).to_list(limit)
    
    # Enrich with user info
    for wallet in wallets:
        user = await db.users.find_one({"id": wallet["user_id"]}, {"_id": 0, "name": 1, "email": 1})
        if user:
            wallet["user_name"] = user.get("name")
            wallet["user_email"] = user.get("email")
    
    return wallets


@wallet_router.post("/admin/credit")
async def admin_credit_wallet(
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Admin credit to user wallet"""
    db = get_db()
    
    if current_user.get("role") not in ["admin", "owner"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    user_id = data.get("user_id")
    amount = float(data.get("amount", 0))
    reason = data.get("reason", "Admin credit")
    
    if amount <= 0:
        raise HTTPException(status_code=400, detail="Invalid amount")
    
    new_balance = await credit_wallet(db, user_id, amount, "admin_credit", reason)
    
    return {"message": "Wallet credited", "new_balance": new_balance}
