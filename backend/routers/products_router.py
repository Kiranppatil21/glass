"""
Products Router - Product catalog and pricing
"""
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, timezone

products_router = APIRouter(prefix="/products", tags=["Products"])

# Database reference
_db = None

def init_products_router(database):
    global _db
    _db = database

def get_db():
    return _db


# =============== MODELS ===============

class Product(BaseModel):
    id: str
    name: str
    category: str
    description: str
    applications: List[str] = []
    thickness_options: List[float] = []
    strength_info: str = ""
    standards: str = ""
    image_url: str = ""


class PriceCalculation(BaseModel):
    product_id: str
    thickness: float
    width: float  # inches
    height: float  # inches
    quantity: int


# =============== ENDPOINTS ===============

@products_router.get("", response_model=List[Product])
async def get_products():
    """Get all products"""
    db = get_db()
    products = await db.products.find({}, {"_id": 0}).to_list(100)
    return products


@products_router.get("/{product_id}")
async def get_product(product_id: str):
    """Get single product by ID"""
    db = get_db()
    product = await db.products.find_one({"id": product_id}, {"_id": 0})
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    return product


@products_router.post("/calculate-price")
async def calculate_price(calc: PriceCalculation):
    """Calculate price for glass item"""
    db = get_db()
    
    # Get pricing rule
    pricing = await db.pricing_rules.find_one({
        "product_id": calc.product_id,
        "thickness": calc.thickness
    }, {"_id": 0})
    
    if not pricing:
        # Default pricing
        base_price = 50  # per sq ft
    else:
        base_price = pricing.get("base_price_per_sqft", 50)
    
    # Calculate area in sq ft
    sqft = (calc.width * calc.height * calc.quantity) / 144
    
    # Calculate price
    price = round(sqft * base_price, 2)
    
    # Apply bulk discount
    if calc.quantity >= 10:
        discount = pricing.get("bulk_discount_percent", 5) if pricing else 5
        price = round(price * (1 - discount / 100), 2)
    
    return {
        "sqft": round(sqft, 2),
        "base_price_per_sqft": base_price,
        "subtotal": price,
        "tax": round(price * 0.18, 2),
        "total": round(price * 1.18, 2)
    }
