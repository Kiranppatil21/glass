"""
Pydantic Models for Lucumaa Glass ERP
"""
from pydantic import BaseModel, Field, ConfigDict, EmailStr
from typing import List, Optional
from datetime import datetime, timezone
import uuid


class User(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    email: EmailStr
    name: str
    phone: str
    role: str = "customer"
    password_hash: str = ""
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class UserRegister(BaseModel):
    email: EmailStr
    name: str
    phone: str
    password: str
    role: str = "customer"


class UserLogin(BaseModel):
    email: EmailStr
    password: str


class Product(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    category: str
    description: str
    applications: List[str] = []
    thickness_options: List[float] = []
    strength_info: str = ""
    standards: str = ""
    image_url: str = ""
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class PricingRule(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    product_id: str
    thickness: float
    base_price_per_sqft: float
    min_quantity: int = 1
    bulk_discount_percent: float = 0
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class GlassItem(BaseModel):
    product_id: str
    product_name: str
    thickness: float
    width: float  # inches
    height: float  # inches
    quantity: int
    unit_price: float
    total_price: float
    edging: Optional[str] = None
    tempering: bool = False
    lamination: bool = False
    notes: Optional[str] = None


class DeliveryAddress(BaseModel):
    full_name: str
    phone: str
    address_line1: str
    address_line2: Optional[str] = None
    city: str
    state: str
    pincode: str
    landmark: Optional[str] = None


class Order(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    order_number: str = ""
    customer_id: str
    customer_name: str
    customer_email: str
    customer_phone: str
    glass_items: List[GlassItem] = []
    total_sqft: float = 0
    subtotal: float = 0
    discount_amount: float = 0
    discount_percent: float = 0
    tax_amount: float = 0
    total_price: float = 0
    advance_amount: float = 0
    advance_percent: float = 0
    remaining_amount: float = 0
    payment_status: str = "pending"
    razorpay_order_id: Optional[str] = None
    razorpay_payment_id: Optional[str] = None
    status: str = "pending"
    delivery_address: Optional[DeliveryAddress] = None
    delivery_type: str = "standard"
    estimated_delivery: Optional[str] = None
    notes: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: Optional[datetime] = None


class OrderCreate(BaseModel):
    customer_name: str
    customer_email: EmailStr
    customer_phone: str
    glass_items: List[GlassItem]
    delivery_address: Optional[DeliveryAddress] = None
    delivery_type: str = "standard"
    notes: Optional[str] = None
    advance_percent: Optional[int] = None
    is_credit_customer: bool = False
    gst_number: Optional[str] = None
    company_name: Optional[str] = None


class AdvanceSettings(BaseModel):
    normal_customer_min: int = 50
    normal_customer_options: List[int] = [50, 75, 100]
    credit_customer_min: int = 0
    credit_customer_options: List[int] = [0, 25, 50, 75, 100]
    admin_override_allowed: bool = True
    max_credit_limit: float = 100000


class PriceCalculation(BaseModel):
    product_id: str
    thickness: float
    width: float
    height: float
    quantity: int


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


class RemainingPaymentVerify(BaseModel):
    razorpay_order_id: str
    razorpay_payment_id: str
    razorpay_signature: str
