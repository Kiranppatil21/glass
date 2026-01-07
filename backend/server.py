from fastapi import FastAPI, APIRouter, HTTPException, UploadFile, File, Form, Depends, status, Request, BackgroundTasks
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field, ConfigDict, EmailStr
from typing import List, Optional
import uuid
from datetime import datetime, timezone, timedelta
import bcrypt
import jwt
import razorpay
import aiosmtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import asyncio
from twilio.rest import Client as TwilioClient

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

app = FastAPI(redirect_slashes=False)
api_router = APIRouter(prefix="/api")

# Health check endpoint for Kubernetes
@app.get("/health")
async def health_check():
    """Health check endpoint for Kubernetes liveness/readiness probes"""
    return {"status": "healthy", "service": "lucumaa-glass-backend"}

security = HTTPBearer()

JWT_SECRET = os.environ.get('JWT_SECRET', 'your-secret-key-change-in-production')
APP_URL = os.environ.get('APP_URL', 'https://lucumaaglass.in')
razorpay_client = razorpay.Client(auth=(os.environ.get('RAZORPAY_KEY_ID', ''), os.environ.get('RAZORPAY_KEY_SECRET', '')))

# SMTP Email Configuration
SMTP_HOST = os.environ.get('SMTP_HOST', 'smtp.hostinger.com')
SMTP_PORT = int(os.environ.get('SMTP_PORT', 465))
SMTP_USER = os.environ.get('SMTP_USER', 'info@lucumaaGlass.in')
SMTP_PASSWORD = os.environ.get('SMTP_PASSWORD', '')
SENDER_EMAIL = os.environ.get('SENDER_EMAIL', 'info@lucumaaGlass.in')
SENDER_NAME = os.environ.get('SENDER_NAME', 'Lucumaa Glass')

twilio_client = TwilioClient(os.environ.get('TWILIO_ACCOUNT_SID', ''), os.environ.get('TWILIO_AUTH_TOKEN', ''))

SENDER_EMAIL = os.environ.get('SENDER_EMAIL', 'onboarding@resend.dev')
TWILIO_PHONE = os.environ.get('TWILIO_PHONE_NUMBER', '')
TWILIO_WHATSAPP = os.environ.get('TWILIO_WHATSAPP_NUMBER', 'whatsapp:+14155238886')

UPLOAD_DIR = ROOT_DIR / 'uploads'
UPLOAD_DIR.mkdir(exist_ok=True)

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
    bulk_discount_percent: float = 0.0
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class Order(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    order_number: str = ""  # 6-digit order number
    user_id: str
    customer_name: str = ""
    company_name: str = ""
    product_id: str
    product_name: str
    thickness: float
    width: float
    height: float
    quantity: int
    area_sqft: float
    total_price: float
    # GST Fields
    delivery_state_code: str = "27"  # Default Maharashtra
    customer_gstin: str = ""
    gst_type: str = "intra_state"  # intra_state or inter_state
    hsn_code: str = "7007"  # Default: Safety glass
    cgst_rate: float = 9.0
    cgst_amount: float = 0
    sgst_rate: float = 9.0
    sgst_amount: float = 0
    igst_rate: float = 0
    igst_amount: float = 0
    total_gst: float = 0
    base_amount: float = 0  # Amount before GST
    # Advance Payment Fields
    advance_percent: int = 100  # 25, 50, 75, or 100
    advance_amount: float = 0
    remaining_amount: float = 0
    advance_payment_status: str = "pending"  # pending, paid
    remaining_payment_status: str = "not_applicable"  # not_applicable, pending, paid, cash_received
    remaining_payment_method: str = ""  # online, cash
    cash_received_by: str = ""  # User ID who received cash
    cash_received_by_name: str = ""  # Name of person who received cash
    cash_received_at: Optional[str] = None  # Timestamp
    # Transport Charges
    transport_charge: float = 0  # Manual entry
    transport_charge_note: str = ""  # Note like "Transport charge extra applicable"
    transport_added_by: str = ""  # User who added transport charge
    transport_vehicle_type: str = ""  # truck, tempo, pickup, etc.
    # Dispatch
    dispatch_slip_number: str = ""  # DS-YYYYMMDD-XXXX
    dispatch_created_at: Optional[str] = None
    dispatch_created_by: str = ""
    dispatched_at: Optional[str] = None
    # Status fields
    status: str = "pending"  # pending, confirmed, processing, ready_for_dispatch, dispatched, delivered
    payment_status: str = "pending"  # pending, partial, completed
    razorpay_order_id: Optional[str] = None
    razorpay_payment_id: Optional[str] = None
    remaining_razorpay_order_id: Optional[str] = None
    remaining_razorpay_payment_id: Optional[str] = None
    file_paths: List[str] = []
    delivery_address: str = ""
    notes: str = ""
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class OrderCreate(BaseModel):
    product_id: str
    thickness: float
    width: float
    height: float
    quantity: int
    delivery_address: str
    notes: Optional[str] = ""
    advance_percent: int = 100  # 25, 50, 75, or 100
    customer_name: Optional[str] = ""
    company_name: Optional[str] = ""
    is_credit_order: bool = False  # Admin approved credit order
    override_total: Optional[float] = None  # For multi-item orders
    # GST fields
    delivery_state_code: Optional[str] = "27"  # Default Maharashtra
    customer_gstin: Optional[str] = ""
    gst_info: Optional[dict] = None  # GST breakdown from frontend
    # Payment preference for remaining amount
    remaining_payment_preference: Optional[str] = "online"  # online or cash
    # Customer Master Integration
    customer_profile_id: Optional[str] = None  # Link to Customer Master profile

class AdvanceSettings(BaseModel):
    """Settings for advance payment rules"""
    no_advance_upto: float = 2000  # No advance option below this amount
    min_advance_percent_upto_5000: int = 50  # Minimum advance % for orders upto 5000
    min_advance_percent_above_5000: int = 25  # Minimum advance % for orders above 5000
    credit_enabled: bool = True  # Allow credit orders (admin only)
    updated_by: Optional[str] = None
    updated_at: Optional[str] = None

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
    message: str
    inquiry_type: str = "general"

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))

def create_token(user_id: str, email: str, role: str) -> str:
    payload = {
        'user_id': user_id,
        'email': email,
        'role': role,
        'exp': datetime.now(timezone.utc) + timedelta(days=7)
    }
    return jwt.encode(payload, JWT_SECRET, algorithm='HS256')

async def generate_order_number() -> str:
    """Generate a unique 6-digit order number"""
    # Find the last order number
    last_order = await db.orders.find_one(
        {"order_number": {"$exists": True, "$ne": ""}},
        {"order_number": 1},
        sort=[("order_number", -1)]
    )
    
    if last_order and last_order.get("order_number"):
        try:
            last_num = int(last_order["order_number"])
            new_num = last_num + 1
        except ValueError:
            new_num = 1
    else:
        new_num = 1
    
    # Format as 6-digit string with leading zeros
    return str(new_num).zfill(6)

async def get_advance_settings():
    """Get current advance payment settings"""
    settings = await db.settings.find_one({"type": "advance_payment"}, {"_id": 0})
    if not settings:
        # Default settings
        return {
            "type": "advance_payment",
            "no_advance_upto": 2000,
            "min_advance_percent_upto_5000": 50,
            "min_advance_percent_above_5000": 25,
            "credit_enabled": True
        }
    return settings

async def validate_advance_percent(total_amount: float, requested_percent: int, is_credit: bool, user_role: str):
    """Validate advance percentage based on settings and user role"""
    settings = await get_advance_settings()
    
    # Credit orders - only admin/owner/super_admin can create
    if is_credit:
        if user_role not in ['admin', 'owner', 'super_admin']:
            raise HTTPException(status_code=403, detail="Only admin can approve credit orders")
        return 0, "credit"  # 0% advance for credit
    
    # Rule 1: Below no_advance_upto - 100% required
    if total_amount <= settings.get("no_advance_upto", 2000):
        if requested_percent < 100:
            return 100, f"Full payment required for orders below ₹{settings.get('no_advance_upto', 2000)}"
        return 100, None
    
    # Rule 2: Between no_advance_upto and 5000 - min 50% required
    if total_amount <= 5000:
        min_required = settings.get("min_advance_percent_upto_5000", 50)
        if requested_percent < min_required:
            return min_required, f"Minimum {min_required}% advance required for orders upto ₹5000"
        return requested_percent, None
    
    # Rule 3: Above 5000 - min 25% required
    min_required = settings.get("min_advance_percent_above_5000", 25)
    if requested_percent < min_required:
        return min_required, f"Minimum {min_required}% advance required for orders above ₹5000"
    return requested_percent, None

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        token = credentials.credentials
        payload = jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
        user = await db.users.find_one({"id": payload['user_id']}, {"_id": 0})
        if not user:
            raise HTTPException(status_code=401, detail="User not found")
        return user
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

async def send_email_notification(recipient: str, subject: str, html_content: str):
    """Send email using Hostinger SMTP with SSL"""
    try:
        message = MIMEMultipart('alternative')
        message['Subject'] = subject
        message['From'] = f"{SENDER_NAME} <{SENDER_EMAIL}>"
        message['To'] = recipient
        
        html_part = MIMEText(html_content, 'html')
        message.attach(html_part)
        
        await aiosmtplib.send(
            message,
            hostname=SMTP_HOST,
            port=SMTP_PORT,
            username=SMTP_USER,
            password=SMTP_PASSWORD,
            use_tls=True,
        )
        logging.info(f"Email sent successfully to {recipient}")
    except Exception as e:
        logging.error(f"Email send failed to {recipient}: {str(e)}")

async def send_sms_notification(phone: str, message: str):
    try:
        if TWILIO_PHONE:
            await asyncio.to_thread(
                lambda: twilio_client.messages.create(
                    body=message,
                    from_=TWILIO_PHONE,
                    to=phone
                )
            )
    except Exception as e:
        logging.error(f"SMS send failed: {str(e)}")

async def send_whatsapp_notification(phone: str, message: str):
    try:
        if TWILIO_WHATSAPP:
            await asyncio.to_thread(
                lambda: twilio_client.messages.create(
                    body=message,
                    from_=TWILIO_WHATSAPP,
                    to=f"whatsapp:{phone}"
                )
            )
    except Exception as e:
        logging.error(f"WhatsApp send failed: {str(e)}")

def generate_order_confirmation_email(order_id: str, customer_name: str, product_name: str, quantity: int, total_price: float):
    """Generate beautiful HTML email for order confirmation"""
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{ font-family: 'Arial', sans-serif; line-height: 1.6; color: #333; }}
            .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
            .header {{ background: linear-gradient(135deg, #0e7490 0%, #0891b2 100%); color: white; padding: 30px; text-align: center; border-radius: 10px 10px 0 0; }}
            .content {{ background: #f8fafc; padding: 30px; border-radius: 0 0 10px 10px; }}
            .order-box {{ background: white; padding: 20px; margin: 20px 0; border-radius: 8px; border-left: 4px solid #0e7490; }}
            .footer {{ text-align: center; padding: 20px; color: #64748b; font-size: 14px; }}
            .button {{ display: inline-block; background: #0e7490; color: white; padding: 12px 30px; text-decoration: none; border-radius: 5px; margin: 20px 0; }}
            .highlight {{ color: #0e7490; font-weight: bold; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🎉 Order Confirmed!</h1>
                <p>Thank you for choosing Lucumaa Glass</p>
            </div>
            <div class="content">
                <h2>Hello {customer_name},</h2>
                <p>Your order has been successfully placed and payment confirmed. We're excited to start manufacturing your premium glass!</p>
                
                <div class="order-box">
                    <h3>Order Details</h3>
                    <p><strong>Order ID:</strong> <span class="highlight">#{order_id[:8].upper()}</span></p>
                    <p><strong>Product:</strong> {product_name}</p>
                    <p><strong>Quantity:</strong> {quantity} pieces</p>
                    <p><strong>Total Amount:</strong> ₹{total_price:,.2f} (incl. GST)</p>
                </div>
                
                <h3>What Happens Next?</h3>
                <ul>
                    <li>✅ <strong>Order Confirmed</strong> - Your order is in our system</li>
                    <li>🏭 <strong>Production</strong> - Manufacturing starts within 2 days</li>
                    <li>🔍 <strong>Quality Check</strong> - Rigorous testing before dispatch</li>
                    <li>🚚 <strong>Delivery</strong> - Safe delivery in 7-14 days</li>
                </ul>
                
                <center>
                    <a href="{APP_URL}/track" class="button">Track Your Order</a>
                </center>
                
                <p style="margin-top: 30px;">Need assistance? Contact us:</p>
                <p>📞 <strong>+91 92847 01985</strong><br>
                📧 <strong>info@lucumaaGlass.in</strong><br>
                💬 <strong>WhatsApp:</strong> <a href="https://wa.me/919284701985">Chat with us</a></p>
            </div>
            <div class="footer">
                <p><strong>Lucumaa Glass</strong> - A Unit of Lucumaa Corporation Pvt. Ltd.</p>
                <p>Factory: Lohegaon, Pune - 411047 | Office: Undri, Pune - 411060</p>
                <p>© 2025 Lucumaa Glass. All rights reserved.</p>
            </div>
        </div>
    </body>
    </html>
    """

def generate_status_update_email(customer_name: str, order_id: str, status: str, product_name: str):
    """Generate HTML email for order status updates"""
    status_messages = {
        'confirmed': '✅ Your order has been confirmed and is ready for production',
        'production': '🏭 Your glass is being manufactured with precision',
        'quality_check': '🔍 Your order is undergoing final quality inspection',
        'dispatched': '🚚 Your order has been dispatched and is on the way',
        'delivered': '🎉 Your order has been delivered successfully'
    }
    
    status_info = status_messages.get(status, 'Your order status has been updated')
    
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{ font-family: 'Arial', sans-serif; line-height: 1.6; color: #333; }}
            .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
            .header {{ background: linear-gradient(135deg, #0e7490 0%, #0891b2 100%); color: white; padding: 30px; text-align: center; border-radius: 10px 10px 0 0; }}
            .content {{ background: #f8fafc; padding: 30px; border-radius: 0 0 10px 10px; }}
            .status-box {{ background: white; padding: 20px; margin: 20px 0; border-radius: 8px; text-align: center; border: 2px solid #0e7490; }}
            .footer {{ text-align: center; padding: 20px; color: #64748b; font-size: 14px; }}
            .button {{ display: inline-block; background: #0e7490; color: white; padding: 12px 30px; text-decoration: none; border-radius: 5px; margin: 20px 0; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>📦 Order Status Update</h1>
                <p>Lucumaa Glass</p>
            </div>
            <div class="content">
                <h2>Hello {customer_name},</h2>
                <p>Great news about your order!</p>
                
                <div class="status-box">
                    <h2 style="color: #0e7490; margin: 0;">{status_info}</h2>
                    <p style="margin: 10px 0 0 0;">Order #{order_id[:8].upper()} - {product_name}</p>
                </div>
                
                <p>You can track your order in real-time using the button below:</p>
                
                <center>
                    <a href="{APP_URL}/track" class="button">Track Order</a>
                </center>
                
                <p style="margin-top: 30px;">Questions? We're here to help:</p>
                <p>📞 +91 92847 01985 | 📧 info@lucumaaGlass.in</p>
            </div>
            <div class="footer">
                <p><strong>Lucumaa Glass</strong> - Premium Quality, Factory Direct</p>
                <p>© 2025 Lucumaa Glass. All rights reserved.</p>
            </div>
        </div>
    </body>
    </html>
    """

@api_router.post("/auth/register")
async def register_user(user_data: UserRegister, background_tasks: BackgroundTasks = None):
    existing_user = await db.users.find_one({"email": user_data.email}, {"_id": 0})
    if existing_user:
        raise HTTPException(status_code=400, detail="Email already registered")
    
    user = User(
        email=user_data.email,
        name=user_data.name,
        phone=user_data.phone,
        role=user_data.role,
        password_hash=hash_password(user_data.password)
    )
    
    doc = user.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    await db.users.insert_one(doc)
    
    token = create_token(user.id, user.email, user.role)
    
    # Send welcome email in background
    if background_tasks:
        welcome_email = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
                .header {{ background: linear-gradient(135deg, #0e7490 0%, #0891b2 100%); color: white; padding: 30px; text-align: center; border-radius: 10px 10px 0 0; }}
                .content {{ background: #f8fafc; padding: 30px; border-radius: 0 0 10px 10px; }}
                .button {{ display: inline-block; background: #0e7490; color: white; padding: 12px 30px; text-decoration: none; border-radius: 5px; margin: 20px 0; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>Welcome to Lucumaa Glass! 🎉</h1>
                </div>
                <div class="content">
                    <h2>Hello {user.name},</h2>
                    <p>Thank you for registering with Lucumaa Glass - your trusted partner for premium quality glass solutions.</p>
                    
                    <h3>What You Can Do:</h3>
                    <ul>
                        <li>🔍 Browse our premium glass collection</li>
                        <li>💰 Get instant price calculations</li>
                        <li>📐 Customize glass to your exact specifications</li>
                        <li>📦 Track your orders in real-time</li>
                        <li>📄 Download invoices & warranty certificates</li>
                    </ul>
                    
                    <center>
                        <a href="{APP_URL}/customize" class="button">Start Your First Order</a>
                    </center>
                    
                    <h3>Why Choose Lucumaa Glass?</h3>
                    <p>✓ Factory-Direct Pricing<br>
                    ✓ IS 2553 Certified Quality<br>
                    ✓ 7-14 Days Delivery<br>
                    ✓ 50,000+ Satisfied Customers</p>
                    
                    <p style="margin-top: 30px;">Need help? Contact us:</p>
                    <p>📞 +91 92847 01985<br>
                    📧 info@lucumaaGlass.in<br>
                    💬 <a href="https://wa.me/919284701985">WhatsApp Support</a></p>
                </div>
                <div style="text-align: center; padding: 20px; color: #64748b; font-size: 14px;">
                    <p><strong>Lucumaa Glass</strong> - A Unit of Lucumaa Corporation Pvt. Ltd.</p>
                    <p>© 2025 Lucumaa Glass. All rights reserved.</p>
                </div>
            </div>
        </body>
        </html>
        """
        background_tasks.add_task(
            send_email_notification,
            user.email,
            "Welcome to Lucumaa Glass - Premium Quality Glass Solutions",
            welcome_email
        )
    
    return {
        "message": "User registered successfully",
        "token": token,
        "user": {
            "id": user.id,
            "email": user.email,
            "name": user.name,
            "role": user.role
        }
    }

@api_router.post("/auth/login")
async def login_user(login_data: UserLogin):
    user = await db.users.find_one({"email": login_data.email}, {"_id": 0})
    if not user or not verify_password(login_data.password, user['password_hash']):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    token = create_token(user['id'], user['email'], user['role'])
    
    return {
        "message": "Login successful",
        "token": token,
        "user": {
            "id": user['id'],
            "email": user['email'],
            "name": user['name'],
            "role": user['role']
        }
    }


# =============== OTP AUTHENTICATION ===============
import random
import string

def generate_otp(length: int = 6) -> str:
    """Generate numeric OTP"""
    return ''.join(random.choices(string.digits, k=length))

def generate_otp_email_html(otp: str, purpose: str = "verification") -> str:
    """Generate beautiful OTP email"""
    title = "Login OTP" if purpose == "login" else "Reset Password OTP" if purpose == "reset" else "Verification OTP"
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
            .container {{ max-width: 500px; margin: 0 auto; padding: 20px; }}
            .header {{ background: linear-gradient(135deg, #0e7490 0%, #0891b2 100%); color: white; padding: 30px; text-align: center; border-radius: 10px 10px 0 0; }}
            .content {{ background: #f8fafc; padding: 30px; border-radius: 0 0 10px 10px; text-align: center; }}
            .otp-box {{ background: white; padding: 20px; margin: 20px 0; border-radius: 8px; border: 2px dashed #0e7490; }}
            .otp {{ font-size: 36px; font-weight: bold; color: #0e7490; letter-spacing: 8px; }}
            .footer {{ text-align: center; padding: 20px; color: #64748b; font-size: 12px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🔐 {title}</h1>
                <p>Lucumaa Glass</p>
            </div>
            <div class="content">
                <p>Use the following OTP to {purpose}:</p>
                <div class="otp-box">
                    <div class="otp">{otp}</div>
                </div>
                <p style="color: #ef4444; font-size: 14px;">⚠️ Valid for 10 minutes only. Do not share with anyone.</p>
            </div>
            <div class="footer">
                <p>If you didn't request this OTP, please ignore this email.</p>
                <p>© 2025 Lucumaa Glass. All rights reserved.</p>
            </div>
        </div>
    </body>
    </html>
    """

@api_router.post("/auth/send-otp")
async def send_otp(data: dict, background_tasks: BackgroundTasks):
    """Send OTP via Email or Mobile"""
    method = data.get("method", "email")  # email or mobile
    identifier = data.get("identifier")  # email or phone number
    purpose = data.get("purpose", "login")  # login, reset, verify
    
    if not identifier:
        raise HTTPException(status_code=400, detail="Email or phone required")
    
    # Generate OTP
    otp = generate_otp(6)
    otp_expiry = datetime.now(timezone.utc) + timedelta(minutes=10)
    
    # Store OTP
    otp_doc = {
        "identifier": identifier,
        "otp": otp,
        "method": method,
        "purpose": purpose,
        "expires_at": otp_expiry.isoformat(),
        "verified": False,
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    # Remove old OTPs for this identifier
    await db.otp_codes.delete_many({"identifier": identifier})
    await db.otp_codes.insert_one(otp_doc)
    
    # Send OTP
    if method == "email":
        html_content = generate_otp_email_html(otp, purpose)
        subject = f"Your Lucumaa Glass OTP: {otp}"
        background_tasks.add_task(send_email_notification, identifier, subject, html_content)
        return {"message": "OTP sent to email", "method": "email"}
    else:
        # Send via SMS
        sms_message = f"Your Lucumaa Glass OTP is: {otp}. Valid for 10 minutes. Do not share with anyone."
        try:
            if TWILIO_PHONE:
                phone = identifier if identifier.startswith("+") else f"+91{identifier}"
                await asyncio.to_thread(
                    lambda: twilio_client.messages.create(
                        body=sms_message,
                        from_=TWILIO_PHONE,
                        to=phone
                    )
                )
        except Exception as e:
            error_msg = str(e)
            logging.error(f"SMS OTP failed: {error_msg}")
            if "unverified" in error_msg.lower():
                raise HTTPException(status_code=400, detail="This mobile number is not verified. Please verify it in Twilio console or use Email OTP.")
            raise HTTPException(status_code=500, detail="Failed to send SMS OTP. Please try Email OTP instead.")
        
        return {"message": "OTP sent to mobile", "method": "mobile"}


@api_router.post("/auth/verify-otp")
async def verify_otp(data: dict):
    """Verify OTP and return token for login/reset"""
    identifier = data.get("identifier")
    otp = data.get("otp")
    purpose = data.get("purpose", "login")
    
    if not identifier or not otp:
        raise HTTPException(status_code=400, detail="Identifier and OTP required")
    
    # Find OTP record
    otp_record = await db.otp_codes.find_one({
        "identifier": identifier,
        "otp": otp,
        "verified": False
    }, {"_id": 0})
    
    if not otp_record:
        raise HTTPException(status_code=400, detail="Invalid OTP")
    
    # Check expiry
    expiry = datetime.fromisoformat(otp_record["expires_at"].replace("Z", "+00:00"))
    if datetime.now(timezone.utc) > expiry:
        raise HTTPException(status_code=400, detail="OTP expired")
    
    # Mark as verified
    await db.otp_codes.update_one(
        {"identifier": identifier, "otp": otp},
        {"$set": {"verified": True}}
    )
    
    # For login purpose, find/create user and return token
    if purpose == "login":
        # Find user by email or phone
        user = await db.users.find_one({
            "$or": [{"email": identifier}, {"phone": identifier}]
        }, {"_id": 0})
        
        if not user:
            raise HTTPException(status_code=404, detail="User not found. Please register first.")
        
        token = create_token(user['id'], user['email'], user['role'])
        
        return {
            "message": "OTP verified",
            "token": token,
            "user": {
                "id": user['id'],
                "email": user['email'],
                "name": user['name'],
                "role": user['role']
            }
        }
    
    elif purpose == "reset":
        # Return a reset token
        user = await db.users.find_one({
            "$or": [{"email": identifier}, {"phone": identifier}]
        }, {"_id": 0})
        
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        reset_token = str(uuid.uuid4())
        await db.password_resets.insert_one({
            "user_id": user['id'],
            "token": reset_token,
            "expires_at": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
            "used": False,
            "created_at": datetime.now(timezone.utc).isoformat()
        })
        
        return {
            "message": "OTP verified. You can now reset your password.",
            "reset_token": reset_token
        }
    
    return {"message": "OTP verified"}


@api_router.post("/auth/reset-password")
async def reset_password(data: dict):
    """Reset password using reset token"""
    reset_token = data.get("reset_token")
    new_password = data.get("new_password")
    
    if not reset_token or not new_password:
        raise HTTPException(status_code=400, detail="Reset token and new password required")
    
    if len(new_password) < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters")
    
    # Find reset token
    reset_record = await db.password_resets.find_one({
        "token": reset_token,
        "used": False
    }, {"_id": 0})
    
    if not reset_record:
        raise HTTPException(status_code=400, detail="Invalid or expired reset token")
    
    # Check expiry
    expiry = datetime.fromisoformat(reset_record["expires_at"].replace("Z", "+00:00"))
    if datetime.now(timezone.utc) > expiry:
        raise HTTPException(status_code=400, detail="Reset token expired")
    
    # Update password
    new_hash = hash_password(new_password)
    await db.users.update_one(
        {"id": reset_record["user_id"]},
        {"$set": {"password_hash": new_hash, "updated_at": datetime.now(timezone.utc).isoformat()}}
    )
    
    # Mark reset token as used
    await db.password_resets.update_one(
        {"token": reset_token},
        {"$set": {"used": True}}
    )
    
    return {"message": "Password reset successful. You can now login with your new password."}


@api_router.get("/auth/me")
async def get_me(current_user: dict = Depends(get_current_user)):
    return {
        "id": current_user['id'],
        "email": current_user['email'],
        "name": current_user['name'],
        "role": current_user['role']
    }

# ERP User Management (Admin only)
@api_router.post("/auth/create-erp-user")
async def create_erp_user(user_data: dict, current_user: dict = Depends(get_current_user)):
    """Create an ERP user with specific role - Admin only"""
    if current_user.get('role') not in ['admin', 'owner']:
        raise HTTPException(status_code=403, detail="Only admin can create ERP users")
    
    valid_roles = ['admin', 'owner', 'manager', 'sales', 'production_manager', 'operator', 'hr', 'accountant', 'store']
    role = user_data.get('role', 'operator')
    
    if role not in valid_roles:
        raise HTTPException(status_code=400, detail=f"Invalid role. Must be one of: {valid_roles}")
    
    # Check if email exists
    existing_user = await db.users.find_one({"email": user_data.get('email')}, {"_id": 0})
    if existing_user:
        raise HTTPException(status_code=400, detail="Email already registered")
    
    user = User(
        email=user_data.get('email'),
        name=user_data.get('name'),
        phone=user_data.get('phone', ''),
        role=role,
        password_hash=hash_password(user_data.get('password', 'Welcome@123'))
    )
    
    doc = user.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    await db.users.insert_one(doc)
    
    return {
        "message": f"ERP user created with role: {role}",
        "user_id": user.id,
        "email": user.email
    }

@api_router.get("/auth/erp-users")
async def get_erp_users(current_user: dict = Depends(get_current_user)):
    """Get all ERP users - Admin only"""
    if current_user.get('role') not in ['admin', 'owner']:
        raise HTTPException(status_code=403, detail="Only admin can view ERP users")
    
    erp_roles = ['admin', 'owner', 'manager', 'sales', 'production_manager', 'operator', 'hr', 'accountant', 'store']
    users = await db.users.find({"role": {"$in": erp_roles}}, {"_id": 0, "password_hash": 0}).to_list(100)
    return users

@api_router.get("/products", response_model=List[Product])
async def get_products():
    products = await db.products.find({}, {"_id": 0}).to_list(100)
    for p in products:
        if isinstance(p.get('created_at'), str):
            p['created_at'] = datetime.fromisoformat(p['created_at'])
    return products

@api_router.get("/products/{product_id}")
async def get_product(product_id: str):
    product = await db.products.find_one({"id": product_id}, {"_id": 0})
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    if isinstance(product.get('created_at'), str):
        product['created_at'] = datetime.fromisoformat(product['created_at'])
    return product

@api_router.post("/pricing/calculate")
async def calculate_price(calc: PriceCalculation):
    pricing = await db.pricing_rules.find_one(
        {"product_id": calc.product_id, "thickness": calc.thickness},
        {"_id": 0}
    )
    
    if not pricing:
        raise HTTPException(status_code=404, detail="Pricing not available for this configuration")
    
    area_sqft = (calc.width * calc.height) / 144
    total_area = area_sqft * calc.quantity
    base_price = total_area * pricing['base_price_per_sqft']
    
    discount = 0
    if calc.quantity >= 10:
        discount = base_price * (pricing.get('bulk_discount_percent', 0) / 100)
    
    final_price = base_price - discount
    gst = final_price * 0.18
    total = final_price + gst
    
    return {
        "area_sqft": round(area_sqft, 2),
        "total_area": round(total_area, 2),
        "base_price": round(base_price, 2),
        "discount": round(discount, 2),
        "subtotal": round(final_price, 2),
        "gst": round(gst, 2),
        "total": round(total, 2)
    }

@api_router.post("/orders")
async def create_order(
    order_data: OrderCreate,
    current_user: dict = Depends(get_current_user)
):
    product = await db.products.find_one({"id": order_data.product_id}, {"_id": 0})
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    # Auto-populate from Customer Master if profile_id provided
    customer_profile = None
    customer_name = order_data.customer_name or current_user.get('name', '')
    company_name = order_data.company_name or ''
    customer_gstin = order_data.customer_gstin or ''
    is_credit_order = order_data.is_credit_order
    
    if order_data.customer_profile_id:
        customer_profile = await db.customer_profiles.find_one(
            {"id": order_data.customer_profile_id, "status": "active"},
            {"_id": 0}
        )
        if customer_profile:
            customer_name = customer_profile.get("display_name") or customer_name
            company_name = customer_profile.get("company_name") or company_name
            customer_gstin = customer_profile.get("gstin") or customer_gstin
            # Auto-enable credit if customer has credit allowed
            if customer_profile.get("credit_type") == "credit_allowed":
                is_credit_order = True
    
    calc = PriceCalculation(
        product_id=order_data.product_id,
        thickness=order_data.thickness,
        width=order_data.width,
        height=order_data.height,
        quantity=order_data.quantity
    )
    price_info = await calculate_price(calc)
    
    # Debug logging for payment amount issue
    logging.info(f"ORDER DEBUG: override_total={order_data.override_total}, price_info_total={price_info['total']}, advance_percent={order_data.advance_percent}")
    
    # Use override_total if provided (for multi-item orders), otherwise use calculated price
    total_price = order_data.override_total if order_data.override_total and order_data.override_total > 0 else price_info['total']
    
    logging.info(f"ORDER DEBUG: final_total_price={total_price}")
    
    # Validate advance percentage based on settings
    validated_percent, message = await validate_advance_percent(
        total_price, 
        order_data.advance_percent, 
        is_credit_order,
        current_user.get('role', 'customer')
    )
    
    # If credit order, set special status
    is_credit = is_credit_order and current_user.get('role') in ['admin', 'owner', 'super_admin']
    
    # Calculate advance and remaining amounts
    advance_percent = 0 if is_credit else validated_percent
    advance_amount = round(total_price * advance_percent / 100, 2)
    remaining_amount = round(total_price - advance_amount, 2)
    
    logging.info(f"ORDER DEBUG: advance_percent={advance_percent}, advance_amount={advance_amount}, remaining_amount={remaining_amount}")
    
    # Generate 6-digit order number
    order_number = await generate_order_number()
    
    # Extract GST info from frontend
    gst_info = order_data.gst_info or {}
    gst_type = gst_info.get('gst_type', 'intra_state')
    base_amount = total_price - gst_info.get('total_gst', 0) if gst_info else total_price
    
    order = Order(
        order_number=order_number,
        user_id=current_user['id'],
        customer_name=customer_name,
        company_name=company_name,
        product_id=order_data.product_id,
        product_name=product['name'],
        thickness=order_data.thickness,
        width=order_data.width,
        height=order_data.height,
        quantity=order_data.quantity,
        area_sqft=price_info['total_area'],
        total_price=total_price,
        # GST fields
        delivery_state_code=order_data.delivery_state_code or "27",
        customer_gstin=customer_gstin,
        gst_type=gst_type,
        hsn_code=gst_info.get('hsn_code', '7007'),
        cgst_rate=gst_info.get('cgst_rate', 9.0),
        cgst_amount=gst_info.get('cgst_amount', 0),
        sgst_rate=gst_info.get('sgst_rate', 9.0),
        sgst_amount=gst_info.get('sgst_amount', 0),
        igst_rate=gst_info.get('igst_rate', 0),
        igst_amount=gst_info.get('igst_amount', 0),
        total_gst=gst_info.get('total_gst', 0),
        base_amount=base_amount,
        # Payment fields
        advance_percent=advance_percent,
        advance_amount=advance_amount,
        remaining_amount=remaining_amount,
        advance_payment_status="not_applicable" if is_credit else "pending",
        remaining_payment_status="pending" if is_credit else ("not_applicable" if advance_percent == 100 else "pending"),
        payment_status="credit" if is_credit else ("pending" if advance_percent == 100 else "partial_pending"),
        delivery_address=order_data.delivery_address,
        notes=order_data.notes or ""
    )
    
    # For credit orders, no Razorpay order needed
    if is_credit:
        doc = order.model_dump()
        doc['created_at'] = doc['created_at'].isoformat()
        doc['updated_at'] = doc['updated_at'].isoformat()
        doc['is_credit_order'] = True
        doc['credit_approved_by'] = current_user.get('name', '')
        doc['credit_approved_at'] = datetime.now(timezone.utc).isoformat()
        doc['customer_profile_id'] = order_data.customer_profile_id  # Link to Customer Master
        await db.orders.insert_one(doc)
        
        return {
            "order_id": order.id,
            "order_number": order_number,
            "is_credit_order": True,
            "total_amount": total_price,
            "advance_percent": 0,
            "advance_amount": 0,
            "remaining_amount": total_price,
            "amount_to_pay_now": 0,
            "message": "Credit order created. Full payment due after delivery.",
            "currency": "INR",
            "customer_profile": customer_profile
        }
    
    # Create Razorpay order for advance amount only
    razor_order = razorpay_client.order.create({
        "amount": int(advance_amount * 100),  # Advance amount in paise
        "currency": "INR",
        "payment_capture": 1,
        "notes": {
            "order_number": order_number,
            "payment_type": "advance",
            "advance_percent": str(advance_percent)
        }
    })
    
    order.razorpay_order_id = razor_order['id']
    
    doc = order.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    doc['updated_at'] = doc['updated_at'].isoformat()
    doc['customer_profile_id'] = order_data.customer_profile_id  # Link to Customer Master
    await db.orders.insert_one(doc)
    
    return {
        "order_id": order.id,
        "order_number": order_number,
        "razorpay_order_id": order.razorpay_order_id,
        "total_amount": total_price,
        "advance_percent": advance_percent,
        "advance_amount": advance_amount,
        "remaining_amount": remaining_amount,
        "amount_to_pay_now": advance_amount,
        "message": message,
        "currency": "INR",
        "customer_profile": customer_profile
    }

@api_router.post("/orders/{order_id}/upload")
async def upload_order_file(
    order_id: str,
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user)
):
    order = await db.orders.find_one({"id": order_id, "user_id": current_user['id']}, {"_id": 0})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    file_path = UPLOAD_DIR / f"{order_id}_{file.filename}"
    with open(file_path, "wb") as f:
        content = await file.read()
        f.write(content)
    
    file_paths = order.get('file_paths', [])
    file_paths.append(str(file_path))
    
    await db.orders.update_one(
        {"id": order_id},
        {"$set": {"file_paths": file_paths}}
    )
    
    return {"message": "File uploaded successfully", "file_path": str(file_path)}

# ============= ADVANCE SETTINGS APIs (Admin Only) =============

@api_router.get("/settings/advance")
async def get_advance_settings_api(current_user: dict = Depends(get_current_user)):
    """Get current advance payment settings"""
    settings = await get_advance_settings()
    return settings

@api_router.put("/settings/advance")
async def update_advance_settings_api(
    data: dict,
    current_user: dict = Depends(get_current_user)
):
    """Update advance payment settings - Admin only"""
    if current_user.get('role') not in ['admin', 'owner', 'super_admin']:
        raise HTTPException(status_code=403, detail="Only admin can update advance settings")
    
    # Validate settings
    no_advance_upto = data.get('no_advance_upto', 2000)
    min_advance_5000 = data.get('min_advance_percent_upto_5000', 50)
    min_advance_above = data.get('min_advance_percent_above_5000', 25)
    credit_enabled = data.get('credit_enabled', True)
    
    if no_advance_upto < 0:
        raise HTTPException(status_code=400, detail="no_advance_upto must be >= 0")
    if min_advance_5000 < 0 or min_advance_5000 > 100:
        raise HTTPException(status_code=400, detail="min_advance_percent_upto_5000 must be 0-100")
    if min_advance_above < 0 or min_advance_above > 100:
        raise HTTPException(status_code=400, detail="min_advance_percent_above_5000 must be 0-100")
    
    settings = {
        "type": "advance_payment",
        "no_advance_upto": float(no_advance_upto),
        "min_advance_percent_upto_5000": int(min_advance_5000),
        "min_advance_percent_above_5000": int(min_advance_above),
        "credit_enabled": credit_enabled,
        "updated_by": current_user.get('name', ''),
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.settings.update_one(
        {"type": "advance_payment"},
        {"$set": settings},
        upsert=True
    )
    
    return {"message": "Advance settings updated successfully", "settings": settings}

@api_router.get("/settings/advance/validate-order")
async def validate_order_advance(
    amount: float,
    advance_percent: int = 25,
    current_user: dict = Depends(get_current_user)
):
    """Validate what advance options are available for given amount"""
    settings = await get_advance_settings()
    
    result = {
        "total_amount": amount,
        "requested_percent": advance_percent,
        "settings": settings
    }
    
    # Rule 1: Below no_advance_upto - 100% required
    if amount <= settings.get("no_advance_upto", 2000):
        result["allowed_percentages"] = [100]
        result["message"] = f"Full payment required for orders below ₹{settings.get('no_advance_upto', 2000)}"
        result["min_required"] = 100
        return result
    
    # Rule 2: Between no_advance_upto and 5000 - min 50% required
    if amount <= 5000:
        min_req = settings.get("min_advance_percent_upto_5000", 50)
        allowed = [p for p in [25, 50, 75, 100] if p >= min_req]
        result["allowed_percentages"] = allowed
        result["message"] = f"Minimum {min_req}% advance required for orders upto ₹5000"
        result["min_required"] = min_req
        return result
    
    # Rule 3: Above 5000 - min 25% required
    min_req = settings.get("min_advance_percent_above_5000", 25)
    allowed = [p for p in [25, 50, 75, 100] if p >= min_req]
    result["allowed_percentages"] = allowed
    result["message"] = f"Minimum {min_req}% advance required for orders above ₹5000"
    result["min_required"] = min_req
    
    # Admin can also offer credit
    if current_user.get('role') in ['admin', 'owner', 'super_admin'] and settings.get('credit_enabled', True):
        result["credit_available"] = True
        result["allowed_percentages"] = [0] + allowed  # 0 = credit
    
    return result

@api_router.post("/orders/{order_id}/payment")
async def verify_payment(
    order_id: str,
    razorpay_payment_id: str = Form(...),
    razorpay_signature: str = Form(...),
    background_tasks: BackgroundTasks = None,
    current_user: dict = Depends(get_current_user)
):
    order = await db.orders.find_one({"id": order_id, "user_id": current_user['id']}, {"_id": 0})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    try:
        razorpay_client.utility.verify_payment_signature({
            'razorpay_order_id': order['razorpay_order_id'],
            'razorpay_payment_id': razorpay_payment_id,
            'razorpay_signature': razorpay_signature
        })
    except:
        raise HTTPException(status_code=400, detail="Payment verification failed")
    
    # Update based on advance payment or full payment
    advance_percent = order.get('advance_percent', 100)
    
    if advance_percent == 100:
        # Full payment - mark as completed
        update_data = {
            "payment_status": "completed",
            "advance_payment_status": "paid",
            "razorpay_payment_id": razorpay_payment_id,
            "status": "confirmed",
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
    else:
        # Partial payment - advance paid, remaining pending
        update_data = {
            "payment_status": "partial",
            "advance_payment_status": "paid",
            "razorpay_payment_id": razorpay_payment_id,
            "status": "confirmed",
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
    
    await db.orders.update_one({"id": order_id}, {"$set": update_data})
    
    # Send automatic confirmation email in background
    if background_tasks:
        email_html = generate_order_confirmation_email(
            order_id=order_id,
            customer_name=current_user['name'],
            product_name=order['product_name'],
            quantity=order['quantity'],
            total_price=order['total_price']
        )
        background_tasks.add_task(
            send_email_notification,
            current_user['email'],
            f"Order Confirmed #{order_id[:8].upper()} - Lucumaa Glass",
            email_html
        )
        
        # WhatsApp notification
        background_tasks.add_task(
            send_whatsapp_notification,
            current_user['phone'],
            f"🎉 Order Confirmed!\n\nHello {current_user['name']},\n\nYour Lucumaa Glass order #{order_id[:8].upper()} has been confirmed.\n\nProduct: {order['product_name']}\nAmount: ₹{order['total_price']:,.2f}\n\nManufacturing starts in 2 days. Track: {APP_URL}/track\n\nThank you!"
        )
    
    return {"message": "Payment verified successfully", "order_id": order_id}

@api_router.post("/orders/{order_id}/pay-remaining")
@api_router.post("/orders/{order_id}/initiate-remaining-payment")
async def initiate_remaining_payment(
    order_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Initiate remaining payment for orders with partial advance"""
    order = await db.orders.find_one({"id": order_id, "user_id": current_user['id']}, {"_id": 0})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    # Check if advance is paid
    if order.get('advance_payment_status') != 'paid':
        raise HTTPException(status_code=400, detail="Advance payment not yet completed")
    
    # Check if remaining payment is applicable
    if order.get('advance_percent', 100) == 100:
        raise HTTPException(status_code=400, detail="No remaining payment for full advance orders")
    
    # Check if remaining is already paid
    if order.get('remaining_payment_status') == 'paid':
        raise HTTPException(status_code=400, detail="Remaining payment already completed")
    
    remaining_amount = order.get('remaining_amount', 0)
    if remaining_amount <= 0:
        raise HTTPException(status_code=400, detail="No remaining amount to pay")
    
    # Create Razorpay order for remaining amount
    razor_order = razorpay_client.order.create({
        "amount": int(remaining_amount * 100),
        "currency": "INR",
        "payment_capture": 1,
        "notes": {
            "order_number": order.get('order_number', ''),
            "payment_type": "remaining"
        }
    })
    
    # Save remaining razorpay order id
    await db.orders.update_one(
        {"id": order_id},
        {"$set": {"remaining_razorpay_order_id": razor_order['id']}}
    )
    
    return {
        "order_id": order_id,
        "order_number": order.get('order_number'),
        "razorpay_order_id": razor_order['id'],
        "amount": remaining_amount,
        "currency": "INR",
        "payment_type": "remaining"
    }

class RemainingPaymentVerify(BaseModel):
    razorpay_payment_id: str
    razorpay_signature: str

@api_router.post("/orders/{order_id}/verify-remaining")
@api_router.post("/orders/{order_id}/verify-remaining-payment")
async def verify_remaining_payment(
    order_id: str,
    payment_data: RemainingPaymentVerify,
    background_tasks: BackgroundTasks = None,
    current_user: dict = Depends(get_current_user)
):
    """Verify remaining payment after advance"""
    order = await db.orders.find_one({"id": order_id, "user_id": current_user['id']}, {"_id": 0})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    try:
        razorpay_client.utility.verify_payment_signature({
            'razorpay_order_id': order['remaining_razorpay_order_id'],
            'razorpay_payment_id': payment_data.razorpay_payment_id,
            'razorpay_signature': payment_data.razorpay_signature
        })
    except:
        raise HTTPException(status_code=400, detail="Payment verification failed")
    
    await db.orders.update_one(
        {"id": order_id},
        {"$set": {
            "remaining_payment_status": "paid",
            "remaining_razorpay_payment_id": payment_data.razorpay_payment_id,
            "payment_status": "completed",
            "status": "ready_for_dispatch",
            "updated_at": datetime.now(timezone.utc).isoformat()
        }}
    )
    
    # Send notification
    if background_tasks:
        background_tasks.add_task(
            send_whatsapp_notification,
            current_user['phone'],
            f"✅ Final Payment Received!\n\nHello {current_user['name']},\n\nYour remaining payment of ₹{order.get('remaining_amount', 0):,.2f} for order #{order.get('order_number', order_id[:8]).upper()} is confirmed.\n\nYour order is now ready for dispatch!\n\nThank you for choosing Lucumaa Glass!"
        )
    
    return {"message": "Remaining payment verified successfully", "order_id": order_id}

# =============== CASH PAYMENT & DISPATCH MANAGEMENT ===============

@api_router.post("/orders/{order_id}/mark-cash-received")
async def mark_cash_received(
    order_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Mark remaining payment as received in cash - Admin/Finance/HR/Supervisor only"""
    allowed_roles = ['admin', 'owner', 'super_admin', 'hr', 'accountant', 'manager', 'finance']
    if current_user.get('role') not in allowed_roles:
        raise HTTPException(status_code=403, detail="Only Admin/Finance/HR can mark cash received")
    
    order = await db.orders.find_one({"id": order_id}, {"_id": 0})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    if order.get('advance_payment_status') != 'paid':
        raise HTTPException(status_code=400, detail="Advance payment not yet completed")
    
    if order.get('remaining_payment_status') == 'paid' or order.get('remaining_payment_status') == 'cash_received':
        raise HTTPException(status_code=400, detail="Remaining payment already completed")
    
    await db.orders.update_one(
        {"id": order_id},
        {"$set": {
            "remaining_payment_status": "cash_received",
            "remaining_payment_method": "cash",
            "cash_received_by": current_user['id'],
            "cash_received_by_name": current_user.get('name', ''),
            "cash_received_at": datetime.now(timezone.utc).isoformat(),
            "payment_status": "completed",
            "status": "ready_for_dispatch",
            "updated_at": datetime.now(timezone.utc).isoformat()
        }}
    )
    
    return {
        "message": "Cash payment marked as received",
        "order_id": order_id,
        "order_number": order.get('order_number'),
        "amount_received": order.get('remaining_amount'),
        "received_by": current_user.get('name')
    }

@api_router.post("/orders/{order_id}/add-transport-charge")
async def add_transport_charge(
    order_id: str,
    data: dict,
    current_user: dict = Depends(get_current_user)
):
    """Add transport charges to order - Admin/Finance/HR/Supervisor only"""
    allowed_roles = ['admin', 'owner', 'super_admin', 'hr', 'accountant', 'manager', 'finance', 'operator']
    if current_user.get('role') not in allowed_roles:
        raise HTTPException(status_code=403, detail="Access denied")
    
    order = await db.orders.find_one({"id": order_id}, {"_id": 0})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    transport_charge = float(data.get('transport_charge', 0))
    transport_note = data.get('transport_note', 'Transport charge extra applicable')
    vehicle_type = data.get('vehicle_type', '')
    
    await db.orders.update_one(
        {"id": order_id},
        {"$set": {
            "transport_charge": transport_charge,
            "transport_charge_note": transport_note,
            "transport_added_by": current_user.get('name', ''),
            "transport_vehicle_type": vehicle_type,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }}
    )
    
    return {
        "message": "Transport charge added",
        "order_id": order_id,
        "transport_charge": transport_charge,
        "vehicle_type": vehicle_type
    }

async def generate_dispatch_slip_number() -> str:
    """Generate dispatch slip number: DS-YYYYMMDD-XXXX"""
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    prefix = f"DS-{today}-"
    
    # Find last dispatch slip for today
    last_slip = await db.orders.find_one(
        {"dispatch_slip_number": {"$regex": f"^{prefix}"}},
        {"dispatch_slip_number": 1},
        sort=[("dispatch_slip_number", -1)]
    )
    
    if last_slip and last_slip.get("dispatch_slip_number"):
        try:
            last_num = int(last_slip["dispatch_slip_number"].split("-")[-1])
            new_num = last_num + 1
        except ValueError:
            new_num = 1
    else:
        new_num = 1
    
    return f"{prefix}{str(new_num).zfill(4)}"

@api_router.post("/orders/{order_id}/create-dispatch-slip")
async def create_dispatch_slip(
    order_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Create dispatch slip for order - Admin/Finance/HR/Supervisor only"""
    allowed_roles = ['admin', 'owner', 'super_admin', 'hr', 'accountant', 'manager', 'finance', 'operator']
    if current_user.get('role') not in allowed_roles:
        raise HTTPException(status_code=403, detail="Access denied")
    
    order = await db.orders.find_one({"id": order_id}, {"_id": 0})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    # Check if payment is complete
    if order.get('payment_status') != 'completed':
        # Allow if advance_percent is 100 and advance is paid
        if not (order.get('advance_percent') == 100 and order.get('advance_payment_status') == 'paid'):
            raise HTTPException(status_code=400, detail="Payment not completed. Cannot create dispatch slip.")
    
    # Check if dispatch slip already exists
    if order.get('dispatch_slip_number'):
        return {
            "message": "Dispatch slip already exists",
            "dispatch_slip_number": order.get('dispatch_slip_number'),
            "order_id": order_id
        }
    
    dispatch_slip_number = await generate_dispatch_slip_number()
    
    await db.orders.update_one(
        {"id": order_id},
        {"$set": {
            "dispatch_slip_number": dispatch_slip_number,
            "dispatch_created_at": datetime.now(timezone.utc).isoformat(),
            "dispatch_created_by": current_user.get('name', ''),
            "status": "ready_for_dispatch",
            "updated_at": datetime.now(timezone.utc).isoformat()
        }}
    )
    
    return {
        "message": "Dispatch slip created successfully",
        "dispatch_slip_number": dispatch_slip_number,
        "order_id": order_id,
        "order_number": order.get('order_number'),
        "customer_name": order.get('customer_name'),
        "company_name": order.get('company_name'),
        "product_name": order.get('product_name'),
        "quantity": order.get('quantity'),
        "total_price": order.get('total_price'),
        "transport_charge": order.get('transport_charge', 0),
        "delivery_address": order.get('delivery_address'),
        "created_by": current_user.get('name')
    }

@api_router.post("/orders/{order_id}/mark-dispatched")
async def mark_order_dispatched(
    order_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Mark order as dispatched"""
    allowed_roles = ['admin', 'owner', 'super_admin', 'hr', 'manager', 'operator']
    if current_user.get('role') not in allowed_roles:
        raise HTTPException(status_code=403, detail="Access denied")
    
    order = await db.orders.find_one({"id": order_id}, {"_id": 0})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    if not order.get('dispatch_slip_number'):
        raise HTTPException(status_code=400, detail="Create dispatch slip first")
    
    # Check payment is settled before allowing dispatch
    payment_settled = (
        order.get('payment_status') == 'completed' or
        (order.get('advance_percent') == 100 and order.get('advance_payment_status') == 'paid') or
        (order.get('advance_payment_status') == 'paid' and order.get('remaining_payment_status') in ['paid', 'cash_received'])
    )
    if not payment_settled:
        raise HTTPException(status_code=400, detail="Cannot dispatch. Payment not fully settled.")
    
    await db.orders.update_one(
        {"id": order_id},
        {"$set": {
            "status": "dispatched",
            "dispatched_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat()
        }}
    )
    
    return {"message": "Order marked as dispatched", "order_id": order_id}

# =============== REPORTS ===============

@api_router.get("/reports/transport-charges")
async def get_transport_charges_report(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    vehicle_type: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    """Transport charges collection report"""
    allowed_roles = ['admin', 'owner', 'super_admin', 'hr', 'accountant', 'manager', 'finance']
    if current_user.get('role') not in allowed_roles:
        raise HTTPException(status_code=403, detail="Access denied")
    
    query = {"transport_charge": {"$gt": 0}}
    
    if start_date:
        query["created_at"] = {"$gte": start_date}
    if end_date:
        if "created_at" in query:
            query["created_at"]["$lte"] = end_date
        else:
            query["created_at"] = {"$lte": end_date}
    if vehicle_type:
        query["transport_vehicle_type"] = vehicle_type
    
    orders = await db.orders.find(query, {"_id": 0}).to_list(1000)
    
    # Calculate totals
    total_transport = sum(o.get('transport_charge', 0) for o in orders)
    
    # Vehicle-wise breakdown
    vehicle_breakdown = {}
    for o in orders:
        v_type = o.get('transport_vehicle_type', 'Unknown')
        if v_type not in vehicle_breakdown:
            vehicle_breakdown[v_type] = {"count": 0, "total": 0}
        vehicle_breakdown[v_type]["count"] += 1
        vehicle_breakdown[v_type]["total"] += o.get('transport_charge', 0)
    
    return {
        "summary": {
            "total_orders_with_transport": len(orders),
            "total_transport_collected": round(total_transport, 2)
        },
        "vehicle_wise": vehicle_breakdown,
        "orders": [
            {
                "order_number": o.get('order_number'),
                "customer_name": o.get('customer_name'),
                "company_name": o.get('company_name'),
                "transport_charge": o.get('transport_charge'),
                "vehicle_type": o.get('transport_vehicle_type'),
                "added_by": o.get('transport_added_by'),
                "created_at": o.get('created_at')
            }
            for o in orders
        ]
    }

@api_router.get("/reports/vehicle-expenses")
async def get_vehicle_expenses_report(
    month: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    """Vehicle-wise expense report including transport collections"""
    allowed_roles = ['admin', 'owner', 'super_admin', 'hr', 'accountant', 'manager', 'finance']
    if current_user.get('role') not in allowed_roles:
        raise HTTPException(status_code=403, detail="Access denied")
    
    target_month = month or datetime.now(timezone.utc).strftime("%Y-%m")
    
    # Get vehicle expenses from expenses collection
    expenses = await db.expenses.find(
        {"category": "vehicle", "date": {"$regex": f"^{target_month}"}},
        {"_id": 0}
    ).to_list(500)
    
    # Get transport collections
    orders = await db.orders.find(
        {"transport_charge": {"$gt": 0}, "created_at": {"$regex": f"^{target_month}"}},
        {"_id": 0, "transport_charge": 1, "transport_vehicle_type": 1}
    ).to_list(1000)
    
    # Calculate vehicle-wise data
    vehicle_data = {}
    
    # Add expenses
    for exp in expenses:
        v_type = exp.get('vehicle_type', exp.get('sub_category', 'Unknown'))
        if v_type not in vehicle_data:
            vehicle_data[v_type] = {"expenses": 0, "collections": 0, "expense_items": [], "collections_count": 0}
        vehicle_data[v_type]["expenses"] += exp.get('amount', 0)
        vehicle_data[v_type]["expense_items"].append({
            "description": exp.get('description'),
            "amount": exp.get('amount'),
            "date": exp.get('date')
        })
    
    # Add collections
    for order in orders:
        v_type = order.get('transport_vehicle_type', 'Unknown')
        if v_type not in vehicle_data:
            vehicle_data[v_type] = {"expenses": 0, "collections": 0, "expense_items": [], "collections_count": 0}
        vehicle_data[v_type]["collections"] += order.get('transport_charge', 0)
        vehicle_data[v_type]["collections_count"] += 1
    
    # Calculate net for each vehicle
    for v_type in vehicle_data:
        vehicle_data[v_type]["net"] = vehicle_data[v_type]["collections"] - vehicle_data[v_type]["expenses"]
    
    total_expenses = sum(v["expenses"] for v in vehicle_data.values())
    total_collections = sum(v["collections"] for v in vehicle_data.values())
    
    return {
        "month": target_month,
        "summary": {
            "total_vehicle_expenses": round(total_expenses, 2),
            "total_transport_collections": round(total_collections, 2),
            "net_profit_loss": round(total_collections - total_expenses, 2)
        },
        "vehicle_wise": {
            k: {
                "expenses": round(v["expenses"], 2),
                "collections": round(v["collections"], 2),
                "collections_count": v["collections_count"],
                "net": round(v["net"], 2),
                "expense_details": v["expense_items"]
            }
            for k, v in vehicle_data.items()
        }
    }

@api_router.get("/reports/pnl-with-transport")
async def get_pnl_with_transport(
    month: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    """P&L Report including transport charges"""
    allowed_roles = ['admin', 'owner', 'super_admin', 'accountant', 'finance']
    if current_user.get('role') not in allowed_roles:
        raise HTTPException(status_code=403, detail="Access denied")
    
    target_month = month or datetime.now(timezone.utc).strftime("%Y-%m")
    
    # Get completed orders
    orders = await db.orders.find(
        {"payment_status": "completed", "created_at": {"$regex": f"^{target_month}"}},
        {"_id": 0}
    ).to_list(1000)
    
    # Get expenses
    expenses = await db.expenses.find(
        {"date": {"$regex": f"^{target_month}"}},
        {"_id": 0}
    ).to_list(500)
    
    # Calculate revenue
    product_revenue = sum(o.get('total_price', 0) for o in orders)
    transport_revenue = sum(o.get('transport_charge', 0) for o in orders)
    total_revenue = product_revenue + transport_revenue
    
    # Calculate expenses by category
    expense_by_category = {}
    for exp in expenses:
        cat = exp.get('category', 'Other')
        if cat not in expense_by_category:
            expense_by_category[cat] = 0
        expense_by_category[cat] += exp.get('amount', 0)
    
    total_expenses = sum(expense_by_category.values())
    
    return {
        "month": target_month,
        "revenue": {
            "product_sales": round(product_revenue, 2),
            "transport_charges": round(transport_revenue, 2),
            "total_revenue": round(total_revenue, 2)
        },
        "expenses": {
            "by_category": {k: round(v, 2) for k, v in expense_by_category.items()},
            "total_expenses": round(total_expenses, 2)
        },
        "profit_loss": {
            "gross_profit": round(total_revenue - total_expenses, 2),
            "profit_margin": round((total_revenue - total_expenses) / max(total_revenue, 1) * 100, 2)
        },
        "order_summary": {
            "total_orders": len(orders),
            "orders_with_transport": len([o for o in orders if o.get('transport_charge', 0) > 0])
        }
    }

@api_router.get("/orders/my-orders")
async def get_my_orders(current_user: dict = Depends(get_current_user)):
    orders = await db.orders.find({"user_id": current_user['id']}, {"_id": 0}).to_list(100)
    for order in orders:
        if isinstance(order.get('created_at'), str):
            order['created_at'] = datetime.fromisoformat(order['created_at'])
        if isinstance(order.get('updated_at'), str):
            order['updated_at'] = datetime.fromisoformat(order['updated_at'])
    return orders

@api_router.get("/orders/track/{order_id}")
async def track_order(order_id: str):
    order = await db.orders.find_one({"id": order_id}, {"_id": 0})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    if isinstance(order.get('created_at'), str):
        order['created_at'] = datetime.fromisoformat(order['created_at'])
    if isinstance(order.get('updated_at'), str):
        order['updated_at'] = datetime.fromisoformat(order['updated_at'])
    
    return {
        "order_id": order['id'],
        "status": order['status'],
        "payment_status": order['payment_status'],
        "product_name": order['product_name'],
        "quantity": order['quantity'],
        "total_price": order['total_price'],
        "created_at": order['created_at']
    }

@api_router.get("/admin/orders")
async def get_all_orders(current_user: dict = Depends(get_current_user)):
    allowed_roles = ['admin', 'super_admin', 'owner', 'manager', 'hr', 'accountant', 'finance', 'operator']
    if current_user['role'] not in allowed_roles:
        raise HTTPException(status_code=403, detail="Access denied")
    
    orders = await db.orders.find({}, {"_id": 0}).to_list(1000)
    for order in orders:
        if isinstance(order.get('created_at'), str):
            order['created_at'] = datetime.fromisoformat(order['created_at'])
        if isinstance(order.get('updated_at'), str):
            order['updated_at'] = datetime.fromisoformat(order['updated_at'])
    return orders

@api_router.patch("/admin/orders/{order_id}/status")
async def update_order_status(
    order_id: str,
    status: str = Form(...),
    background_tasks: BackgroundTasks = None,
    current_user: dict = Depends(get_current_user)
):
    allowed_roles = ['admin', 'super_admin', 'owner', 'manager', 'hr', 'accountant', 'finance', 'operator']
    if current_user['role'] not in allowed_roles:
        raise HTTPException(status_code=403, detail="Access denied")
    
    order = await db.orders.find_one({"id": order_id}, {"_id": 0})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    await db.orders.update_one(
        {"id": order_id},
        {"$set": {"status": status, "updated_at": datetime.now(timezone.utc).isoformat()}}
    )
    
    user = await db.users.find_one({"id": order['user_id']}, {"_id": 0})
    if user and background_tasks:
        # Send beautiful status update email
        email_html = generate_status_update_email(
            customer_name=user['name'],
            order_id=order_id,
            status=status,
            product_name=order['product_name']
        )
        background_tasks.add_task(
            send_email_notification,
            user['email'],
            f"Order Update: {status.title().replace('_', ' ')} - #{order_id[:8].upper()}",
            email_html
        )
        
        # WhatsApp notification
        status_messages = {
            'confirmed': '✅ Order confirmed! Manufacturing starts soon.',
            'production': '🏭 Your glass is being manufactured.',
            'quality_check': '🔍 Final quality inspection in progress.',
            'dispatched': '🚚 Order dispatched! Delivery in 1-3 days.',
            'delivered': '🎉 Order delivered! Thank you for choosing us.'
        }
        whatsapp_msg = status_messages.get(status, f'Order status: {status}')
        background_tasks.add_task(
            send_whatsapp_notification,
            user['phone'],
            f"Lucumaa Glass Update\n\n{whatsapp_msg}\n\nOrder #{order_id[:8].upper()}\nTrack: {APP_URL}/track"
        )
    
    return {"message": "Status updated successfully"}

@api_router.post("/contact")
async def submit_contact(inquiry: ContactInquiry, background_tasks: BackgroundTasks = None):
    doc = inquiry.model_dump()
    doc['id'] = str(uuid.uuid4())
    doc['created_at'] = datetime.now(timezone.utc).isoformat()
    await db.inquiries.insert_one(doc)
    
    if background_tasks:
        background_tasks.add_task(
            send_email_notification,
            inquiry.email,
            "Thank you for contacting Lucumaa Glass",
            f"<p>Hi {inquiry.name},</p><p>We've received your inquiry. Our team will get back to you soon.</p>"
        )
    
    return {"message": "Inquiry submitted successfully"}

# Inquiry endpoint model for frontend compatibility
class InquiryRequest(BaseModel):
    name: str
    email: EmailStr
    phone: str
    message: Optional[str] = ""
    subject: Optional[str] = "general"
    type: Optional[str] = "contact_form"
    company: Optional[str] = ""
    requirements: Optional[str] = ""
    calculator_data: Optional[dict] = None
    estimated_total: Optional[float] = None

class ProfileUpdateRequest(BaseModel):
    name: Optional[str] = None
    phone: Optional[str] = None
    company_name: Optional[str] = None
    gst_number: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    pincode: Optional[str] = None

@api_router.get("/users/profile")
async def get_user_profile(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Get current user profile"""
    try:
        token = credentials.credentials
        payload = jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
        user = await db.users.find_one({"id": payload['user_id']}, {"_id": 0, "password": 0})
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        return user
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

@api_router.put("/users/profile")
async def update_user_profile(profile: ProfileUpdateRequest, credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Update current user profile"""
    try:
        token = credentials.credentials
        payload = jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
        
        update_data = {k: v for k, v in profile.model_dump().items() if v is not None}
        update_data['updated_at'] = datetime.now(timezone.utc).isoformat()
        
        result = await db.users.update_one(
            {"id": payload['user_id']},
            {"$set": update_data}
        )
        
        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="User not found")
        
        return {"message": "Profile updated successfully"}
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

@api_router.post("/inquiry")
async def submit_inquiry(inquiry: InquiryRequest, background_tasks: BackgroundTasks = None):
    """Submit inquiry from contact form or pricing quote form"""
    doc = inquiry.model_dump()
    doc['id'] = str(uuid.uuid4())
    doc['created_at'] = datetime.now(timezone.utc).isoformat()
    await db.inquiries.insert_one(doc)
    
    if background_tasks:
        background_tasks.add_task(
            send_email_notification,
            inquiry.email,
            "Thank you for contacting Lucumaa Glass",
            f"<p>Hi {inquiry.name},</p><p>We've received your inquiry. Our team will get back to you soon.</p>"
        )
    
    return {"message": "Inquiry submitted successfully"}

app.include_router(api_router)

# Include AI Chat router
try:
    from routers.ai_chat import chat_router
    app.include_router(chat_router, prefix="/api")
    print("✅ AI Chat router loaded successfully")
except Exception as e:
    print(f"❌ Failed to load AI Chat router: {e}")

# SEO Routes (sitemap, robots.txt, RSS)
try:
    from routers.seo import sitemap_router
    app.include_router(sitemap_router)
    print("✅ SEO routes loaded (sitemap, robots.txt, RSS)")
except Exception as e:
    print(f"❌ Failed to load SEO routes: {e}")

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=os.environ.get('CORS_ORIGINS', '*').split(','),
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Include ERP routes after logger is defined
try:
    from erp_routes import erp_router, init_erp_routes
    init_erp_routes(db, get_current_user)
    app.include_router(erp_router)
    logger.info("✅ ERP routes loaded successfully")
except Exception as e:
    logger.error(f"❌ Failed to load ERP routes: {str(e)}")
    import traceback
    logger.error(traceback.format_exc())

@app.on_event("shutdown")
async def shutdown_db_client():
    # Stop scheduler
    try:
        from utils.scheduler import stop_scheduler
        stop_scheduler()
    except Exception as e:
        logger.warning(f"Scheduler stop warning: {e}")
    
    client.close()

# Include ERP routes
try:
    import sys
    sys.path.insert(0, str(ROOT_DIR))
    from erp_server import erp_router as erp_routes
    app.include_router(erp_routes)
    logger.info("ERP routes loaded successfully")
except Exception as e:
    logger.error(f"Failed to load ERP routes: {str(e)}")

@app.on_event("startup")
async def startup_tasks():
    # Initialize and start scheduler
    try:
        from utils.scheduler import init_scheduler, start_scheduler
        init_scheduler(db)
        start_scheduler()
        logger.info("Payment alerts scheduler started (runs daily at 9:00 AM IST)")
    except Exception as e:
        logger.error(f"Scheduler initialization warning: {e}")
    
    # Seed initial data
    await seed_initial_data()

async def seed_initial_data():
    product_count = await db.products.count_documents({})
    if product_count == 0:
        products = [
            Product(
                name="Toughened Glass",
                category="toughened",
                description="High-strength safety glass with 5x more resistance than standard glass",
                applications=["Windows", "Doors", "Shower Enclosures", "Balustrades"],
                thickness_options=[5.0, 6.0, 8.0, 10.0, 12.0],
                strength_info="5-7 times stronger than standard glass",
                standards="IS 2553, ASTM C1048",
                image_url="https://images.unsplash.com/photo-1757372430354-18832839f897?crop=entropy&cs=srgb&fm=jpg&q=85"
            ),
            Product(
                name="Laminated Safety Glass",
                category="laminated",
                description="Two or more glass layers bonded with PVB interlayer for enhanced safety",
                applications=["Skylights", "Glass Floors", "Security Windows", "Facades"],
                thickness_options=[6.38, 8.38, 10.38, 12.38],
                strength_info="Holds together when shattered, prevents injuries",
                standards="IS 2553 Part 3, EN 12543",
                image_url="https://images.unsplash.com/photo-1765279077820-d3f4f2bcdca5?crop=entropy&cs=srgb&fm=jpg&q=85"
            ),
            Product(
                name="Insulated Glass (DGU)",
                category="insulated",
                description="Double glazing unit for thermal and acoustic insulation",
                applications=["Energy Efficient Buildings", "Noise Reduction", "Climate Control"],
                thickness_options=[18.0, 20.0, 24.0, 28.0],
                strength_info="Superior insulation with 50% energy savings",
                standards="IS 2553 Part 4, IGMA",
                image_url="https://images.unsplash.com/photo-1763209230598-3efb61fc36bb?crop=entropy&cs=srgb&fm=jpg&q=85"
            ),
            Product(
                name="Frosted Glass",
                category="frosted",
                description="Acid-etched glass for privacy with elegant aesthetics",
                applications=["Bathroom Partitions", "Office Cabins", "Decorative Panels"],
                thickness_options=[5.0, 6.0, 8.0, 10.0],
                strength_info="Maintains strength while providing privacy",
                standards="IS 2553",
                image_url="https://images.unsplash.com/photo-1721433730939-34917128c2bd?crop=entropy&cs=srgb&fm=jpg&q=85"
            ),
            Product(
                name="Printed / Designer Glass",
                category="designer",
                description="Custom printed glass with decorative patterns and designs",
                applications=["Interior Décor", "Partitions", "Furniture", "Feature Walls"],
                thickness_options=[5.0, 6.0, 8.0, 10.0],
                strength_info="Aesthetic appeal with structural integrity",
                standards="IS 2553",
                image_url="https://images.unsplash.com/photo-1618221195710-dd6b41faaea6?crop=entropy&cs=srgb&fm=jpg&q=85"
            ),
            Product(
                name="Fire Rated Glass",
                category="fire_rated",
                description="Fire-resistant glass providing protection up to 120 minutes",
                applications=["Fire Doors", "Safety Partitions", "Emergency Exits", "Stairwells"],
                thickness_options=[6.0, 8.0, 10.0, 12.0],
                strength_info="Fire resistance up to 120 minutes, prevents smoke spread",
                standards="IS 3043, BS 476 Part 22",
                image_url="https://images.unsplash.com/photo-1697281679290-ad7be1b10682?crop=entropy&cs=srgb&fm=jpg&q=85"
            ),
            Product(
                name="Smart Glass (PDLC)",
                category="smart_glass",
                description="Switchable privacy glass - transparent to opaque on demand (Coming Soon)",
                applications=["Conference Rooms", "Privacy Windows", "Shower Enclosures", "Luxury Projects"],
                thickness_options=[10.38, 12.38],
                strength_info="Electronic control with laminated safety",
                standards="CE, UL Certified",
                image_url="https://images.unsplash.com/photo-1765279077820-d3f4f2bcdca5?crop=entropy&cs=srgb&fm=jpg&q=85"
            )
        ]
        
        for product in products:
            doc = product.model_dump()
            doc['created_at'] = doc['created_at'].isoformat()
            await db.products.insert_one(doc)
            
            for thickness in product.thickness_options:
                pricing = PricingRule(
                    product_id=product.id,
                    thickness=thickness,
                    base_price_per_sqft=50 + (thickness * 5),
                    bulk_discount_percent=10.0
                )
                pricing_doc = pricing.model_dump()
                pricing_doc['created_at'] = pricing_doc['created_at'].isoformat()
                await db.pricing_rules.insert_one(pricing_doc)
        
        logger.info("Initial products and pricing seeded")
