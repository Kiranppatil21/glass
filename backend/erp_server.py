from fastapi import FastAPI, APIRouter, HTTPException, UploadFile, File, Form, Depends, status, Request, BackgroundTasks, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import StreamingResponse
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field, ConfigDict, EmailStr
from typing import List, Optional, Dict, Any
from enum import Enum
import uuid
from datetime import datetime, timezone, timedelta, date
import bcrypt
import jwt
import razorpay
import aiosmtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import asyncio
import qrcode
import barcode
from barcode.writer import ImageWriter
from io import BytesIO
import pandas as pd

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

app = FastAPI(title="Lucumaa Glass Factory ERP")
erp_router = APIRouter(prefix="/api/erp")
security = HTTPBearer()

# Configuration
JWT_SECRET = os.environ.get('JWT_SECRET', 'your-secret-key')
razorpay_client = razorpay.Client(auth=(os.environ.get('RAZORPAY_KEY_ID', ''), os.environ.get('RAZORPAY_KEY_SECRET', '')))
SMTP_HOST = os.environ.get('SMTP_HOST', 'smtp.hostinger.com')
SMTP_PORT = int(os.environ.get('SMTP_PORT', 465))
SMTP_USER = os.environ.get('SMTP_USER', 'info@lucumaaGlass.in')
SMTP_PASSWORD = os.environ.get('SMTP_PASSWORD', '')
SENDER_EMAIL = os.environ.get('SENDER_EMAIL', 'info@lucumaaGlass.in')

# Enums
class LeadStatus(str, Enum):
    NEW = "new"
    CONTACTED = "contacted"
    QUOTED = "quoted"
    NEGOTIATION = "negotiation"
    WON = "won"
    LOST = "lost"

class OrderStatus(str, Enum):
    PENDING = "pending"
    CUTTING = "cutting"
    POLISHING = "polishing"
    GRINDING = "grinding"
    TOUGHENING = "toughening"
    QUALITY_CHECK = "quality_check"
    PACKING = "packing"
    DISPATCHED = "dispatched"
    DELIVERED = "delivered"

class BreakageStage(str, Enum):
    CUTTING = "cutting"
    POLISHING = "polishing"
    GRINDING = "grinding"
    TOUGHENING = "toughening"
    PACKING = "packing"

class EmployeeRole(str, Enum):
    OPERATOR = "operator"
    SUPERVISOR = "supervisor"
    MANAGER = "manager"
    SALES = "sales"
    ADMIN = "admin"
    ACCOUNTANT = "accountant"
    HR = "hr"

# =============== MODELS ===============

# CRM Models
class Lead(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    email: Optional[EmailStr] = None
    phone: str
    company: Optional[str] = None
    source: str = "website"  # website, whatsapp, manual, reference
    customer_type: str = "retail"  # retail, dealer, project, architect
    status: LeadStatus = LeadStatus.NEW
    assigned_to: Optional[str] = None  # sales person ID
    enquiry_details: str = ""
    expected_value: float = 0.0
    follow_up_date: Optional[datetime] = None
    notes: List[Dict[str, Any]] = []
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class Quotation(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    lead_id: str
    quotation_number: str
    items: List[Dict[str, Any]] = []
    subtotal: float
    tax: float
    total: float
    valid_until: date
    terms: str = ""
    created_by: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

# Production Models
class ProductionOrder(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    job_card_number: str
    customer_order_id: str
    glass_type: str
    thickness: float
    width: float
    height: float
    quantity: int
    current_stage: OrderStatus = OrderStatus.PENDING
    priority: int = 1  # 1=normal, 2=high, 3=urgent
    assigned_operator: Optional[str] = None
    stage_timestamps: Dict[str, str] = {}
    expected_completion: Optional[datetime] = None
    actual_completion: Optional[datetime] = None
    qr_code: Optional[str] = None
    barcode: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

# Machine & Operator Models
class Machine(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    machine_type: str  # cutting, polishing, grinding, toughening
    model_number: str
    capacity: str
    status: str = "operational"  # operational, maintenance, down
    last_maintenance: Optional[date] = None
    next_maintenance: Optional[date] = None
    running_hours: float = 0.0
    total_output: int = 0
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class OperatorTask(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    production_order_id: str
    operator_id: str
    machine_id: str
    stage: str
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    quantity_completed: int = 0
    quantity_broken: int = 0
    status: str = "assigned"  # assigned, in_progress, completed
    notes: str = ""
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

# Breakage Models
class BreakageEntry(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    production_order_id: str
    job_card_number: str
    stage: BreakageStage
    operator_id: str
    machine_id: str
    quantity_broken: int
    glass_type: str
    size: str
    reason: str
    cost_per_unit: float
    total_loss: float
    approved_by: Optional[str] = None
    approval_status: str = "pending"  # pending, approved, rejected
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

# Inventory Models
class RawMaterial(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    material_name: str
    category: str
    unit: str
    current_stock: float
    minimum_stock: float
    reorder_level: float
    unit_price: float
    location: str = ""
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class StockTransaction(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    material_id: str
    transaction_type: str  # IN, OUT, ADJUSTMENT
    quantity: float
    reference_type: str  # PRODUCTION, PURCHASE, WASTAGE
    reference_id: str
    performed_by: str
    notes: str = ""
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

# Purchase Models
class Vendor(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    contact_person: str
    email: EmailStr
    phone: str
    address: str
    gst_number: str = ""
    payment_terms: str = ""
    rating: float = 0.0
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class PurchaseOrder(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    po_number: str
    vendor_id: str
    items: List[Dict[str, Any]] = []
    subtotal: float
    tax: float
    total: float
    status: str = "pending"  # pending, approved, received, cancelled
    expected_delivery: Optional[date] = None
    approved_by: Optional[str] = None
    created_by: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

# HR Models
class Employee(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    emp_code: str
    name: str
    email: Optional[EmailStr] = None
    phone: str
    role: EmployeeRole
    department: str
    designation: str
    date_of_joining: date
    salary: float
    bank_account: str = ""
    bank_ifsc: str = ""
    status: str = "active"  # active, on_leave, resigned, terminated
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class Attendance(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    employee_id: str
    date: date
    check_in: Optional[datetime] = None
    check_out: Optional[datetime] = None
    hours_worked: float = 0.0
    overtime_hours: float = 0.0
    status: str = "present"  # present, absent, half_day, leave
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class SalaryPayment(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    employee_id: str
    month: str  # YYYY-MM
    basic_salary: float
    allowances: float = 0.0
    deductions: float = 0.0
    overtime_pay: float = 0.0
    incentives: float = 0.0
    net_salary: float
    payment_status: str = "pending"  # pending, approved, paid, failed
    payment_date: Optional[datetime] = None
    razorpay_payout_id: Optional[str] = None
    approved_by: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

# Accounting Models
class Invoice(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    invoice_number: str
    customer_id: str
    order_id: str
    items: List[Dict[str, Any]] = []
    subtotal: float
    cgst: float
    sgst: float
    igst: float
    total: float
    payment_status: str = "pending"  # pending, partial, paid
    payment_due_date: date
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

# =============== HELPER FUNCTIONS ===============

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))

def create_token(user_id: str, email: str, role: str) -> str:
    payload = {
        'user_id': user_id,
        'email': email,
        'role': role,
        'exp': datetime.now(timezone.utc) + timedelta(days=30)
    }
    return jwt.encode(payload, JWT_SECRET, algorithm='HS256')

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        token = credentials.credentials
        payload = jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
        user = await db.employees.find_one({"id": payload['user_id']}, {"_id": 0})
        if not user:
            raise HTTPException(status_code=401, detail="User not found")
        return user
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

def generate_job_card_number() -> str:
    return f"JC{datetime.now().strftime('%Y%m%d')}{uuid.uuid4().hex[:6].upper()}"

def generate_po_number() -> str:
    return f"PO{datetime.now().strftime('%Y%m%d')}{uuid.uuid4().hex[:6].upper()}"

def generate_invoice_number() -> str:
    return f"INV{datetime.now().strftime('%Y%m%d')}{uuid.uuid4().hex[:6].upper()}"

async def generate_qr_code(data: str) -> str:
    """Generate QR code and return base64 string"""
    qr = qrcode.QRCode(version=1, box_size=10, border=5)
    qr.add_data(data)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buffer = BytesIO()
    img.save(buffer, format='PNG')
    buffer.seek(0)
    import base64
    return base64.b64encode(buffer.getvalue()).decode()

# =============== ERP ROUTES ===============

# Admin Dashboard
@erp_router.get("/admin/dashboard")
async def get_admin_dashboard(current_user: dict = Depends(get_current_user)):
    if current_user['role'] not in ['admin', 'manager']:
        raise HTTPException(status_code=403, detail="Access denied")
    
    today = datetime.now(timezone.utc).date()
    
    # Today's orders
    orders_today = await db.production_orders.count_documents({
        "created_at": {"$gte": datetime.combine(today, datetime.min.time())}
    })
    
    # Production status
    production_stats = {}
    for status in OrderStatus:
        count = await db.production_orders.count_documents({"current_stage": status.value})
        production_stats[status.value] = count
    
    # Breakage value today
    breakage_pipeline = [
        {"$match": {"created_at": {"$gte": datetime.combine(today, datetime.min.time())}}},
        {"$group": {"_id": None, "total_loss": {"$sum": "$total_loss"}}}
    ]
    breakage_result = await db.breakage_entries.aggregate(breakage_pipeline).to_list(1)
    breakage_today = breakage_result[0]['total_loss'] if breakage_result else 0
    
    # Stock alerts
    low_stock = await db.raw_materials.count_documents({"$expr": {"$lte": ["$current_stock", "$minimum_stock"]}})
    
    # Pending POs
    pending_pos = await db.purchase_orders.count_documents({"status": "pending"})
    
    # Attendance summary
    present_today = await db.attendance.count_documents({
        "date": today,
        "status": "present"
    })
    
    return {
        "orders_today": orders_today,
        "production_stats": production_stats,
        "breakage_today": round(breakage_today, 2),
        "low_stock_items": low_stock,
        "pending_pos": pending_pos,
        "present_employees": present_today
    }

# CRM - Leads
@erp_router.post("/crm/leads")
async def create_lead(lead: Lead, current_user: dict = Depends(get_current_user)):
    doc = lead.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    doc['updated_at'] = doc['updated_at'].isoformat()
    if doc.get('follow_up_date'):
        doc['follow_up_date'] = doc['follow_up_date'].isoformat()
    await db.leads.insert_one(doc)
    return {"message": "Lead created", "lead_id": lead.id}

@erp_router.get("/crm/leads")
async def get_leads(
    status: Optional[str] = None,
    assigned_to: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    query = {}
    if status:
        query['status'] = status
    if assigned_to:
        query['assigned_to'] = assigned_to
    
    leads = await db.leads.find(query, {"_id": 0}).sort("created_at", -1).to_list(100)
    return leads

# Production Orders
@erp_router.post("/production/orders")
async def create_production_order(order: ProductionOrder, current_user: dict = Depends(get_current_user)):
    order.job_card_number = generate_job_card_number()
    
    # Generate QR code
    qr_data = f"JOB:{order.job_card_number}|GLASS:{order.glass_type}|SIZE:{order.width}x{order.height}|QTY:{order.quantity}"
    order.qr_code = await generate_qr_code(qr_data)
    
    doc = order.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    if doc.get('expected_completion'):
        doc['expected_completion'] = doc['expected_completion'].isoformat()
    if doc.get('actual_completion'):
        doc['actual_completion'] = doc['actual_completion'].isoformat()
    
    await db.production_orders.insert_one(doc)
    return {"message": "Production order created", "job_card": order.job_card_number, "qr_code": order.qr_code}

@erp_router.get("/production/orders")
async def get_production_orders(
    stage: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    query = {}
    if stage:
        query['current_stage'] = stage
    
    orders = await db.production_orders.find(query, {"_id": 0}).sort("priority", -1).to_list(100)
    return orders

# Breakage Management
@erp_router.post("/production/breakage")
async def create_breakage_entry(breakage: BreakageEntry, current_user: dict = Depends(get_current_user)):
    doc = breakage.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    await db.breakage_entries.insert_one(doc)
    return {"message": "Breakage entry created", "breakage_id": breakage.id}

@erp_router.get("/production/breakage/analytics")
async def get_breakage_analytics(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    query = {}
    if start_date and end_date:
        query['created_at'] = {
            "$gte": datetime.fromisoformat(start_date),
            "$lte": datetime.fromisoformat(end_date)
        }
    
    # Breakage by stage
    stage_pipeline = [
        {"$match": query},
        {"$group": {
            "_id": "$stage",
            "total_quantity": {"$sum": "$quantity_broken"},
            "total_loss": {"$sum": "$total_loss"}
        }}
    ]
    by_stage = await db.breakage_entries.aggregate(stage_pipeline).to_list(10)
    
    # Breakage by operator
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

# HR - Employees
@erp_router.post("/hr/employees")
async def create_employee(employee: Employee, current_user: dict = Depends(get_current_user)):
    if current_user['role'] not in ['admin', 'hr']:
        raise HTTPException(status_code=403, detail="Access denied")
    
    doc = employee.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    doc['date_of_joining'] = doc['date_of_joining'].isoformat()
    await db.employees.insert_one(doc)
    return {"message": "Employee created", "emp_id": employee.id}

# Salary Management
@erp_router.post("/hr/salary/calculate/{employee_id}")
async def calculate_salary(
    employee_id: str,
    month: str,
    current_user: dict = Depends(get_current_user)
):
    if current_user['role'] not in ['admin', 'hr', 'accountant']:
        raise HTTPException(status_code=403, detail="Access denied")
    
    employee = await db.employees.find_one({"id": employee_id}, {"_id": 0})
    if not employee:
        raise HTTPException(status_code=404, detail="Employee not found")
    
    # Calculate attendance
    start_date = datetime.strptime(f"{month}-01", "%Y-%m-%d").date()
    end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    
    attendance_records = await db.attendance.find({
        "employee_id": employee_id,
        "date": {"$gte": start_date, "$lte": end_date}
    }, {"_id": 0}).to_list(31)
    
    present_days = sum(1 for a in attendance_records if a['status'] == 'present')
    overtime_hours = sum(a.get('overtime_hours', 0) for a in attendance_records)
    
    # Calculate salary components
    basic_salary = employee['salary']
    per_day_salary = basic_salary / 30
    earned_salary = per_day_salary * present_days
    overtime_pay = (basic_salary / 208) * overtime_hours * 1.5  # 208 working hours/month
    
    salary_payment = SalaryPayment(
        employee_id=employee_id,
        month=month,
        basic_salary=basic_salary,
        overtime_pay=round(overtime_pay, 2),
        net_salary=round(earned_salary + overtime_pay, 2)
    )
    
    doc = salary_payment.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    if doc.get('payment_date'):
        doc['payment_date'] = doc['payment_date'].isoformat()
    
    await db.salary_payments.insert_one(doc)
    return {"message": "Salary calculated", "salary_id": salary_payment.id, "net_salary": salary_payment.net_salary}

@erp_router.post("/hr/salary/pay/{salary_id}")
async def process_salary_payment(
    salary_id: str,
    current_user: dict = Depends(get_current_user)
):
    if current_user['role'] not in ['admin', 'accountant']:
        raise HTTPException(status_code=403, detail="Only admin/accountant can approve payments")
    
    salary = await db.salary_payments.find_one({"id": salary_id}, {"_id": 0})
    if not salary:
        raise HTTPException(status_code=404, detail="Salary record not found")
    
    employee = await db.employees.find_one({"id": salary['employee_id']}, {"_id": 0})
    
    # Process Razorpay Payout (commented for now - needs Razorpay Payout API setup)
    # payout = razorpay_client.payout.create({
    #     "account_number": "XXXXXXX",
    #     "fund_account_id": employee['fund_account_id'],
    #     "amount": int(salary['net_salary'] * 100),
    #     "currency": "INR",
    #     "mode": "IMPS",
    #     "purpose": "salary"
    # })
    
    await db.salary_payments.update_one(
        {"id": salary_id},
        {"$set": {
            "payment_status": "paid",
            "payment_date": datetime.now(timezone.utc).isoformat(),
            "approved_by": current_user['id']
        }}
    )
    
    return {"message": "Salary payment processed successfully"}

app.include_router(erp_router)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=os.environ.get('CORS_ORIGINS', '*').split(','),
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
