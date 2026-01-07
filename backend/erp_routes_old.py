from fastapi import APIRouter, HTTPException, Depends, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import StreamingResponse
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone, date, timedelta
import uuid
import io

erp_router = APIRouter(prefix="/api/erp")

# Import from main server
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

# Will use db and other dependencies from server.py
db = None
_auth_dependency = None
security = HTTPBearer(auto_error=False)

def init_erp_routes(database, auth_dependency):
    global db, _auth_dependency
    db = database
    _auth_dependency = auth_dependency

async def get_erp_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """
    Wrapper authentication dependency for ERP routes.
    Returns the authenticated user or a system user for unauthenticated requests.
    """
    if credentials and _auth_dependency:
        try:
            return await _auth_dependency(credentials)
        except:
            pass
    # Return system user for unauthenticated/failed auth
    return {"id": "system", "role": "admin", "email": "system@erp"}

# =============== ERP ENDPOINTS ===============

@erp_router.get("/admin/dashboard")
async def get_admin_dashboard(current_user: dict = Depends(get_erp_user)):
    """Get real-time admin dashboard metrics"""
    today = datetime.now(timezone.utc).date()
    today_start = datetime.combine(today, datetime.min.time()).replace(tzinfo=timezone.utc)
    
    # Today's orders
    orders_today = await db.production_orders.count_documents({
        "created_at": {"$gte": today_start.isoformat()}
    })
    
    # Production status counts
    production_stats = {}
    stages = ['pending', 'cutting', 'polishing', 'grinding', 'toughening', 'quality_check', 'packing', 'dispatched']
    for stage in stages:
        count = await db.production_orders.count_documents({"current_stage": stage})
        production_stats[stage] = count
    
    # Breakage today
    breakage_pipeline = [
        {"$match": {"created_at": {"$gte": today_start.isoformat()}}},
        {"$group": {"_id": None, "total_loss": {"$sum": "$total_loss"}}}
    ]
    breakage_result = await db.breakage_entries.aggregate(breakage_pipeline).to_list(1)
    breakage_today = breakage_result[0]['total_loss'] if breakage_result else 0
    
    # Low stock
    low_stock = await db.raw_materials.count_documents({"$expr": {"$lte": ["$current_stock", "$minimum_stock"]}})
    
    # Pending POs
    pending_pos = await db.purchase_orders.count_documents({"status": "pending"})
    
    # Present employees
    present_today = await db.attendance.count_documents({
        "date": today.isoformat(),
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

# CRM Endpoints
@erp_router.post("/crm/leads")
async def create_lead(lead_data: Dict[str, Any], current_user: dict = Depends(get_erp_user)):
    """Create new lead"""
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

@erp_router.get("/crm/leads")
async def get_leads(
    status: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get all leads with optional status filter"""
    query = {}
    if status:
        query['status'] = status
    
    leads = await db.leads.find(query, {"_id": 0}).sort("created_at", -1).to_list(100)
    return leads

# Production Endpoints
@erp_router.post("/production/orders")
async def create_production_order(order_data: Dict[str, Any], current_user: dict = Depends(get_erp_user)):
    """Create new production order with job card"""
    job_card_number = f"JC{datetime.now().strftime('%Y%m%d')}{uuid.uuid4().hex[:6].upper()}"
    
    order = {
        "id": str(uuid.uuid4()),
        "job_card_number": job_card_number,
        "customer_order_id": order_data.get("customer_order_id", ""),
        "glass_type": order_data.get("glass_type"),
        "thickness": float(order_data.get("thickness")),
        "width": float(order_data.get("width")),
        "height": float(order_data.get("height")),
        "quantity": int(order_data.get("quantity")),
        "current_stage": "pending",
        "priority": int(order_data.get("priority", 1)),
        "stage_timestamps": {},
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.production_orders.insert_one(order)
    return {
        "message": "Production order created",
        "job_card": job_card_number,
        "order_id": order["id"]
    }

@erp_router.get("/production/orders")
async def get_production_orders(
    stage: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get production orders with optional stage filter"""
    query = {}
    if stage:
        query['current_stage'] = stage
    
    orders = await db.production_orders.find(query, {"_id": 0}).sort("priority", -1).to_list(100)
    return orders

@erp_router.patch("/production/orders/{order_id}/stage")
async def update_production_stage(
    order_id: str,
    stage_data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Update production order stage"""
    new_stage = stage_data.get("stage")
    
    order = await db.production_orders.find_one({"id": order_id}, {"_id": 0})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    stage_timestamps = order.get("stage_timestamps", {})
    stage_timestamps[new_stage] = datetime.now(timezone.utc).isoformat()
    
    await db.production_orders.update_one(
        {"id": order_id},
        {"$set": {
            "current_stage": new_stage,
            "stage_timestamps": stage_timestamps,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }}
    )
    
    return {"message": "Stage updated successfully"}

# Breakage Endpoints
@erp_router.post("/production/breakage")
async def create_breakage_entry(breakage_data: Dict[str, Any], current_user: dict = Depends(get_erp_user)):
    """Record breakage entry"""
    breakage = {
        "id": str(uuid.uuid4()),
        "production_order_id": breakage_data.get("production_order_id"),
        "job_card_number": breakage_data.get("job_card_number"),
        "stage": breakage_data.get("stage"),
        "operator_id": breakage_data.get("operator_id"),
        "machine_id": breakage_data.get("machine_id", ""),
        "quantity_broken": int(breakage_data.get("quantity_broken")),
        "glass_type": breakage_data.get("glass_type"),
        "size": breakage_data.get("size"),
        "reason": breakage_data.get("reason"),
        "cost_per_unit": float(breakage_data.get("cost_per_unit", 0)),
        "total_loss": float(breakage_data.get("quantity_broken", 0)) * float(breakage_data.get("cost_per_unit", 0)),
        "approval_status": "pending",
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.breakage_entries.insert_one(breakage)
    return {"message": "Breakage entry created", "breakage_id": breakage["id"]}

@erp_router.get("/production/breakage/analytics")
async def get_breakage_analytics(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get breakage analytics"""
    query = {}
    if start_date and end_date:
        query['created_at'] = {
            "$gte": start_date,
            "$lte": end_date
        }
    
    # By stage
    stage_pipeline = [
        {"$match": query},
        {"$group": {
            "_id": "$stage",
            "total_quantity": {"$sum": "$quantity_broken"},
            "total_loss": {"$sum": "$total_loss"}
        }}
    ]
    by_stage = await db.breakage_entries.aggregate(stage_pipeline).to_list(10)
    
    # By operator
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

# HR Endpoints
@erp_router.post("/hr/employees")
async def create_employee(employee_data: Dict[str, Any], current_user: dict = Depends(get_erp_user)):
    """Create new employee"""
    employee = {
        "id": str(uuid.uuid4()),
        "emp_code": employee_data.get("emp_code"),
        "name": employee_data.get("name"),
        "email": employee_data.get("email"),
        "phone": employee_data.get("phone"),
        "role": employee_data.get("role", "operator"),
        "department": employee_data.get("department"),
        "designation": employee_data.get("designation"),
        "date_of_joining": employee_data.get("date_of_joining"),
        "salary": float(employee_data.get("salary")),
        "bank_account": employee_data.get("bank_account", ""),
        "bank_ifsc": employee_data.get("bank_ifsc", ""),
        "status": "active",
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.employees.insert_one(employee)
    return {"message": "Employee created", "emp_id": employee["id"]}

@erp_router.get("/hr/employees")
async def get_employees(current_user: dict = Depends(get_erp_user)):
    """Get all employees"""
    employees = await db.employees.find({"status": "active"}, {"_id": 0}).to_list(1000)
    return employees

@erp_router.post("/hr/salary/calculate/{employee_id}")
async def calculate_salary(
    employee_id: str,
    salary_data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Calculate monthly salary"""
    employee = await db.employees.find_one({"id": employee_id}, {"_id": 0})
    if not employee:
        raise HTTPException(status_code=404, detail="Employee not found")
    
    month = salary_data.get("month")
    
    # Simple salary calculation (can be enhanced)
    basic_salary = employee['salary']
    overtime_pay = float(salary_data.get("overtime_pay", 0))
    deductions = float(salary_data.get("deductions", 0))
    net_salary = basic_salary + overtime_pay - deductions
    
    salary_record = {
        "id": str(uuid.uuid4()),
        "employee_id": employee_id,
        "month": month,
        "basic_salary": basic_salary,
        "overtime_pay": overtime_pay,
        "deductions": deductions,
        "net_salary": net_salary,
        "payment_status": "pending",
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.salary_payments.insert_one(salary_record)
    return {
        "message": "Salary calculated",
        "salary_id": salary_record["id"],
        "net_salary": net_salary
    }

@erp_router.get("/hr/salary")
async def get_salary_records(
    status: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get salary payment records"""
    query = {}
    if status:
        query['payment_status'] = status
    
    salaries = await db.salary_payments.find(query, {"_id": 0}).sort("created_at", -1).to_list(100)
    return salaries

@erp_router.post("/hr/salary/approve/{salary_id}")
async def approve_salary(
    salary_id: str,
    current_user: dict = Depends(get_erp_user)
):
    """Approve salary for payment"""
    if current_user.get('role') not in ['admin', 'accountant']:
        raise HTTPException(status_code=403, detail="Only admin/accountant can approve")
    
    salary = await db.salary_payments.find_one({"id": salary_id}, {"_id": 0})
    if not salary:
        raise HTTPException(status_code=404, detail="Salary record not found")
    
    await db.salary_payments.update_one(
        {"id": salary_id},
        {"$set": {
            "payment_status": "approved",
            "approved_by": current_user['id'],
            "approved_at": datetime.now(timezone.utc).isoformat()
        }}
    )
    
    return {"message": "Salary approved for payment"}

# =============== INVENTORY MODULE ===============

@erp_router.post("/inventory/materials")
async def create_material(material_data: Dict[str, Any], current_user: dict = Depends(get_erp_user)):
    """Create new raw material"""
    material = {
        "id": str(uuid.uuid4()),
        "name": material_data.get("name"),
        "category": material_data.get("category", "glass"),  # glass, chemical, packing, spare
        "unit": material_data.get("unit", "pcs"),  # pcs, kg, ltr, sqft
        "current_stock": float(material_data.get("current_stock", 0)),
        "minimum_stock": float(material_data.get("minimum_stock", 10)),
        "unit_price": float(material_data.get("unit_price", 0)),
        "location": material_data.get("location", "Main Store"),
        "supplier_id": material_data.get("supplier_id", ""),
        "last_restocked": None,
        "status": "active",
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    await db.raw_materials.insert_one(material)
    return {"message": "Material created", "material_id": material["id"]}

@erp_router.get("/inventory/materials")
async def get_materials(
    category: Optional[str] = None,
    low_stock: Optional[bool] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get all materials with optional filters"""
    query = {"status": "active"}
    if category:
        query["category"] = category
    
    materials = await db.raw_materials.find(query, {"_id": 0}).to_list(500)
    
    # Filter low stock if requested
    if low_stock:
        materials = [m for m in materials if m.get("current_stock", 0) <= m.get("minimum_stock", 10)]
    
    return materials

@erp_router.get("/inventory/materials/{material_id}")
async def get_material(material_id: str, current_user: dict = Depends(get_erp_user)):
    """Get single material details"""
    material = await db.raw_materials.find_one({"id": material_id}, {"_id": 0})
    if not material:
        raise HTTPException(status_code=404, detail="Material not found")
    return material

@erp_router.patch("/inventory/materials/{material_id}")
async def update_material(
    material_id: str,
    update_data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Update material details"""
    material = await db.raw_materials.find_one({"id": material_id}, {"_id": 0})
    if not material:
        raise HTTPException(status_code=404, detail="Material not found")
    
    update_fields = {}
    allowed_fields = ["name", "category", "unit", "minimum_stock", "unit_price", "location", "supplier_id"]
    for field in allowed_fields:
        if field in update_data:
            update_fields[field] = update_data[field]
    
    update_fields["updated_at"] = datetime.now(timezone.utc).isoformat()
    
    await db.raw_materials.update_one({"id": material_id}, {"$set": update_fields})
    return {"message": "Material updated"}

@erp_router.post("/inventory/transactions")
async def create_inventory_transaction(
    txn_data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Create stock transaction (IN/OUT)"""
    material = await db.raw_materials.find_one({"id": txn_data.get("material_id")}, {"_id": 0})
    if not material:
        raise HTTPException(status_code=404, detail="Material not found")
    
    txn_type = txn_data.get("type", "IN")  # IN, OUT, ADJUST
    quantity = float(txn_data.get("quantity", 0))
    
    # Calculate new stock
    current_stock = material.get("current_stock", 0)
    if txn_type == "IN":
        new_stock = current_stock + quantity
    elif txn_type == "OUT":
        if quantity > current_stock:
            raise HTTPException(status_code=400, detail="Insufficient stock")
        new_stock = current_stock - quantity
    else:  # ADJUST
        new_stock = quantity
    
    transaction = {
        "id": str(uuid.uuid4()),
        "material_id": txn_data.get("material_id"),
        "material_name": material.get("name"),
        "type": txn_type,
        "quantity": quantity,
        "previous_stock": current_stock,
        "new_stock": new_stock,
        "reference": txn_data.get("reference", ""),  # PO number, job card, etc.
        "notes": txn_data.get("notes", ""),
        "created_by": current_user.get("id", "system"),
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.inventory_transactions.insert_one(transaction)
    
    # Update material stock
    update_fields = {"current_stock": new_stock, "updated_at": datetime.now(timezone.utc).isoformat()}
    if txn_type == "IN":
        update_fields["last_restocked"] = datetime.now(timezone.utc).isoformat()
    
    await db.raw_materials.update_one({"id": txn_data.get("material_id")}, {"$set": update_fields})
    
    return {"message": "Transaction recorded", "transaction_id": transaction["id"], "new_stock": new_stock}

@erp_router.get("/inventory/transactions")
async def get_inventory_transactions(
    material_id: Optional[str] = None,
    txn_type: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get inventory transactions"""
    query = {}
    if material_id:
        query["material_id"] = material_id
    if txn_type:
        query["type"] = txn_type
    
    transactions = await db.inventory_transactions.find(query, {"_id": 0}).sort("created_at", -1).to_list(200)
    return transactions

@erp_router.get("/inventory/low-stock")
async def get_low_stock_items(current_user: dict = Depends(get_erp_user)):
    """Get items below minimum stock level"""
    materials = await db.raw_materials.find({"status": "active"}, {"_id": 0}).to_list(500)
    low_stock = [m for m in materials if m.get("current_stock", 0) <= m.get("minimum_stock", 10)]
    return low_stock

# =============== PURCHASE ORDER MODULE ===============

@erp_router.post("/purchase/suppliers")
async def create_supplier(supplier_data: Dict[str, Any], current_user: dict = Depends(get_erp_user)):
    """Create new supplier"""
    supplier = {
        "id": str(uuid.uuid4()),
        "name": supplier_data.get("name"),
        "contact_person": supplier_data.get("contact_person", ""),
        "email": supplier_data.get("email", ""),
        "phone": supplier_data.get("phone"),
        "address": supplier_data.get("address", ""),
        "gst_number": supplier_data.get("gst_number", ""),
        "payment_terms": supplier_data.get("payment_terms", "Net 30"),
        "status": "active",
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    await db.suppliers.insert_one(supplier)
    return {"message": "Supplier created", "supplier_id": supplier["id"]}

@erp_router.get("/purchase/suppliers")
async def get_suppliers(current_user: dict = Depends(get_erp_user)):
    """Get all suppliers"""
    suppliers = await db.suppliers.find({"status": "active"}, {"_id": 0}).to_list(200)
    return suppliers

@erp_router.post("/purchase/orders")
async def create_purchase_order(po_data: Dict[str, Any], current_user: dict = Depends(get_erp_user)):
    """Create new purchase order"""
    po_number = f"PO{datetime.now().strftime('%Y%m%d')}{uuid.uuid4().hex[:6].upper()}"
    
    # Calculate totals
    items = po_data.get("items", [])
    subtotal = sum(item.get("quantity", 0) * item.get("unit_price", 0) for item in items)
    gst = subtotal * 0.18
    total = subtotal + gst
    
    purchase_order = {
        "id": str(uuid.uuid4()),
        "po_number": po_number,
        "supplier_id": po_data.get("supplier_id"),
        "supplier_name": po_data.get("supplier_name", ""),
        "items": items,
        "subtotal": round(subtotal, 2),
        "gst": round(gst, 2),
        "total": round(total, 2),
        "status": "pending",  # pending, approved, ordered, received, cancelled
        "expected_delivery": po_data.get("expected_delivery", ""),
        "notes": po_data.get("notes", ""),
        "created_by": current_user.get("id", "system"),
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.purchase_orders.insert_one(purchase_order)
    return {"message": "Purchase order created", "po_number": po_number, "po_id": purchase_order["id"]}

@erp_router.get("/purchase/orders")
async def get_purchase_orders(
    status: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get purchase orders"""
    query = {}
    if status:
        query["status"] = status
    
    pos = await db.purchase_orders.find(query, {"_id": 0}).sort("created_at", -1).to_list(200)
    return pos

@erp_router.get("/purchase/orders/{po_id}")
async def get_purchase_order(po_id: str, current_user: dict = Depends(get_erp_user)):
    """Get single purchase order"""
    po = await db.purchase_orders.find_one({"id": po_id}, {"_id": 0})
    if not po:
        raise HTTPException(status_code=404, detail="Purchase order not found")
    return po

@erp_router.patch("/purchase/orders/{po_id}/status")
async def update_po_status(
    po_id: str,
    status_data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Update PO status"""
    new_status = status_data.get("status")
    valid_statuses = ["pending", "approved", "ordered", "received", "cancelled"]
    
    if new_status not in valid_statuses:
        raise HTTPException(status_code=400, detail=f"Invalid status. Must be one of: {valid_statuses}")
    
    po = await db.purchase_orders.find_one({"id": po_id}, {"_id": 0})
    if not po:
        raise HTTPException(status_code=404, detail="Purchase order not found")
    
    update_fields = {
        "status": new_status,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    
    # If received, update inventory
    if new_status == "received" and po.get("status") != "received":
        for item in po.get("items", []):
            material = await db.raw_materials.find_one({"id": item.get("material_id")}, {"_id": 0})
            if material:
                new_stock = material.get("current_stock", 0) + item.get("quantity", 0)
                await db.raw_materials.update_one(
                    {"id": item.get("material_id")},
                    {"$set": {"current_stock": new_stock, "last_restocked": datetime.now(timezone.utc).isoformat()}}
                )
                
                # Create inventory transaction
                txn = {
                    "id": str(uuid.uuid4()),
                    "material_id": item.get("material_id"),
                    "material_name": item.get("material_name", material.get("name")),
                    "type": "IN",
                    "quantity": item.get("quantity"),
                    "previous_stock": material.get("current_stock", 0),
                    "new_stock": new_stock,
                    "reference": po.get("po_number"),
                    "notes": f"Received from PO {po.get('po_number')}",
                    "created_by": current_user.get("id", "system"),
                    "created_at": datetime.now(timezone.utc).isoformat()
                }
                await db.inventory_transactions.insert_one(txn)
        
        update_fields["received_at"] = datetime.now(timezone.utc).isoformat()
        update_fields["received_by"] = current_user.get("id", "system")
    
    await db.purchase_orders.update_one({"id": po_id}, {"$set": update_fields})
    return {"message": f"PO status updated to {new_status}"}

# =============== LEAD STATUS UPDATE ===============

@erp_router.patch("/crm/leads/{lead_id}/status")
async def update_lead_status(
    lead_id: str,
    status_data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Update lead status"""
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

# =============== ATTENDANCE MODULE ===============

@erp_router.post("/hr/attendance")
async def mark_attendance(
    attendance_data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Mark employee attendance"""
    attendance = {
        "id": str(uuid.uuid4()),
        "employee_id": attendance_data.get("employee_id"),
        "date": attendance_data.get("date", datetime.now(timezone.utc).date().isoformat()),
        "status": attendance_data.get("status", "present"),  # present, absent, half_day, leave
        "check_in": attendance_data.get("check_in", ""),
        "check_out": attendance_data.get("check_out", ""),
        "overtime_hours": float(attendance_data.get("overtime_hours", 0)),
        "notes": attendance_data.get("notes", ""),
        "marked_by": current_user.get("id", "system"),
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    # Check if attendance already exists for this employee and date
    existing = await db.attendance.find_one({
        "employee_id": attendance_data.get("employee_id"),
        "date": attendance["date"]
    }, {"_id": 0})
    
    if existing:
        # Update existing
        await db.attendance.update_one(
            {"id": existing["id"]},
            {"$set": {
                "status": attendance["status"],
                "check_in": attendance["check_in"],
                "check_out": attendance["check_out"],
                "overtime_hours": attendance["overtime_hours"],
                "notes": attendance["notes"],
                "updated_at": datetime.now(timezone.utc).isoformat()
            }}
        )
        return {"message": "Attendance updated", "attendance_id": existing["id"]}
    
    await db.attendance.insert_one(attendance)
    return {"message": "Attendance marked", "attendance_id": attendance["id"]}

@erp_router.get("/hr/attendance")
async def get_attendance(
    employee_id: Optional[str] = None,
    date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get attendance records"""
    query = {}
    if employee_id:
        query["employee_id"] = employee_id
    if date:
        query["date"] = date
    if start_date and end_date:
        query["date"] = {"$gte": start_date, "$lte": end_date}
    
    attendance = await db.attendance.find(query, {"_id": 0}).sort("date", -1).to_list(500)
    return attendance

@erp_router.get("/hr/attendance/summary")
async def get_attendance_summary(
    month: str,  # Format: YYYY-MM
    current_user: dict = Depends(get_erp_user)
):
    """Get monthly attendance summary for all employees"""
    # Get all active employees
    employees = await db.employees.find({"status": "active"}, {"_id": 0}).to_list(500)
    
    summary = []
    for emp in employees:
        # Get attendance for this employee for the month
        attendance = await db.attendance.find({
            "employee_id": emp["id"],
            "date": {"$regex": f"^{month}"}
        }, {"_id": 0}).to_list(31)
        
        present = len([a for a in attendance if a.get("status") == "present"])
        absent = len([a for a in attendance if a.get("status") == "absent"])
        half_day = len([a for a in attendance if a.get("status") == "half_day"])
        leave = len([a for a in attendance if a.get("status") == "leave"])
        total_overtime = sum(a.get("overtime_hours", 0) for a in attendance)
        
        summary.append({
            "employee_id": emp["id"],
            "employee_name": emp.get("name"),
            "emp_code": emp.get("emp_code"),
            "department": emp.get("department"),
            "present": present,
            "absent": absent,
            "half_day": half_day,
            "leave": leave,
            "total_days": present + half_day * 0.5,
            "overtime_hours": total_overtime
        })
    
    return summary

# =============== ACCOUNTING MODULE ===============

@erp_router.post("/accounts/invoices")
async def create_invoice(
    invoice_data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Create new sales invoice"""
    invoice_number = f"INV{datetime.now().strftime('%Y%m%d')}{uuid.uuid4().hex[:6].upper()}"
    
    items = invoice_data.get("items", [])
    subtotal = sum(item.get("quantity", 0) * item.get("unit_price", 0) for item in items)
    
    # GST calculation
    cgst = subtotal * 0.09
    sgst = subtotal * 0.09
    igst = 0  # For inter-state, use IGST instead
    if invoice_data.get("is_interstate", False):
        igst = subtotal * 0.18
        cgst = 0
        sgst = 0
    
    total_tax = cgst + sgst + igst
    total = subtotal + total_tax
    
    invoice = {
        "id": str(uuid.uuid4()),
        "invoice_number": invoice_number,
        "customer_id": invoice_data.get("customer_id", ""),
        "customer_name": invoice_data.get("customer_name"),
        "customer_gst": invoice_data.get("customer_gst", ""),
        "customer_address": invoice_data.get("customer_address", ""),
        "order_id": invoice_data.get("order_id", ""),
        "items": items,
        "subtotal": round(subtotal, 2),
        "cgst": round(cgst, 2),
        "sgst": round(sgst, 2),
        "igst": round(igst, 2),
        "total_tax": round(total_tax, 2),
        "total": round(total, 2),
        "payment_status": "pending",  # pending, partial, paid
        "amount_paid": 0,
        "due_date": invoice_data.get("due_date", ""),
        "notes": invoice_data.get("notes", ""),
        "created_by": current_user.get("id", "system"),
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.invoices.insert_one(invoice)
    
    # Create ledger entry
    ledger_entry = {
        "id": str(uuid.uuid4()),
        "date": datetime.now(timezone.utc).date().isoformat(),
        "type": "invoice",
        "reference": invoice_number,
        "description": f"Sales Invoice - {invoice_data.get('customer_name')}",
        "debit": round(total, 2),  # Accounts Receivable
        "credit": 0,
        "account": "Accounts Receivable",
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    await db.ledger.insert_one(ledger_entry)
    
    return {
        "message": "Invoice created",
        "invoice_number": invoice_number,
        "invoice_id": invoice["id"],
        "total": total
    }

@erp_router.get("/accounts/invoices")
async def get_invoices(
    status: Optional[str] = None,
    customer_id: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get all invoices"""
    query = {}
    if status:
        query["payment_status"] = status
    if customer_id:
        query["customer_id"] = customer_id
    
    invoices = await db.invoices.find(query, {"_id": 0}).sort("created_at", -1).to_list(500)
    return invoices

@erp_router.get("/accounts/invoices/{invoice_id}")
async def get_invoice(invoice_id: str, current_user: dict = Depends(get_erp_user)):
    """Get single invoice"""
    invoice = await db.invoices.find_one({"id": invoice_id}, {"_id": 0})
    if not invoice:
        raise HTTPException(status_code=404, detail="Invoice not found")
    return invoice

@erp_router.post("/accounts/invoices/{invoice_id}/payment")
async def record_payment(
    invoice_id: str,
    payment_data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Record payment against invoice"""
    invoice = await db.invoices.find_one({"id": invoice_id}, {"_id": 0})
    if not invoice:
        raise HTTPException(status_code=404, detail="Invoice not found")
    
    amount = float(payment_data.get("amount", 0))
    new_paid = invoice.get("amount_paid", 0) + amount
    
    # Determine payment status
    if new_paid >= invoice["total"]:
        payment_status = "paid"
    elif new_paid > 0:
        payment_status = "partial"
    else:
        payment_status = "pending"
    
    # Create payment record
    payment = {
        "id": str(uuid.uuid4()),
        "invoice_id": invoice_id,
        "invoice_number": invoice["invoice_number"],
        "amount": amount,
        "payment_method": payment_data.get("payment_method", "cash"),
        "reference": payment_data.get("reference", ""),
        "notes": payment_data.get("notes", ""),
        "received_by": current_user.get("id", "system"),
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    await db.payments.insert_one(payment)
    
    # Update invoice
    await db.invoices.update_one(
        {"id": invoice_id},
        {"$set": {
            "amount_paid": new_paid,
            "payment_status": payment_status,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }}
    )
    
    # Create ledger entry
    ledger_entry = {
        "id": str(uuid.uuid4()),
        "date": datetime.now(timezone.utc).date().isoformat(),
        "type": "payment",
        "reference": payment["id"],
        "description": f"Payment received - {invoice['invoice_number']}",
        "debit": 0,
        "credit": amount,  # Cash/Bank
        "account": "Cash",
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    await db.ledger.insert_one(ledger_entry)
    
    return {
        "message": "Payment recorded",
        "payment_id": payment["id"],
        "new_balance": invoice["total"] - new_paid,
        "payment_status": payment_status
    }

@erp_router.get("/accounts/ledger")
async def get_ledger(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    account: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get ledger entries"""
    query = {}
    if start_date and end_date:
        query["date"] = {"$gte": start_date, "$lte": end_date}
    if account:
        query["account"] = account
    
    entries = await db.ledger.find(query, {"_id": 0}).sort("date", -1).to_list(1000)
    return entries

@erp_router.get("/accounts/dashboard")
async def get_accounts_dashboard(current_user: dict = Depends(get_erp_user)):
    """Get accounting dashboard metrics"""
    today = datetime.now(timezone.utc).date()
    month_start = today.replace(day=1).isoformat()
    
    # Total receivables
    pending_invoices = await db.invoices.find(
        {"payment_status": {"$in": ["pending", "partial"]}},
        {"_id": 0}
    ).to_list(1000)
    total_receivables = sum(inv["total"] - inv.get("amount_paid", 0) for inv in pending_invoices)
    
    # Monthly sales
    monthly_invoices = await db.invoices.find(
        {"created_at": {"$gte": month_start}},
        {"_id": 0}
    ).to_list(1000)
    monthly_sales = sum(inv["total"] for inv in monthly_invoices)
    monthly_gst = sum(inv.get("total_tax", 0) for inv in monthly_invoices)
    
    # Monthly collections
    monthly_payments = await db.payments.find(
        {"created_at": {"$gte": month_start}},
        {"_id": 0}
    ).to_list(1000)
    monthly_collections = sum(p["amount"] for p in monthly_payments)
    
    # Overdue invoices
    overdue_invoices = await db.invoices.find({
        "payment_status": {"$in": ["pending", "partial"]},
        "due_date": {"$lt": today.isoformat(), "$ne": ""}
    }, {"_id": 0}).to_list(100)
    overdue_amount = sum(inv["total"] - inv.get("amount_paid", 0) for inv in overdue_invoices)
    
    return {
        "total_receivables": round(total_receivables, 2),
        "monthly_sales": round(monthly_sales, 2),
        "monthly_gst_collected": round(monthly_gst, 2),
        "monthly_collections": round(monthly_collections, 2),
        "overdue_count": len(overdue_invoices),
        "overdue_amount": round(overdue_amount, 2),
        "pending_invoices": len(pending_invoices)
    }

@erp_router.get("/accounts/gst-report")
async def get_gst_report(
    month: str,  # Format: YYYY-MM
    current_user: dict = Depends(get_erp_user)
):
    """Get GST report for a month"""
    # Sales invoices for the month
    invoices = await db.invoices.find(
        {"created_at": {"$regex": f"^{month}"}},
        {"_id": 0}
    ).to_list(1000)
    
    total_taxable = sum(inv["subtotal"] for inv in invoices)
    total_cgst = sum(inv.get("cgst", 0) for inv in invoices)
    total_sgst = sum(inv.get("sgst", 0) for inv in invoices)
    total_igst = sum(inv.get("igst", 0) for inv in invoices)
    
    # Purchase orders for input credit
    pos = await db.purchase_orders.find(
        {"created_at": {"$regex": f"^{month}"}, "status": "received"},
        {"_id": 0}
    ).to_list(1000)
    
    input_gst = sum(po.get("gst", 0) for po in pos)
    
    return {
        "month": month,
        "output_gst": {
            "taxable_amount": round(total_taxable, 2),
            "cgst": round(total_cgst, 2),
            "sgst": round(total_sgst, 2),
            "igst": round(total_igst, 2),
            "total": round(total_cgst + total_sgst + total_igst, 2)
        },
        "input_gst": round(input_gst, 2),
        "net_gst_liability": round((total_cgst + total_sgst + total_igst) - input_gst, 2),
        "invoice_count": len(invoices),
        "purchase_count": len(pos)
    }


# =============== PROFIT & LOSS REPORT ===============

@erp_router.get("/accounts/profit-loss")
async def get_profit_loss(
    start_date: str,
    end_date: str,
    current_user: dict = Depends(get_erp_user)
):
    """Get Profit & Loss statement for date range"""
    
    # Revenue (Sales Invoices)
    invoices = await db.invoices.find({
        "created_at": {"$gte": start_date, "$lte": end_date + "T23:59:59"}
    }, {"_id": 0}).to_list(5000)
    
    total_revenue = sum(inv.get("subtotal", 0) for inv in invoices)
    total_gst_collected = sum(inv.get("total_tax", 0) for inv in invoices)
    
    # Cost of Goods (Purchase Orders received)
    pos = await db.purchase_orders.find({
        "status": "received",
        "received_at": {"$gte": start_date, "$lte": end_date + "T23:59:59"}
    }, {"_id": 0}).to_list(5000)
    
    total_purchases = sum(po.get("subtotal", 0) for po in pos)
    total_gst_paid = sum(po.get("gst", 0) for po in pos)
    
    # Breakage/Wastage Costs
    breakages = await db.breakage_entries.find({
        "created_at": {"$gte": start_date, "$lte": end_date + "T23:59:59"}
    }, {"_id": 0}).to_list(5000)
    
    total_breakage_loss = sum(b.get("total_cost", 0) for b in breakages)
    
    # Salary Expenses
    salaries = await db.salary_payments.find({
        "created_at": {"$gte": start_date, "$lte": end_date + "T23:59:59"},
        "status": {"$in": ["approved", "paid"]}
    }, {"_id": 0}).to_list(5000)
    
    total_salaries = sum(s.get("net_salary", s.get("amount", 0)) for s in salaries)
    
    # Calculations
    gross_profit = total_revenue - total_purchases
    operating_expenses = total_breakage_loss + total_salaries
    net_profit = gross_profit - operating_expenses
    gst_liability = total_gst_collected - total_gst_paid
    
    return {
        "period": {"start_date": start_date, "end_date": end_date},
        "revenue": {
            "total_sales": round(total_revenue, 2),
            "invoice_count": len(invoices),
            "gst_collected": round(total_gst_collected, 2)
        },
        "cost_of_goods": {
            "total_purchases": round(total_purchases, 2),
            "po_count": len(pos),
            "gst_paid": round(total_gst_paid, 2)
        },
        "gross_profit": round(gross_profit, 2),
        "operating_expenses": {
            "breakage_loss": round(total_breakage_loss, 2),
            "salaries": round(total_salaries, 2),
            "total": round(operating_expenses, 2)
        },
        "net_profit": round(net_profit, 2),
        "profit_margin": round((net_profit / total_revenue * 100) if total_revenue > 0 else 0, 2),
        "gst_summary": {
            "collected": round(total_gst_collected, 2),
            "paid": round(total_gst_paid, 2),
            "net_liability": round(gst_liability, 2)
        }
    }

# =============== REPORTS EXPORT ===============

@erp_router.get("/reports/invoices/export")
async def export_invoices(
    start_date: str,
    end_date: str,
    format: str = "excel",  # excel or pdf
    current_user: dict = Depends(get_erp_user)
):
    """Export invoices to Excel or PDF"""
    invoices = await db.invoices.find({
        "created_at": {"$gte": start_date, "$lte": end_date + "T23:59:59"}
    }, {"_id": 0}).sort("created_at", -1).to_list(5000)
    
    if format == "excel":
        import xlsxwriter
        
        output = io.BytesIO()
        workbook = xlsxwriter.Workbook(output, {'in_memory': True})
        worksheet = workbook.add_worksheet("Invoices")
        
        # Formats
        header_format = workbook.add_format({'bold': True, 'bg_color': '#0d9488', 'font_color': 'white', 'border': 1})
        money_format = workbook.add_format({'num_format': '₹#,##0.00', 'border': 1})
        cell_format = workbook.add_format({'border': 1})
        
        # Headers
        headers = ['Invoice No', 'Date', 'Customer', 'GST No', 'Subtotal', 'CGST', 'SGST', 'IGST', 'Total', 'Paid', 'Balance', 'Status']
        for col, header in enumerate(headers):
            worksheet.write(0, col, header, header_format)
            worksheet.set_column(col, col, 15)
        
        # Data
        for row, inv in enumerate(invoices, 1):
            balance = inv.get('total', 0) - inv.get('amount_paid', 0)
            worksheet.write(row, 0, inv.get('invoice_number', ''), cell_format)
            worksheet.write(row, 1, inv.get('created_at', '')[:10], cell_format)
            worksheet.write(row, 2, inv.get('customer_name', ''), cell_format)
            worksheet.write(row, 3, inv.get('customer_gst', ''), cell_format)
            worksheet.write(row, 4, inv.get('subtotal', 0), money_format)
            worksheet.write(row, 5, inv.get('cgst', 0), money_format)
            worksheet.write(row, 6, inv.get('sgst', 0), money_format)
            worksheet.write(row, 7, inv.get('igst', 0), money_format)
            worksheet.write(row, 8, inv.get('total', 0), money_format)
            worksheet.write(row, 9, inv.get('amount_paid', 0), money_format)
            worksheet.write(row, 10, balance, money_format)
            worksheet.write(row, 11, inv.get('payment_status', '').upper(), cell_format)
        
        # Summary row
        summary_row = len(invoices) + 2
        worksheet.write(summary_row, 3, 'TOTAL:', header_format)
        worksheet.write(summary_row, 4, sum(i.get('subtotal', 0) for i in invoices), money_format)
        worksheet.write(summary_row, 5, sum(i.get('cgst', 0) for i in invoices), money_format)
        worksheet.write(summary_row, 6, sum(i.get('sgst', 0) for i in invoices), money_format)
        worksheet.write(summary_row, 7, sum(i.get('igst', 0) for i in invoices), money_format)
        worksheet.write(summary_row, 8, sum(i.get('total', 0) for i in invoices), money_format)
        worksheet.write(summary_row, 9, sum(i.get('amount_paid', 0) for i in invoices), money_format)
        
        workbook.close()
        output.seek(0)
        
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename=invoices_{start_date}_to_{end_date}.xlsx"}
        )
    
    elif format == "pdf":
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        
        output = io.BytesIO()
        doc = SimpleDocTemplate(output, pagesize=landscape(A4), topMargin=30, bottomMargin=30)
        elements = []
        styles = getSampleStyleSheet()
        
        # Title
        title_style = ParagraphStyle('Title', parent=styles['Heading1'], fontSize=18, textColor=colors.HexColor('#0d9488'))
        elements.append(Paragraph(f"Invoice Report: {start_date} to {end_date}", title_style))
        elements.append(Spacer(1, 20))
        
        # Table data
        data = [['Invoice No', 'Date', 'Customer', 'Subtotal', 'GST', 'Total', 'Paid', 'Status']]
        for inv in invoices:
            data.append([
                inv.get('invoice_number', ''),
                inv.get('created_at', '')[:10],
                inv.get('customer_name', '')[:30],
                f"₹{inv.get('subtotal', 0):,.2f}",
                f"₹{inv.get('total_tax', 0):,.2f}",
                f"₹{inv.get('total', 0):,.2f}",
                f"₹{inv.get('amount_paid', 0):,.2f}",
                inv.get('payment_status', '').upper()
            ])
        
        # Summary
        data.append(['', '', 'TOTAL',
            f"₹{sum(i.get('subtotal', 0) for i in invoices):,.2f}",
            f"₹{sum(i.get('total_tax', 0) for i in invoices):,.2f}",
            f"₹{sum(i.get('total', 0) for i in invoices):,.2f}",
            f"₹{sum(i.get('amount_paid', 0) for i in invoices):,.2f}",
            ''
        ])
        
        table = Table(data, repeatRows=1)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0d9488')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
            ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor('#e6f7f5')),
            ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('ALIGN', (3, 0), (-1, -1), 'RIGHT'),
        ]))
        elements.append(table)
        
        doc.build(elements)
        output.seek(0)
        
        return StreamingResponse(
            output,
            media_type="application/pdf",
            headers={"Content-Disposition": f"attachment; filename=invoices_{start_date}_to_{end_date}.pdf"}
        )

@erp_router.get("/reports/profit-loss/export")
async def export_profit_loss(
    start_date: str,
    end_date: str,
    format: str = "excel",
    current_user: dict = Depends(get_erp_user)
):
    """Export Profit & Loss to Excel or PDF"""
    # Get P&L data
    pl_data = await get_profit_loss(start_date, end_date, current_user)
    
    if format == "excel":
        import xlsxwriter
        
        output = io.BytesIO()
        workbook = xlsxwriter.Workbook(output, {'in_memory': True})
        worksheet = workbook.add_worksheet("Profit & Loss")
        
        # Formats
        title_format = workbook.add_format({'bold': True, 'font_size': 16, 'font_color': '#0d9488'})
        header_format = workbook.add_format({'bold': True, 'bg_color': '#0d9488', 'font_color': 'white'})
        money_format = workbook.add_format({'num_format': '₹#,##0.00', 'align': 'right'})
        bold_money = workbook.add_format({'num_format': '₹#,##0.00', 'bold': True, 'align': 'right'})
        label_format = workbook.add_format({'bold': True})
        profit_format = workbook.add_format({'num_format': '₹#,##0.00', 'bold': True, 'font_color': 'green', 'align': 'right'})
        loss_format = workbook.add_format({'num_format': '₹#,##0.00', 'bold': True, 'font_color': 'red', 'align': 'right'})
        
        worksheet.set_column(0, 0, 30)
        worksheet.set_column(1, 1, 20)
        
        # Title
        worksheet.write(0, 0, f"Profit & Loss Statement", title_format)
        worksheet.write(1, 0, f"Period: {start_date} to {end_date}")
        
        row = 3
        # Revenue Section
        worksheet.write(row, 0, "REVENUE", header_format)
        worksheet.write(row, 1, "", header_format)
        row += 1
        worksheet.write(row, 0, "Total Sales")
        worksheet.write(row, 1, pl_data['revenue']['total_sales'], money_format)
        row += 1
        worksheet.write(row, 0, f"  ({pl_data['revenue']['invoice_count']} invoices)")
        row += 2
        
        # Cost of Goods
        worksheet.write(row, 0, "COST OF GOODS SOLD", header_format)
        worksheet.write(row, 1, "", header_format)
        row += 1
        worksheet.write(row, 0, "Total Purchases")
        worksheet.write(row, 1, pl_data['cost_of_goods']['total_purchases'], money_format)
        row += 2
        
        # Gross Profit
        worksheet.write(row, 0, "GROSS PROFIT", label_format)
        worksheet.write(row, 1, pl_data['gross_profit'], bold_money)
        row += 2
        
        # Operating Expenses
        worksheet.write(row, 0, "OPERATING EXPENSES", header_format)
        worksheet.write(row, 1, "", header_format)
        row += 1
        worksheet.write(row, 0, "Breakage/Wastage Loss")
        worksheet.write(row, 1, pl_data['operating_expenses']['breakage_loss'], money_format)
        row += 1
        worksheet.write(row, 0, "Salaries & Wages")
        worksheet.write(row, 1, pl_data['operating_expenses']['salaries'], money_format)
        row += 1
        worksheet.write(row, 0, "Total Operating Expenses")
        worksheet.write(row, 1, pl_data['operating_expenses']['total'], bold_money)
        row += 2
        
        # Net Profit
        worksheet.write(row, 0, "NET PROFIT / (LOSS)", label_format)
        profit_fmt = profit_format if pl_data['net_profit'] >= 0 else loss_format
        worksheet.write(row, 1, pl_data['net_profit'], profit_fmt)
        row += 1
        worksheet.write(row, 0, f"Profit Margin: {pl_data['profit_margin']}%")
        row += 2
        
        # GST Summary
        worksheet.write(row, 0, "GST SUMMARY", header_format)
        worksheet.write(row, 1, "", header_format)
        row += 1
        worksheet.write(row, 0, "GST Collected (Output)")
        worksheet.write(row, 1, pl_data['gst_summary']['collected'], money_format)
        row += 1
        worksheet.write(row, 0, "GST Paid (Input Credit)")
        worksheet.write(row, 1, pl_data['gst_summary']['paid'], money_format)
        row += 1
        worksheet.write(row, 0, "Net GST Liability")
        worksheet.write(row, 1, pl_data['gst_summary']['net_liability'], bold_money)
        
        workbook.close()
        output.seek(0)
        
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename=profit_loss_{start_date}_to_{end_date}.xlsx"}
        )
    
    elif format == "pdf":
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        
        output = io.BytesIO()
        doc = SimpleDocTemplate(output, pagesize=A4, topMargin=30, bottomMargin=30)
        elements = []
        styles = getSampleStyleSheet()
        
        # Title
        title_style = ParagraphStyle('Title', parent=styles['Heading1'], fontSize=18, textColor=colors.HexColor('#0d9488'))
        elements.append(Paragraph("Profit & Loss Statement", title_style))
        elements.append(Paragraph(f"Period: {start_date} to {end_date}", styles['Normal']))
        elements.append(Spacer(1, 20))
        
        # Build table data
        data = [
            ['REVENUE', ''],
            ['Total Sales', f"₹{pl_data['revenue']['total_sales']:,.2f}"],
            [f"({pl_data['revenue']['invoice_count']} invoices)", ''],
            ['', ''],
            ['COST OF GOODS SOLD', ''],
            ['Total Purchases', f"₹{pl_data['cost_of_goods']['total_purchases']:,.2f}"],
            ['', ''],
            ['GROSS PROFIT', f"₹{pl_data['gross_profit']:,.2f}"],
            ['', ''],
            ['OPERATING EXPENSES', ''],
            ['Breakage/Wastage Loss', f"₹{pl_data['operating_expenses']['breakage_loss']:,.2f}"],
            ['Salaries & Wages', f"₹{pl_data['operating_expenses']['salaries']:,.2f}"],
            ['Total Operating Expenses', f"₹{pl_data['operating_expenses']['total']:,.2f}"],
            ['', ''],
            ['NET PROFIT / (LOSS)', f"₹{pl_data['net_profit']:,.2f}"],
            [f"Profit Margin: {pl_data['profit_margin']}%", ''],
            ['', ''],
            ['GST SUMMARY', ''],
            ['GST Collected', f"₹{pl_data['gst_summary']['collected']:,.2f}"],
            ['GST Paid (Input)', f"₹{pl_data['gst_summary']['paid']:,.2f}"],
            ['Net GST Liability', f"₹{pl_data['gst_summary']['net_liability']:,.2f}"],
        ]
        
        table = Table(data, colWidths=[300, 150])
        table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (0, 0), 'Helvetica-Bold'),
            ('FONTNAME', (0, 4), (0, 4), 'Helvetica-Bold'),
            ('FONTNAME', (0, 7), (-1, 7), 'Helvetica-Bold'),
            ('FONTNAME', (0, 9), (0, 9), 'Helvetica-Bold'),
            ('FONTNAME', (0, 14), (-1, 14), 'Helvetica-Bold'),
            ('FONTNAME', (0, 17), (0, 17), 'Helvetica-Bold'),
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#e6f7f5')),
            ('BACKGROUND', (0, 4), (-1, 4), colors.HexColor('#e6f7f5')),
            ('BACKGROUND', (0, 9), (-1, 9), colors.HexColor('#e6f7f5')),
            ('BACKGROUND', (0, 17), (-1, 17), colors.HexColor('#e6f7f5')),
            ('TEXTCOLOR', (1, 14), (1, 14), colors.green if pl_data['net_profit'] >= 0 else colors.red),
            ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
            ('LINEBELOW', (0, 7), (-1, 7), 1, colors.black),
            ('LINEBELOW', (0, 14), (-1, 14), 2, colors.black),
        ]))
        elements.append(table)
        
        doc.build(elements)
        output.seek(0)
        
        return StreamingResponse(
            output,
            media_type="application/pdf",
            headers={"Content-Disposition": f"attachment; filename=profit_loss_{start_date}_to_{end_date}.pdf"}
        )

@erp_router.get("/reports/ledger/export")
async def export_ledger(
    start_date: str,
    end_date: str,
    format: str = "excel",
    current_user: dict = Depends(get_erp_user)
):
    """Export ledger entries to Excel"""
    entries = await db.ledger.find({
        "date": {"$gte": start_date, "$lte": end_date}
    }, {"_id": 0}).sort("date", 1).to_list(10000)
    
    if format == "excel":
        import xlsxwriter
        
        output = io.BytesIO()
        workbook = xlsxwriter.Workbook(output, {'in_memory': True})
        worksheet = workbook.add_worksheet("Ledger")
        
        header_format = workbook.add_format({'bold': True, 'bg_color': '#0d9488', 'font_color': 'white', 'border': 1})
        money_format = workbook.add_format({'num_format': '₹#,##0.00', 'border': 1})
        cell_format = workbook.add_format({'border': 1})
        
        headers = ['Date', 'Type', 'Reference', 'Description', 'Debit', 'Credit', 'Account']
        for col, header in enumerate(headers):
            worksheet.write(0, col, header, header_format)
            worksheet.set_column(col, col, 18 if col == 3 else 12)
        
        running_balance = 0
        for row, entry in enumerate(entries, 1):
            running_balance += entry.get('debit', 0) - entry.get('credit', 0)
            worksheet.write(row, 0, entry.get('date', ''), cell_format)
            worksheet.write(row, 1, entry.get('type', '').upper(), cell_format)
            worksheet.write(row, 2, entry.get('reference', ''), cell_format)
            worksheet.write(row, 3, entry.get('description', ''), cell_format)
            worksheet.write(row, 4, entry.get('debit', 0), money_format)
            worksheet.write(row, 5, entry.get('credit', 0), money_format)
            worksheet.write(row, 6, entry.get('account', ''), cell_format)
        
        # Totals
        total_row = len(entries) + 2
        worksheet.write(total_row, 3, 'TOTAL:', header_format)
        worksheet.write(total_row, 4, sum(e.get('debit', 0) for e in entries), money_format)
        worksheet.write(total_row, 5, sum(e.get('credit', 0) for e in entries), money_format)
        
        workbook.close()
        output.seek(0)
        
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename=ledger_{start_date}_to_{end_date}.xlsx"}
        )

@erp_router.get("/reports/payments/export")
async def export_payments(
    start_date: str,
    end_date: str,
    format: str = "excel",
    current_user: dict = Depends(get_erp_user)
):
    """Export payments to Excel"""
    payments = await db.payments.find({
        "created_at": {"$gte": start_date, "$lte": end_date + "T23:59:59"}
    }, {"_id": 0}).sort("created_at", -1).to_list(5000)
    
    if format == "excel":
        import xlsxwriter
        
        output = io.BytesIO()
        workbook = xlsxwriter.Workbook(output, {'in_memory': True})
        worksheet = workbook.add_worksheet("Payments")
        
        header_format = workbook.add_format({'bold': True, 'bg_color': '#0d9488', 'font_color': 'white', 'border': 1})
        money_format = workbook.add_format({'num_format': '₹#,##0.00', 'border': 1})
        cell_format = workbook.add_format({'border': 1})
        
        headers = ['Date', 'Invoice No', 'Amount', 'Method', 'Reference', 'Notes']
        for col, header in enumerate(headers):
            worksheet.write(0, col, header, header_format)
            worksheet.set_column(col, col, 15)
        
        for row, pmt in enumerate(payments, 1):
            worksheet.write(row, 0, pmt.get('created_at', '')[:10], cell_format)
            worksheet.write(row, 1, pmt.get('invoice_number', ''), cell_format)
            worksheet.write(row, 2, pmt.get('amount', 0), money_format)
            worksheet.write(row, 3, pmt.get('payment_method', '').upper(), cell_format)
            worksheet.write(row, 4, pmt.get('reference', ''), cell_format)
            worksheet.write(row, 5, pmt.get('notes', ''), cell_format)
        
        # Total
        total_row = len(payments) + 2
        worksheet.write(total_row, 1, 'TOTAL:', header_format)
        worksheet.write(total_row, 2, sum(p.get('amount', 0) for p in payments), money_format)
        
        workbook.close()
        output.seek(0)
        
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename=payments_{start_date}_to_{end_date}.xlsx"}
        )

