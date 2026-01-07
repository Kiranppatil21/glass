"""
HR Router - Employee management, salary processing, attendance tracking
"""
from fastapi import APIRouter, Depends, HTTPException
from typing import Optional, Dict, Any
from datetime import datetime, timezone
import uuid
from .base import get_erp_user, get_db

hr_router = APIRouter(prefix="/hr", tags=["HR & Payroll"])


# =============== EMPLOYEE MANAGEMENT ===============

@hr_router.post("/employees")
async def create_employee(employee_data: Dict[str, Any], current_user: dict = Depends(get_erp_user)):
    """Create new employee"""
    db = get_db()
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


@hr_router.get("/employees")
async def get_employees(current_user: dict = Depends(get_erp_user)):
    """Get all employees"""
    db = get_db()
    employees = await db.employees.find({"status": "active"}, {"_id": 0}).to_list(1000)
    return employees


# =============== SALARY MANAGEMENT ===============

@hr_router.post("/salary/calculate/{employee_id}")
async def calculate_salary(
    employee_id: str,
    salary_data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Calculate monthly salary"""
    db = get_db()
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


@hr_router.get("/salary")
async def get_salary_records(
    status: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get salary payment records"""
    db = get_db()
    query = {}
    if status:
        query['payment_status'] = status
    
    salaries = await db.salary_payments.find(query, {"_id": 0}).sort("created_at", -1).to_list(100)
    return salaries


@hr_router.post("/salary/approve/{salary_id}")
async def approve_salary(
    salary_id: str,
    current_user: dict = Depends(get_erp_user)
):
    """Approve salary for payment"""
    db = get_db()
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


# =============== ATTENDANCE MANAGEMENT ===============

@hr_router.post("/attendance")
async def mark_attendance(
    attendance_data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Mark employee attendance"""
    db = get_db()
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


@hr_router.get("/attendance")
async def get_attendance(
    employee_id: Optional[str] = None,
    date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get attendance records"""
    db = get_db()
    query = {}
    if employee_id:
        query["employee_id"] = employee_id
    if date:
        query["date"] = date
    if start_date and end_date:
        query["date"] = {"$gte": start_date, "$lte": end_date}
    
    attendance = await db.attendance.find(query, {"_id": 0}).sort("date", -1).to_list(500)
    return attendance


@hr_router.get("/attendance/summary")
async def get_attendance_summary(
    month: str,  # Format: YYYY-MM
    current_user: dict = Depends(get_erp_user)
):
    """Get monthly attendance summary for all employees"""
    db = get_db()
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
