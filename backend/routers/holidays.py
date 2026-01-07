"""
Holiday & Salary Impact Router
Manages company holidays, attendance, and salary calculation impact
"""
from fastapi import APIRouter, Depends, HTTPException
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone, timedelta, date
import uuid
import calendar
from .base import get_erp_user, get_db

holiday_router = APIRouter(prefix="/holidays", tags=["Holidays & Calendar"])

HOLIDAY_TYPES = [
    {"id": "national", "name": "National Holiday", "color": "#ef4444", "paid": True},
    {"id": "state", "name": "State Holiday", "color": "#f59e0b", "paid": True},
    {"id": "company", "name": "Company Holiday", "color": "#3b82f6", "paid": True},
    {"id": "optional", "name": "Optional/Restricted", "color": "#8b5cf6", "paid": False},
    {"id": "weekly_off", "name": "Weekly Off", "color": "#64748b", "paid": True},
]


# =============== HOLIDAY CALENDAR ===============

@holiday_router.get("/types")
async def get_holiday_types(current_user: dict = Depends(get_erp_user)):
    """Get all holiday types"""
    return HOLIDAY_TYPES


@holiday_router.get("/settings")
async def get_holiday_settings(current_user: dict = Depends(get_erp_user)):
    """Get holiday settings"""
    db = get_db()
    settings = await db.holiday_settings.find_one({"id": "default"}, {"_id": 0})
    
    if not settings:
        settings = {
            "id": "default",
            "year": datetime.now().year,
            "weekly_offs": ["sunday"],  # sunday, saturday, etc.
            "half_day_weekly_off": [],  # e.g., ["saturday"] for alternate Saturdays
            "overtime_rate": 2.0,  # 2x normal rate for working on holidays
            "comp_off_enabled": True,  # Compensatory off for holiday work
            "comp_off_validity_days": 30,  # Days within which comp off must be used
            "auto_mark_attendance": True,  # Auto-mark holiday attendance
            "departments_with_custom_holidays": [],  # Departments with different calendars
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        await db.holiday_settings.insert_one(settings)
    
    return settings


@holiday_router.put("/settings")
async def update_holiday_settings(
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Update holiday settings (Admin/HR only)"""
    if current_user.get("role") not in ["admin", "owner", "hr"]:
        raise HTTPException(status_code=403, detail="Admin/HR access required")
    
    db = get_db()
    allowed_fields = [
        "weekly_offs", "half_day_weekly_off", "overtime_rate",
        "comp_off_enabled", "comp_off_validity_days", "auto_mark_attendance",
        "departments_with_custom_holidays"
    ]
    
    update_data = {k: v for k, v in data.items() if k in allowed_fields}
    update_data["updated_at"] = datetime.now(timezone.utc).isoformat()
    
    await db.holiday_settings.update_one(
        {"id": "default"},
        {"$set": update_data},
        upsert=True
    )
    
    return {"message": "Settings updated"}


@holiday_router.post("/")
async def create_holiday(
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Create a new holiday"""
    if current_user.get("role") not in ["admin", "owner", "hr"]:
        raise HTTPException(status_code=403, detail="Admin/HR access required")
    
    db = get_db()
    
    holiday = {
        "id": str(uuid.uuid4()),
        "date": data["date"],
        "name": data["name"],
        "type": data.get("type", "company"),
        "paid": data.get("paid", True),
        "departments": data.get("departments", []),  # Empty = all departments
        "shifts": data.get("shifts", []),  # Empty = all shifts
        "year": int(data["date"][:4]),
        "description": data.get("description", ""),
        "created_by": current_user["id"],
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    # Check for duplicate
    existing = await db.holidays.find_one({"date": data["date"]}, {"_id": 0})
    if existing:
        raise HTTPException(status_code=400, detail="Holiday already exists for this date")
    
    await db.holidays.insert_one(holiday)
    
    return {"message": "Holiday created", "holiday": {k: v for k, v in holiday.items() if k != "_id"}}


@holiday_router.get("/")
async def get_holidays(
    year: Optional[int] = None,
    month: Optional[int] = None,
    holiday_type: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get holidays for a year/month"""
    db = get_db()
    
    if not year:
        year = datetime.now().year
    
    query = {"year": year}
    if holiday_type:
        query["type"] = holiday_type
    
    holidays = await db.holidays.find(query, {"_id": 0}).sort("date", 1).to_list(100)
    
    if month:
        month_str = f"{year}-{month:02d}"
        holidays = [h for h in holidays if h["date"].startswith(month_str)]
    
    return holidays


@holiday_router.get("/calendar/{year}")
async def get_year_calendar(year: int, current_user: dict = Depends(get_erp_user)):
    """Get full calendar for a year with holidays and working days"""
    db = get_db()
    
    holidays = await db.holidays.find({"year": year}, {"_id": 0}).to_list(100)
    settings = await get_holiday_settings(current_user)
    
    holiday_dates = {h["date"]: h for h in holidays}
    weekly_offs = settings.get("weekly_offs", ["sunday"])
    
    # Build month-wise calendar
    calendar_data = []
    
    for month in range(1, 13):
        month_name = calendar.month_name[month]
        _, days_in_month = calendar.monthrange(year, month)
        
        working_days = 0
        holidays_count = 0
        weekly_off_count = 0
        
        days = []
        for day in range(1, days_in_month + 1):
            date_str = f"{year}-{month:02d}-{day:02d}"
            day_date = date(year, month, day)
            day_name = calendar.day_name[day_date.weekday()].lower()
            
            day_info = {
                "date": date_str,
                "day": day,
                "day_name": day_name,
                "is_holiday": False,
                "is_weekly_off": False,
                "holiday_name": None,
                "type": "working"
            }
            
            if date_str in holiday_dates:
                day_info["is_holiday"] = True
                day_info["holiday_name"] = holiday_dates[date_str]["name"]
                day_info["type"] = holiday_dates[date_str]["type"]
                holidays_count += 1
            elif day_name in weekly_offs:
                day_info["is_weekly_off"] = True
                day_info["type"] = "weekly_off"
                weekly_off_count += 1
            else:
                working_days += 1
            
            days.append(day_info)
        
        calendar_data.append({
            "month": month,
            "month_name": month_name,
            "total_days": days_in_month,
            "working_days": working_days,
            "holidays": holidays_count,
            "weekly_offs": weekly_off_count,
            "days": days
        })
    
    # Year summary
    total_working = sum(m["working_days"] for m in calendar_data)
    total_holidays = sum(m["holidays"] for m in calendar_data)
    total_weekly_offs = sum(m["weekly_offs"] for m in calendar_data)
    
    return {
        "year": year,
        "summary": {
            "total_days": 366 if calendar.isleap(year) else 365,
            "working_days": total_working,
            "holidays": total_holidays,
            "weekly_offs": total_weekly_offs
        },
        "months": calendar_data
    }


@holiday_router.delete("/{holiday_id}")
async def delete_holiday(holiday_id: str, current_user: dict = Depends(get_erp_user)):
    """Delete a holiday"""
    if current_user.get("role") not in ["admin", "owner", "hr"]:
        raise HTTPException(status_code=403, detail="Admin/HR access required")
    
    db = get_db()
    result = await db.holidays.delete_one({"id": holiday_id})
    
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Holiday not found")
    
    return {"message": "Holiday deleted"}


# =============== OVERTIME & COMP-OFF ===============

@holiday_router.post("/overtime")
async def record_holiday_overtime(
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Record overtime work on holiday"""
    db = get_db()
    
    record = {
        "id": str(uuid.uuid4()),
        "employee_id": data["employee_id"],
        "employee_name": data.get("employee_name", ""),
        "date": data["date"],
        "hours_worked": float(data["hours_worked"]),
        "overtime_type": data.get("type", "holiday"),  # holiday, weekly_off
        "status": "pending",  # pending, approved, rejected, comp_off_availed
        "comp_off_requested": data.get("comp_off_requested", False),
        "comp_off_date": data.get("comp_off_date"),
        "reason": data.get("reason", ""),
        "recorded_by": current_user["id"],
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.holiday_overtime.insert_one(record)
    
    return {"message": "Overtime recorded", "record": {k: v for k, v in record.items() if k != "_id"}}


@holiday_router.get("/overtime")
async def get_holiday_overtime(
    employee_id: Optional[str] = None,
    status: Optional[str] = None,
    month: Optional[str] = None,
    limit: int = 100,
    current_user: dict = Depends(get_erp_user)
):
    """Get holiday overtime records"""
    db = get_db()
    
    query = {}
    if employee_id:
        query["employee_id"] = employee_id
    if status:
        query["status"] = status
    if month:
        query["date"] = {"$regex": f"^{month}"}
    
    records = await db.holiday_overtime.find(query, {"_id": 0}).sort("date", -1).to_list(limit)
    return records


@holiday_router.post("/overtime/{record_id}/approve")
async def approve_overtime(
    record_id: str,
    data: Dict[str, Any],
    current_user: dict = Depends(get_erp_user)
):
    """Approve holiday overtime"""
    if current_user.get("role") not in ["admin", "owner", "hr", "manager"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    db = get_db()
    action = data.get("action", "approve")
    
    update_data = {
        "status": "approved" if action == "approve" else "rejected",
        "approved_by": current_user["id"],
        "approved_at": datetime.now(timezone.utc).isoformat(),
        "approval_note": data.get("note", "")
    }
    
    await db.holiday_overtime.update_one({"id": record_id}, {"$set": update_data})
    
    return {"message": f"Overtime {action}d"}


# =============== SALARY IMPACT ===============

@holiday_router.get("/salary-impact/{employee_id}")
async def get_salary_impact(
    employee_id: str,
    month: str,  # Format: YYYY-MM
    current_user: dict = Depends(get_erp_user)
):
    """Calculate salary impact of holidays for an employee"""
    db = get_db()
    settings = await get_holiday_settings(current_user)
    
    year = int(month[:4])
    month_num = int(month[5:7])
    _, days_in_month = calendar.monthrange(year, month_num)
    
    # Get employee details
    employee = await db.employees.find_one({"id": employee_id}, {"_id": 0})
    if not employee:
        raise HTTPException(status_code=404, detail="Employee not found")
    
    daily_rate = float(employee.get("salary", 0)) / days_in_month
    
    # Get holidays for the month
    month_start = f"{month}-01"
    month_end = f"{month}-{days_in_month:02d}"
    
    holidays = await db.holidays.find({
        "date": {"$gte": month_start, "$lte": month_end},
        "paid": True
    }, {"_id": 0}).to_list(50)
    
    # Get overtime records
    overtime_records = await db.holiday_overtime.find({
        "employee_id": employee_id,
        "date": {"$gte": month_start, "$lte": month_end},
        "status": "approved"
    }, {"_id": 0}).to_list(50)
    
    # Calculate
    paid_holidays = len(holidays)
    weekly_offs = settings.get("weekly_offs", ["sunday"])
    weekly_off_days = sum(1 for day in range(1, days_in_month + 1) 
                         if calendar.day_name[date(year, month_num, day).weekday()].lower() in weekly_offs)
    
    working_days = days_in_month - paid_holidays - weekly_off_days
    
    # Overtime calculation
    overtime_hours = sum(r["hours_worked"] for r in overtime_records)
    overtime_rate = settings.get("overtime_rate", 2.0)
    hourly_rate = daily_rate / 8
    overtime_pay = overtime_hours * hourly_rate * overtime_rate
    
    # Get attendance (if available)
    attendance_records = await db.attendance.find({
        "employee_id": employee_id,
        "date": {"$gte": month_start, "$lte": month_end}
    }, {"_id": 0}).to_list(50)
    
    days_present = sum(1 for a in attendance_records if a.get("status") in ["present", "half_day"])
    half_days = sum(1 for a in attendance_records if a.get("status") == "half_day")
    days_absent = working_days - days_present
    
    # Deductions
    absent_deduction = days_absent * daily_rate
    half_day_deduction = half_days * (daily_rate / 2)
    
    return {
        "employee_id": employee_id,
        "employee_name": employee.get("name"),
        "month": month,
        "basic_salary": employee.get("salary", 0),
        "daily_rate": round(daily_rate, 2),
        "days_breakdown": {
            "total_days": days_in_month,
            "working_days": working_days,
            "paid_holidays": paid_holidays,
            "weekly_offs": weekly_off_days,
            "days_present": days_present,
            "half_days": half_days,
            "days_absent": days_absent
        },
        "overtime": {
            "hours": overtime_hours,
            "rate": overtime_rate,
            "amount": round(overtime_pay, 2)
        },
        "deductions": {
            "absent": round(absent_deduction, 2),
            "half_day": round(half_day_deduction, 2),
            "total": round(absent_deduction + half_day_deduction, 2)
        },
        "net_impact": {
            "overtime_addition": round(overtime_pay, 2),
            "attendance_deduction": round(absent_deduction + half_day_deduction, 2),
            "net_adjustment": round(overtime_pay - absent_deduction - half_day_deduction, 2)
        }
    }


@holiday_router.get("/salary-preview/{month}")
async def get_monthly_salary_preview(
    month: str,  # Format: YYYY-MM
    current_user: dict = Depends(get_erp_user)
):
    """Get salary impact preview for all employees for a month"""
    if current_user.get("role") not in ["admin", "owner", "hr", "accountant"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    db = get_db()
    
    # Get all active employees
    employees = await db.employees.find({"status": "active"}, {"_id": 0}).to_list(500)
    
    results = []
    total_salary = 0
    total_deductions = 0
    total_overtime = 0
    
    for emp in employees:
        impact = await get_salary_impact(emp["id"], month, current_user)
        
        net_salary = emp.get("salary", 0) + impact["net_impact"]["net_adjustment"]
        
        results.append({
            "employee_id": emp["id"],
            "employee_name": emp.get("name"),
            "basic_salary": emp.get("salary", 0),
            "overtime_pay": impact["overtime"]["amount"],
            "deductions": impact["deductions"]["total"],
            "net_salary": round(net_salary, 2),
            "days_present": impact["days_breakdown"]["days_present"],
            "days_absent": impact["days_breakdown"]["days_absent"]
        })
        
        total_salary += net_salary
        total_deductions += impact["deductions"]["total"]
        total_overtime += impact["overtime"]["amount"]
    
    return {
        "month": month,
        "employees": results,
        "summary": {
            "total_employees": len(employees),
            "total_salary_liability": round(total_salary, 2),
            "total_deductions": round(total_deductions, 2),
            "total_overtime": round(total_overtime, 2)
        }
    }


# =============== REPORTS ===============

@holiday_router.get("/reports/yearly/{year}")
async def get_yearly_holiday_report(
    year: int,
    current_user: dict = Depends(get_erp_user)
):
    """Get yearly holiday report with working days analysis"""
    db = get_db()
    
    holidays = await db.holidays.find({"year": year}, {"_id": 0}).to_list(100)
    settings = await get_holiday_settings(current_user)
    
    # Count by type
    type_counts = {}
    for h in holidays:
        t = h.get("type", "company")
        type_counts[t] = type_counts.get(t, 0) + 1
    
    # Calculate total working days
    total_days = 366 if calendar.isleap(year) else 365
    weekly_offs = settings.get("weekly_offs", ["sunday"])
    
    # Count weekly offs
    weekly_off_count = 0
    for month in range(1, 13):
        _, days_in_month = calendar.monthrange(year, month)
        for day in range(1, days_in_month + 1):
            if calendar.day_name[date(year, month, day).weekday()].lower() in weekly_offs:
                weekly_off_count += 1
    
    working_days = total_days - len(holidays) - weekly_off_count
    
    return {
        "year": year,
        "total_days": total_days,
        "working_days": working_days,
        "total_holidays": len(holidays),
        "weekly_offs": weekly_off_count,
        "by_type": type_counts,
        "holidays": sorted(holidays, key=lambda x: x["date"])
    }


@holiday_router.get("/reports/overtime-summary/{month}")
async def get_overtime_summary(
    month: str,
    current_user: dict = Depends(get_erp_user)
):
    """Get overtime summary for a month"""
    if current_user.get("role") not in ["admin", "owner", "hr", "accountant"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    db = get_db()
    
    pipeline = [
        {"$match": {"date": {"$regex": f"^{month}"}, "status": "approved"}},
        {"$group": {
            "_id": "$employee_id",
            "employee_name": {"$first": "$employee_name"},
            "total_hours": {"$sum": "$hours_worked"},
            "count": {"$sum": 1}
        }},
        {"$sort": {"total_hours": -1}}
    ]
    
    results = await db.holiday_overtime.aggregate(pipeline).to_list(100)
    
    settings = await get_holiday_settings(current_user)
    overtime_rate = settings.get("overtime_rate", 2.0)
    
    return {
        "month": month,
        "overtime_rate": overtime_rate,
        "employees": results,
        "total_hours": sum(r["total_hours"] for r in results)
    }
