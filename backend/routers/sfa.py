"""
Field Sales Automation (SFA) Router
Attendance, Movement Intelligence, GPS Tracking, Route & Distance
Part of: Field Sales Attendance, Movement, Communication & Expense Intelligence System
"""
from fastapi import APIRouter, Depends, HTTPException, Request
from typing import Optional, List
from datetime import datetime, timezone, timedelta
from pydantic import BaseModel
import uuid
import math
from .base import get_erp_user, get_db
from .audit import log_action

sfa_router = APIRouter(prefix="/sfa", tags=["Sales Force Automation"])

# ==================== MODELS ====================

class DayStartRequest(BaseModel):
    latitude: float
    longitude: float
    address: Optional[str] = None
    vehicle_type: str = "bike"  # bike, car, company_vehicle
    odometer_reading: Optional[float] = None

class DayEndRequest(BaseModel):
    latitude: float
    longitude: float
    address: Optional[str] = None
    odometer_reading: Optional[float] = None

class LocationPingRequest(BaseModel):
    latitude: float
    longitude: float
    accuracy: Optional[float] = None
    speed: Optional[float] = None
    battery_level: Optional[int] = None

class VisitRequest(BaseModel):
    lead_id: Optional[str] = None
    customer_name: str
    latitude: float
    longitude: float
    address: Optional[str] = None
    purpose: str
    notes: Optional[str] = None

class VisitEndRequest(BaseModel):
    outcome: str  # successful, follow_up, not_interested, not_available
    notes: Optional[str] = None
    next_action: Optional[str] = None
    next_action_date: Optional[str] = None


# ==================== HELPER FUNCTIONS ====================

def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance between two GPS coordinates using Haversine formula (in KM)"""
    R = 6371  # Earth's radius in kilometers
    
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    
    a = math.sin(delta_lat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
    return round(R * c, 2)


def get_fuel_rate(vehicle_type: str) -> float:
    """Get fuel allowance rate per KM based on vehicle type"""
    rates = {
        "bike": 3.50,      # ₹3.50 per KM
        "car": 8.00,       # ₹8.00 per KM
        "company_vehicle": 0.00  # No allowance for company vehicle
    }
    return rates.get(vehicle_type, 3.50)


# ==================== ATTENDANCE APIs ====================

@sfa_router.post("/day-start")
async def start_day(
    request: Request,
    data: DayStartRequest,
    current_user: dict = Depends(get_erp_user)
):
    """
    Employee Day Start - Mark attendance and start GPS tracking
    Records: Home location, start time, vehicle type
    """
    db = get_db()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    # Check if day already started
    existing = await db.sfa_attendance.find_one({
        "user_id": current_user["id"],
        "date": today
    })
    
    if existing and existing.get("day_start_time"):
        raise HTTPException(status_code=400, detail="Day already started. Cannot start again.")
    
    attendance_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    
    attendance_record = {
        "id": attendance_id,
        "user_id": current_user["id"],
        "user_name": current_user["name"],
        "user_role": current_user.get("role", "sales"),
        "date": today,
        "day_start_time": now.isoformat(),
        "day_start_location": {
            "latitude": data.latitude,
            "longitude": data.longitude,
            "address": data.address,
            "timestamp": now.isoformat()
        },
        "vehicle_type": data.vehicle_type,
        "fuel_rate_per_km": get_fuel_rate(data.vehicle_type),
        "odometer_start": data.odometer_reading,
        "day_end_time": None,
        "day_end_location": None,
        "odometer_end": None,
        "total_distance_km": 0,
        "total_fuel_allowance": 0,
        "location_tracking": True,
        "location_pings": [],
        "visits": [],
        "stops": [],
        "route_coordinates": [{
            "lat": data.latitude,
            "lng": data.longitude,
            "time": now.isoformat()
        }],
        "status": "active",
        "created_at": now.isoformat()
    }
    
    await db.sfa_attendance.insert_one(attendance_record)
    
    # Log action
    await log_action(
        user_id=current_user["id"],
        user_name=current_user["name"],
        user_role=current_user.get("role", "sales"),
        action="create",
        module="sfa_attendance",
        details={"action": "day_start", "vehicle": data.vehicle_type, "location": data.address},
        record_id=attendance_id,
        ip_address=request.client.host if request.client else None
    )
    
    return {
        "message": "Day started successfully",
        "attendance_id": attendance_id,
        "start_time": now.isoformat(),
        "vehicle_type": data.vehicle_type,
        "fuel_rate": get_fuel_rate(data.vehicle_type)
    }


@sfa_router.post("/day-end")
async def end_day(
    request: Request,
    data: DayEndRequest,
    current_user: dict = Depends(get_erp_user)
):
    """
    Employee Day End - Complete attendance and calculate totals
    """
    db = get_db()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    attendance = await db.sfa_attendance.find_one({
        "user_id": current_user["id"],
        "date": today,
        "status": "active"
    })
    
    if not attendance:
        raise HTTPException(status_code=400, detail="No active day found. Please start day first.")
    
    if attendance.get("day_end_time"):
        raise HTTPException(status_code=400, detail="Day already ended.")
    
    now = datetime.now(timezone.utc)
    
    # Calculate total distance from route coordinates
    route = attendance.get("route_coordinates", [])
    total_distance = 0
    for i in range(1, len(route)):
        total_distance += calculate_distance(
            route[i-1]["lat"], route[i-1]["lng"],
            route[i]["lat"], route[i]["lng"]
        )
    
    # Add final leg to end location
    if route:
        total_distance += calculate_distance(
            route[-1]["lat"], route[-1]["lng"],
            data.latitude, data.longitude
        )
    
    # Calculate fuel allowance
    fuel_rate = attendance.get("fuel_rate_per_km", 0)
    fuel_allowance = round(total_distance * fuel_rate, 2)
    
    # Calculate working hours
    start_time = datetime.fromisoformat(attendance["day_start_time"].replace('Z', '+00:00'))
    working_hours = round((now - start_time).total_seconds() / 3600, 2)
    
    # Update attendance record
    update_data = {
        "day_end_time": now.isoformat(),
        "day_end_location": {
            "latitude": data.latitude,
            "longitude": data.longitude,
            "address": data.address,
            "timestamp": now.isoformat()
        },
        "odometer_end": data.odometer_reading,
        "total_distance_km": round(total_distance, 2),
        "total_fuel_allowance": fuel_allowance,
        "working_hours": working_hours,
        "location_tracking": False,
        "status": "completed"
    }
    
    # Add final coordinate to route
    route.append({
        "lat": data.latitude,
        "lng": data.longitude,
        "time": now.isoformat()
    })
    update_data["route_coordinates"] = route
    
    await db.sfa_attendance.update_one(
        {"id": attendance["id"]},
        {"$set": update_data}
    )
    
    # Log action
    await log_action(
        user_id=current_user["id"],
        user_name=current_user["name"],
        user_role=current_user.get("role", "sales"),
        action="update",
        module="sfa_attendance",
        details={
            "action": "day_end",
            "distance_km": round(total_distance, 2),
            "fuel_allowance": fuel_allowance,
            "working_hours": working_hours
        },
        record_id=attendance["id"],
        ip_address=request.client.host if request.client else None
    )
    
    return {
        "message": "Day ended successfully",
        "summary": {
            "working_hours": working_hours,
            "total_distance_km": round(total_distance, 2),
            "fuel_allowance": fuel_allowance,
            "visits_count": len(attendance.get("visits", [])),
            "stops_count": len(attendance.get("stops", []))
        }
    }


@sfa_router.post("/location-ping")
async def record_location_ping(
    data: LocationPingRequest,
    current_user: dict = Depends(get_erp_user)
):
    """
    GPS Location Ping - Record current location (called every 1-5 minutes)
    Used for continuous movement tracking
    """
    db = get_db()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    attendance = await db.sfa_attendance.find_one({
        "user_id": current_user["id"],
        "date": today,
        "status": "active"
    })
    
    if not attendance:
        raise HTTPException(status_code=400, detail="No active day found")
    
    now = datetime.now(timezone.utc)
    
    # Create location ping record
    ping = {
        "latitude": data.latitude,
        "longitude": data.longitude,
        "accuracy": data.accuracy,
        "speed": data.speed,
        "battery_level": data.battery_level,
        "timestamp": now.isoformat()
    }
    
    # Add to route coordinates
    route_point = {
        "lat": data.latitude,
        "lng": data.longitude,
        "time": now.isoformat()
    }
    
    # Check for stops (if speed is 0 or very low for extended period)
    last_pings = attendance.get("location_pings", [])[-5:]  # Last 5 pings
    
    # Detect if stopped (low speed or same location)
    is_stopped = False
    if last_pings:
        last_ping = last_pings[-1]
        distance_moved = calculate_distance(
            last_ping["latitude"], last_ping["longitude"],
            data.latitude, data.longitude
        )
        if distance_moved < 0.05:  # Less than 50 meters
            is_stopped = True
    
    # Update attendance with new ping
    await db.sfa_attendance.update_one(
        {"id": attendance["id"]},
        {
            "$push": {
                "location_pings": ping,
                "route_coordinates": route_point
            },
            "$set": {
                "last_ping_time": now.isoformat(),
                "is_stationary": is_stopped
            }
        }
    )
    
    return {
        "message": "Location recorded",
        "timestamp": now.isoformat(),
        "is_stationary": is_stopped
    }


# ==================== VISIT TRACKING ====================

@sfa_router.post("/visit-start")
async def start_visit(
    request: Request,
    data: VisitRequest,
    current_user: dict = Depends(get_erp_user)
):
    """
    Start a customer/lead visit - Check-in at location
    """
    db = get_db()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    attendance = await db.sfa_attendance.find_one({
        "user_id": current_user["id"],
        "date": today,
        "status": "active"
    })
    
    if not attendance:
        raise HTTPException(status_code=400, detail="Please start your day first")
    
    now = datetime.now(timezone.utc)
    visit_id = str(uuid.uuid4())
    
    visit = {
        "id": visit_id,
        "lead_id": data.lead_id,
        "customer_name": data.customer_name,
        "start_time": now.isoformat(),
        "end_time": None,
        "duration_minutes": None,
        "start_location": {
            "latitude": data.latitude,
            "longitude": data.longitude,
            "address": data.address
        },
        "purpose": data.purpose,
        "notes": data.notes,
        "outcome": None,
        "status": "in_progress"
    }
    
    await db.sfa_attendance.update_one(
        {"id": attendance["id"]},
        {
            "$push": {"visits": visit},
            "$set": {"current_visit_id": visit_id}
        }
    )
    
    # Log action
    await log_action(
        user_id=current_user["id"],
        user_name=current_user["name"],
        user_role=current_user.get("role", "sales"),
        action="create",
        module="sfa_visits",
        details={"customer": data.customer_name, "purpose": data.purpose},
        record_id=visit_id,
        ip_address=request.client.host if request.client else None
    )
    
    return {
        "message": "Visit started",
        "visit_id": visit_id,
        "start_time": now.isoformat()
    }


@sfa_router.post("/visit-end/{visit_id}")
async def end_visit(
    request: Request,
    visit_id: str,
    data: VisitEndRequest,
    current_user: dict = Depends(get_erp_user)
):
    """
    End a customer/lead visit - Check-out with outcome
    """
    db = get_db()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    attendance = await db.sfa_attendance.find_one({
        "user_id": current_user["id"],
        "date": today,
        "status": "active"
    })
    
    if not attendance:
        raise HTTPException(status_code=400, detail="No active day found")
    
    now = datetime.now(timezone.utc)
    
    # Find and update the visit
    visits = attendance.get("visits", [])
    visit_found = False
    
    for visit in visits:
        if visit["id"] == visit_id:
            visit_found = True
            start_time = datetime.fromisoformat(visit["start_time"].replace('Z', '+00:00'))
            duration = round((now - start_time).total_seconds() / 60, 2)
            
            visit["end_time"] = now.isoformat()
            visit["duration_minutes"] = duration
            visit["outcome"] = data.outcome
            visit["notes"] = data.notes or visit.get("notes")
            visit["next_action"] = data.next_action
            visit["next_action_date"] = data.next_action_date
            visit["status"] = "completed"
            break
    
    if not visit_found:
        raise HTTPException(status_code=404, detail="Visit not found")
    
    await db.sfa_attendance.update_one(
        {"id": attendance["id"]},
        {
            "$set": {
                "visits": visits,
                "current_visit_id": None
            }
        }
    )
    
    # Update lead/CRM if linked
    if visit.get("lead_id"):
        await db.erp_leads.update_one(
            {"id": visit["lead_id"]},
            {
                "$push": {
                    "visit_history": {
                        "visit_id": visit_id,
                        "date": today,
                        "outcome": data.outcome,
                        "notes": data.notes,
                        "duration_minutes": duration,
                        "visited_by": current_user["name"]
                    }
                },
                "$set": {
                    "last_visit_date": today,
                    "last_visit_outcome": data.outcome
                }
            }
        )
    
    # Log action
    await log_action(
        user_id=current_user["id"],
        user_name=current_user["name"],
        user_role=current_user.get("role", "sales"),
        action="update",
        module="sfa_visits",
        details={"outcome": data.outcome, "duration_minutes": duration},
        record_id=visit_id,
        ip_address=request.client.host if request.client else None
    )
    
    return {
        "message": "Visit completed",
        "duration_minutes": duration,
        "outcome": data.outcome
    }


# ==================== DASHBOARD & REPORTS ====================

@sfa_router.get("/my-day")
async def get_my_day(
    date: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """
    Get current user's day summary with route, visits, and stats
    """
    db = get_db()
    target_date = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    attendance = await db.sfa_attendance.find_one(
        {"user_id": current_user["id"], "date": target_date},
        {"_id": 0}
    )
    
    if not attendance:
        return {
            "date": target_date,
            "status": "not_started",
            "message": "No attendance record for this date"
        }
    
    return {
        "date": target_date,
        "status": attendance.get("status"),
        "day_start": attendance.get("day_start_time"),
        "day_end": attendance.get("day_end_time"),
        "working_hours": attendance.get("working_hours"),
        "vehicle_type": attendance.get("vehicle_type"),
        "total_distance_km": attendance.get("total_distance_km", 0),
        "fuel_allowance": attendance.get("total_fuel_allowance", 0),
        "visits": attendance.get("visits", []),
        "visits_count": len(attendance.get("visits", [])),
        "route_coordinates": attendance.get("route_coordinates", []),
        "start_location": attendance.get("day_start_location"),
        "end_location": attendance.get("day_end_location"),
        "is_active": attendance.get("status") == "active"
    }


@sfa_router.get("/team-dashboard")
async def get_team_dashboard(
    date: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """
    Get team dashboard - For managers to see all team members' status
    """
    if current_user.get("role") not in ["super_admin", "admin", "owner", "manager", "sales_manager"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    target_date = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    # Get all sales team attendance for the day
    attendances = await db.sfa_attendance.find(
        {"date": target_date},
        {"_id": 0}
    ).to_list(100)
    
    # Get all sales employees
    employees = await db.users.find(
        {"role": {"$in": ["sales", "sales_executive", "sales_manager"]}},
        {"_id": 0, "id": 1, "name": 1, "email": 1, "phone": 1, "role": 1}
    ).to_list(100)
    
    # Build team status
    team_status = []
    for emp in employees:
        attendance = next((a for a in attendances if a["user_id"] == emp["id"]), None)
        
        status = {
            "employee": emp,
            "date": target_date,
            "status": "absent",
            "day_start": None,
            "day_end": None,
            "current_location": None,
            "visits_count": 0,
            "distance_km": 0,
            "is_tracking": False
        }
        
        if attendance:
            status["status"] = attendance.get("status", "active")
            status["day_start"] = attendance.get("day_start_time")
            status["day_end"] = attendance.get("day_end_time")
            status["visits_count"] = len(attendance.get("visits", []))
            status["distance_km"] = attendance.get("total_distance_km", 0)
            status["is_tracking"] = attendance.get("location_tracking", False)
            
            # Get last known location
            pings = attendance.get("location_pings", [])
            if pings:
                last_ping = pings[-1]
                status["current_location"] = {
                    "latitude": last_ping["latitude"],
                    "longitude": last_ping["longitude"],
                    "timestamp": last_ping["timestamp"]
                }
        
        team_status.append(status)
    
    # Summary stats
    active_count = len([t for t in team_status if t["status"] == "active"])
    completed_count = len([t for t in team_status if t["status"] == "completed"])
    absent_count = len([t for t in team_status if t["status"] == "absent"])
    total_visits = sum(t["visits_count"] for t in team_status)
    total_distance = sum(t["distance_km"] for t in team_status)
    
    return {
        "date": target_date,
        "summary": {
            "total_employees": len(employees),
            "active": active_count,
            "completed": completed_count,
            "absent": absent_count,
            "total_visits": total_visits,
            "total_distance_km": round(total_distance, 2)
        },
        "team": team_status
    }


@sfa_router.get("/employee-timeline/{user_id}")
async def get_employee_timeline(
    user_id: str,
    date: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """
    Get detailed timeline for an employee's day
    Shows: Home → Visits → Stops → Home with timestamps
    """
    if current_user.get("role") not in ["super_admin", "admin", "owner", "manager", "sales_manager"]:
        if current_user["id"] != user_id:
            raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    target_date = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    attendance = await db.sfa_attendance.find_one(
        {"user_id": user_id, "date": target_date},
        {"_id": 0}
    )
    
    if not attendance:
        raise HTTPException(status_code=404, detail="No attendance record found")
    
    # Build timeline events
    timeline = []
    
    # Day Start
    if attendance.get("day_start_time"):
        timeline.append({
            "type": "day_start",
            "time": attendance["day_start_time"],
            "title": "Day Started",
            "location": attendance.get("day_start_location"),
            "details": {
                "vehicle": attendance.get("vehicle_type"),
                "odometer": attendance.get("odometer_start")
            }
        })
    
    # Visits
    for visit in attendance.get("visits", []):
        timeline.append({
            "type": "visit_start",
            "time": visit["start_time"],
            "title": f"Visit: {visit['customer_name']}",
            "location": visit.get("start_location"),
            "details": {
                "purpose": visit.get("purpose"),
                "status": visit.get("status")
            }
        })
        
        if visit.get("end_time"):
            timeline.append({
                "type": "visit_end",
                "time": visit["end_time"],
                "title": f"Visit Completed: {visit['customer_name']}",
                "details": {
                    "outcome": visit.get("outcome"),
                    "duration": visit.get("duration_minutes")
                }
            })
    
    # Day End
    if attendance.get("day_end_time"):
        timeline.append({
            "type": "day_end",
            "time": attendance["day_end_time"],
            "title": "Day Ended",
            "location": attendance.get("day_end_location"),
            "details": {
                "working_hours": attendance.get("working_hours"),
                "distance_km": attendance.get("total_distance_km"),
                "fuel_allowance": attendance.get("total_fuel_allowance")
            }
        })
    
    # Sort timeline by time
    timeline.sort(key=lambda x: x["time"])
    
    return {
        "user_id": user_id,
        "user_name": attendance.get("user_name"),
        "date": target_date,
        "status": attendance.get("status"),
        "summary": {
            "working_hours": attendance.get("working_hours"),
            "distance_km": attendance.get("total_distance_km", 0),
            "fuel_allowance": attendance.get("total_fuel_allowance", 0),
            "visits_count": len(attendance.get("visits", []))
        },
        "timeline": timeline,
        "route_coordinates": attendance.get("route_coordinates", [])
    }


@sfa_router.get("/reports/daily-summary")
async def get_daily_summary_report(
    date: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """
    Daily Summary Report - All employees' attendance, distance, visits
    """
    if current_user.get("role") not in ["super_admin", "admin", "owner", "manager", "sales_manager", "hr", "accountant"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    target_date = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    attendances = await db.sfa_attendance.find(
        {"date": target_date},
        {"_id": 0, "location_pings": 0, "route_coordinates": 0}
    ).to_list(100)
    
    report = []
    total_distance = 0
    total_fuel = 0
    total_visits = 0
    
    for att in attendances:
        visits_completed = len([v for v in att.get("visits", []) if v.get("status") == "completed"])
        visits_successful = len([v for v in att.get("visits", []) if v.get("outcome") == "successful"])
        
        entry = {
            "employee_name": att.get("user_name"),
            "employee_id": att.get("user_id"),
            "vehicle_type": att.get("vehicle_type"),
            "day_start": att.get("day_start_time"),
            "day_end": att.get("day_end_time"),
            "working_hours": att.get("working_hours", 0),
            "distance_km": att.get("total_distance_km", 0),
            "fuel_allowance": att.get("total_fuel_allowance", 0),
            "visits_total": len(att.get("visits", [])),
            "visits_completed": visits_completed,
            "visits_successful": visits_successful,
            "status": att.get("status")
        }
        
        report.append(entry)
        total_distance += entry["distance_km"]
        total_fuel += entry["fuel_allowance"]
        total_visits += entry["visits_total"]
    
    return {
        "date": target_date,
        "summary": {
            "total_employees": len(report),
            "total_distance_km": round(total_distance, 2),
            "total_fuel_allowance": round(total_fuel, 2),
            "total_visits": total_visits
        },
        "report": report
    }


@sfa_router.get("/reports/monthly-summary")
async def get_monthly_summary_report(
    month: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """
    Monthly Summary Report - Employee-wise totals
    """
    if current_user.get("role") not in ["super_admin", "admin", "owner", "manager", "sales_manager", "hr", "accountant"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    target_month = month or datetime.now(timezone.utc).strftime("%Y-%m")
    
    # Get all attendance for the month
    pipeline = [
        {"$match": {"date": {"$regex": f"^{target_month}"}}},
        {"$group": {
            "_id": {"user_id": "$user_id", "user_name": "$user_name"},
            "days_worked": {"$sum": 1},
            "total_distance_km": {"$sum": "$total_distance_km"},
            "total_fuel_allowance": {"$sum": "$total_fuel_allowance"},
            "total_working_hours": {"$sum": "$working_hours"},
            "total_visits": {"$sum": {"$size": "$visits"}}
        }},
        {"$sort": {"total_distance_km": -1}}
    ]
    
    results = await db.sfa_attendance.aggregate(pipeline).to_list(100)
    
    report = []
    for r in results:
        report.append({
            "employee_id": r["_id"]["user_id"],
            "employee_name": r["_id"]["user_name"],
            "days_worked": r["days_worked"],
            "total_distance_km": round(r["total_distance_km"], 2),
            "total_fuel_allowance": round(r["total_fuel_allowance"], 2),
            "total_working_hours": round(r["total_working_hours"], 2),
            "total_visits": r["total_visits"],
            "avg_distance_per_day": round(r["total_distance_km"] / max(r["days_worked"], 1), 2),
            "avg_visits_per_day": round(r["total_visits"] / max(r["days_worked"], 1), 2)
        })
    
    return {
        "month": target_month,
        "summary": {
            "total_employees": len(report),
            "total_distance_km": round(sum(r["total_distance_km"] for r in report), 2),
            "total_fuel_allowance": round(sum(r["total_fuel_allowance"] for r in report), 2),
            "total_visits": sum(r["total_visits"] for r in report)
        },
        "report": report
    }



# ==================== PHASE 2: ALERTS, PERFORMANCE, STOPS ====================

@sfa_router.get("/alerts")
async def get_sfa_alerts(
    date: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """
    Get all active alerts - Location OFF, Long Stops, Idle Time
    For managers to monitor team exceptions
    """
    if current_user.get("role") not in ["super_admin", "admin", "owner", "manager", "sales_manager"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    target_date = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    alerts = []
    
    # Get all attendance records for today
    attendances = await db.sfa_attendance.find(
        {"date": target_date, "status": "active"},
        {"_id": 0}
    ).to_list(100)
    
    now = datetime.now(timezone.utc)
    
    for att in attendances:
        user_name = att.get("user_name", "Unknown")
        user_id = att.get("user_id")
        
        # Alert 1: Location OFF - No ping in last 15 minutes
        last_ping_time = att.get("last_ping_time")
        if last_ping_time:
            last_ping_dt = datetime.fromisoformat(last_ping_time.replace('Z', '+00:00'))
            minutes_since_ping = (now - last_ping_dt).total_seconds() / 60
            if minutes_since_ping > 15:
                alerts.append({
                    "id": f"loc_off_{user_id}_{target_date}",
                    "type": "location_off",
                    "severity": "high",
                    "title": f"Location OFF: {user_name}",
                    "message": f"GPS signal lost for {int(minutes_since_ping)} minutes. Last seen at {last_ping_dt.strftime('%H:%M')}",
                    "user_id": user_id,
                    "user_name": user_name,
                    "timestamp": now.isoformat(),
                    "last_location": att.get("location_pings", [{}])[-1] if att.get("location_pings") else None
                })
        
        # Alert 2: Long Stop - Stationary for more than 30 minutes without a visit
        pings = att.get("location_pings", [])
        if len(pings) >= 6:  # At least 30 minutes of pings (5-min intervals)
            # Check last 6 pings for stationary
            recent_pings = pings[-6:]
            if all(p.get("speed", 0) < 2 for p in recent_pings):
                # Check if there's an active visit
                current_visit = att.get("current_visit_id")
                if not current_visit:
                    first_ping = recent_pings[0]
                    stop_duration = len(recent_pings) * 5  # Approximate minutes
                    alerts.append({
                        "id": f"long_stop_{user_id}_{target_date}",
                        "type": "long_stop",
                        "severity": "medium",
                        "title": f"Long Stop: {user_name}",
                        "message": f"Stationary for ~{stop_duration} minutes without checking into a visit",
                        "user_id": user_id,
                        "user_name": user_name,
                        "timestamp": now.isoformat(),
                        "stop_location": {
                            "latitude": first_ping.get("latitude"),
                            "longitude": first_ping.get("longitude")
                        }
                    })
        
        # Alert 3: Low Battery - Battery below 20%
        if pings:
            last_battery = pings[-1].get("battery_level")
            if last_battery is not None and last_battery < 20:
                alerts.append({
                    "id": f"low_battery_{user_id}_{target_date}",
                    "type": "low_battery",
                    "severity": "low",
                    "title": f"Low Battery: {user_name}",
                    "message": f"Device battery at {last_battery}%. Tracking may stop soon.",
                    "user_id": user_id,
                    "user_name": user_name,
                    "timestamp": now.isoformat()
                })
        
        # Alert 4: Zero Visits - Day started but no visits after 2 hours
        day_start = att.get("day_start_time")
        if day_start:
            start_dt = datetime.fromisoformat(day_start.replace('Z', '+00:00'))
            hours_since_start = (now - start_dt).total_seconds() / 3600
            visits_count = len(att.get("visits", []))
            if hours_since_start > 2 and visits_count == 0:
                alerts.append({
                    "id": f"zero_visits_{user_id}_{target_date}",
                    "type": "zero_visits",
                    "severity": "medium",
                    "title": f"No Visits: {user_name}",
                    "message": f"Day started {int(hours_since_start)} hours ago but no customer visits logged",
                    "user_id": user_id,
                    "user_name": user_name,
                    "timestamp": now.isoformat()
                })
    
    # Sort by severity
    severity_order = {"high": 0, "medium": 1, "low": 2}
    alerts.sort(key=lambda x: severity_order.get(x["severity"], 3))
    
    return {
        "date": target_date,
        "total_alerts": len(alerts),
        "alerts": alerts
    }


@sfa_router.get("/stops/{user_id}")
async def get_employee_stops(
    user_id: str,
    date: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """
    Detect and return all stops/halts for an employee
    Stop = Location unchanged for more than 5 minutes
    """
    if current_user.get("role") not in ["super_admin", "admin", "owner", "manager", "sales_manager"]:
        if current_user["id"] != user_id:
            raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    target_date = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    attendance = await db.sfa_attendance.find_one(
        {"user_id": user_id, "date": target_date},
        {"_id": 0}
    )
    
    if not attendance:
        raise HTTPException(status_code=404, detail="No attendance record found")
    
    pings = attendance.get("location_pings", [])
    visits = attendance.get("visits", [])
    stops = []
    
    # Analyze pings to detect stops
    if len(pings) < 2:
        return {"user_id": user_id, "date": target_date, "stops": [], "total_stops": 0}
    
    stop_start = None
    stop_location = None
    
    for i, ping in enumerate(pings):
        if i == 0:
            stop_start = ping
            stop_location = (ping["latitude"], ping["longitude"])
            continue
        
        current_loc = (ping["latitude"], ping["longitude"])
        distance = calculate_distance(stop_location[0], stop_location[1], current_loc[0], current_loc[1])
        
        # If moved more than 100 meters, end current stop and start new potential stop
        if distance > 0.1:
            if stop_start:
                # Calculate stop duration
                start_time = datetime.fromisoformat(stop_start["timestamp"].replace('Z', '+00:00'))
                end_time = datetime.fromisoformat(pings[i-1]["timestamp"].replace('Z', '+00:00'))
                duration_minutes = (end_time - start_time).total_seconds() / 60
                
                # Only record stops longer than 5 minutes
                if duration_minutes >= 5:
                    # Check if this stop corresponds to a visit
                    is_visit = False
                    visit_info = None
                    for visit in visits:
                        if visit.get("start_location"):
                            v_lat = visit["start_location"]["latitude"]
                            v_lng = visit["start_location"]["longitude"]
                            if calculate_distance(stop_location[0], stop_location[1], v_lat, v_lng) < 0.1:
                                is_visit = True
                                visit_info = {
                                    "customer_name": visit.get("customer_name"),
                                    "purpose": visit.get("purpose"),
                                    "outcome": visit.get("outcome")
                                }
                                break
                    
                    stops.append({
                        "start_time": stop_start["timestamp"],
                        "end_time": pings[i-1]["timestamp"],
                        "duration_minutes": round(duration_minutes, 1),
                        "latitude": stop_location[0],
                        "longitude": stop_location[1],
                        "is_visit": is_visit,
                        "visit_info": visit_info,
                        "type": "visit" if is_visit else "stop"
                    })
            
            stop_start = ping
            stop_location = current_loc
    
    # Handle last stop
    if stop_start and len(pings) > 1:
        start_time = datetime.fromisoformat(stop_start["timestamp"].replace('Z', '+00:00'))
        end_time = datetime.fromisoformat(pings[-1]["timestamp"].replace('Z', '+00:00'))
        duration_minutes = (end_time - start_time).total_seconds() / 60
        
        if duration_minutes >= 5:
            is_visit = False
            for visit in visits:
                if visit.get("start_location"):
                    v_lat = visit["start_location"]["latitude"]
                    v_lng = visit["start_location"]["longitude"]
                    if calculate_distance(stop_location[0], stop_location[1], v_lat, v_lng) < 0.1:
                        is_visit = True
                        break
            
            stops.append({
                "start_time": stop_start["timestamp"],
                "end_time": pings[-1]["timestamp"],
                "duration_minutes": round(duration_minutes, 1),
                "latitude": stop_location[0],
                "longitude": stop_location[1],
                "is_visit": is_visit,
                "type": "visit" if is_visit else "stop"
            })
    
    # Calculate summary
    total_stop_time = sum(s["duration_minutes"] for s in stops)
    visit_stops = [s for s in stops if s["is_visit"]]
    idle_stops = [s for s in stops if not s["is_visit"]]
    
    return {
        "user_id": user_id,
        "user_name": attendance.get("user_name"),
        "date": target_date,
        "summary": {
            "total_stops": len(stops),
            "visit_stops": len(visit_stops),
            "idle_stops": len(idle_stops),
            "total_stop_time_minutes": round(total_stop_time, 1),
            "visit_time_minutes": round(sum(s["duration_minutes"] for s in visit_stops), 1),
            "idle_time_minutes": round(sum(s["duration_minutes"] for s in idle_stops), 1)
        },
        "stops": stops
    }


@sfa_router.get("/performance-scorecard")
async def get_performance_scorecard(
    month: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """
    Employee Performance Scorecard with KPIs
    - Attendance Score: Days worked vs working days
    - Distance Score: Actual vs target distance
    - Visit Score: Visits completed vs target
    - Conversion Score: Successful visits vs total
    - Time Utilization: Productive vs idle time
    """
    if current_user.get("role") not in ["super_admin", "admin", "owner", "manager", "sales_manager"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    target_month = month or datetime.now(timezone.utc).strftime("%Y-%m")
    
    # Get all attendance for the month
    pipeline = [
        {"$match": {"date": {"$regex": f"^{target_month}"}}},
        {"$group": {
            "_id": {"user_id": "$user_id", "user_name": "$user_name"},
            "days_worked": {"$sum": 1},
            "total_distance_km": {"$sum": "$total_distance_km"},
            "total_working_hours": {"$sum": "$working_hours"},
            "total_fuel_allowance": {"$sum": "$total_fuel_allowance"},
            "attendance_records": {"$push": {
                "date": "$date",
                "visits": "$visits",
                "status": "$status",
                "working_hours": "$working_hours"
            }}
        }},
        {"$sort": {"total_distance_km": -1}}
    ]
    
    results = await db.sfa_attendance.aggregate(pipeline).to_list(100)
    
    # Targets (these could come from settings in production)
    TARGETS = {
        "days_per_month": 22,  # Working days target
        "distance_per_day": 50,  # KM target per day
        "visits_per_day": 8,  # Visits target per day
        "working_hours_per_day": 8  # Hours target
    }
    
    scorecards = []
    
    for r in results:
        user_id = r["_id"]["user_id"]
        user_name = r["_id"]["user_name"]
        
        # Calculate totals
        total_visits = 0
        successful_visits = 0
        total_visit_duration = 0
        
        for att in r.get("attendance_records", []):
            visits = att.get("visits", [])
            total_visits += len(visits)
            successful_visits += len([v for v in visits if v.get("outcome") == "successful"])
            total_visit_duration += sum(v.get("duration_minutes", 0) for v in visits if v.get("duration_minutes"))
        
        days_worked = r["days_worked"]
        total_distance = r["total_distance_km"] or 0
        total_hours = r["total_working_hours"] or 0
        
        # Calculate scores (0-100)
        attendance_score = min(100, round((days_worked / TARGETS["days_per_month"]) * 100))
        
        distance_target = days_worked * TARGETS["distance_per_day"]
        distance_score = min(100, round((total_distance / max(distance_target, 1)) * 100)) if distance_target > 0 else 0
        
        visit_target = days_worked * TARGETS["visits_per_day"]
        visit_score = min(100, round((total_visits / max(visit_target, 1)) * 100)) if visit_target > 0 else 0
        
        conversion_score = round((successful_visits / max(total_visits, 1)) * 100) if total_visits > 0 else 0
        
        # Time utilization (visit time vs total time)
        total_minutes = total_hours * 60
        time_utilization = round((total_visit_duration / max(total_minutes, 1)) * 100) if total_minutes > 0 else 0
        
        # Overall score (weighted average)
        overall_score = round(
            attendance_score * 0.2 +
            distance_score * 0.2 +
            visit_score * 0.25 +
            conversion_score * 0.25 +
            time_utilization * 0.1
        )
        
        # Grade
        grade = (
            "A+" if overall_score >= 90 else
            "A" if overall_score >= 80 else
            "B+" if overall_score >= 70 else
            "B" if overall_score >= 60 else
            "C" if overall_score >= 50 else
            "D" if overall_score >= 40 else "F"
        )
        
        scorecards.append({
            "user_id": user_id,
            "employee_name": user_name,
            "month": target_month,
            "metrics": {
                "days_worked": days_worked,
                "total_distance_km": round(total_distance, 2),
                "total_working_hours": round(total_hours, 2),
                "total_visits": total_visits,
                "successful_visits": successful_visits,
                "visit_duration_minutes": round(total_visit_duration, 1),
                "total_fuel_allowance": round(r["total_fuel_allowance"] or 0, 2)
            },
            "scores": {
                "attendance": attendance_score,
                "distance": distance_score,
                "visits": visit_score,
                "conversion": conversion_score,
                "time_utilization": time_utilization,
                "overall": overall_score
            },
            "grade": grade,
            "targets": {
                "days_target": TARGETS["days_per_month"],
                "distance_target": distance_target,
                "visits_target": visit_target,
                "conversion_target": 50  # 50% target
            },
            "avg_per_day": {
                "distance": round(total_distance / max(days_worked, 1), 2),
                "visits": round(total_visits / max(days_worked, 1), 2),
                "hours": round(total_hours / max(days_worked, 1), 2)
            }
        })
    
    # Sort by overall score
    scorecards.sort(key=lambda x: x["scores"]["overall"], reverse=True)
    
    # Summary stats
    total_employees = len(scorecards)
    avg_score = round(sum(s["scores"]["overall"] for s in scorecards) / max(total_employees, 1))
    
    return {
        "month": target_month,
        "summary": {
            "total_employees": total_employees,
            "average_score": avg_score,
            "top_performers": len([s for s in scorecards if s["grade"] in ["A+", "A"]]),
            "needs_improvement": len([s for s in scorecards if s["grade"] in ["D", "F"]])
        },
        "targets_used": TARGETS,
        "report": scorecards
    }


@sfa_router.get("/performance/{user_id}")
async def get_individual_performance(
    user_id: str,
    month: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """
    Get individual employee's detailed performance report
    """
    if current_user.get("role") not in ["super_admin", "admin", "owner", "manager", "sales_manager"]:
        if current_user["id"] != user_id:
            raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    target_month = month or datetime.now(timezone.utc).strftime("%Y-%m")
    
    # Get all attendance for the user this month
    attendances = await db.sfa_attendance.find(
        {"user_id": user_id, "date": {"$regex": f"^{target_month}"}},
        {"_id": 0}
    ).to_list(31)
    
    if not attendances:
        raise HTTPException(status_code=404, detail="No records found for this employee")
    
    # Daily breakdown
    daily_stats = []
    total_distance = 0
    total_hours = 0
    total_visits = 0
    successful_visits = 0
    total_fuel = 0
    
    for att in sorted(attendances, key=lambda x: x["date"]):
        visits = att.get("visits", [])
        day_stats = {
            "date": att["date"],
            "status": att.get("status"),
            "start_time": att.get("day_start_time"),
            "end_time": att.get("day_end_time"),
            "distance_km": att.get("total_distance_km", 0),
            "working_hours": att.get("working_hours", 0),
            "visits_count": len(visits),
            "successful_visits": len([v for v in visits if v.get("outcome") == "successful"]),
            "fuel_allowance": att.get("total_fuel_allowance", 0),
            "vehicle": att.get("vehicle_type")
        }
        daily_stats.append(day_stats)
        
        total_distance += day_stats["distance_km"]
        total_hours += day_stats["working_hours"]
        total_visits += day_stats["visits_count"]
        successful_visits += day_stats["successful_visits"]
        total_fuel += day_stats["fuel_allowance"]
    
    days_worked = len(attendances)
    
    return {
        "user_id": user_id,
        "user_name": attendances[0].get("user_name") if attendances else "Unknown",
        "month": target_month,
        "summary": {
            "days_worked": days_worked,
            "total_distance_km": round(total_distance, 2),
            "total_working_hours": round(total_hours, 2),
            "total_visits": total_visits,
            "successful_visits": successful_visits,
            "conversion_rate": round((successful_visits / max(total_visits, 1)) * 100, 1),
            "total_fuel_allowance": round(total_fuel, 2),
            "avg_distance_per_day": round(total_distance / max(days_worked, 1), 2),
            "avg_visits_per_day": round(total_visits / max(days_worked, 1), 2)
        },
        "daily_breakdown": daily_stats
    }
