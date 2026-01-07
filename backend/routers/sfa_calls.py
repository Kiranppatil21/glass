"""
SFA Call Recording & CRM Integration Router
Auto-linked call records with leads/customers
Part of: Field Sales Attendance, Movement, Communication & Expense Intelligence System
"""
from fastapi import APIRouter, Depends, HTTPException, Request
from typing import Optional, List
from datetime import datetime, timezone, timedelta
from pydantic import BaseModel
import uuid
from .base import get_erp_user, get_db
from .audit import log_action

sfa_calls_router = APIRouter(prefix="/sfa-calls", tags=["SFA Call Recording & CRM"])


# ==================== MODELS ====================

class CallLogRequest(BaseModel):
    phone_number: str
    call_type: str  # incoming, outgoing
    duration_seconds: int
    call_start_time: str
    lead_id: Optional[str] = None
    customer_name: Optional[str] = None
    recording_url: Optional[str] = None
    notes: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None


class CallNotesUpdateRequest(BaseModel):
    notes: str
    outcome: Optional[str] = None  # interested, not_interested, callback, no_answer, wrong_number


# ==================== CALL LOGGING ====================

@sfa_calls_router.post("/log")
async def log_call(
    request: Request,
    data: CallLogRequest,
    current_user: dict = Depends(get_erp_user)
):
    """
    Log a call record (auto-synced from mobile app)
    Auto-links to existing lead/customer based on phone number
    """
    db = get_db()
    now = datetime.now(timezone.utc)
    call_id = str(uuid.uuid4())
    
    # Try to find matching lead/customer by phone number
    lead = None
    if not data.lead_id:
        # Search in leads
        lead = await db.erp_leads.find_one(
            {"$or": [
                {"phone": data.phone_number},
                {"phone": data.phone_number.replace("+91", "")},
                {"phone": "+91" + data.phone_number}
            ]},
            {"_id": 0}
        )
        
        # If not in leads, search in customers
        if not lead:
            customer = await db.users.find_one(
                {"$or": [
                    {"phone": data.phone_number},
                    {"phone": data.phone_number.replace("+91", "")}
                ], "role": {"$in": ["customer", "dealer"]}},
                {"_id": 0}
            )
            if customer:
                lead = {"id": customer["id"], "name": customer["name"], "type": "customer"}
    else:
        lead = await db.erp_leads.find_one({"id": data.lead_id}, {"_id": 0})
    
    # Check if call is flagged (call to number not in CRM)
    is_flagged = lead is None
    
    call_record = {
        "id": call_id,
        "user_id": current_user["id"],
        "user_name": current_user["name"],
        "phone_number": data.phone_number,
        "call_type": data.call_type,
        "duration_seconds": data.duration_seconds,
        "duration_formatted": f"{data.duration_seconds // 60}:{data.duration_seconds % 60:02d}",
        "call_start_time": data.call_start_time,
        "call_end_time": (datetime.fromisoformat(data.call_start_time.replace('Z', '+00:00')) + timedelta(seconds=data.duration_seconds)).isoformat(),
        "lead_id": lead["id"] if lead else None,
        "lead_name": lead.get("name") if lead else data.customer_name,
        "lead_type": lead.get("type", "lead") if lead else None,
        "recording_url": data.recording_url,
        "has_recording": data.recording_url is not None,
        "notes": data.notes,
        "outcome": None,
        "location": {
            "latitude": data.latitude,
            "longitude": data.longitude
        } if data.latitude else None,
        "is_flagged": is_flagged,
        "flag_reason": "Call to unknown number (not in CRM)" if is_flagged else None,
        "is_editable": False,  # Calls cannot be edited
        "is_deletable": False,  # Calls cannot be deleted
        "created_at": now.isoformat(),
        "date": now.strftime("%Y-%m-%d"),
        "month": now.strftime("%Y-%m")
    }
    
    await db.sfa_calls.insert_one(call_record)
    
    # Update lead's call history if linked
    if lead and lead.get("id"):
        await db.erp_leads.update_one(
            {"id": lead["id"]},
            {
                "$push": {
                    "call_history": {
                        "call_id": call_id,
                        "type": data.call_type,
                        "duration": data.duration_seconds,
                        "timestamp": data.call_start_time,
                        "called_by": current_user["name"]
                    }
                },
                "$set": {
                    "last_call_date": now.strftime("%Y-%m-%d"),
                    "last_contacted_by": current_user["name"]
                }
            }
        )
    
    # Log action (immutable audit trail)
    await log_action(
        user_id=current_user["id"],
        user_name=current_user["name"],
        user_role=current_user.get("role", "sales"),
        action="create",
        module="sfa_calls",
        details={
            "call_type": data.call_type,
            "phone": data.phone_number[-4:],  # Only last 4 digits for privacy
            "duration": data.duration_seconds,
            "linked_to": lead["name"] if lead else "Unknown",
            "flagged": is_flagged
        },
        record_id=call_id,
        ip_address=request.client.host if request.client else None
    )
    
    return {
        "message": "Call logged successfully",
        "call_id": call_id,
        "linked_to": lead["name"] if lead else None,
        "is_flagged": is_flagged,
        "flag_reason": call_record.get("flag_reason")
    }


@sfa_calls_router.put("/notes/{call_id}")
async def update_call_notes(
    request: Request,
    call_id: str,
    data: CallNotesUpdateRequest,
    current_user: dict = Depends(get_erp_user)
):
    """
    Add/update notes and outcome for a call
    Only notes and outcome can be updated, not the call details
    """
    db = get_db()
    
    call = await db.sfa_calls.find_one({"id": call_id}, {"_id": 0})
    if not call:
        raise HTTPException(status_code=404, detail="Call not found")
    
    # Only the call owner or managers can add notes
    if call["user_id"] != current_user["id"]:
        if current_user.get("role") not in ["super_admin", "admin", "owner", "manager", "sales_manager"]:
            raise HTTPException(status_code=403, detail="Access denied")
    
    now = datetime.now(timezone.utc)
    
    # Only allow adding notes, not editing existing ones (append mode)
    existing_notes = call.get("notes") or ""
    new_notes = f"{existing_notes}\n[{now.strftime('%Y-%m-%d %H:%M')}] {data.notes}" if existing_notes else data.notes
    
    update_data = {
        "notes": new_notes,
        "notes_updated_at": now.isoformat(),
        "notes_updated_by": current_user["name"]
    }
    
    if data.outcome:
        update_data["outcome"] = data.outcome
    
    await db.sfa_calls.update_one(
        {"id": call_id},
        {"$set": update_data}
    )
    
    # Log action
    await log_action(
        user_id=current_user["id"],
        user_name=current_user["name"],
        user_role=current_user.get("role", "sales"),
        action="update",
        module="sfa_calls",
        details={"call_id": call_id, "added_notes": True, "outcome": data.outcome},
        record_id=call_id,
        ip_address=request.client.host if request.client else None
    )
    
    return {"message": "Notes updated successfully"}


@sfa_calls_router.get("/my-calls")
async def get_my_calls(
    date: Optional[str] = None,
    call_type: Optional[str] = None,
    flagged_only: bool = False,
    current_user: dict = Depends(get_erp_user)
):
    """
    Get current user's call logs
    """
    db = get_db()
    
    query = {"user_id": current_user["id"]}
    if date:
        query["date"] = date
    if call_type:
        query["call_type"] = call_type
    if flagged_only:
        query["is_flagged"] = True
    
    calls = await db.sfa_calls.find(
        query,
        {"_id": 0}
    ).sort("call_start_time", -1).to_list(100)
    
    # Calculate stats
    total_duration = sum(c["duration_seconds"] for c in calls)
    incoming = len([c for c in calls if c["call_type"] == "incoming"])
    outgoing = len([c for c in calls if c["call_type"] == "outgoing"])
    flagged = len([c for c in calls if c["is_flagged"]])
    
    return {
        "calls": calls,
        "stats": {
            "total_calls": len(calls),
            "incoming": incoming,
            "outgoing": outgoing,
            "flagged": flagged,
            "total_duration_minutes": round(total_duration / 60, 2)
        }
    }


@sfa_calls_router.get("/team-calls")
async def get_team_calls(
    date: Optional[str] = None,
    user_id: Optional[str] = None,
    flagged_only: bool = False,
    current_user: dict = Depends(get_erp_user)
):
    """
    Get team call logs (for managers)
    """
    if current_user.get("role") not in ["super_admin", "admin", "owner", "manager", "sales_manager"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    query = {}
    if date:
        query["date"] = date
    if user_id:
        query["user_id"] = user_id
    if flagged_only:
        query["is_flagged"] = True
    
    calls = await db.sfa_calls.find(
        query,
        {"_id": 0}
    ).sort("call_start_time", -1).to_list(500)
    
    # Group by user
    user_stats = {}
    for call in calls:
        uid = call["user_id"]
        if uid not in user_stats:
            user_stats[uid] = {
                "user_id": uid,
                "user_name": call["user_name"],
                "total_calls": 0,
                "incoming": 0,
                "outgoing": 0,
                "flagged": 0,
                "total_duration": 0
            }
        
        user_stats[uid]["total_calls"] += 1
        user_stats[uid]["total_duration"] += call["duration_seconds"]
        if call["call_type"] == "incoming":
            user_stats[uid]["incoming"] += 1
        else:
            user_stats[uid]["outgoing"] += 1
        if call["is_flagged"]:
            user_stats[uid]["flagged"] += 1
    
    return {
        "calls": calls[:100],  # Return latest 100 for display
        "user_stats": list(user_stats.values()),
        "summary": {
            "total_calls": len(calls),
            "flagged_calls": len([c for c in calls if c["is_flagged"]]),
            "total_duration_minutes": round(sum(c["duration_seconds"] for c in calls) / 60, 2)
        }
    }


@sfa_calls_router.get("/flagged-calls")
async def get_flagged_calls(
    date: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """
    Get flagged calls (calls to numbers not in CRM) - for management review
    """
    if current_user.get("role") not in ["super_admin", "admin", "owner", "manager", "sales_manager"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    query = {"is_flagged": True}
    if date:
        query["date"] = date
    
    calls = await db.sfa_calls.find(
        query,
        {"_id": 0}
    ).sort("call_start_time", -1).to_list(200)
    
    return {
        "flagged_calls": calls,
        "count": len(calls)
    }


@sfa_calls_router.get("/lead-calls/{lead_id}")
async def get_lead_calls(
    lead_id: str,
    current_user: dict = Depends(get_erp_user)
):
    """
    Get all calls related to a specific lead
    """
    db = get_db()
    
    calls = await db.sfa_calls.find(
        {"lead_id": lead_id},
        {"_id": 0}
    ).sort("call_start_time", -1).to_list(50)
    
    return {
        "lead_id": lead_id,
        "calls": calls,
        "total_calls": len(calls),
        "total_duration_minutes": round(sum(c["duration_seconds"] for c in calls) / 60, 2)
    }


# ==================== REPORTS ====================

@sfa_calls_router.get("/reports/daily")
async def get_daily_call_report(
    date: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """
    Daily call report - Employee-wise breakdown
    """
    if current_user.get("role") not in ["super_admin", "admin", "owner", "manager", "sales_manager", "hr"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    target_date = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    pipeline = [
        {"$match": {"date": target_date}},
        {"$group": {
            "_id": {"user_id": "$user_id", "user_name": "$user_name"},
            "total_calls": {"$sum": 1},
            "incoming_calls": {"$sum": {"$cond": [{"$eq": ["$call_type", "incoming"]}, 1, 0]}},
            "outgoing_calls": {"$sum": {"$cond": [{"$eq": ["$call_type", "outgoing"]}, 1, 0]}},
            "flagged_calls": {"$sum": {"$cond": ["$is_flagged", 1, 0]}},
            "total_duration": {"$sum": "$duration_seconds"},
            "linked_calls": {"$sum": {"$cond": [{"$ne": ["$lead_id", None]}, 1, 0]}}
        }},
        {"$sort": {"total_calls": -1}}
    ]
    
    results = await db.sfa_calls.aggregate(pipeline).to_list(100)
    
    report = []
    for r in results:
        report.append({
            "employee_id": r["_id"]["user_id"],
            "employee_name": r["_id"]["user_name"],
            "total_calls": r["total_calls"],
            "incoming": r["incoming_calls"],
            "outgoing": r["outgoing_calls"],
            "flagged": r["flagged_calls"],
            "linked_to_crm": r["linked_calls"],
            "total_duration_minutes": round(r["total_duration"] / 60, 2)
        })
    
    return {
        "date": target_date,
        "report": report,
        "summary": {
            "total_calls": sum(r["total_calls"] for r in report),
            "total_flagged": sum(r["flagged"] for r in report),
            "total_duration_minutes": round(sum(r["total_duration_minutes"] for r in report), 2)
        }
    }


@sfa_calls_router.get("/reports/monthly")
async def get_monthly_call_report(
    month: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """
    Monthly call report
    """
    if current_user.get("role") not in ["super_admin", "admin", "owner", "manager", "sales_manager", "hr"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    target_month = month or datetime.now(timezone.utc).strftime("%Y-%m")
    
    pipeline = [
        {"$match": {"month": target_month}},
        {"$group": {
            "_id": {"user_id": "$user_id", "user_name": "$user_name"},
            "total_calls": {"$sum": 1},
            "flagged_calls": {"$sum": {"$cond": ["$is_flagged", 1, 0]}},
            "total_duration": {"$sum": "$duration_seconds"},
            "linked_calls": {"$sum": {"$cond": [{"$ne": ["$lead_id", None]}, 1, 0]}},
            "days_active": {"$addToSet": "$date"}
        }},
        {"$sort": {"total_calls": -1}}
    ]
    
    results = await db.sfa_calls.aggregate(pipeline).to_list(100)
    
    report = []
    for r in results:
        report.append({
            "employee_id": r["_id"]["user_id"],
            "employee_name": r["_id"]["user_name"],
            "total_calls": r["total_calls"],
            "flagged_calls": r["flagged_calls"],
            "linked_to_crm": r["linked_calls"],
            "crm_link_rate": round(r["linked_calls"] / max(r["total_calls"], 1) * 100, 2),
            "total_duration_hours": round(r["total_duration"] / 3600, 2),
            "days_active": len(r["days_active"]),
            "avg_calls_per_day": round(r["total_calls"] / max(len(r["days_active"]), 1), 2)
        })
    
    return {
        "month": target_month,
        "report": report
    }
