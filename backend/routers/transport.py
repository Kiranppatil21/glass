"""
Transport Management System
- Vehicle & Driver Management
- Distance & Cost Calculation
- Dispatch Assignment & Tracking
- Customer Notifications
"""
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime, timezone
import uuid
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
from haversine import haversine, Unit
import asyncio
import logging

from routers.base import get_db, get_erp_user

transport_router = APIRouter(prefix="/transport", tags=["Transport Management"])

# Factory Location - Lucumaa Glass, Pune
FACTORY_LOCATION = {
    "name": "Lucumaa Glass Factory",
    "address": "Pune, Maharashtra, India",
    "lat": 18.5204,  # Pune coordinates
    "lng": 73.8567
}

# ============ PYDANTIC MODELS ============

class TransportSettings(BaseModel):
    base_charge: float = 500  # Minimum charge
    base_km: float = 10  # KM included in base charge
    per_km_rate: float = 15  # Rate after base km
    per_sqft_rate: float = 2  # Additional per sq.ft for heavy loads
    min_sqft_for_load_charge: float = 50  # Apply load charge above this
    gst_percent: float = 18  # GST on transport
    active: bool = True
    updated_by: Optional[str] = None
    updated_at: Optional[str] = None

class Vehicle(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    vehicle_number: str
    vehicle_type: str  # tempo, truck, mini-truck
    capacity_sqft: float = 500
    driver_id: Optional[str] = None
    status: str = "available"  # available, on_trip, maintenance
    notes: Optional[str] = ""
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

class Driver(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    phone: str
    license_number: str
    vehicle_id: Optional[str] = None
    status: str = "available"  # available, on_trip, off_duty
    address: Optional[str] = ""
    emergency_contact: Optional[str] = ""
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

class DispatchCreate(BaseModel):
    order_id: str
    vehicle_id: str
    driver_id: str
    estimated_delivery_time: Optional[str] = None
    notes: Optional[str] = ""

class LocationInput(BaseModel):
    address: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    landmark: Optional[str] = ""

class TransportCostRequest(BaseModel):
    delivery_location: LocationInput
    total_sqft: float
    include_gst: bool = True

# ============ TRANSPORT SETTINGS ============

@transport_router.get("/settings")
async def get_transport_settings():
    """Get transport pricing settings"""
    db = get_db()
    settings = await db.transport_settings.find_one({}, {"_id": 0})
    if not settings:
        settings = TransportSettings().model_dump()
    settings["factory_location"] = FACTORY_LOCATION
    return settings

@transport_router.put("/settings")
async def update_transport_settings(
    settings: TransportSettings,
    current_user: dict = Depends(get_erp_user)
):
    """Update transport pricing settings (Admin only)"""
    if current_user.get("role") not in ["super_admin", "admin", "owner"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    db = get_db()
    doc = settings.model_dump()
    doc["updated_by"] = current_user.get("name", "")
    doc["updated_at"] = datetime.now(timezone.utc).isoformat()
    
    await db.transport_settings.update_one({}, {"$set": doc}, upsert=True)
    return {"message": "Transport settings updated", "settings": doc}

# ============ DISTANCE & COST CALCULATION ============

@transport_router.post("/calculate-distance")
async def calculate_distance(location: LocationInput):
    """Calculate distance from factory to delivery location"""
    try:
        # Get delivery coordinates
        if location.lat and location.lng:
            delivery_coords = (location.lat, location.lng)
        elif location.address:
            # Geocode address using Nominatim (OpenStreetMap)
            geolocator = Nominatim(user_agent="lucumaa_glass_erp")
            search_query = location.address
            if location.landmark:
                search_query += f", near {location.landmark}"
            
            geo_location = geolocator.geocode(search_query, timeout=10)
            if not geo_location:
                raise HTTPException(status_code=400, detail="Could not find location. Please provide more details or use map pin.")
            delivery_coords = (geo_location.latitude, geo_location.longitude)
        else:
            raise HTTPException(status_code=400, detail="Please provide address or coordinates")
        
        # Factory coordinates
        factory_coords = (FACTORY_LOCATION["lat"], FACTORY_LOCATION["lng"])
        
        # Calculate distance using haversine formula
        distance_km = haversine(factory_coords, delivery_coords, unit=Unit.KILOMETERS)
        
        return {
            "factory": FACTORY_LOCATION,
            "delivery": {
                "lat": delivery_coords[0],
                "lng": delivery_coords[1],
                "address": location.address or "Pin location"
            },
            "distance_km": round(distance_km, 2),
            "distance_display": f"{round(distance_km, 1)} km"
        }
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Distance calculation error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Distance calculation failed: {str(e)}")

@transport_router.post("/calculate-cost")
async def calculate_transport_cost(request: TransportCostRequest):
    """Calculate transport cost based on distance and load"""
    db = get_db()
    
    # Get settings
    settings = await db.transport_settings.find_one({}, {"_id": 0})
    if not settings:
        settings = TransportSettings().model_dump()
    
    # Calculate distance
    distance_result = await calculate_distance(request.delivery_location)
    distance_km = distance_result["distance_km"]
    
    # Base calculation
    base_charge = settings.get("base_charge", 500)
    base_km = settings.get("base_km", 10)
    per_km_rate = settings.get("per_km_rate", 15)
    per_sqft_rate = settings.get("per_sqft_rate", 2)
    min_sqft_for_load = settings.get("min_sqft_for_load_charge", 50)
    gst_percent = settings.get("gst_percent", 18)
    
    # Distance charge
    if distance_km <= base_km:
        distance_charge = base_charge
    else:
        extra_km = distance_km - base_km
        distance_charge = base_charge + (extra_km * per_km_rate)
    
    # Load charge (for heavy orders)
    load_charge = 0
    if request.total_sqft > min_sqft_for_load:
        load_charge = (request.total_sqft - min_sqft_for_load) * per_sqft_rate
    
    # Subtotal
    subtotal = round(distance_charge + load_charge, 2)
    
    # GST
    gst_amount = 0
    if request.include_gst:
        gst_amount = round(subtotal * gst_percent / 100, 2)
    
    total = round(subtotal + gst_amount, 2)
    
    return {
        "distance_km": distance_km,
        "breakdown": {
            "base_charge": base_charge,
            "base_km_included": base_km,
            "extra_km": max(0, round(distance_km - base_km, 2)),
            "per_km_rate": per_km_rate,
            "distance_charge": round(distance_charge, 2),
            "load_sqft": request.total_sqft,
            "load_charge": round(load_charge, 2),
            "subtotal": subtotal,
            "gst_percent": gst_percent if request.include_gst else 0,
            "gst_amount": gst_amount,
            "total": total
        },
        "total_transport_cost": total,
        "delivery_location": distance_result["delivery"]
    }

# ============ VEHICLE MANAGEMENT ============

@transport_router.get("/vehicles")
async def get_vehicles(status: Optional[str] = None):
    """Get all vehicles"""
    db = get_db()
    query = {}
    if status:
        query["status"] = status
    vehicles = await db.vehicles.find(query, {"_id": 0}).sort("vehicle_number", 1).to_list(100)
    
    # Attach driver info
    for vehicle in vehicles:
        if vehicle.get("driver_id"):
            driver = await db.drivers.find_one({"id": vehicle["driver_id"]}, {"_id": 0, "name": 1, "phone": 1})
            vehicle["driver"] = driver
    
    return {"vehicles": vehicles, "count": len(vehicles)}

@transport_router.post("/vehicles")
async def create_vehicle(vehicle: Vehicle, current_user: dict = Depends(get_erp_user)):
    """Add new vehicle"""
    if current_user.get("role") not in ["super_admin", "admin", "owner", "manager"]:
        raise HTTPException(status_code=403, detail="Permission denied")
    
    db = get_db()
    
    # Check duplicate vehicle number
    existing = await db.vehicles.find_one({"vehicle_number": vehicle.vehicle_number.upper()})
    if existing:
        raise HTTPException(status_code=400, detail="Vehicle number already exists")
    
    doc = vehicle.model_dump()
    doc["vehicle_number"] = doc["vehicle_number"].upper()
    doc["created_by"] = current_user.get("name", "")
    
    await db.vehicles.insert_one(doc)
    return {"message": "Vehicle added", "vehicle": {k: v for k, v in doc.items() if k != "_id"}}

@transport_router.put("/vehicles/{vehicle_id}")
async def update_vehicle(vehicle_id: str, vehicle: Vehicle, current_user: dict = Depends(get_erp_user)):
    """Update vehicle"""
    if current_user.get("role") not in ["super_admin", "admin", "owner", "manager"]:
        raise HTTPException(status_code=403, detail="Permission denied")
    
    db = get_db()
    doc = vehicle.model_dump()
    doc["vehicle_number"] = doc["vehicle_number"].upper()
    doc["updated_at"] = datetime.now(timezone.utc).isoformat()
    
    result = await db.vehicles.update_one({"id": vehicle_id}, {"$set": doc})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Vehicle not found")
    
    return {"message": "Vehicle updated"}

@transport_router.delete("/vehicles/{vehicle_id}")
async def delete_vehicle(vehicle_id: str, current_user: dict = Depends(get_erp_user)):
    """Delete vehicle"""
    if current_user.get("role") not in ["super_admin", "admin"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    db = get_db()
    await db.vehicles.delete_one({"id": vehicle_id})
    return {"message": "Vehicle deleted"}

# ============ DRIVER MANAGEMENT ============

@transport_router.get("/drivers")
async def get_drivers(status: Optional[str] = None):
    """Get all drivers"""
    db = get_db()
    query = {}
    if status:
        query["status"] = status
    drivers = await db.drivers.find(query, {"_id": 0}).sort("name", 1).to_list(100)
    
    # Attach vehicle info
    for driver in drivers:
        if driver.get("vehicle_id"):
            vehicle = await db.vehicles.find_one({"id": driver["vehicle_id"]}, {"_id": 0, "vehicle_number": 1, "vehicle_type": 1})
            driver["vehicle"] = vehicle
    
    return {"drivers": drivers, "count": len(drivers)}

@transport_router.post("/drivers")
async def create_driver(driver: Driver, current_user: dict = Depends(get_erp_user)):
    """Add new driver"""
    if current_user.get("role") not in ["super_admin", "admin", "owner", "manager"]:
        raise HTTPException(status_code=403, detail="Permission denied")
    
    db = get_db()
    
    # Check duplicate phone
    existing = await db.drivers.find_one({"phone": driver.phone})
    if existing:
        raise HTTPException(status_code=400, detail="Driver with this phone already exists")
    
    doc = driver.model_dump()
    doc["created_by"] = current_user.get("name", "")
    
    await db.drivers.insert_one(doc)
    return {"message": "Driver added", "driver": {k: v for k, v in doc.items() if k != "_id"}}

@transport_router.put("/drivers/{driver_id}")
async def update_driver(driver_id: str, driver: Driver, current_user: dict = Depends(get_erp_user)):
    """Update driver"""
    if current_user.get("role") not in ["super_admin", "admin", "owner", "manager"]:
        raise HTTPException(status_code=403, detail="Permission denied")
    
    db = get_db()
    doc = driver.model_dump()
    doc["updated_at"] = datetime.now(timezone.utc).isoformat()
    
    result = await db.drivers.update_one({"id": driver_id}, {"$set": doc})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Driver not found")
    
    return {"message": "Driver updated"}

@transport_router.delete("/drivers/{driver_id}")
async def delete_driver(driver_id: str, current_user: dict = Depends(get_erp_user)):
    """Delete driver"""
    if current_user.get("role") not in ["super_admin", "admin"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    db = get_db()
    await db.drivers.delete_one({"id": driver_id})
    return {"message": "Driver deleted"}

# ============ DISPATCH MANAGEMENT ============

@transport_router.post("/dispatch")
async def create_dispatch(
    dispatch: DispatchCreate,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_erp_user)
):
    """Create dispatch assignment and notify customer"""
    if current_user.get("role") not in ["super_admin", "admin", "owner", "manager", "supervisor"]:
        raise HTTPException(status_code=403, detail="Permission denied")
    
    db = get_db()
    
    # Get order details
    order = await db.orders.find_one({"id": dispatch.order_id}, {"_id": 0})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    # Check payment is fully settled before allowing dispatch
    payment_settled = (
        order.get('payment_status') == 'completed' or
        (order.get('advance_percent') == 100 and order.get('advance_payment_status') == 'paid') or
        (order.get('advance_payment_status') == 'paid' and order.get('remaining_payment_status') in ['paid', 'cash_received'])
    )
    if not payment_settled:
        raise HTTPException(status_code=400, detail="Cannot dispatch. Payment not fully settled. Please complete payment first.")
    
    # Get vehicle details
    vehicle = await db.vehicles.find_one({"id": dispatch.vehicle_id}, {"_id": 0})
    if not vehicle:
        raise HTTPException(status_code=404, detail="Vehicle not found")
    
    # Get driver details
    driver = await db.drivers.find_one({"id": dispatch.driver_id}, {"_id": 0})
    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")
    
    # Create dispatch record
    dispatch_record = {
        "id": str(uuid.uuid4()),
        "order_id": dispatch.order_id,
        "order_number": order.get("order_number", ""),
        "vehicle_id": dispatch.vehicle_id,
        "vehicle_number": vehicle.get("vehicle_number", ""),
        "vehicle_type": vehicle.get("vehicle_type", ""),
        "driver_id": dispatch.driver_id,
        "driver_name": driver.get("name", ""),
        "driver_phone": driver.get("phone", ""),
        "customer_name": order.get("customer_name", ""),
        "customer_phone": order.get("customer_phone", ""),
        "customer_email": order.get("customer_email", ""),
        "delivery_address": order.get("delivery_address", ""),
        "estimated_delivery_time": dispatch.estimated_delivery_time,
        "notes": dispatch.notes,
        "dispatched_by": current_user.get("name", ""),
        "dispatched_at": datetime.now(timezone.utc).isoformat(),
        "status": "dispatched"
    }
    
    await db.dispatches.insert_one(dispatch_record)
    
    # Update order status
    await db.orders.update_one(
        {"id": dispatch.order_id},
        {"$set": {
            "status": "dispatched",
            "dispatch_id": dispatch_record["id"],
            "dispatched_at": dispatch_record["dispatched_at"],
            "vehicle_number": vehicle.get("vehicle_number", ""),
            "driver_name": driver.get("name", ""),
            "driver_phone": driver.get("phone", "")
        }}
    )
    
    # Update vehicle and driver status
    await db.vehicles.update_one({"id": dispatch.vehicle_id}, {"$set": {"status": "on_trip"}})
    await db.drivers.update_one({"id": dispatch.driver_id}, {"$set": {"status": "on_trip"}})
    
    # Get customer details for notification
    user = await db.users.find_one({"id": order.get("user_id")}, {"_id": 0, "email": 1, "phone": 1})
    
    # Send notifications in background
    if user:
        background_tasks.add_task(
            send_dispatch_notification,
            dispatch_record,
            user.get("email"),
            user.get("phone")
        )
    
    return {
        "message": "Order dispatched successfully",
        "dispatch": {k: v for k, v in dispatch_record.items() if k != "_id"}
    }

@transport_router.get("/dispatches")
async def get_dispatches(
    status: Optional[str] = None,
    date: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get dispatch records"""
    db = get_db()
    query = {}
    if status:
        query["status"] = status
    if date:
        query["dispatched_at"] = {"$regex": f"^{date}"}
    
    dispatches = await db.dispatches.find(query, {"_id": 0}).sort("dispatched_at", -1).to_list(200)
    return {"dispatches": dispatches, "count": len(dispatches)}

@transport_router.patch("/dispatches/{dispatch_id}/status")
async def update_dispatch_status(
    dispatch_id: str,
    status: str,
    current_user: dict = Depends(get_erp_user)
):
    """Update dispatch status (delivered, returned, etc.)"""
    db = get_db()
    
    valid_statuses = ["dispatched", "in_transit", "delivered", "returned", "cancelled"]
    if status not in valid_statuses:
        raise HTTPException(status_code=400, detail=f"Invalid status. Must be one of: {valid_statuses}")
    
    dispatch = await db.dispatches.find_one({"id": dispatch_id}, {"_id": 0})
    if not dispatch:
        raise HTTPException(status_code=404, detail="Dispatch not found")
    
    update_data = {
        "status": status,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "updated_by": current_user.get("name", "")
    }
    
    if status == "delivered":
        update_data["delivered_at"] = datetime.now(timezone.utc).isoformat()
        # Free up vehicle and driver
        await db.vehicles.update_one({"id": dispatch["vehicle_id"]}, {"$set": {"status": "available"}})
        await db.drivers.update_one({"id": dispatch["driver_id"]}, {"$set": {"status": "available"}})
        # Update order status
        await db.orders.update_one({"id": dispatch["order_id"]}, {"$set": {"status": "delivered", "delivered_at": update_data["delivered_at"]}})
    
    await db.dispatches.update_one({"id": dispatch_id}, {"$set": update_data})
    
    return {"message": f"Dispatch status updated to {status}"}

@transport_router.get("/dashboard")
async def get_transport_dashboard(current_user: dict = Depends(get_erp_user)):
    """Get transport dashboard stats"""
    db = get_db()
    
    # Vehicle stats
    total_vehicles = await db.vehicles.count_documents({})
    available_vehicles = await db.vehicles.count_documents({"status": "available"})
    on_trip_vehicles = await db.vehicles.count_documents({"status": "on_trip"})
    
    # Driver stats
    total_drivers = await db.drivers.count_documents({})
    available_drivers = await db.drivers.count_documents({"status": "available"})
    on_trip_drivers = await db.drivers.count_documents({"status": "on_trip"})
    
    # Today's dispatches
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    today_dispatches = await db.dispatches.count_documents({"dispatched_at": {"$regex": f"^{today}"}})
    pending_delivery = await db.dispatches.count_documents({"status": {"$in": ["dispatched", "in_transit"]}})
    
    # Recent dispatches
    recent = await db.dispatches.find({}, {"_id": 0}).sort("dispatched_at", -1).limit(5).to_list(5)
    
    return {
        "vehicles": {
            "total": total_vehicles,
            "available": available_vehicles,
            "on_trip": on_trip_vehicles
        },
        "drivers": {
            "total": total_drivers,
            "available": available_drivers,
            "on_trip": on_trip_drivers
        },
        "dispatches": {
            "today": today_dispatches,
            "pending_delivery": pending_delivery
        },
        "recent_dispatches": recent
    }

# ============ NOTIFICATION HELPERS ============

async def send_dispatch_notification(dispatch: dict, email: str, phone: str):
    """Send dispatch notification via WhatsApp and Email"""
    try:
        from routers.notifications import send_whatsapp_message, send_email_notification
        
        message = f"""🚚 *Order Dispatched!*

Your order #{dispatch.get('order_number')} has been dispatched.

*Driver Details:*
👤 Name: {dispatch.get('driver_name')}
📞 Phone: {dispatch.get('driver_phone')}

*Vehicle Details:*
🚛 Number: {dispatch.get('vehicle_number')}
📦 Type: {dispatch.get('vehicle_type')}

*Delivery Address:*
{dispatch.get('delivery_address')}

{f"⏰ Estimated Delivery: {dispatch.get('estimated_delivery_time')}" if dispatch.get('estimated_delivery_time') else ""}

Thank you for choosing Lucumaa Glass!
"""
        
        # Send WhatsApp
        if phone:
            await send_whatsapp_message(phone, message)
        
        # Send Email
        if email:
            await send_email_notification(
                email,
                f"Order #{dispatch.get('order_number')} Dispatched - Lucumaa Glass",
                message.replace('*', '').replace('🚚', '').replace('👤', '').replace('📞', '').replace('🚛', '').replace('📦', '').replace('⏰', '')
            )
        
        logging.info(f"Dispatch notification sent for order {dispatch.get('order_number')}")
    except Exception as e:
        logging.error(f"Failed to send dispatch notification: {str(e)}")
