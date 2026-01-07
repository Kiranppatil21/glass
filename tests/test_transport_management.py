"""
Transport Management System Tests
- Transport Settings API
- Distance & Cost Calculation
- Vehicles CRUD
- Drivers CRUD
- Transport Dashboard
"""
import pytest
import requests
import os

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', 'https://glassmesh.preview.emergentagent.com').rstrip('/')

# Test credentials
ADMIN_EMAIL = "admin@lucumaa.in"
ADMIN_PASSWORD = "adminpass"
CUSTOMER_EMAIL = "test@lucumaa.com"
CUSTOMER_PASSWORD = "test123"


class TestTransportSettings:
    """Transport Settings API Tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup - get admin token"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        
        # Login as admin
        response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        if response.status_code == 200:
            token = response.json().get("token")
            self.session.headers.update({"Authorization": f"Bearer {token}"})
            self.admin_token = token
        else:
            pytest.skip("Admin login failed - skipping transport tests")
    
    def test_get_transport_settings(self):
        """GET /api/erp/transport/settings - Get transport pricing settings"""
        response = self.session.get(f"{BASE_URL}/api/erp/transport/settings")
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        # Verify settings structure
        assert "base_charge" in data, "Missing base_charge in settings"
        assert "base_km" in data, "Missing base_km in settings"
        assert "per_km_rate" in data, "Missing per_km_rate in settings"
        assert "per_sqft_rate" in data, "Missing per_sqft_rate in settings"
        assert "gst_percent" in data, "Missing gst_percent in settings"
        assert "factory_location" in data, "Missing factory_location in settings"
        
        # Verify factory location
        factory = data["factory_location"]
        assert factory["lat"] == 18.5204, "Factory latitude should be Pune coordinates"
        assert factory["lng"] == 73.8567, "Factory longitude should be Pune coordinates"
        
        print(f"✓ Transport settings retrieved: Base ₹{data['base_charge']} for {data['base_km']}km, ₹{data['per_km_rate']}/km after")
    
    def test_update_transport_settings(self):
        """PUT /api/erp/transport/settings - Update transport pricing settings"""
        new_settings = {
            "base_charge": 500,
            "base_km": 10,
            "per_km_rate": 15,
            "per_sqft_rate": 2,
            "min_sqft_for_load_charge": 50,
            "gst_percent": 18,
            "active": True
        }
        
        response = self.session.put(f"{BASE_URL}/api/erp/transport/settings", json=new_settings)
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        assert "message" in data, "Missing message in response"
        assert "settings" in data, "Missing settings in response"
        
        # Verify settings were updated
        settings = data["settings"]
        assert settings["base_charge"] == 500
        assert settings["per_km_rate"] == 15
        
        print(f"✓ Transport settings updated successfully")


class TestDistanceAndCostCalculation:
    """Distance & Cost Calculation API Tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup - get customer token"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        
        # Login as customer (no auth needed for calculate-cost)
        response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": CUSTOMER_EMAIL,
            "password": CUSTOMER_PASSWORD
        })
        if response.status_code == 200:
            token = response.json().get("token")
            self.session.headers.update({"Authorization": f"Bearer {token}"})
    
    def test_calculate_distance_with_address(self):
        """POST /api/erp/transport/calculate-distance - Calculate distance from address"""
        location = {
            "address": "Mumbai, Maharashtra, India",
            "landmark": ""
        }
        
        response = self.session.post(f"{BASE_URL}/api/erp/transport/calculate-distance", json=location)
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        assert "factory" in data, "Missing factory in response"
        assert "delivery" in data, "Missing delivery in response"
        assert "distance_km" in data, "Missing distance_km in response"
        
        # Pune to Mumbai should be approximately 120-150 km
        distance = data["distance_km"]
        assert 100 < distance < 200, f"Pune to Mumbai distance should be ~120km, got {distance}km"
        
        print(f"✓ Distance calculated: Pune to Mumbai = {distance} km")
    
    def test_calculate_distance_with_coordinates(self):
        """POST /api/erp/transport/calculate-distance - Calculate distance from GPS coordinates"""
        # Mumbai coordinates
        location = {
            "lat": 19.0760,
            "lng": 72.8777
        }
        
        response = self.session.post(f"{BASE_URL}/api/erp/transport/calculate-distance", json=location)
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        assert "distance_km" in data
        
        print(f"✓ Distance from GPS: {data['distance_km']} km")
    
    def test_calculate_transport_cost_mumbai(self):
        """POST /api/erp/transport/calculate-cost - Calculate transport cost to Mumbai"""
        request_data = {
            "delivery_location": {
                "address": "Mumbai, Maharashtra, India"
            },
            "total_sqft": 100,
            "include_gst": True
        }
        
        response = self.session.post(f"{BASE_URL}/api/erp/transport/calculate-cost", json=request_data)
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        assert "distance_km" in data, "Missing distance_km"
        assert "breakdown" in data, "Missing breakdown"
        assert "total_transport_cost" in data, "Missing total_transport_cost"
        
        breakdown = data["breakdown"]
        assert "base_charge" in breakdown
        assert "distance_charge" in breakdown
        assert "load_charge" in breakdown
        assert "gst_amount" in breakdown
        assert "total" in breakdown
        
        # Verify calculation logic
        # For 100 sqft (above 50 sqft threshold), load charge should apply
        assert breakdown["load_sqft"] == 100
        
        print(f"✓ Transport cost to Mumbai: ₹{data['total_transport_cost']} (Distance: {data['distance_km']}km)")
        print(f"  Breakdown: Base ₹{breakdown['base_charge']}, Distance ₹{breakdown['distance_charge']}, Load ₹{breakdown['load_charge']}, GST ₹{breakdown['gst_amount']}")
    
    def test_calculate_transport_cost_delhi(self):
        """POST /api/erp/transport/calculate-cost - Calculate transport cost to Delhi"""
        request_data = {
            "delivery_location": {
                "address": "New Delhi, India"
            },
            "total_sqft": 200,
            "include_gst": True
        }
        
        response = self.session.post(f"{BASE_URL}/api/erp/transport/calculate-cost", json=request_data)
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        # Delhi is ~1400km from Pune
        assert data["distance_km"] > 1000, f"Delhi should be >1000km from Pune, got {data['distance_km']}km"
        
        print(f"✓ Transport cost to Delhi: ₹{data['total_transport_cost']} (Distance: {data['distance_km']}km)")
    
    def test_calculate_transport_cost_without_gst(self):
        """POST /api/erp/transport/calculate-cost - Calculate without GST"""
        request_data = {
            "delivery_location": {
                "address": "Nashik, Maharashtra, India"
            },
            "total_sqft": 50,
            "include_gst": False
        }
        
        response = self.session.post(f"{BASE_URL}/api/erp/transport/calculate-cost", json=request_data)
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        breakdown = data["breakdown"]
        
        # GST should be 0 when include_gst is False
        assert breakdown["gst_amount"] == 0, "GST should be 0 when include_gst is False"
        assert breakdown["gst_percent"] == 0, "GST percent should be 0"
        
        print(f"✓ Transport cost without GST: ₹{data['total_transport_cost']}")
    
    def test_calculate_transport_cost_invalid_address(self):
        """POST /api/erp/transport/calculate-cost - Invalid address should return error"""
        request_data = {
            "delivery_location": {
                "address": "xyznonexistentplace12345"
            },
            "total_sqft": 50,
            "include_gst": True
        }
        
        response = self.session.post(f"{BASE_URL}/api/erp/transport/calculate-cost", json=request_data)
        
        # Should return 400 for invalid location
        assert response.status_code == 400, f"Expected 400 for invalid address, got {response.status_code}"
        
        print(f"✓ Invalid address correctly returns error")


class TestVehicleManagement:
    """Vehicle CRUD API Tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup - get admin token"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        
        # Login as admin
        response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        if response.status_code == 200:
            token = response.json().get("token")
            self.session.headers.update({"Authorization": f"Bearer {token}"})
        else:
            pytest.skip("Admin login failed")
    
    def test_get_vehicles(self):
        """GET /api/erp/transport/vehicles - Get all vehicles"""
        response = self.session.get(f"{BASE_URL}/api/erp/transport/vehicles")
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        assert "vehicles" in data, "Missing vehicles in response"
        assert "count" in data, "Missing count in response"
        
        print(f"✓ Retrieved {data['count']} vehicles")
    
    def test_create_vehicle(self):
        """POST /api/erp/transport/vehicles - Create new vehicle"""
        vehicle_data = {
            "vehicle_number": "TEST_MH12XY9999",
            "vehicle_type": "tempo",
            "capacity_sqft": 500,
            "status": "available",
            "notes": "Test vehicle for automated testing"
        }
        
        response = self.session.post(f"{BASE_URL}/api/erp/transport/vehicles", json=vehicle_data)
        
        # Could be 200 or 400 if vehicle already exists
        if response.status_code == 400 and "already exists" in response.text:
            print(f"✓ Vehicle already exists (expected for repeat tests)")
            return
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        assert "message" in data
        assert "vehicle" in data
        assert data["vehicle"]["vehicle_number"] == "TEST_MH12XY9999"
        
        print(f"✓ Vehicle created: {data['vehicle']['vehicle_number']}")
    
    def test_get_vehicles_by_status(self):
        """GET /api/erp/transport/vehicles?status=available - Filter by status"""
        response = self.session.get(f"{BASE_URL}/api/erp/transport/vehicles", params={"status": "available"})
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        # All returned vehicles should have status=available
        for vehicle in data["vehicles"]:
            assert vehicle["status"] == "available", f"Vehicle {vehicle['vehicle_number']} has status {vehicle['status']}"
        
        print(f"✓ Retrieved {data['count']} available vehicles")


class TestDriverManagement:
    """Driver CRUD API Tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup - get admin token"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        
        # Login as admin
        response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        if response.status_code == 200:
            token = response.json().get("token")
            self.session.headers.update({"Authorization": f"Bearer {token}"})
        else:
            pytest.skip("Admin login failed")
    
    def test_get_drivers(self):
        """GET /api/erp/transport/drivers - Get all drivers"""
        response = self.session.get(f"{BASE_URL}/api/erp/transport/drivers")
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        assert "drivers" in data, "Missing drivers in response"
        assert "count" in data, "Missing count in response"
        
        print(f"✓ Retrieved {data['count']} drivers")
    
    def test_create_driver(self):
        """POST /api/erp/transport/drivers - Create new driver"""
        driver_data = {
            "name": "TEST_Driver Automated",
            "phone": "9999999999",
            "license_number": "TEST_MH1234567890",
            "status": "available",
            "address": "Test Address, Pune",
            "emergency_contact": "8888888888"
        }
        
        response = self.session.post(f"{BASE_URL}/api/erp/transport/drivers", json=driver_data)
        
        # Could be 200 or 400 if driver already exists
        if response.status_code == 400 and "already exists" in response.text:
            print(f"✓ Driver already exists (expected for repeat tests)")
            return
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        assert "message" in data
        assert "driver" in data
        assert data["driver"]["name"] == "TEST_Driver Automated"
        
        print(f"✓ Driver created: {data['driver']['name']}")
    
    def test_get_drivers_by_status(self):
        """GET /api/erp/transport/drivers?status=available - Filter by status"""
        response = self.session.get(f"{BASE_URL}/api/erp/transport/drivers", params={"status": "available"})
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        # All returned drivers should have status=available
        for driver in data["drivers"]:
            assert driver["status"] == "available", f"Driver {driver['name']} has status {driver['status']}"
        
        print(f"✓ Retrieved {data['count']} available drivers")


class TestTransportDashboard:
    """Transport Dashboard API Tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup - get admin token"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        
        # Login as admin
        response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        if response.status_code == 200:
            token = response.json().get("token")
            self.session.headers.update({"Authorization": f"Bearer {token}"})
        else:
            pytest.skip("Admin login failed")
    
    def test_get_transport_dashboard(self):
        """GET /api/erp/transport/dashboard - Get transport dashboard stats"""
        response = self.session.get(f"{BASE_URL}/api/erp/transport/dashboard")
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        
        # Verify dashboard structure
        assert "vehicles" in data, "Missing vehicles stats"
        assert "drivers" in data, "Missing drivers stats"
        assert "dispatches" in data, "Missing dispatches stats"
        assert "recent_dispatches" in data, "Missing recent_dispatches"
        
        # Verify vehicles stats structure
        vehicles = data["vehicles"]
        assert "total" in vehicles
        assert "available" in vehicles
        assert "on_trip" in vehicles
        
        # Verify drivers stats structure
        drivers = data["drivers"]
        assert "total" in drivers
        assert "available" in drivers
        assert "on_trip" in drivers
        
        # Verify dispatches stats structure
        dispatches = data["dispatches"]
        assert "today" in dispatches
        assert "pending_delivery" in dispatches
        
        print(f"✓ Dashboard stats: {vehicles['total']} vehicles, {drivers['total']} drivers, {dispatches['today']} dispatches today")


class TestDispatchManagement:
    """Dispatch API Tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup - get admin token"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        
        # Login as admin
        response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        if response.status_code == 200:
            token = response.json().get("token")
            self.session.headers.update({"Authorization": f"Bearer {token}"})
        else:
            pytest.skip("Admin login failed")
    
    def test_get_dispatches(self):
        """GET /api/erp/transport/dispatches - Get all dispatches"""
        response = self.session.get(f"{BASE_URL}/api/erp/transport/dispatches")
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        assert "dispatches" in data, "Missing dispatches in response"
        assert "count" in data, "Missing count in response"
        
        print(f"✓ Retrieved {data['count']} dispatches")


# Cleanup test data
class TestCleanup:
    """Cleanup test data"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup - get admin token"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        
        # Login as admin
        response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        if response.status_code == 200:
            token = response.json().get("token")
            self.session.headers.update({"Authorization": f"Bearer {token}"})
    
    def test_cleanup_test_vehicles(self):
        """Delete test vehicles"""
        # Get all vehicles
        response = self.session.get(f"{BASE_URL}/api/erp/transport/vehicles")
        if response.status_code == 200:
            vehicles = response.json().get("vehicles", [])
            for v in vehicles:
                if v.get("vehicle_number", "").startswith("TEST_"):
                    self.session.delete(f"{BASE_URL}/api/erp/transport/vehicles/{v['id']}")
                    print(f"  Deleted test vehicle: {v['vehicle_number']}")
        print("✓ Test vehicles cleanup complete")
    
    def test_cleanup_test_drivers(self):
        """Delete test drivers"""
        # Get all drivers
        response = self.session.get(f"{BASE_URL}/api/erp/transport/drivers")
        if response.status_code == 200:
            drivers = response.json().get("drivers", [])
            for d in drivers:
                if d.get("name", "").startswith("TEST_") or d.get("license_number", "").startswith("TEST_"):
                    self.session.delete(f"{BASE_URL}/api/erp/transport/drivers/{d['id']}")
                    print(f"  Deleted test driver: {d['name']}")
        print("✓ Test drivers cleanup complete")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
