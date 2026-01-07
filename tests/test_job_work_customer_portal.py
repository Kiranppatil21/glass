"""
Test Job Work Module and Customer Portal Enhanced
- Job Work API: Labour rates, calculation, order creation
- Customer Portal: Order summary, payment options
"""
import pytest
import requests
import os

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', '').rstrip('/')

# Test credentials
CUSTOMER_EMAIL = "test@lucumaa.com"
CUSTOMER_PASSWORD = "test123"
ADMIN_EMAIL = "admin@lucumaa.in"
ADMIN_PASSWORD = "adminpass"


class TestJobWorkPublicAPIs:
    """Test public Job Work APIs (no auth required)"""
    
    def test_get_labour_rates(self):
        """Test GET /api/erp/job-work/labour-rates - returns all rates per thickness"""
        response = requests.get(f"{BASE_URL}/api/erp/job-work/labour-rates")
        assert response.status_code == 200
        
        data = response.json()
        assert "labour_rates" in data
        rates = data["labour_rates"]
        
        # Verify all expected thickness rates
        expected_rates = {
            "4": 8, "5": 10, "6": 12, "8": 15,
            "10": 18, "12": 22, "15": 28, "19": 35
        }
        for mm, rate in expected_rates.items():
            assert str(mm) in rates or mm in rates, f"Missing rate for {mm}mm"
            actual_rate = rates.get(str(mm)) or rates.get(mm)
            assert actual_rate == rate, f"Rate for {mm}mm should be {rate}, got {actual_rate}"
    
    def test_calculate_job_work_cost_single_item(self):
        """Test POST /api/erp/job-work/calculate - single item"""
        payload = [
            {"thickness_mm": 6, "width_inch": 24, "height_inch": 36, "quantity": 2, "notes": ""}
        ]
        response = requests.post(
            f"{BASE_URL}/api/erp/job-work/calculate",
            json=payload
        )
        assert response.status_code == 200
        
        data = response.json()
        assert "items" in data
        assert "summary" in data
        
        # Verify item calculation
        item = data["items"][0]
        assert item["thickness_mm"] == 6
        assert item["quantity"] == 2
        # 24 x 36 / 144 = 6 sqft per piece, 2 pieces = 12 sqft
        assert item["sqft_per_piece"] == 6.0
        assert item["total_sqft"] == 12.0
        # 6mm rate is ₹12/sqft, so 12 sqft = ₹144
        assert item["labour_rate"] == 12
        assert item["labour_cost"] == 144.0
        
        # Verify summary
        summary = data["summary"]
        assert summary["total_sqft"] == 12.0
        assert summary["total_pieces"] == 2
        assert summary["labour_charges"] == 144.0
        assert summary["gst_rate"] == 18.0
        # GST = 144 * 18% = 25.92
        assert summary["gst_amount"] == 25.92
        # Grand total = 144 + 25.92 = 169.92
        assert summary["grand_total"] == 169.92
    
    def test_calculate_job_work_cost_multiple_items(self):
        """Test POST /api/erp/job-work/calculate - multiple items with different thickness"""
        payload = [
            {"thickness_mm": 4, "width_inch": 12, "height_inch": 12, "quantity": 10, "notes": "4mm glass"},
            {"thickness_mm": 10, "width_inch": 48, "height_inch": 72, "quantity": 1, "notes": "10mm glass"}
        ]
        response = requests.post(
            f"{BASE_URL}/api/erp/job-work/calculate",
            json=payload
        )
        assert response.status_code == 200
        
        data = response.json()
        assert len(data["items"]) == 2
        
        # Item 1: 4mm, 12x12 = 1 sqft per piece, 10 pieces = 10 sqft, rate ₹8 = ₹80
        item1 = data["items"][0]
        assert item1["labour_rate"] == 8
        assert item1["total_sqft"] == 10.0
        assert item1["labour_cost"] == 80.0
        
        # Item 2: 10mm, 48x72 = 24 sqft per piece, 1 piece = 24 sqft, rate ₹18 = ₹432
        item2 = data["items"][1]
        assert item2["labour_rate"] == 18
        assert item2["total_sqft"] == 24.0
        assert item2["labour_cost"] == 432.0
        
        # Summary: 10 + 24 = 34 sqft, ₹80 + ₹432 = ₹512 labour
        summary = data["summary"]
        assert summary["total_sqft"] == 34.0
        assert summary["total_pieces"] == 11
        assert summary["labour_charges"] == 512.0
        # GST = 512 * 18% = 92.16
        assert summary["gst_amount"] == 92.16
        # Grand total = 512 + 92.16 = 604.16
        assert summary["grand_total"] == 604.16
    
    def test_get_disclaimer(self):
        """Test GET /api/erp/job-work/disclaimer - returns disclaimer text"""
        response = requests.get(f"{BASE_URL}/api/erp/job-work/disclaimer")
        assert response.status_code == 200
        
        data = response.json()
        assert "disclaimer" in data
        assert "summary_points" in data
        
        # Verify key disclaimer points
        disclaimer = data["disclaimer"]
        assert "NOT responsible" in disclaimer
        assert "breakage" in disclaimer.lower()
        assert "NO compensation" in disclaimer
        
        # Verify summary points
        points = data["summary_points"]
        assert len(points) >= 3
        assert any("NOT responsible" in p for p in points)


class TestJobWorkAuthenticatedAPIs:
    """Test authenticated Job Work APIs"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Login and get token"""
        response = requests.post(
            f"{BASE_URL}/api/auth/login",
            json={"email": CUSTOMER_EMAIL, "password": CUSTOMER_PASSWORD}
        )
        if response.status_code != 200:
            pytest.skip("Customer login failed")
        self.token = response.json()["token"]
        self.headers = {"Authorization": f"Bearer {self.token}"}
    
    def test_create_job_work_order_success(self):
        """Test POST /api/erp/job-work/orders - successful creation with disclaimer"""
        payload = {
            "customer_name": "Test Customer",
            "company_name": "Test Company",
            "phone": "9876543210",
            "email": "test@lucumaa.com",
            "items": [
                {"thickness_mm": 8, "width_inch": 30, "height_inch": 40, "quantity": 3, "notes": "Test item"}
            ],
            "delivery_address": "123 Test Street, Mumbai",
            "disclaimer_accepted": True,
            "notes": "Test order"
        }
        response = requests.post(
            f"{BASE_URL}/api/erp/job-work/orders",
            json=payload,
            headers=self.headers
        )
        assert response.status_code == 200
        
        data = response.json()
        assert data["message"] == "Job work order created successfully"
        assert "job_work_number" in data
        assert data["job_work_number"].startswith("JW-")
        
        order = data["order"]
        assert order["customer_name"] == "Test Customer"
        assert order["status"] == "pending"
        assert order["disclaimer_accepted"] == True
        assert "disclaimer_accepted_at" in order
        assert order["breakage_count"] == 0
        
        # Verify summary calculated correctly
        # 30x40 = 1200 sq.in / 144 = 8.33 sqft per piece, 3 pieces = 25 sqft
        # 8mm rate = ₹15/sqft, so 25 * 15 = ₹375 labour
        summary = order["summary"]
        assert summary["total_pieces"] == 3
        assert summary["labour_charges"] > 0
        assert summary["gst_rate"] == 18.0
        assert summary["grand_total"] > summary["labour_charges"]
    
    def test_create_job_work_order_without_disclaimer_fails(self):
        """Test POST /api/erp/job-work/orders - fails without disclaimer"""
        payload = {
            "customer_name": "Test Customer",
            "phone": "9876543210",
            "items": [
                {"thickness_mm": 6, "width_inch": 24, "height_inch": 36, "quantity": 1, "notes": ""}
            ],
            "delivery_address": "123 Test Street",
            "disclaimer_accepted": False
        }
        response = requests.post(
            f"{BASE_URL}/api/erp/job-work/orders",
            json=payload,
            headers=self.headers
        )
        assert response.status_code == 400
        
        data = response.json()
        assert "disclaimer" in data["detail"].lower()
        assert "breakage" in data["detail"].lower()
    
    def test_get_my_job_work_orders(self):
        """Test GET /api/erp/job-work/my-orders - returns customer's orders"""
        response = requests.get(
            f"{BASE_URL}/api/erp/job-work/my-orders",
            headers=self.headers
        )
        assert response.status_code == 200
        
        data = response.json()
        assert isinstance(data, list)
        
        # If there are orders, verify structure
        if len(data) > 0:
            order = data[0]
            assert "id" in order
            assert "job_work_number" in order
            assert "status" in order
            assert "summary" in order
            assert "items" in order


class TestCustomerPortalAPIs:
    """Test Customer Portal related APIs"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Login and get token"""
        response = requests.post(
            f"{BASE_URL}/api/auth/login",
            json={"email": CUSTOMER_EMAIL, "password": CUSTOMER_PASSWORD}
        )
        if response.status_code != 200:
            pytest.skip("Customer login failed")
        self.token = response.json()["token"]
        self.headers = {"Authorization": f"Bearer {self.token}"}
    
    def test_get_my_orders(self):
        """Test GET /api/orders/my-orders - returns customer's orders"""
        response = requests.get(
            f"{BASE_URL}/api/orders/my-orders",
            headers=self.headers
        )
        assert response.status_code == 200
        
        data = response.json()
        assert isinstance(data, list)
        
        # If there are orders, verify structure for order summary display
        if len(data) > 0:
            order = data[0]
            # Required fields for order summary display
            assert "id" in order
            assert "order_number" in order or "id" in order  # Order number or ID
            assert "quantity" in order
            assert "created_at" in order
            assert "total_price" in order
            assert "status" in order
    
    def test_get_user_profile(self):
        """Test GET /api/users/profile - returns user profile"""
        response = requests.get(
            f"{BASE_URL}/api/users/profile",
            headers=self.headers
        )
        assert response.status_code == 200
        
        data = response.json()
        assert "email" in data
        assert data["email"] == CUSTOMER_EMAIL


class TestJobWorkStatusFlow:
    """Test Job Work status flow (Admin operations)"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Login as admin"""
        response = requests.post(
            f"{BASE_URL}/api/auth/login",
            json={"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD}
        )
        if response.status_code != 200:
            pytest.skip("Admin login failed")
        self.admin_token = response.json()["token"]
        self.admin_headers = {"Authorization": f"Bearer {self.admin_token}"}
        
        # Also login as customer to create test order
        response = requests.post(
            f"{BASE_URL}/api/auth/login",
            json={"email": CUSTOMER_EMAIL, "password": CUSTOMER_PASSWORD}
        )
        if response.status_code == 200:
            self.customer_token = response.json()["token"]
            self.customer_headers = {"Authorization": f"Bearer {self.customer_token}"}
    
    def test_job_work_status_flow(self):
        """Test status flow: pending -> accepted -> material_received -> in_process -> completed -> ready_for_delivery -> delivered"""
        # Create a job work order as customer
        payload = {
            "customer_name": "Status Test Customer",
            "phone": "9876543210",
            "items": [
                {"thickness_mm": 6, "width_inch": 24, "height_inch": 36, "quantity": 1, "notes": ""}
            ],
            "delivery_address": "123 Test Street",
            "disclaimer_accepted": True
        }
        create_response = requests.post(
            f"{BASE_URL}/api/erp/job-work/orders",
            json=payload,
            headers=self.customer_headers
        )
        
        if create_response.status_code != 200:
            pytest.skip("Could not create test order")
        
        order_id = create_response.json()["order"]["id"]
        
        # Test status transitions
        status_flow = ["accepted", "material_received", "in_process", "completed", "ready_for_delivery", "delivered"]
        
        for status in status_flow:
            response = requests.patch(
                f"{BASE_URL}/api/erp/job-work/orders/{order_id}/status",
                json={"status": status, "notes": f"Updated to {status}"},
                headers=self.admin_headers
            )
            assert response.status_code == 200, f"Failed to update status to {status}"
            assert response.json()["status"] == status


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
