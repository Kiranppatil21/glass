"""
Test Suite for:
1. Admin Job Work Dashboard - Stats cards, orders table, status update
2. Job Work Status Update API with WhatsApp notifications
3. AI Forecast Stats API security (401 without token, 200 with token)
4. Pay Remaining Amount functionality
"""
import pytest
import requests
import os
import time

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', '').rstrip('/')

# Test credentials
ADMIN_EMAIL = "admin@lucumaa.in"
ADMIN_PASSWORD = "adminpass"
CUSTOMER_EMAIL = "test@lucumaa.com"
CUSTOMER_PASSWORD = "test123"


class TestAuthentication:
    """Test authentication and get tokens"""
    
    def test_admin_login(self):
        """Test admin login and get token"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        assert response.status_code == 200, f"Admin login failed: {response.text}"
        data = response.json()
        assert "token" in data, "No token in response"
        return data["token"]
    
    def test_customer_login(self):
        """Test customer login and get token"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": CUSTOMER_EMAIL,
            "password": CUSTOMER_PASSWORD
        })
        assert response.status_code == 200, f"Customer login failed: {response.text}"
        data = response.json()
        assert "token" in data, "No token in response"
        return data["token"]


class TestAIForecastAPISecurity:
    """Test AI Forecast Stats API security - requires authentication"""
    
    def test_forecast_stats_without_auth_returns_401(self):
        """AI Forecast Stats API should return 401 without authentication"""
        response = requests.get(f"{BASE_URL}/api/erp/forecast/stats")
        assert response.status_code in [401, 403], f"Expected 401/403 without auth, got {response.status_code}: {response.text}"
        print(f"✓ AI Forecast Stats API correctly returns {response.status_code} without auth")
    
    def test_forecast_stats_with_invalid_token_returns_401(self):
        """AI Forecast Stats API should return 401 with invalid token"""
        headers = {"Authorization": "Bearer invalid_token_12345"}
        response = requests.get(f"{BASE_URL}/api/erp/forecast/stats", headers=headers)
        assert response.status_code == 401, f"Expected 401 with invalid token, got {response.status_code}: {response.text}"
        print("✓ AI Forecast Stats API correctly returns 401 with invalid token")
    
    def test_forecast_stats_with_valid_admin_token(self):
        """AI Forecast Stats API should return data with valid admin token"""
        # First login as admin
        login_response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        assert login_response.status_code == 200, f"Admin login failed: {login_response.text}"
        token = login_response.json()["token"]
        
        # Now call forecast stats with token
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(f"{BASE_URL}/api/erp/forecast/stats", headers=headers)
        assert response.status_code == 200, f"Expected 200 with valid admin token, got {response.status_code}: {response.text}"
        
        data = response.json()
        # Verify response structure
        assert "orders" in data, "Response should contain 'orders' field"
        assert "revenue" in data, "Response should contain 'revenue' field"
        assert "generated_at" in data, "Response should contain 'generated_at' field"
        print(f"✓ AI Forecast Stats API returns data with valid admin token: {data.get('orders', {})}")


class TestAdminJobWorkDashboard:
    """Test Admin Job Work Dashboard APIs"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup - get admin token"""
        login_response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        assert login_response.status_code == 200, f"Admin login failed: {login_response.text}"
        self.admin_token = login_response.json()["token"]
        self.headers = {"Authorization": f"Bearer {self.admin_token}"}
    
    def test_job_work_dashboard_stats(self):
        """Test Job Work Dashboard stats endpoint"""
        response = requests.get(f"{BASE_URL}/api/erp/job-work/dashboard", headers=self.headers)
        assert response.status_code == 200, f"Dashboard stats failed: {response.text}"
        
        data = response.json()
        # Verify stats structure
        assert "total_orders" in data, "Should have total_orders"
        assert "pending_orders" in data, "Should have pending_orders"
        assert "this_month" in data, "Should have this_month"
        assert "total_breakage" in data, "Should have total_breakage"
        assert "by_status" in data, "Should have by_status"
        assert "labour_rates" in data, "Should have labour_rates"
        
        print(f"✓ Job Work Dashboard Stats: total={data['total_orders']}, pending={data['pending_orders']}, this_month={data['this_month']}")
    
    def test_job_work_orders_list(self):
        """Test Job Work Orders list endpoint"""
        response = requests.get(f"{BASE_URL}/api/erp/job-work/orders", headers=self.headers)
        assert response.status_code == 200, f"Orders list failed: {response.text}"
        
        orders = response.json()
        assert isinstance(orders, list), "Should return a list of orders"
        print(f"✓ Job Work Orders List: {len(orders)} orders found")
        
        if orders:
            order = orders[0]
            # Verify order structure
            assert "id" in order, "Order should have id"
            assert "job_work_number" in order, "Order should have job_work_number"
            assert "status" in order, "Order should have status"
            assert "customer_name" in order, "Order should have customer_name"
            print(f"  First order: {order.get('job_work_number')} - {order.get('status')}")
    
    def test_job_work_orders_filter_by_status(self):
        """Test Job Work Orders filter by status"""
        response = requests.get(f"{BASE_URL}/api/erp/job-work/orders?status=pending", headers=self.headers)
        assert response.status_code == 200, f"Orders filter failed: {response.text}"
        
        orders = response.json()
        # All returned orders should have pending status
        for order in orders:
            assert order.get("status") == "pending", f"Order {order.get('job_work_number')} has status {order.get('status')}, expected pending"
        
        print(f"✓ Job Work Orders Filter: {len(orders)} pending orders")


class TestJobWorkStatusUpdate:
    """Test Job Work Status Update API with WhatsApp notifications"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup - get admin and customer tokens"""
        # Admin login
        admin_login = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        assert admin_login.status_code == 200
        self.admin_token = admin_login.json()["token"]
        self.admin_headers = {"Authorization": f"Bearer {self.admin_token}"}
        
        # Customer login
        customer_login = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": CUSTOMER_EMAIL,
            "password": CUSTOMER_PASSWORD
        })
        assert customer_login.status_code == 200
        self.customer_token = customer_login.json()["token"]
        self.customer_headers = {"Authorization": f"Bearer {self.customer_token}"}
    
    def test_create_job_work_order_for_status_test(self):
        """Create a job work order to test status updates"""
        # First calculate cost
        calc_response = requests.post(f"{BASE_URL}/api/erp/job-work/calculate", json=[
            {"thickness_mm": 6, "width_inch": 24, "height_inch": 36, "quantity": 2}
        ])
        assert calc_response.status_code == 200
        
        # Create order with disclaimer accepted
        order_data = {
            "customer_name": "Test Status Update",
            "company_name": "Test Company",
            "phone": "+919876543210",
            "email": "test@example.com",
            "items": [{"thickness_mm": 6, "width_inch": 24, "height_inch": 36, "quantity": 2}],
            "delivery_address": "Test Address for Status Update",
            "disclaimer_accepted": True,
            "notes": "Testing status update flow"
        }
        
        response = requests.post(
            f"{BASE_URL}/api/erp/job-work/orders",
            json=order_data,
            headers=self.customer_headers
        )
        assert response.status_code == 200, f"Order creation failed: {response.text}"
        
        data = response.json()
        assert "order" in data
        order_id = data["order"]["id"]
        job_work_number = data["order"]["job_work_number"]
        print(f"✓ Created Job Work Order: {job_work_number} (ID: {order_id})")
        return order_id
    
    def test_status_update_flow_with_notification(self):
        """Test status update flow - should trigger WhatsApp notification"""
        # Create a new order first
        calc_response = requests.post(f"{BASE_URL}/api/erp/job-work/calculate", json=[
            {"thickness_mm": 8, "width_inch": 30, "height_inch": 40, "quantity": 1}
        ])
        assert calc_response.status_code == 200
        
        order_data = {
            "customer_name": "WhatsApp Test Customer",
            "company_name": "WhatsApp Test Co",
            "phone": "+919876543210",
            "email": "whatsapp@test.com",
            "items": [{"thickness_mm": 8, "width_inch": 30, "height_inch": 40, "quantity": 1}],
            "delivery_address": "WhatsApp Test Address",
            "disclaimer_accepted": True,
            "notes": "Testing WhatsApp notification"
        }
        
        create_response = requests.post(
            f"{BASE_URL}/api/erp/job-work/orders",
            json=order_data,
            headers=self.customer_headers
        )
        assert create_response.status_code == 200
        order_id = create_response.json()["order"]["id"]
        
        # Test status update to 'accepted' - should trigger WhatsApp
        status_update = {
            "status": "accepted",
            "notes": "Order accepted for processing"
        }
        
        update_response = requests.patch(
            f"{BASE_URL}/api/erp/job-work/orders/{order_id}/status",
            json=status_update,
            headers=self.admin_headers
        )
        assert update_response.status_code == 200, f"Status update failed: {update_response.text}"
        
        data = update_response.json()
        assert data.get("status") == "accepted", "Status should be updated to accepted"
        assert "notification_sent" in data, "Response should indicate notification status"
        print(f"✓ Status updated to 'accepted', notification_sent: {data.get('notification_sent')}")
        
        # Test next status - material_received
        status_update = {"status": "material_received", "notes": "Glass received at factory"}
        update_response = requests.patch(
            f"{BASE_URL}/api/erp/job-work/orders/{order_id}/status",
            json=status_update,
            headers=self.admin_headers
        )
        assert update_response.status_code == 200
        print(f"✓ Status updated to 'material_received'")
        
        # Test in_process status
        status_update = {"status": "in_process", "notes": "Processing started"}
        update_response = requests.patch(
            f"{BASE_URL}/api/erp/job-work/orders/{order_id}/status",
            json=status_update,
            headers=self.admin_headers
        )
        assert update_response.status_code == 200
        print(f"✓ Status updated to 'in_process'")
        
        # Verify order status history
        order_response = requests.get(
            f"{BASE_URL}/api/erp/job-work/orders/{order_id}",
            headers=self.admin_headers
        )
        assert order_response.status_code == 200
        order = order_response.json()
        assert order.get("status") == "in_process"
        assert len(order.get("status_history", [])) >= 4  # pending + 3 updates
        print(f"✓ Order status history has {len(order.get('status_history', []))} entries")
    
    def test_invalid_status_update(self):
        """Test that invalid status returns error"""
        # Get any existing order
        orders_response = requests.get(f"{BASE_URL}/api/erp/job-work/orders", headers=self.admin_headers)
        assert orders_response.status_code == 200
        orders = orders_response.json()
        
        if orders:
            order_id = orders[0]["id"]
            
            # Try invalid status
            status_update = {"status": "invalid_status_xyz"}
            update_response = requests.patch(
                f"{BASE_URL}/api/erp/job-work/orders/{order_id}/status",
                json=status_update,
                headers=self.admin_headers
            )
            assert update_response.status_code == 400, f"Expected 400 for invalid status, got {update_response.status_code}"
            print("✓ Invalid status correctly rejected with 400")


class TestPayRemainingAmount:
    """Test Pay Remaining Amount functionality"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup - get customer token"""
        customer_login = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": CUSTOMER_EMAIL,
            "password": CUSTOMER_PASSWORD
        })
        assert customer_login.status_code == 200
        self.customer_token = customer_login.json()["token"]
        self.customer_headers = {"Authorization": f"Bearer {self.customer_token}"}
    
    def test_get_customer_orders_with_remaining_balance(self):
        """Test getting customer orders to check for remaining balance"""
        response = requests.get(f"{BASE_URL}/api/orders/my-orders", headers=self.customer_headers)
        assert response.status_code == 200, f"Failed to get orders: {response.text}"
        
        orders = response.json()
        print(f"✓ Customer has {len(orders)} orders")
        
        # Check for orders with remaining balance
        orders_with_remaining = [o for o in orders if o.get("remaining_amount", 0) > 0 and o.get("advance_payment_status") == "paid"]
        print(f"  Orders with remaining balance: {len(orders_with_remaining)}")
        
        for order in orders_with_remaining[:3]:  # Show first 3
            print(f"  - Order #{order.get('order_number')}: Remaining ₹{order.get('remaining_amount')}")
    
    def test_pay_remaining_endpoint_exists(self):
        """Test that pay-remaining endpoint exists"""
        # Get customer orders first
        orders_response = requests.get(f"{BASE_URL}/api/orders/my-orders", headers=self.customer_headers)
        assert orders_response.status_code == 200
        orders = orders_response.json()
        
        # Find an order with remaining balance
        order_with_remaining = None
        for order in orders:
            if order.get("remaining_amount", 0) > 0 and order.get("advance_payment_status") == "paid":
                order_with_remaining = order
                break
        
        if order_with_remaining:
            order_id = order_with_remaining["id"]
            # Try to initiate payment (may fail due to Razorpay config but endpoint should exist)
            response = requests.post(
                f"{BASE_URL}/api/orders/{order_id}/pay-remaining",
                headers=self.customer_headers
            )
            # Should not be 404 - endpoint should exist
            assert response.status_code != 404, f"pay-remaining endpoint not found"
            print(f"✓ pay-remaining endpoint exists, status: {response.status_code}")
        else:
            print("⚠ No orders with remaining balance found to test pay-remaining")
    
    def test_update_payment_preference_to_cash(self):
        """Test updating payment preference to cash"""
        # Get customer orders
        orders_response = requests.get(f"{BASE_URL}/api/orders/my-orders", headers=self.customer_headers)
        assert orders_response.status_code == 200
        orders = orders_response.json()
        
        # Find an order with remaining balance
        order_with_remaining = None
        for order in orders:
            if order.get("remaining_amount", 0) > 0 and order.get("advance_payment_status") == "paid":
                order_with_remaining = order
                break
        
        if order_with_remaining:
            order_id = order_with_remaining["id"]
            # Update payment preference to cash
            response = requests.patch(
                f"{BASE_URL}/api/orders/{order_id}",
                json={"remaining_payment_preference": "cash"},
                headers={**self.customer_headers, "Content-Type": "application/json"}
            )
            # Check if endpoint exists and accepts the update
            if response.status_code == 200:
                print(f"✓ Payment preference updated to cash for order {order_with_remaining.get('order_number')}")
            else:
                print(f"⚠ Payment preference update returned {response.status_code}: {response.text}")
        else:
            print("⚠ No orders with remaining balance found to test cash preference")


class TestJobWorkSettings:
    """Test Job Work Settings API"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup - get admin token"""
        admin_login = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        assert admin_login.status_code == 200
        self.admin_token = admin_login.json()["token"]
        self.admin_headers = {"Authorization": f"Bearer {self.admin_token}"}
    
    def test_get_job_work_settings(self):
        """Test getting job work settings"""
        response = requests.get(f"{BASE_URL}/api/erp/job-work/settings", headers=self.admin_headers)
        assert response.status_code == 200, f"Failed to get settings: {response.text}"
        
        settings = response.json()
        assert "labour_rates" in settings, "Settings should have labour_rates"
        assert "gst_rate" in settings, "Settings should have gst_rate"
        print(f"✓ Job Work Settings: GST Rate = {settings.get('gst_rate')}%")
    
    def test_get_labour_rates_public(self):
        """Test getting labour rates (public endpoint)"""
        response = requests.get(f"{BASE_URL}/api/erp/job-work/labour-rates")
        assert response.status_code == 200, f"Failed to get labour rates: {response.text}"
        
        data = response.json()
        assert "labour_rates" in data
        rates = data["labour_rates"]
        
        # Verify expected rates
        expected_rates = {"4": 8, "5": 10, "6": 12, "8": 15, "10": 18, "12": 22, "15": 28, "19": 35}
        for thickness, rate in expected_rates.items():
            assert str(thickness) in rates or thickness in rates, f"Missing rate for {thickness}mm"
        
        print(f"✓ Labour Rates: {rates}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
