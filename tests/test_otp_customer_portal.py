"""
Test OTP Authentication and Customer Portal APIs
Features tested:
- OTP Send API (email method)
- OTP Verify API
- Password Reset API
- Customer Dashboard
- Customer Orders
- Customer Order Detail
- Customer Profile
"""

import pytest
import requests
import os
import time
import uuid

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', '').rstrip('/')

# Test credentials
ADMIN_EMAIL = "admin@lucumaa.in"
ADMIN_PASSWORD = "adminpass"

# Test user for OTP testing
TEST_EMAIL = f"test_otp_{uuid.uuid4().hex[:8]}@example.com"
TEST_PHONE = "9876543210"
TEST_PASSWORD = "testpass123"


class TestOTPAuthentication:
    """OTP Authentication endpoint tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test session"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
    
    def test_send_otp_email_method(self):
        """Test POST /api/auth/send-otp with email method"""
        # First register a test user
        register_response = self.session.post(f"{BASE_URL}/api/auth/register", json={
            "email": TEST_EMAIL,
            "name": "OTP Test User",
            "phone": TEST_PHONE,
            "password": TEST_PASSWORD
        })
        
        # Send OTP to the registered email
        response = self.session.post(f"{BASE_URL}/api/auth/send-otp", json={
            "method": "email",
            "identifier": TEST_EMAIL,
            "purpose": "login"
        })
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        data = response.json()
        assert "message" in data
        assert data.get("method") == "email"
        print(f"✅ OTP Send (email) - Status: {response.status_code}, Message: {data.get('message')}")
    
    def test_send_otp_missing_identifier(self):
        """Test POST /api/auth/send-otp with missing identifier"""
        response = self.session.post(f"{BASE_URL}/api/auth/send-otp", json={
            "method": "email",
            "purpose": "login"
        })
        
        assert response.status_code == 400, f"Expected 400, got {response.status_code}"
        print(f"✅ OTP Send (missing identifier) - Correctly returns 400")
    
    def test_send_otp_reset_purpose(self):
        """Test POST /api/auth/send-otp for password reset"""
        response = self.session.post(f"{BASE_URL}/api/auth/send-otp", json={
            "method": "email",
            "identifier": ADMIN_EMAIL,
            "purpose": "reset"
        })
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        data = response.json()
        assert "message" in data
        print(f"✅ OTP Send (reset purpose) - Status: {response.status_code}")
    
    def test_verify_otp_invalid(self):
        """Test POST /api/auth/verify-otp with invalid OTP"""
        response = self.session.post(f"{BASE_URL}/api/auth/verify-otp", json={
            "identifier": ADMIN_EMAIL,
            "otp": "000000",
            "purpose": "login"
        })
        
        assert response.status_code == 400, f"Expected 400, got {response.status_code}"
        data = response.json()
        assert "Invalid OTP" in data.get("detail", "")
        print(f"✅ OTP Verify (invalid) - Correctly returns 400")
    
    def test_verify_otp_missing_fields(self):
        """Test POST /api/auth/verify-otp with missing fields"""
        response = self.session.post(f"{BASE_URL}/api/auth/verify-otp", json={
            "identifier": ADMIN_EMAIL
        })
        
        assert response.status_code == 400, f"Expected 400, got {response.status_code}"
        print(f"✅ OTP Verify (missing fields) - Correctly returns 400")
    
    def test_reset_password_missing_token(self):
        """Test POST /api/auth/reset-password with missing token"""
        response = self.session.post(f"{BASE_URL}/api/auth/reset-password", json={
            "new_password": "newpassword123"
        })
        
        assert response.status_code == 400, f"Expected 400, got {response.status_code}"
        print(f"✅ Reset Password (missing token) - Correctly returns 400")
    
    def test_reset_password_invalid_token(self):
        """Test POST /api/auth/reset-password with invalid token"""
        response = self.session.post(f"{BASE_URL}/api/auth/reset-password", json={
            "reset_token": "invalid-token-12345",
            "new_password": "newpassword123"
        })
        
        assert response.status_code == 400, f"Expected 400, got {response.status_code}"
        data = response.json()
        assert "Invalid" in data.get("detail", "") or "expired" in data.get("detail", "")
        print(f"✅ Reset Password (invalid token) - Correctly returns 400")
    
    def test_reset_password_short_password(self):
        """Test POST /api/auth/reset-password with short password"""
        response = self.session.post(f"{BASE_URL}/api/auth/reset-password", json={
            "reset_token": "some-token",
            "new_password": "123"
        })
        
        assert response.status_code == 400, f"Expected 400, got {response.status_code}"
        print(f"✅ Reset Password (short password) - Correctly returns 400")


class TestCustomerPortalAPIs:
    """Customer Portal API tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup authenticated session"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        
        # Login as admin
        login_response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        
        if login_response.status_code == 200:
            token = login_response.json().get("token")
            self.session.headers.update({"Authorization": f"Bearer {token}"})
            self.token = token
        else:
            pytest.skip("Login failed - skipping authenticated tests")
    
    def test_customer_dashboard(self):
        """Test GET /api/erp/customer/dashboard"""
        response = self.session.get(f"{BASE_URL}/api/erp/customer/dashboard")
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        data = response.json()
        
        # Verify response structure
        assert "stats" in data, "Response should contain 'stats'"
        assert "recent_orders" in data, "Response should contain 'recent_orders'"
        
        # Verify stats structure
        stats = data["stats"]
        assert "total_orders" in stats, "Stats should contain 'total_orders'"
        assert "total_spent" in stats, "Stats should contain 'total_spent'"
        assert "pending_orders" in stats, "Stats should contain 'pending_orders'"
        assert "wallet_balance" in stats, "Stats should contain 'wallet_balance'"
        
        # Verify referral info
        assert "referral_code" in data, "Response should contain 'referral_code'"
        assert "referral_count" in data, "Response should contain 'referral_count'"
        
        print(f"✅ Customer Dashboard - Stats: {stats}")
        print(f"   Referral Code: {data.get('referral_code')}, Referral Count: {data.get('referral_count')}")
    
    def test_customer_orders(self):
        """Test GET /api/erp/customer/orders"""
        response = self.session.get(f"{BASE_URL}/api/erp/customer/orders")
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        data = response.json()
        
        # Should return a list
        assert isinstance(data, list), "Response should be a list of orders"
        
        # If orders exist, verify structure
        if len(data) > 0:
            order = data[0]
            expected_fields = ["id", "user_id", "product_name", "status"]
            for field in expected_fields:
                assert field in order, f"Order should contain '{field}'"
        
        print(f"✅ Customer Orders - Found {len(data)} orders")
    
    def test_customer_orders_with_status_filter(self):
        """Test GET /api/erp/customer/orders with status filter"""
        response = self.session.get(f"{BASE_URL}/api/erp/customer/orders?status=pending")
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        data = response.json()
        
        # All returned orders should have pending status
        for order in data:
            assert order.get("status") == "pending", f"Order status should be 'pending', got {order.get('status')}"
        
        print(f"✅ Customer Orders (filtered) - Found {len(data)} pending orders")
    
    def test_customer_order_detail_not_found(self):
        """Test GET /api/erp/customer/orders/{id} with non-existent order"""
        fake_order_id = "non-existent-order-id-12345"
        response = self.session.get(f"{BASE_URL}/api/erp/customer/orders/{fake_order_id}")
        
        assert response.status_code == 404, f"Expected 404, got {response.status_code}"
        print(f"✅ Customer Order Detail (not found) - Correctly returns 404")
    
    def test_customer_profile(self):
        """Test GET /api/erp/customer/profile"""
        response = self.session.get(f"{BASE_URL}/api/erp/customer/profile")
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        data = response.json()
        
        # Verify response structure
        assert "user" in data, "Response should contain 'user'"
        assert "wallet" in data, "Response should contain 'wallet'"
        assert "addresses" in data, "Response should contain 'addresses'"
        
        # Verify user structure
        user = data["user"]
        assert "id" in user, "User should contain 'id'"
        assert "email" in user, "User should contain 'email'"
        assert "name" in user, "User should contain 'name'"
        assert "password_hash" not in user, "User should NOT contain 'password_hash'"
        
        # Verify wallet structure
        wallet = data["wallet"]
        assert "balance" in wallet, "Wallet should contain 'balance'"
        assert "referral_code" in wallet, "Wallet should contain 'referral_code'"
        assert "referral_count" in wallet, "Wallet should contain 'referral_count'"
        
        print(f"✅ Customer Profile - User: {user.get('name')}, Email: {user.get('email')}")
        print(f"   Wallet Balance: ₹{wallet.get('balance')}, Referral Code: {wallet.get('referral_code')}")
    
    def test_customer_profile_update(self):
        """Test PUT /api/erp/customer/profile"""
        update_data = {
            "name": "Updated Test Name",
            "phone": "9999999999"
        }
        
        response = self.session.put(f"{BASE_URL}/api/erp/customer/profile", json=update_data)
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        data = response.json()
        assert "message" in data
        
        print(f"✅ Customer Profile Update - Message: {data.get('message')}")
    
    def test_customer_invoices(self):
        """Test GET /api/erp/customer/invoices"""
        response = self.session.get(f"{BASE_URL}/api/erp/customer/invoices")
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        data = response.json()
        
        # Should return a list
        assert isinstance(data, list), "Response should be a list of invoices"
        
        print(f"✅ Customer Invoices - Found {len(data)} invoices")
    
    def test_customer_support_ticket_create(self):
        """Test POST /api/erp/customer/support/ticket"""
        ticket_data = {
            "subject": "TEST_Support Ticket",
            "description": "This is a test support ticket",
            "category": "general",
            "priority": "normal"
        }
        
        response = self.session.post(f"{BASE_URL}/api/erp/customer/support/ticket", json=ticket_data)
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        data = response.json()
        assert "ticket_id" in data, "Response should contain 'ticket_id'"
        
        print(f"✅ Support Ticket Create - Ticket ID: {data.get('ticket_id')}")
    
    def test_customer_support_tickets_list(self):
        """Test GET /api/erp/customer/support/tickets"""
        response = self.session.get(f"{BASE_URL}/api/erp/customer/support/tickets")
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        data = response.json()
        
        # Should return a list
        assert isinstance(data, list), "Response should be a list of tickets"
        
        print(f"✅ Support Tickets List - Found {len(data)} tickets")
    
    def test_customer_add_address(self):
        """Test POST /api/erp/customer/addresses"""
        address_data = {
            "label": "TEST_Home",
            "address_line1": "123 Test Street",
            "address_line2": "Apt 456",
            "city": "Pune",
            "state": "Maharashtra",
            "pincode": "411001",
            "phone": "9876543210",
            "is_default": False
        }
        
        response = self.session.post(f"{BASE_URL}/api/erp/customer/addresses", json=address_data)
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        data = response.json()
        assert "address_id" in data, "Response should contain 'address_id'"
        
        print(f"✅ Add Address - Address ID: {data.get('address_id')}")
        
        # Store for cleanup
        self.test_address_id = data.get("address_id")


class TestCustomerPortalUnauthorized:
    """Test Customer Portal APIs without authentication
    
    NOTE: The ERP routes use auto_error=False for HTTPBearer and return a system user
    for unauthenticated requests. This is by design for some ERP routes but is a 
    SECURITY CONCERN for customer-specific endpoints.
    """
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup unauthenticated session"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
    
    def test_dashboard_unauthorized_returns_empty_data(self):
        """Test GET /api/erp/customer/dashboard without auth - returns empty data for system user"""
        response = self.session.get(f"{BASE_URL}/api/erp/customer/dashboard")
        
        # Current behavior: returns 200 with empty data for system user
        # This is a SECURITY CONCERN - should return 401
        assert response.status_code == 200, f"Expected 200 (current behavior), got {response.status_code}"
        data = response.json()
        # System user has no orders
        assert data["stats"]["total_orders"] == 0
        print(f"⚠️ Dashboard (unauthorized) - Returns 200 with empty data (SECURITY CONCERN)")
    
    def test_orders_unauthorized_returns_empty_list(self):
        """Test GET /api/erp/customer/orders without auth - returns empty list for system user"""
        response = self.session.get(f"{BASE_URL}/api/erp/customer/orders")
        
        # Current behavior: returns 200 with empty list for system user
        assert response.status_code == 200, f"Expected 200 (current behavior), got {response.status_code}"
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 0  # System user has no orders
        print(f"⚠️ Orders (unauthorized) - Returns 200 with empty list (SECURITY CONCERN)")
    
    def test_profile_unauthorized_returns_404(self):
        """Test GET /api/erp/customer/profile without auth - returns 404 for system user"""
        response = self.session.get(f"{BASE_URL}/api/erp/customer/profile")
        
        # Current behavior: returns 404 because system user doesn't exist in users collection
        assert response.status_code == 404, f"Expected 404 (current behavior), got {response.status_code}"
        print(f"⚠️ Profile (unauthorized) - Returns 404 (system user not found)")


class TestPublicOrderTracking:
    """Test public order tracking endpoint"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup session"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
    
    def test_track_order_not_found(self):
        """Test GET /api/erp/customer/orders/{id}/track with non-existent order"""
        fake_order_id = "non-existent-order-12345"
        response = self.session.get(f"{BASE_URL}/api/erp/customer/orders/{fake_order_id}/track")
        
        assert response.status_code == 404, f"Expected 404, got {response.status_code}"
        print(f"✅ Track Order (not found) - Correctly returns 404")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
