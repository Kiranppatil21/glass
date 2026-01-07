"""
Job Work E2E Flow Tests
Tests:
1. Job Work order creation flow
2. 100%/50% advance rule verification
3. Payment flow (online/cash)
4. PDF generation (Invoice, Receipt, Merged)
5. Share modal functionality
"""
import pytest
import requests
import os
import time

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', 'https://glassmesh.preview.emergentagent.com')

# Test credentials
TEST_CUSTOMER_EMAIL = "test@lucumaa.com"
TEST_CUSTOMER_PASSWORD = "test123"
ADMIN_EMAIL = "admin@lucumaa.in"
ADMIN_PASSWORD = "adminpass"


class TestJobWorkE2E:
    """Job Work End-to-End Flow Tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test session"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        self.customer_token = None
        self.admin_token = None
        self.created_order_id = None
        self.created_order_number = None
    
    def login_customer(self):
        """Login as test customer"""
        response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": TEST_CUSTOMER_EMAIL,
            "password": TEST_CUSTOMER_PASSWORD
        })
        if response.status_code == 200:
            data = response.json()
            self.customer_token = data.get("token")
            return True
        return False
    
    def login_admin(self):
        """Login as admin"""
        response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        if response.status_code == 200:
            data = response.json()
            self.admin_token = data.get("token")
            return True
        return False
    
    def get_customer_headers(self):
        """Get headers with customer token"""
        return {"Authorization": f"Bearer {self.customer_token}", "Content-Type": "application/json"}
    
    def get_admin_headers(self):
        """Get headers with admin token"""
        return {"Authorization": f"Bearer {self.admin_token}", "Content-Type": "application/json"}
    
    # ============ LABOUR RATES ============
    
    def test_01_get_labour_rates(self):
        """Test: Get labour rates (public endpoint)"""
        response = self.session.get(f"{BASE_URL}/api/erp/job-work/labour-rates")
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        
        data = response.json()
        assert "labour_rates" in data, "Response should contain labour_rates"
        
        rates = data["labour_rates"]
        # Verify expected thickness rates exist
        expected_thicknesses = ["4", "5", "6", "8", "10", "12", "15", "19"]
        for thickness in expected_thicknesses:
            assert thickness in rates, f"Labour rate for {thickness}mm should exist"
            assert isinstance(rates[thickness], (int, float)), f"Rate for {thickness}mm should be numeric"
        
        print(f"SUCCESS: Labour rates retrieved - {rates}")
    
    # ============ COST CALCULATION ============
    
    def test_02_calculate_cost_single_item(self):
        """Test: Calculate cost for single item (should require 100% advance)"""
        items = [{
            "thickness_mm": 6,
            "width_inch": 24,
            "height_inch": 36,
            "quantity": 1,
            "notes": "Test single item"
        }]
        
        response = self.session.post(f"{BASE_URL}/api/erp/job-work/calculate", json=items)
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        
        data = response.json()
        assert "items" in data, "Response should contain items"
        assert "summary" in data, "Response should contain summary"
        
        summary = data["summary"]
        assert summary["total_pieces"] == 1, "Total pieces should be 1"
        assert summary["total_sqft"] > 0, "Total sqft should be positive"
        assert summary["labour_charges"] > 0, "Labour charges should be positive"
        assert summary["gst_amount"] > 0, "GST amount should be positive"
        assert summary["grand_total"] > 0, "Grand total should be positive"
        
        print(f"SUCCESS: Single item cost calculated - Total: ₹{summary['grand_total']}")
    
    def test_03_calculate_cost_multiple_items(self):
        """Test: Calculate cost for multiple items (should require 50% advance)"""
        items = [
            {"thickness_mm": 6, "width_inch": 24, "height_inch": 36, "quantity": 2, "notes": ""},
            {"thickness_mm": 8, "width_inch": 30, "height_inch": 40, "quantity": 1, "notes": ""}
        ]
        
        response = self.session.post(f"{BASE_URL}/api/erp/job-work/calculate", json=items)
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        
        data = response.json()
        summary = data["summary"]
        assert summary["total_pieces"] == 3, "Total pieces should be 3"
        
        print(f"SUCCESS: Multiple items cost calculated - {summary['total_pieces']} pieces, Total: ₹{summary['grand_total']}")
    
    # ============ ORDER CREATION ============
    
    def test_04_create_order_without_disclaimer(self):
        """Test: Order creation should fail without disclaimer acceptance"""
        assert self.login_customer(), "Customer login failed"
        
        order_data = {
            "customer_name": "Test Customer",
            "company_name": "Test Company",
            "phone": "9876543210",
            "email": TEST_CUSTOMER_EMAIL,
            "items": [{"thickness_mm": 6, "width_inch": 24, "height_inch": 36, "quantity": 1, "notes": ""}],
            "delivery_address": "Test Address, City",
            "disclaimer_accepted": False,  # Not accepted
            "notes": ""
        }
        
        response = self.session.post(
            f"{BASE_URL}/api/erp/job-work/orders",
            json=order_data,
            headers=self.get_customer_headers()
        )
        
        assert response.status_code == 400, f"Expected 400 for missing disclaimer, got {response.status_code}"
        print("SUCCESS: Order creation correctly rejected without disclaimer")
    
    def test_05_create_single_item_order_100_percent_advance(self):
        """Test: Create single item order - should require 100% advance"""
        assert self.login_customer(), "Customer login failed"
        
        order_data = {
            "customer_name": "TEST_E2E Customer",
            "company_name": "TEST_E2E Company",
            "phone": "9876543210",
            "email": TEST_CUSTOMER_EMAIL,
            "items": [{"thickness_mm": 6, "width_inch": 24, "height_inch": 36, "quantity": 1, "notes": "Single item test"}],
            "delivery_address": "Test Address, City 123",
            "disclaimer_accepted": True,
            "notes": "E2E Test Order - Single Item",
            "transport_required": False
        }
        
        response = self.session.post(
            f"{BASE_URL}/api/erp/job-work/orders",
            json=order_data,
            headers=self.get_customer_headers()
        )
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        assert "order" in data, "Response should contain order"
        assert "job_work_number" in data, "Response should contain job_work_number"
        
        order = data["order"]
        self.created_order_id = order["id"]
        self.created_order_number = order["job_work_number"]
        
        # Verify 100% advance for single item
        assert order["advance_percent"] == 100, f"Single item should require 100% advance, got {order['advance_percent']}%"
        assert order["advance_required"] == order["summary"]["grand_total"], "Advance required should equal grand total for single item"
        
        print(f"SUCCESS: Single item order created - {self.created_order_number}, 100% advance: ₹{order['advance_required']}")
        return order
    
    def test_06_create_multi_item_order_50_percent_advance(self):
        """Test: Create multi-item order - should require 50% advance"""
        assert self.login_customer(), "Customer login failed"
        
        order_data = {
            "customer_name": "TEST_E2E Customer Multi",
            "company_name": "TEST_E2E Company",
            "phone": "9876543211",
            "email": TEST_CUSTOMER_EMAIL,
            "items": [
                {"thickness_mm": 6, "width_inch": 24, "height_inch": 36, "quantity": 2, "notes": ""},
                {"thickness_mm": 8, "width_inch": 30, "height_inch": 40, "quantity": 1, "notes": ""}
            ],
            "delivery_address": "Test Address Multi, City 456",
            "disclaimer_accepted": True,
            "notes": "E2E Test Order - Multiple Items",
            "transport_required": False
        }
        
        response = self.session.post(
            f"{BASE_URL}/api/erp/job-work/orders",
            json=order_data,
            headers=self.get_customer_headers()
        )
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        order = data["order"]
        
        # Verify 50% advance for multiple items
        assert order["advance_percent"] == 50, f"Multi-item should require 50% advance, got {order['advance_percent']}%"
        expected_advance = round(order["summary"]["grand_total"] * 0.5, 2)
        assert order["advance_required"] == expected_advance, f"Advance required should be 50% of grand total"
        
        print(f"SUCCESS: Multi-item order created - {order['job_work_number']}, 50% advance: ₹{order['advance_required']}")
        return order
    
    # ============ GET ORDERS ============
    
    def test_07_get_my_orders(self):
        """Test: Get customer's own job work orders"""
        assert self.login_customer(), "Customer login failed"
        
        response = self.session.get(
            f"{BASE_URL}/api/erp/job-work/my-orders",
            headers=self.get_customer_headers()
        )
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        
        orders = response.json()
        assert isinstance(orders, list), "Response should be a list"
        
        # Verify order structure
        if orders:
            order = orders[0]
            required_fields = ["id", "job_work_number", "customer_name", "status", "payment_status", "summary"]
            for field in required_fields:
                assert field in order, f"Order should have {field} field"
        
        print(f"SUCCESS: Retrieved {len(orders)} job work orders")
    
    # ============ PAYMENT FLOW ============
    
    def test_08_set_cash_preference(self):
        """Test: Set cash payment preference"""
        assert self.login_customer(), "Customer login failed"
        
        # First create an order
        order = self.test_05_create_single_item_order_100_percent_advance()
        order_id = order["id"]
        
        response = self.session.post(
            f"{BASE_URL}/api/erp/job-work/orders/{order_id}/set-cash-preference",
            headers=self.get_customer_headers()
        )
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        
        data = response.json()
        assert "message" in data, "Response should contain message"
        
        print(f"SUCCESS: Cash preference set for order {order['job_work_number']}")
    
    def test_09_initiate_online_payment(self):
        """Test: Initiate online payment (Razorpay)"""
        assert self.login_customer(), "Customer login failed"
        
        # Create a new order for payment test
        order_data = {
            "customer_name": "TEST_Payment Customer",
            "company_name": "TEST_Payment Company",
            "phone": "9876543212",
            "email": TEST_CUSTOMER_EMAIL,
            "items": [{"thickness_mm": 6, "width_inch": 24, "height_inch": 36, "quantity": 1, "notes": ""}],
            "delivery_address": "Payment Test Address",
            "disclaimer_accepted": True,
            "notes": "Payment Test Order"
        }
        
        create_response = self.session.post(
            f"{BASE_URL}/api/erp/job-work/orders",
            json=order_data,
            headers=self.get_customer_headers()
        )
        
        assert create_response.status_code == 200, f"Order creation failed: {create_response.text}"
        order = create_response.json()["order"]
        order_id = order["id"]
        
        # Initiate payment
        response = self.session.post(
            f"{BASE_URL}/api/erp/job-work/orders/{order_id}/initiate-payment",
            headers=self.get_customer_headers()
        )
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        assert "razorpay_order_id" in data, "Response should contain razorpay_order_id"
        assert "amount" in data, "Response should contain amount"
        assert "amount_rupees" in data, "Response should contain amount_rupees"
        assert "advance_percent" in data, "Response should contain advance_percent"
        
        print(f"SUCCESS: Payment initiated - Razorpay Order: {data['razorpay_order_id']}, Amount: ₹{data['amount_rupees']}")


class TestPDFGeneration:
    """PDF Generation Tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test session"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        self.admin_token = None
        self.customer_token = None
    
    def login_admin(self):
        """Login as admin"""
        response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        if response.status_code == 200:
            self.admin_token = response.json().get("token")
            return True
        return False
    
    def login_customer(self):
        """Login as customer"""
        response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": TEST_CUSTOMER_EMAIL,
            "password": TEST_CUSTOMER_PASSWORD
        })
        if response.status_code == 200:
            self.customer_token = response.json().get("token")
            return True
        return False
    
    def get_admin_headers(self):
        return {"Authorization": f"Bearer {self.admin_token}"}
    
    def get_customer_headers(self):
        return {"Authorization": f"Bearer {self.customer_token}"}
    
    def get_job_work_order_id(self):
        """Get a job work order ID for testing"""
        assert self.login_customer(), "Customer login failed"
        
        response = self.session.get(
            f"{BASE_URL}/api/erp/job-work/my-orders",
            headers=self.get_customer_headers()
        )
        
        if response.status_code == 200:
            orders = response.json()
            if orders:
                return orders[0]["id"]
        return None
    
    def get_regular_order_id(self):
        """Get a regular order ID for testing"""
        assert self.login_customer(), "Customer login failed"
        
        response = self.session.get(
            f"{BASE_URL}/api/orders/my-orders",
            headers=self.get_customer_headers()
        )
        
        if response.status_code == 200:
            orders = response.json()
            if orders:
                return orders[0]["id"]
        return None
    
    def test_10_job_work_invoice_pdf(self):
        """Test: Generate Job Work Invoice PDF"""
        assert self.login_admin(), "Admin login failed"
        
        order_id = self.get_job_work_order_id()
        if not order_id:
            pytest.skip("No job work orders available for PDF test")
        
        response = self.session.get(
            f"{BASE_URL}/api/erp/pdf/job-work-invoice/{order_id}?token={self.admin_token}",
            headers=self.get_admin_headers()
        )
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        assert response.headers.get("content-type") == "application/pdf", "Response should be PDF"
        assert len(response.content) > 1000, "PDF content should be substantial"
        
        print(f"SUCCESS: Job Work Invoice PDF generated - {len(response.content)} bytes")
    
    def test_11_payment_receipt_pdf(self):
        """Test: Generate Payment Receipt PDF"""
        assert self.login_admin(), "Admin login failed"
        
        order_id = self.get_job_work_order_id()
        if not order_id:
            pytest.skip("No job work orders available for PDF test")
        
        response = self.session.get(
            f"{BASE_URL}/api/erp/pdf/payment-receipt/{order_id}?token={self.admin_token}",
            headers=self.get_admin_headers()
        )
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        assert response.headers.get("content-type") == "application/pdf", "Response should be PDF"
        
        print(f"SUCCESS: Payment Receipt PDF generated - {len(response.content)} bytes")
    
    def test_12_merged_pdf(self):
        """Test: Generate Merged PDF (Invoice + Receipt)"""
        assert self.login_admin(), "Admin login failed"
        
        order_id = self.get_job_work_order_id()
        if not order_id:
            pytest.skip("No job work orders available for PDF test")
        
        response = self.session.get(
            f"{BASE_URL}/api/erp/pdf/merged/{order_id}?token={self.admin_token}",
            headers=self.get_admin_headers()
        )
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        assert response.headers.get("content-type") == "application/pdf", "Response should be PDF"
        
        print(f"SUCCESS: Merged PDF generated - {len(response.content)} bytes")
    
    def test_13_regular_invoice_pdf(self):
        """Test: Generate Regular Order Invoice PDF"""
        assert self.login_admin(), "Admin login failed"
        
        order_id = self.get_regular_order_id()
        if not order_id:
            pytest.skip("No regular orders available for PDF test")
        
        response = self.session.get(
            f"{BASE_URL}/api/erp/pdf/invoice/{order_id}?token={self.admin_token}",
            headers=self.get_admin_headers()
        )
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        assert response.headers.get("content-type") == "application/pdf", "Response should be PDF"
        
        print(f"SUCCESS: Regular Invoice PDF generated - {len(response.content)} bytes")
    
    def test_14_share_log_action(self):
        """Test: Log share action for audit trail"""
        assert self.login_admin(), "Admin login failed"
        
        order_id = self.get_job_work_order_id()
        if not order_id:
            pytest.skip("No job work orders available for share log test")
        
        response = self.session.post(
            f"{BASE_URL}/api/erp/pdf/share-log?order_id={order_id}&document_type=invoice&share_channel=download",
            headers=self.get_admin_headers()
        )
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        
        data = response.json()
        assert "log_id" in data, "Response should contain log_id"
        
        print(f"SUCCESS: Share action logged - {data['log_id']}")


class TestCustomerPortal:
    """Customer Portal Tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test session"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        self.customer_token = None
    
    def login_customer(self):
        """Login as customer"""
        response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": TEST_CUSTOMER_EMAIL,
            "password": TEST_CUSTOMER_PASSWORD
        })
        if response.status_code == 200:
            self.customer_token = response.json().get("token")
            return True
        return False
    
    def get_customer_headers(self):
        return {"Authorization": f"Bearer {self.customer_token}", "Content-Type": "application/json"}
    
    def test_15_customer_login(self):
        """Test: Customer login"""
        response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": TEST_CUSTOMER_EMAIL,
            "password": TEST_CUSTOMER_PASSWORD
        })
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        
        data = response.json()
        assert "token" in data, "Response should contain token"
        assert "user" in data, "Response should contain user"
        
        print(f"SUCCESS: Customer logged in - {data['user'].get('email')}")
    
    def test_16_get_customer_profile(self):
        """Test: Get customer profile"""
        assert self.login_customer(), "Customer login failed"
        
        response = self.session.get(
            f"{BASE_URL}/api/users/profile",
            headers=self.get_customer_headers()
        )
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        
        data = response.json()
        assert "email" in data or "id" in data, "Profile should contain user info"
        
        print(f"SUCCESS: Customer profile retrieved")
    
    def test_17_get_all_orders(self):
        """Test: Get all orders (regular + job work)"""
        assert self.login_customer(), "Customer login failed"
        
        # Get regular orders
        regular_response = self.session.get(
            f"{BASE_URL}/api/orders/my-orders",
            headers=self.get_customer_headers()
        )
        
        # Get job work orders
        jw_response = self.session.get(
            f"{BASE_URL}/api/erp/job-work/my-orders",
            headers=self.get_customer_headers()
        )
        
        assert regular_response.status_code == 200, f"Regular orders failed: {regular_response.status_code}"
        assert jw_response.status_code == 200, f"Job work orders failed: {jw_response.status_code}"
        
        regular_orders = regular_response.json()
        jw_orders = jw_response.json()
        
        print(f"SUCCESS: Retrieved {len(regular_orders)} regular orders, {len(jw_orders)} job work orders")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
