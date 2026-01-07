"""
Test Payment Flow Features:
1. Job Work 100%/50% Advance Rule: Single item = 100% advance, 2+ items = 50% advance
2. Customer Portal Payment Selection Flow
3. Job Work Page Payment Step Flow
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


class TestAdvancePaymentRule:
    """Test 100%/50% advance payment rule for job work orders"""
    
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
    
    def test_single_item_100_percent_advance(self):
        """Test: Single item (1 piece) requires 100% advance"""
        payload = {
            "customer_name": "Test Single Item",
            "phone": "9876543210",
            "items": [
                {"thickness_mm": 6, "width_inch": 24, "height_inch": 36, "quantity": 1, "notes": "Single piece"}
            ],
            "delivery_address": "123 Test Street",
            "disclaimer_accepted": True
        }
        response = requests.post(
            f"{BASE_URL}/api/erp/job-work/orders",
            json=payload,
            headers=self.headers
        )
        assert response.status_code == 200
        
        data = response.json()
        order = data["order"]
        
        # Verify 100% advance for single item
        assert order["advance_percent"] == 100, f"Expected 100% advance for single item, got {order['advance_percent']}%"
        assert order["advance_required"] == order["summary"]["grand_total"], "Advance required should equal grand total for single item"
        
        print(f"✓ Single item order: advance_percent={order['advance_percent']}%, advance_required=₹{order['advance_required']}, grand_total=₹{order['summary']['grand_total']}")
    
    def test_multiple_items_50_percent_advance(self):
        """Test: Multiple items (2+ pieces) requires 50% advance"""
        payload = {
            "customer_name": "Test Multiple Items",
            "phone": "9876543210",
            "items": [
                {"thickness_mm": 6, "width_inch": 24, "height_inch": 36, "quantity": 2, "notes": "Two pieces"}
            ],
            "delivery_address": "123 Test Street",
            "disclaimer_accepted": True
        }
        response = requests.post(
            f"{BASE_URL}/api/erp/job-work/orders",
            json=payload,
            headers=self.headers
        )
        assert response.status_code == 200
        
        data = response.json()
        order = data["order"]
        
        # Verify 50% advance for multiple items
        assert order["advance_percent"] == 50, f"Expected 50% advance for multiple items, got {order['advance_percent']}%"
        expected_advance = round(order["summary"]["grand_total"] * 0.5, 2)
        assert order["advance_required"] == expected_advance, f"Advance required should be 50% of grand total. Expected {expected_advance}, got {order['advance_required']}"
        
        print(f"✓ Multiple items order: advance_percent={order['advance_percent']}%, advance_required=₹{order['advance_required']}, grand_total=₹{order['summary']['grand_total']}")
    
    def test_multiple_different_items_50_percent_advance(self):
        """Test: Multiple different items (total 3+ pieces) requires 50% advance"""
        payload = {
            "customer_name": "Test Mixed Items",
            "phone": "9876543210",
            "items": [
                {"thickness_mm": 6, "width_inch": 24, "height_inch": 36, "quantity": 1, "notes": "Item 1"},
                {"thickness_mm": 8, "width_inch": 30, "height_inch": 40, "quantity": 2, "notes": "Item 2"}
            ],
            "delivery_address": "123 Test Street",
            "disclaimer_accepted": True
        }
        response = requests.post(
            f"{BASE_URL}/api/erp/job-work/orders",
            json=payload,
            headers=self.headers
        )
        assert response.status_code == 200
        
        data = response.json()
        order = data["order"]
        
        # Total pieces = 1 + 2 = 3, so should be 50% advance
        total_pieces = order["summary"]["total_pieces"]
        assert total_pieces == 3, f"Expected 3 total pieces, got {total_pieces}"
        assert order["advance_percent"] == 50, f"Expected 50% advance for {total_pieces} pieces, got {order['advance_percent']}%"
        
        print(f"✓ Mixed items order ({total_pieces} pieces): advance_percent={order['advance_percent']}%, advance_required=₹{order['advance_required']}")


class TestJobWorkPaymentAPIs:
    """Test Job Work payment initiation and cash preference APIs"""
    
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
    
    def test_set_cash_preference(self):
        """Test POST /api/erp/job-work/orders/{id}/set-cash-preference"""
        # First create an order
        payload = {
            "customer_name": "Cash Preference Test",
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
            headers=self.headers
        )
        assert create_response.status_code == 200
        order_id = create_response.json()["order"]["id"]
        
        # Set cash preference
        response = requests.post(
            f"{BASE_URL}/api/erp/job-work/orders/{order_id}/set-cash-preference",
            headers=self.headers
        )
        assert response.status_code == 200
        
        data = response.json()
        assert "cash" in data["message"].lower() or "preference" in data["message"].lower()
        
        # Verify order has cash preference
        order_response = requests.get(
            f"{BASE_URL}/api/erp/job-work/orders/{order_id}",
            headers=self.headers
        )
        assert order_response.status_code == 200
        order = order_response.json()
        assert order.get("payment_preference") == "cash", "Order should have cash payment preference"
        
        print(f"✓ Cash preference set successfully for order {order_id}")
    
    def test_initiate_online_payment(self):
        """Test POST /api/erp/job-work/orders/{id}/initiate-payment"""
        # First create an order
        payload = {
            "customer_name": "Online Payment Test",
            "phone": "9876543210",
            "items": [
                {"thickness_mm": 6, "width_inch": 24, "height_inch": 36, "quantity": 2, "notes": ""}
            ],
            "delivery_address": "123 Test Street",
            "disclaimer_accepted": True
        }
        create_response = requests.post(
            f"{BASE_URL}/api/erp/job-work/orders",
            json=payload,
            headers=self.headers
        )
        assert create_response.status_code == 200
        order = create_response.json()["order"]
        order_id = order["id"]
        
        # Initiate payment
        response = requests.post(
            f"{BASE_URL}/api/erp/job-work/orders/{order_id}/initiate-payment",
            headers=self.headers
        )
        assert response.status_code == 200
        
        data = response.json()
        assert "razorpay_order_id" in data, "Response should contain razorpay_order_id"
        assert "amount" in data, "Response should contain amount"
        assert "advance_percent" in data, "Response should contain advance_percent"
        
        # Verify amount matches advance required
        expected_advance = order["advance_required"]
        assert data["amount_rupees"] == expected_advance, f"Payment amount should be {expected_advance}, got {data['amount_rupees']}"
        
        print(f"✓ Online payment initiated: razorpay_order_id={data['razorpay_order_id']}, amount=₹{data['amount_rupees']}, advance_percent={data['advance_percent']}%")


class TestCustomerPortalPaymentBadges:
    """Test payment badge display logic"""
    
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
    
    def test_job_work_orders_have_payment_fields(self):
        """Test that job work orders have required payment fields for badge display"""
        response = requests.get(
            f"{BASE_URL}/api/erp/job-work/my-orders",
            headers=self.headers
        )
        assert response.status_code == 200
        
        orders = response.json()
        if len(orders) == 0:
            pytest.skip("No job work orders to test")
        
        # Check first order has required fields
        order = orders[0]
        required_fields = ["payment_status", "advance_percent", "advance_paid", "summary"]
        for field in required_fields:
            assert field in order, f"Order missing required field: {field}"
        
        # Verify summary has grand_total
        assert "grand_total" in order["summary"], "Summary missing grand_total"
        
        print(f"✓ Job work order has all payment fields: payment_status={order['payment_status']}, advance_percent={order['advance_percent']}%, advance_paid=₹{order['advance_paid']}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
