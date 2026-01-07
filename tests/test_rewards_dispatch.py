"""
Test Suite for Rewards System and Order-Payment-Dispatch Flow
Tests:
- Rewards settings API (GET/PUT)
- Referral code generation
- Credit balance API
- Admin rewards dashboard
- Admin adjust credit
- Payment validation before dispatch
"""
import pytest
import requests
import os
import uuid

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', 'https://glassmesh.preview.emergentagent.com').rstrip('/')

# Test credentials
ADMIN_EMAIL = "admin@lucumaa.in"
ADMIN_PASSWORD = "adminpass"
CUSTOMER_EMAIL = "test@lucumaa.com"
CUSTOMER_PASSWORD = "test123"


@pytest.fixture(scope="module")
def admin_token():
    """Get admin authentication token"""
    response = requests.post(f"{BASE_URL}/api/auth/login", json={
        "email": ADMIN_EMAIL,
        "password": ADMIN_PASSWORD
    })
    assert response.status_code == 200, f"Admin login failed: {response.text}"
    return response.json()["token"]


@pytest.fixture(scope="module")
def customer_token():
    """Get customer authentication token"""
    response = requests.post(f"{BASE_URL}/api/auth/login", json={
        "email": CUSTOMER_EMAIL,
        "password": CUSTOMER_PASSWORD
    })
    assert response.status_code == 200, f"Customer login failed: {response.text}"
    return response.json()["token"]


@pytest.fixture(scope="module")
def customer_id(customer_token):
    """Get customer user ID"""
    response = requests.post(f"{BASE_URL}/api/auth/login", json={
        "email": CUSTOMER_EMAIL,
        "password": CUSTOMER_PASSWORD
    })
    return response.json()["user"]["id"]


class TestRewardsSettings:
    """Test rewards settings API"""
    
    def test_get_rewards_settings_public(self):
        """GET /api/erp/rewards/settings - Public access"""
        response = requests.get(f"{BASE_URL}/api/erp/rewards/settings")
        assert response.status_code == 200
        
        data = response.json()
        assert "referrer_reward_percent" in data
        assert "referee_discount_percent" in data
        assert "min_order_for_referral" in data
        assert "max_referral_reward" in data
        assert "reward_points_per_rupee" in data
        assert "points_to_rupee_ratio" in data
        assert "active" in data
        
        # Verify default values
        assert data["referrer_reward_percent"] == 5
        assert data["referee_discount_percent"] == 10
        assert data["reward_points_per_rupee"] == 1
        assert data["points_to_rupee_ratio"] == 10
    
    def test_update_rewards_settings_admin(self, admin_token):
        """PUT /api/erp/rewards/settings - Admin only"""
        headers = {"Authorization": f"Bearer {admin_token}"}
        
        new_settings = {
            "referrer_reward_percent": 5,
            "referee_discount_percent": 10,
            "min_order_for_referral": 1000,
            "max_referral_reward": 500,
            "reward_points_per_rupee": 1,
            "points_to_rupee_ratio": 10,
            "active": True
        }
        
        response = requests.put(
            f"{BASE_URL}/api/erp/rewards/settings",
            headers=headers,
            json=new_settings
        )
        assert response.status_code == 200
        
        data = response.json()
        assert data["message"] == "Settings updated"
        assert "settings" in data
        assert data["settings"]["referrer_reward_percent"] == 5
    
    def test_update_rewards_settings_customer_forbidden(self, customer_token):
        """PUT /api/erp/rewards/settings - Customer should be forbidden"""
        headers = {"Authorization": f"Bearer {customer_token}"}
        
        response = requests.put(
            f"{BASE_URL}/api/erp/rewards/settings",
            headers=headers,
            json={"referrer_reward_percent": 10}
        )
        assert response.status_code == 403


class TestReferralCode:
    """Test referral code generation"""
    
    def test_get_referral_code_admin(self, admin_token):
        """GET /api/erp/rewards/my-referral-code - Admin"""
        headers = {"Authorization": f"Bearer {admin_token}"}
        
        response = requests.get(
            f"{BASE_URL}/api/erp/rewards/my-referral-code",
            headers=headers
        )
        assert response.status_code == 200
        
        data = response.json()
        assert "referral_code" in data
        assert "referral_link" in data
        assert "total_referrals" in data
        assert "total_earned" in data
        
        # Verify referral code format (4 letters + 4 digits)
        code = data["referral_code"]
        assert len(code) == 8
    
    def test_get_referral_code_customer(self, customer_token):
        """GET /api/erp/rewards/my-referral-code - Customer"""
        headers = {"Authorization": f"Bearer {customer_token}"}
        
        response = requests.get(
            f"{BASE_URL}/api/erp/rewards/my-referral-code",
            headers=headers
        )
        assert response.status_code == 200
        
        data = response.json()
        assert data["referral_code"] == "TEST4239"  # Known test customer code
        assert "lucumaaglass.in/register?ref=TEST4239" in data["referral_link"]


class TestCreditBalance:
    """Test credit balance API"""
    
    def test_get_my_balance(self, customer_token):
        """GET /api/erp/rewards/my-balance"""
        headers = {"Authorization": f"Bearer {customer_token}"}
        
        response = requests.get(
            f"{BASE_URL}/api/erp/rewards/my-balance",
            headers=headers
        )
        assert response.status_code == 200
        
        data = response.json()
        assert "credit_balance" in data
        assert "points_balance" in data
        assert "points_value" in data
        assert "total_available" in data
        assert "recent_transactions" in data
        
        # Verify types
        assert isinstance(data["credit_balance"], (int, float))
        assert isinstance(data["points_balance"], int)
    
    def test_get_transactions(self, customer_token):
        """GET /api/erp/rewards/transactions"""
        headers = {"Authorization": f"Bearer {customer_token}"}
        
        response = requests.get(
            f"{BASE_URL}/api/erp/rewards/transactions",
            headers=headers
        )
        assert response.status_code == 200
        
        data = response.json()
        assert "transactions" in data
        assert "count" in data


class TestAdminRewardsDashboard:
    """Test admin rewards dashboard"""
    
    def test_get_admin_dashboard(self, admin_token):
        """GET /api/erp/rewards/admin/dashboard"""
        headers = {"Authorization": f"Bearer {admin_token}"}
        
        response = requests.get(
            f"{BASE_URL}/api/erp/rewards/admin/dashboard",
            headers=headers
        )
        assert response.status_code == 200
        
        data = response.json()
        assert "credit_stats" in data
        assert "referral_stats" in data
        assert "top_referrers" in data
        
        # Verify credit stats structure
        assert "total_issued" in data["credit_stats"]
        assert "total_redeemed" in data["credit_stats"]
        assert "outstanding" in data["credit_stats"]
        
        # Verify referral stats structure
        assert "total" in data["referral_stats"]
        assert "completed" in data["referral_stats"]
        assert "conversion_rate" in data["referral_stats"]
    
    def test_get_admin_dashboard_customer_forbidden(self, customer_token):
        """GET /api/erp/rewards/admin/dashboard - Customer forbidden"""
        headers = {"Authorization": f"Bearer {customer_token}"}
        
        response = requests.get(
            f"{BASE_URL}/api/erp/rewards/admin/dashboard",
            headers=headers
        )
        assert response.status_code == 403
    
    def test_get_all_referrals(self, admin_token):
        """GET /api/erp/rewards/admin/referrals"""
        headers = {"Authorization": f"Bearer {admin_token}"}
        
        response = requests.get(
            f"{BASE_URL}/api/erp/rewards/admin/referrals",
            headers=headers
        )
        assert response.status_code == 200
        
        data = response.json()
        assert "referrals" in data
        assert "summary" in data
        assert "total" in data["summary"]
        assert "completed" in data["summary"]
        assert "pending" in data["summary"]


class TestAdminAdjustCredit:
    """Test admin credit adjustment"""
    
    def test_adjust_credit_add(self, admin_token, customer_id):
        """POST /api/erp/rewards/admin/adjust-credit - Add credit"""
        headers = {"Authorization": f"Bearer {admin_token}"}
        
        response = requests.post(
            f"{BASE_URL}/api/erp/rewards/admin/adjust-credit",
            headers=headers,
            json={
                "user_id": customer_id,
                "amount": 50,
                "type": "credit",
                "reason": "Test credit addition"
            }
        )
        assert response.status_code == 200
        
        data = response.json()
        assert "Credit added successfully" in data["message"]
        assert data["amount"] == 50
        assert "new_balance" in data
    
    def test_adjust_credit_debit(self, admin_token, customer_id):
        """POST /api/erp/rewards/admin/adjust-credit - Deduct credit"""
        headers = {"Authorization": f"Bearer {admin_token}"}
        
        response = requests.post(
            f"{BASE_URL}/api/erp/rewards/admin/adjust-credit",
            headers=headers,
            json={
                "user_id": customer_id,
                "amount": 25,
                "type": "debit",
                "reason": "Test credit deduction"
            }
        )
        assert response.status_code == 200
        
        data = response.json()
        assert "deducted" in data["message"]
        assert data["amount"] == 25
    
    def test_adjust_credit_invalid_user(self, admin_token):
        """POST /api/erp/rewards/admin/adjust-credit - Invalid user"""
        headers = {"Authorization": f"Bearer {admin_token}"}
        
        response = requests.post(
            f"{BASE_URL}/api/erp/rewards/admin/adjust-credit",
            headers=headers,
            json={
                "user_id": "invalid-user-id",
                "amount": 50,
                "type": "credit",
                "reason": "Test"
            }
        )
        assert response.status_code == 404
    
    def test_adjust_credit_customer_forbidden(self, customer_token, customer_id):
        """POST /api/erp/rewards/admin/adjust-credit - Customer forbidden"""
        headers = {"Authorization": f"Bearer {customer_token}"}
        
        response = requests.post(
            f"{BASE_URL}/api/erp/rewards/admin/adjust-credit",
            headers=headers,
            json={
                "user_id": customer_id,
                "amount": 50,
                "type": "credit",
                "reason": "Test"
            }
        )
        assert response.status_code == 403


class TestPaymentValidationBeforeDispatch:
    """Test payment validation before dispatch operations"""
    
    def test_dispatch_slip_blocked_without_payment(self, admin_token):
        """POST /api/orders/{id}/create-dispatch-slip - Should fail without payment"""
        headers = {"Authorization": f"Bearer {admin_token}"}
        
        # Get orders
        orders_response = requests.get(
            f"{BASE_URL}/api/admin/orders",
            headers=headers
        )
        assert orders_response.status_code == 200
        orders = orders_response.json()
        
        # Find an unpaid order
        unpaid_order = None
        for order in orders:
            payment_settled = (
                order.get('payment_status') == 'completed' or
                (order.get('advance_percent') == 100 and order.get('advance_payment_status') == 'paid') or
                (order.get('advance_payment_status') == 'paid' and order.get('remaining_payment_status') in ['paid', 'cash_received'])
            )
            if not payment_settled:
                unpaid_order = order
                break
        
        if unpaid_order:
            response = requests.post(
                f"{BASE_URL}/api/orders/{unpaid_order['id']}/create-dispatch-slip",
                headers=headers
            )
            # Should fail with 400 - payment not completed
            assert response.status_code == 400
            assert "Payment not completed" in response.json().get("detail", "")
        else:
            pytest.skip("No unpaid order found for testing")
    
    def test_mark_dispatched_blocked_without_payment(self, admin_token):
        """POST /api/orders/{id}/mark-dispatched - Should fail without payment"""
        headers = {"Authorization": f"Bearer {admin_token}"}
        
        # Get orders
        orders_response = requests.get(
            f"{BASE_URL}/api/admin/orders",
            headers=headers
        )
        orders = orders_response.json()
        
        # Find an order with dispatch slip but no payment
        for order in orders:
            if order.get('dispatch_slip_number') and order.get('payment_status') != 'completed':
                response = requests.post(
                    f"{BASE_URL}/api/orders/{order['id']}/mark-dispatched",
                    headers=headers
                )
                # Should fail with 400
                assert response.status_code == 400
                return
        
        pytest.skip("No suitable order found for testing")
    
    def test_transport_dispatch_blocked_without_payment(self, admin_token):
        """POST /api/erp/transport/dispatch - Should fail without payment"""
        headers = {"Authorization": f"Bearer {admin_token}"}
        
        # Get orders
        orders_response = requests.get(
            f"{BASE_URL}/api/admin/orders",
            headers=headers
        )
        orders = orders_response.json()
        
        # Find an unpaid order
        for order in orders:
            payment_settled = (
                order.get('payment_status') == 'completed' or
                (order.get('advance_percent') == 100 and order.get('advance_payment_status') == 'paid') or
                (order.get('advance_payment_status') == 'paid' and order.get('remaining_payment_status') in ['paid', 'cash_received'])
            )
            if not payment_settled:
                # Try to dispatch
                response = requests.post(
                    f"{BASE_URL}/api/erp/transport/dispatch",
                    headers=headers,
                    json={
                        "order_id": order['id'],
                        "vehicle_id": "test-vehicle",
                        "driver_id": "test-driver"
                    }
                )
                # Should fail - either 400 (payment not settled) or 404 (vehicle/driver not found)
                assert response.status_code in [400, 404]
                if response.status_code == 400:
                    assert "Payment not fully settled" in response.json().get("detail", "")
                return
        
        pytest.skip("No unpaid order found for testing")


class TestAdminUserBalance:
    """Test admin get user balance"""
    
    def test_admin_get_user_balance(self, admin_token, customer_id):
        """GET /api/erp/rewards/admin/user/{user_id}/balance"""
        headers = {"Authorization": f"Bearer {admin_token}"}
        
        response = requests.get(
            f"{BASE_URL}/api/erp/rewards/admin/user/{customer_id}/balance",
            headers=headers
        )
        assert response.status_code == 200
        
        data = response.json()
        assert "user" in data
        assert "credit_balance" in data
        assert "points_balance" in data
        assert "transactions" in data
    
    def test_admin_get_user_balance_invalid_user(self, admin_token):
        """GET /api/erp/rewards/admin/user/{user_id}/balance - Invalid user"""
        headers = {"Authorization": f"Bearer {admin_token}"}
        
        response = requests.get(
            f"{BASE_URL}/api/erp/rewards/admin/user/invalid-user-id/balance",
            headers=headers
        )
        assert response.status_code == 404


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
