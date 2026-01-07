"""
Wallet & Referral System API Tests
Tests for wallet balance, transactions, referral codes, admin settings, and admin credit functionality
"""
import pytest
import requests
import os

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', '').rstrip('/')

class TestWalletSettings:
    """Wallet Settings API Tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test fixtures"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        # Login as admin
        login_response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": "admin@lucumaa.in",
            "password": "adminpass"
        })
        if login_response.status_code == 200:
            token = login_response.json().get("token")
            self.session.headers.update({"Authorization": f"Bearer {token}"})
            self.user_id = login_response.json().get("user", {}).get("id")
        else:
            pytest.skip("Authentication failed")
    
    def test_get_wallet_settings(self):
        """Test GET /api/erp/wallet/settings - returns default config"""
        response = self.session.get(f"{BASE_URL}/api/erp/wallet/settings")
        assert response.status_code == 200
        
        data = response.json()
        # Verify default settings structure
        assert "referral_enabled" in data
        assert "referral_bonus_type" in data
        assert "referral_flat_bonus" in data
        assert "referral_percentage_bonus" in data
        assert "referral_bonus_cap" in data
        assert "referee_bonus_enabled" in data
        assert "referee_bonus_amount" in data
        assert "wallet_enabled" in data
        assert "wallet_usage_type" in data
        assert "wallet_max_percentage" in data
        assert "wallet_max_fixed" in data
        assert "min_order_for_wallet" in data
        assert "cashback_enabled" in data
        assert "cashback_percentage" in data
        assert "cashback_cap" in data
        assert "min_order_for_cashback" in data
        
        # Verify default values
        assert data["referral_flat_bonus"] == 100
        assert data["referral_percentage_bonus"] == 5
        assert data["referral_bonus_cap"] == 500
        assert data["referee_bonus_amount"] == 50
        assert data["wallet_max_percentage"] == 25
        assert data["min_order_for_wallet"] == 500
    
    def test_update_wallet_settings_referral_bonus(self):
        """Test PUT /api/erp/wallet/settings - update referral flat bonus"""
        # Update referral flat bonus
        response = self.session.put(f"{BASE_URL}/api/erp/wallet/settings", json={
            "referral_flat_bonus": 150
        })
        assert response.status_code == 200
        assert response.json().get("message") == "Settings updated successfully"
        
        # Verify update
        get_response = self.session.get(f"{BASE_URL}/api/erp/wallet/settings")
        assert get_response.status_code == 200
        assert get_response.json()["referral_flat_bonus"] == 150
        
        # Reset to default
        self.session.put(f"{BASE_URL}/api/erp/wallet/settings", json={
            "referral_flat_bonus": 100
        })
    
    def test_update_wallet_settings_wallet_percentage(self):
        """Test PUT /api/erp/wallet/settings - update wallet max percentage"""
        response = self.session.put(f"{BASE_URL}/api/erp/wallet/settings", json={
            "wallet_max_percentage": 30
        })
        assert response.status_code == 200
        
        # Verify update
        get_response = self.session.get(f"{BASE_URL}/api/erp/wallet/settings")
        assert get_response.json()["wallet_max_percentage"] == 30
        
        # Reset to default
        self.session.put(f"{BASE_URL}/api/erp/wallet/settings", json={
            "wallet_max_percentage": 25
        })
    
    def test_update_wallet_settings_toggle_referral(self):
        """Test PUT /api/erp/wallet/settings - toggle referral enabled"""
        # Get current state
        current = self.session.get(f"{BASE_URL}/api/erp/wallet/settings").json()
        original_state = current["referral_enabled"]
        
        # Toggle
        response = self.session.put(f"{BASE_URL}/api/erp/wallet/settings", json={
            "referral_enabled": not original_state
        })
        assert response.status_code == 200
        
        # Verify toggle
        updated = self.session.get(f"{BASE_URL}/api/erp/wallet/settings").json()
        assert updated["referral_enabled"] == (not original_state)
        
        # Reset
        self.session.put(f"{BASE_URL}/api/erp/wallet/settings", json={
            "referral_enabled": original_state
        })


class TestWalletBalance:
    """Wallet Balance API Tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test fixtures"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        # Login as admin
        login_response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": "admin@lucumaa.in",
            "password": "adminpass"
        })
        if login_response.status_code == 200:
            token = login_response.json().get("token")
            self.session.headers.update({"Authorization": f"Bearer {token}"})
            self.user_id = login_response.json().get("user", {}).get("id")
        else:
            pytest.skip("Authentication failed")
    
    def test_get_wallet_balance(self):
        """Test GET /api/erp/wallet/balance - returns user wallet with referral code"""
        response = self.session.get(f"{BASE_URL}/api/erp/wallet/balance")
        assert response.status_code == 200
        
        data = response.json()
        # Verify wallet structure
        assert "balance" in data
        assert "total_earned" in data
        assert "total_spent" in data
        assert "referral_code" in data
        assert "referral_count" in data
        
        # Verify referral code format (8 char alphanumeric)
        assert len(data["referral_code"]) == 8
        assert data["referral_code"].isalnum()
    
    def test_wallet_balance_numeric_values(self):
        """Test wallet balance returns numeric values"""
        response = self.session.get(f"{BASE_URL}/api/erp/wallet/balance")
        assert response.status_code == 200
        
        data = response.json()
        assert isinstance(data["balance"], (int, float))
        assert isinstance(data["total_earned"], (int, float))
        assert isinstance(data["total_spent"], (int, float))
        assert isinstance(data["referral_count"], int)


class TestWalletTransactions:
    """Wallet Transactions API Tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test fixtures"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        # Login as admin
        login_response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": "admin@lucumaa.in",
            "password": "adminpass"
        })
        if login_response.status_code == 200:
            token = login_response.json().get("token")
            self.session.headers.update({"Authorization": f"Bearer {token}"})
            self.user_id = login_response.json().get("user", {}).get("id")
        else:
            pytest.skip("Authentication failed")
    
    def test_get_wallet_transactions(self):
        """Test GET /api/erp/wallet/transactions - returns transaction list"""
        response = self.session.get(f"{BASE_URL}/api/erp/wallet/transactions")
        assert response.status_code == 200
        
        data = response.json()
        assert isinstance(data, list)
    
    def test_get_wallet_transactions_with_limit(self):
        """Test GET /api/erp/wallet/transactions with limit parameter"""
        response = self.session.get(f"{BASE_URL}/api/erp/wallet/transactions", params={"limit": 10})
        assert response.status_code == 200
        
        data = response.json()
        assert isinstance(data, list)
        assert len(data) <= 10


class TestCalculateWalletUsage:
    """Calculate Wallet Usage API Tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test fixtures"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        # Login as admin
        login_response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": "admin@lucumaa.in",
            "password": "adminpass"
        })
        if login_response.status_code == 200:
            token = login_response.json().get("token")
            self.session.headers.update({"Authorization": f"Bearer {token}"})
            self.user_id = login_response.json().get("user", {}).get("id")
        else:
            pytest.skip("Authentication failed")
    
    def test_calculate_wallet_usage(self):
        """Test POST /api/erp/wallet/calculate-usage"""
        response = self.session.post(f"{BASE_URL}/api/erp/wallet/calculate-usage", json={
            "order_amount": 1000
        })
        assert response.status_code == 200
        
        data = response.json()
        assert "usable_amount" in data
        assert isinstance(data["usable_amount"], (int, float))
    
    def test_calculate_wallet_usage_below_minimum(self):
        """Test calculate-usage with order below minimum"""
        response = self.session.post(f"{BASE_URL}/api/erp/wallet/calculate-usage", json={
            "order_amount": 100  # Below min_order_for_wallet (500)
        })
        assert response.status_code == 200
        
        data = response.json()
        assert data["usable_amount"] == 0
        assert "reason" in data
    
    def test_calculate_wallet_usage_response_structure(self):
        """Test calculate-usage response structure"""
        response = self.session.post(f"{BASE_URL}/api/erp/wallet/calculate-usage", json={
            "order_amount": 2000
        })
        assert response.status_code == 200
        
        data = response.json()
        # Should have these fields when wallet has balance or not
        assert "usable_amount" in data


class TestApplyReferralCode:
    """Apply Referral Code API Tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test fixtures"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        # Login as admin
        login_response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": "admin@lucumaa.in",
            "password": "adminpass"
        })
        if login_response.status_code == 200:
            token = login_response.json().get("token")
            self.session.headers.update({"Authorization": f"Bearer {token}"})
            self.user_id = login_response.json().get("user", {}).get("id")
        else:
            pytest.skip("Authentication failed")
    
    def test_apply_invalid_referral_code(self):
        """Test POST /api/erp/wallet/apply-referral with invalid code"""
        response = self.session.post(f"{BASE_URL}/api/erp/wallet/apply-referral", json={
            "referral_code": "INVALID123"
        })
        # Should return 404 for invalid code
        assert response.status_code == 404
        assert "Invalid referral code" in response.json().get("detail", "")
    
    def test_apply_own_referral_code(self):
        """Test POST /api/erp/wallet/apply-referral with own code"""
        # Get own referral code
        balance_response = self.session.get(f"{BASE_URL}/api/erp/wallet/balance")
        own_code = balance_response.json().get("referral_code")
        
        response = self.session.post(f"{BASE_URL}/api/erp/wallet/apply-referral", json={
            "referral_code": own_code
        })
        # Should return 400 for own code
        assert response.status_code == 400
        assert "Cannot use your own referral code" in response.json().get("detail", "")


class TestAdminWalletStats:
    """Admin Wallet Stats API Tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test fixtures"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        # Login as admin
        login_response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": "admin@lucumaa.in",
            "password": "adminpass"
        })
        if login_response.status_code == 200:
            token = login_response.json().get("token")
            self.session.headers.update({"Authorization": f"Bearer {token}"})
            self.user_id = login_response.json().get("user", {}).get("id")
        else:
            pytest.skip("Authentication failed")
    
    def test_get_admin_stats(self):
        """Test GET /api/erp/wallet/admin/stats"""
        response = self.session.get(f"{BASE_URL}/api/erp/wallet/admin/stats")
        assert response.status_code == 200
        
        data = response.json()
        # Verify stats structure
        assert "total_wallets" in data
        assert "total_balance_outstanding" in data
        assert "total_rewards_given" in data
        assert "total_referrals" in data
        assert "recent_transactions" in data
        
        # Verify types
        assert isinstance(data["total_wallets"], int)
        assert isinstance(data["total_balance_outstanding"], (int, float))
        assert isinstance(data["total_rewards_given"], (int, float))
        assert isinstance(data["total_referrals"], int)
        assert isinstance(data["recent_transactions"], list)
    
    def test_get_all_user_wallets(self):
        """Test GET /api/erp/wallet/admin/users"""
        response = self.session.get(f"{BASE_URL}/api/erp/wallet/admin/users")
        assert response.status_code == 200
        
        data = response.json()
        assert isinstance(data, list)
        
        # If wallets exist, verify structure
        if len(data) > 0:
            wallet = data[0]
            assert "id" in wallet
            assert "user_id" in wallet
            assert "balance" in wallet
            assert "referral_code" in wallet


class TestAdminCreditWallet:
    """Admin Credit Wallet API Tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test fixtures"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        # Login as admin
        login_response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": "admin@lucumaa.in",
            "password": "adminpass"
        })
        if login_response.status_code == 200:
            token = login_response.json().get("token")
            self.session.headers.update({"Authorization": f"Bearer {token}"})
            self.user_id = login_response.json().get("user", {}).get("id")
        else:
            pytest.skip("Authentication failed")
    
    def test_admin_credit_wallet(self):
        """Test POST /api/erp/wallet/admin/credit"""
        # Get initial balance
        balance_before = self.session.get(f"{BASE_URL}/api/erp/wallet/balance").json()
        initial_balance = balance_before["balance"]
        
        # Credit wallet
        response = self.session.post(f"{BASE_URL}/api/erp/wallet/admin/credit", json={
            "user_id": self.user_id,
            "amount": 100,
            "reason": "TEST_Admin credit for testing"
        })
        assert response.status_code == 200
        
        data = response.json()
        assert data["message"] == "Wallet credited"
        assert data["new_balance"] == initial_balance + 100
        
        # Verify balance increased
        balance_after = self.session.get(f"{BASE_URL}/api/erp/wallet/balance").json()
        assert balance_after["balance"] == initial_balance + 100
    
    def test_admin_credit_wallet_invalid_amount(self):
        """Test POST /api/erp/wallet/admin/credit with invalid amount"""
        response = self.session.post(f"{BASE_URL}/api/erp/wallet/admin/credit", json={
            "user_id": self.user_id,
            "amount": 0,
            "reason": "Invalid amount test"
        })
        assert response.status_code == 400
        assert "Invalid amount" in response.json().get("detail", "")
    
    def test_admin_credit_wallet_negative_amount(self):
        """Test POST /api/erp/wallet/admin/credit with negative amount"""
        response = self.session.post(f"{BASE_URL}/api/erp/wallet/admin/credit", json={
            "user_id": self.user_id,
            "amount": -50,
            "reason": "Negative amount test"
        })
        assert response.status_code == 400


class TestWalletTransactionHistory:
    """Test wallet transaction history after credit"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test fixtures"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        # Login as admin
        login_response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": "admin@lucumaa.in",
            "password": "adminpass"
        })
        if login_response.status_code == 200:
            token = login_response.json().get("token")
            self.session.headers.update({"Authorization": f"Bearer {token}"})
            self.user_id = login_response.json().get("user", {}).get("id")
        else:
            pytest.skip("Authentication failed")
    
    def test_transaction_created_after_credit(self):
        """Test that transaction is created after admin credit"""
        # Credit wallet
        self.session.post(f"{BASE_URL}/api/erp/wallet/admin/credit", json={
            "user_id": self.user_id,
            "amount": 50,
            "reason": "TEST_Transaction history test"
        })
        
        # Get transactions
        response = self.session.get(f"{BASE_URL}/api/erp/wallet/transactions")
        assert response.status_code == 200
        
        transactions = response.json()
        assert len(transactions) > 0
        
        # Find the test transaction
        test_txn = next((t for t in transactions if "TEST_Transaction history test" in t.get("description", "")), None)
        assert test_txn is not None
        assert test_txn["type"] == "credit"
        assert test_txn["category"] == "admin_credit"
        assert test_txn["amount"] == 50


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
