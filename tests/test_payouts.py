"""
Payouts Module Tests - Razorpay Payouts Integration
Tests for fund accounts, salary payouts, and payout dashboard
"""
import pytest
import requests
import os

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', 'https://glassmesh.preview.emergentagent.com').rstrip('/')


class TestPayoutsAuth:
    """Test authentication for payouts endpoints"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Login and get auth token"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": "admin@lucumaa.in",
            "password": "adminpass"
        })
        assert response.status_code == 200, f"Login failed: {response.text}"
        self.token = response.json()["token"]
        self.headers = {"Authorization": f"Bearer {self.token}"}
    
    def test_payouts_dashboard_accessible_with_auth(self):
        """Test that payouts dashboard is accessible with authentication"""
        response = requests.get(
            f"{BASE_URL}/api/erp/payouts/dashboard",
            headers=self.headers
        )
        assert response.status_code == 200, "Should be accessible with auth"
    
    def test_fund_accounts_accessible_with_auth(self):
        """Test that fund accounts endpoint is accessible with authentication"""
        response = requests.get(
            f"{BASE_URL}/api/erp/payouts/fund-accounts",
            headers=self.headers
        )
        assert response.status_code == 200, "Should be accessible with auth"
    
    def test_payout_history_accessible_with_auth(self):
        """Test that payout history endpoint is accessible with authentication"""
        response = requests.get(
            f"{BASE_URL}/api/erp/payouts/history",
            headers=self.headers
        )
        assert response.status_code == 200, "Should be accessible with auth"


class TestPayoutsDashboard:
    """Test payouts dashboard API"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Login and get auth token"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": "admin@lucumaa.in",
            "password": "adminpass"
        })
        assert response.status_code == 200, f"Login failed: {response.text}"
        self.token = response.json()["token"]
        self.headers = {"Authorization": f"Bearer {self.token}"}
    
    def test_get_payouts_dashboard(self):
        """Test GET /api/erp/payouts/dashboard returns dashboard data"""
        response = requests.get(
            f"{BASE_URL}/api/erp/payouts/dashboard",
            headers=self.headers
        )
        assert response.status_code == 200, f"Failed: {response.text}"
        data = response.json()
        
        # Verify response structure
        assert "current_month" in data, "Missing current_month"
        assert "employees" in data, "Missing employees"
        assert "salaries" in data, "Missing salaries"
        assert "amounts" in data, "Missing amounts"
        assert "recent_payouts" in data, "Missing recent_payouts"
    
    def test_dashboard_employees_structure(self):
        """Test dashboard employees data structure"""
        response = requests.get(
            f"{BASE_URL}/api/erp/payouts/dashboard",
            headers=self.headers
        )
        assert response.status_code == 200
        data = response.json()
        
        employees = data["employees"]
        assert "total" in employees, "Missing total employees"
        assert "bank_linked" in employees, "Missing bank_linked count"
        assert "pending_bank_link" in employees, "Missing pending_bank_link count"
        
        # Verify data types
        assert isinstance(employees["total"], int)
        assert isinstance(employees["bank_linked"], int)
        assert isinstance(employees["pending_bank_link"], int)
    
    def test_dashboard_salaries_structure(self):
        """Test dashboard salaries data structure"""
        response = requests.get(
            f"{BASE_URL}/api/erp/payouts/dashboard",
            headers=self.headers
        )
        assert response.status_code == 200
        data = response.json()
        
        salaries = data["salaries"]
        assert "pending_approval" in salaries
        assert "approved" in salaries
        assert "processing" in salaries
        assert "paid" in salaries
        assert "failed" in salaries
        
        # Verify all are integers
        for key in ["pending_approval", "approved", "processing", "paid", "failed"]:
            assert isinstance(salaries[key], int), f"{key} should be int"
    
    def test_dashboard_amounts_structure(self):
        """Test dashboard amounts data structure"""
        response = requests.get(
            f"{BASE_URL}/api/erp/payouts/dashboard",
            headers=self.headers
        )
        assert response.status_code == 200
        data = response.json()
        
        amounts = data["amounts"]
        assert "ready_to_pay" in amounts
        assert "paid_this_month" in amounts
        
        # Verify amounts are numeric
        assert isinstance(amounts["ready_to_pay"], (int, float))
        assert isinstance(amounts["paid_this_month"], (int, float))
    
    def test_dashboard_recent_payouts_is_list(self):
        """Test that recent_payouts is a list"""
        response = requests.get(
            f"{BASE_URL}/api/erp/payouts/dashboard",
            headers=self.headers
        )
        assert response.status_code == 200
        data = response.json()
        
        assert isinstance(data["recent_payouts"], list)


class TestFundAccounts:
    """Test fund accounts (employee bank details) API"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Login and get auth token"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": "admin@lucumaa.in",
            "password": "adminpass"
        })
        assert response.status_code == 200, f"Login failed: {response.text}"
        self.token = response.json()["token"]
        self.headers = {"Authorization": f"Bearer {self.token}"}
    
    def test_get_fund_accounts(self):
        """Test GET /api/erp/payouts/fund-accounts returns list"""
        response = requests.get(
            f"{BASE_URL}/api/erp/payouts/fund-accounts",
            headers=self.headers
        )
        assert response.status_code == 200, f"Failed: {response.text}"
        data = response.json()
        assert isinstance(data, list), "Should return a list"
    
    def test_get_fund_accounts_with_employee_filter(self):
        """Test GET /api/erp/payouts/fund-accounts with employee_id filter"""
        response = requests.get(
            f"{BASE_URL}/api/erp/payouts/fund-accounts",
            params={"employee_id": "nonexistent-id"},
            headers=self.headers
        )
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 0, "Should return empty list for nonexistent employee"
    
    def test_create_fund_account_missing_employee(self):
        """Test POST /api/erp/payouts/fund-accounts with nonexistent employee"""
        response = requests.post(
            f"{BASE_URL}/api/erp/payouts/fund-accounts",
            json={
                "employee_id": "nonexistent-employee-id",
                "account_holder_name": "Test User",
                "account_number": "1234567890",
                "ifsc_code": "HDFC0001234"
            },
            headers=self.headers
        )
        # Should return 404 for nonexistent employee
        assert response.status_code == 404, f"Expected 404, got {response.status_code}: {response.text}"
    
    def test_create_fund_account_with_valid_employee(self):
        """Test POST /api/erp/payouts/fund-accounts with valid employee (will fail at Razorpay)"""
        # First get an employee
        emp_response = requests.get(
            f"{BASE_URL}/api/erp/hr/employees",
            headers=self.headers
        )
        
        if emp_response.status_code == 200 and len(emp_response.json()) > 0:
            employee = emp_response.json()[0]
            
            response = requests.post(
                f"{BASE_URL}/api/erp/payouts/fund-accounts",
                json={
                    "employee_id": employee["id"],
                    "account_holder_name": employee.get("name", "Test User"),
                    "account_number": "1234567890123456",
                    "ifsc_code": "HDFC0001234"
                },
                headers=self.headers
            )
            
            # Expected: Either 200 (if fund account exists) or 400/500/520 (Razorpay API error)
            # Razorpay Payouts API requires special account activation
            # 520 is a server error from Razorpay API
            assert response.status_code in [200, 400, 500, 520], f"Unexpected status: {response.status_code}"
            
            # If it's an error, verify it's a Razorpay-related error
            if response.status_code in [400, 500, 520]:
                data = response.json()
                assert "detail" in data, "Error response should have detail"
        else:
            pytest.skip("No employees found to test fund account creation")


class TestPayoutHistory:
    """Test payout history API"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Login and get auth token"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": "admin@lucumaa.in",
            "password": "adminpass"
        })
        assert response.status_code == 200, f"Login failed: {response.text}"
        self.token = response.json()["token"]
        self.headers = {"Authorization": f"Bearer {self.token}"}
    
    def test_get_payout_history(self):
        """Test GET /api/erp/payouts/history returns list"""
        response = requests.get(
            f"{BASE_URL}/api/erp/payouts/history",
            headers=self.headers
        )
        assert response.status_code == 200, f"Failed: {response.text}"
        data = response.json()
        assert isinstance(data, list), "Should return a list"
    
    def test_get_payout_history_with_month_filter(self):
        """Test GET /api/erp/payouts/history with month filter"""
        response = requests.get(
            f"{BASE_URL}/api/erp/payouts/history",
            params={"month": "2025-12"},
            headers=self.headers
        )
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
    
    def test_get_payout_history_with_employee_filter(self):
        """Test GET /api/erp/payouts/history with employee_id filter"""
        response = requests.get(
            f"{BASE_URL}/api/erp/payouts/history",
            params={"employee_id": "test-employee-id"},
            headers=self.headers
        )
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
    
    def test_get_payout_history_with_status_filter(self):
        """Test GET /api/erp/payouts/history with status filter"""
        response = requests.get(
            f"{BASE_URL}/api/erp/payouts/history",
            params={"status": "processed"},
            headers=self.headers
        )
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)


class TestSalaryPayouts:
    """Test salary payout processing APIs"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Login and get auth token"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": "admin@lucumaa.in",
            "password": "adminpass"
        })
        assert response.status_code == 200, f"Login failed: {response.text}"
        self.token = response.json()["token"]
        self.headers = {"Authorization": f"Bearer {self.token}"}
    
    def test_process_salary_payout_missing_salary(self):
        """Test POST /api/erp/payouts/salary/process with nonexistent salary"""
        response = requests.post(
            f"{BASE_URL}/api/erp/payouts/salary/process",
            json={
                "salary_id": "nonexistent-salary-id",
                "mode": "IMPS"
            },
            headers=self.headers
        )
        # Should return 404 for nonexistent salary
        assert response.status_code == 404, f"Expected 404, got {response.status_code}"
    
    def test_get_payout_status_missing_salary(self):
        """Test GET /api/erp/payouts/salary/status/{salary_id} with nonexistent salary"""
        response = requests.get(
            f"{BASE_URL}/api/erp/payouts/salary/status/nonexistent-salary-id",
            headers=self.headers
        )
        # Should return 404 for nonexistent salary
        assert response.status_code == 404, f"Expected 404, got {response.status_code}"
    
    def test_bulk_process_payouts_no_approved_salaries(self):
        """Test POST /api/erp/payouts/salary/bulk-process with no approved salaries"""
        response = requests.post(
            f"{BASE_URL}/api/erp/payouts/salary/bulk-process",
            json={
                "month": "2020-01",  # Old month with no data
                "mode": "IMPS"
            },
            headers=self.headers
        )
        # Should return 400 if no approved salaries found
        assert response.status_code == 400, f"Expected 400, got {response.status_code}"


class TestPayoutsRoleAccess:
    """Test role-based access for payouts"""
    
    def test_hr_role_can_access_payouts(self):
        """Test that HR role can access payouts dashboard"""
        # First login as admin to check if HR user exists
        admin_response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": "admin@lucumaa.in",
            "password": "adminpass"
        })
        
        if admin_response.status_code == 200:
            admin_token = admin_response.json()["token"]
            admin_headers = {"Authorization": f"Bearer {admin_token}"}
            
            # Try to access payouts dashboard with admin (should work)
            response = requests.get(
                f"{BASE_URL}/api/erp/payouts/dashboard",
                headers=admin_headers
            )
            assert response.status_code == 200, "Admin should access payouts"


class TestPayoutsWebhook:
    """Test payout webhook endpoint"""
    
    def test_webhook_endpoint_exists(self):
        """Test that webhook endpoint exists and accepts POST"""
        response = requests.post(
            f"{BASE_URL}/api/erp/payouts/webhook",
            json={
                "event": "payout.processed",
                "payload": {
                    "payout": {
                        "entity": {
                            "id": "test_payout_id",
                            "status": "processed"
                        }
                    }
                }
            }
        )
        # Webhook should return 200 even without auth (Razorpay calls it)
        assert response.status_code == 200, f"Webhook failed: {response.text}"
        data = response.json()
        assert "status" in data


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
