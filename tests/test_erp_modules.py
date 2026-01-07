"""
Test suite for ERP Modules: Expenses, Assets, and Holidays
Tests all CRUD operations and critical edge cases for the 5 new modules:
1. Daily Expense Management
2. Rented Asset Management
3. Company Owned Asset Management
4. Asset Handover to Employee
5. Company Holiday & Salary Impact Module
"""
import pytest
import requests
import os
from datetime import datetime, timedelta

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', '').rstrip('/')

# ==================== FIXTURES ====================

@pytest.fixture(scope="module")
def api_client():
    """Shared requests session"""
    session = requests.Session()
    session.headers.update({"Content-Type": "application/json"})
    return session

@pytest.fixture(scope="module")
def auth_token(api_client):
    """Get authentication token for admin user"""
    response = api_client.post(f"{BASE_URL}/api/auth/login", json={
        "email": "admin@lucumaa.in",
        "password": "adminpass"
    })
    if response.status_code == 200:
        return response.json().get("token")
    pytest.skip("Authentication failed - skipping authenticated tests")

@pytest.fixture(scope="module")
def authenticated_client(api_client, auth_token):
    """Session with auth header"""
    api_client.headers.update({"Authorization": f"Bearer {auth_token}"})
    return api_client


# ==================== EXPENSE MODULE TESTS ====================

class TestExpenseCategories:
    """Test expense categories endpoints"""
    
    def test_get_expense_categories(self, authenticated_client):
        """GET /api/erp/expenses/categories - should return list of categories"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/expenses/categories")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) >= 9  # Default categories
        # Verify category structure
        if data:
            cat = data[0]
            assert "id" in cat
            assert "name" in cat
            assert "icon" in cat
            assert "color" in cat

    def test_expense_categories_have_required_fields(self, authenticated_client):
        """Verify all default categories are present"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/expenses/categories")
        data = response.json()
        category_ids = [c["id"] for c in data]
        expected_ids = ["electricity", "fuel", "repair", "labour", "consumables", "transport", "raw_material", "office", "misc"]
        for expected in expected_ids:
            assert expected in category_ids, f"Missing category: {expected}"


class TestExpenseSettings:
    """Test expense settings endpoints"""
    
    def test_get_expense_settings(self, authenticated_client):
        """GET /api/erp/expenses/settings - should return settings"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/expenses/settings")
        assert response.status_code == 200
        data = response.json()
        assert "approval_enabled" in data
        assert "approval_levels" in data
        assert "admin_direct_approval" in data
        assert "daily_limit" in data
        assert "monthly_limit" in data
        assert "departments" in data


class TestExpenseDashboard:
    """Test expense dashboard endpoint"""
    
    def test_get_expense_dashboard(self, authenticated_client):
        """GET /api/erp/expenses/dashboard - should return dashboard stats"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/expenses/dashboard")
        assert response.status_code == 200
        data = response.json()
        # Verify dashboard structure
        assert "today" in data
        assert "month" in data
        assert "pending_approvals" in data
        assert "by_category" in data
        assert "by_department" in data
        assert "recent_entries" in data
        # Verify today stats structure
        assert "total" in data["today"]
        assert "count" in data["today"]
        assert "limit" in data["today"]

    def test_dashboard_category_breakdown(self, authenticated_client):
        """Verify category breakdown structure"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/expenses/dashboard")
        data = response.json()
        if data["by_category"]:
            cat = data["by_category"][0]
            assert "category" in cat
            assert "total" in cat
            assert "count" in cat

    def test_dashboard_department_breakdown(self, authenticated_client):
        """Verify department breakdown structure"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/expenses/dashboard")
        data = response.json()
        if data["by_department"]:
            dept = data["by_department"][0]
            assert "department" in dept
            assert "total" in dept


class TestExpenseEntries:
    """Test expense entry CRUD operations"""
    
    def test_create_expense_entry(self, authenticated_client):
        """POST /api/erp/expenses/entries - create new expense"""
        expense_data = {
            "category_id": "electricity",
            "category_name": "Electricity",
            "amount": 2500,
            "description": "TEST_Monthly electricity bill",
            "payment_mode": "bank",
            "department": "Admin",
            "vendor_name": "State Electricity Board",
            "date": datetime.now().strftime("%Y-%m-%d")
        }
        response = authenticated_client.post(f"{BASE_URL}/api/erp/expenses/entries", json=expense_data)
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        assert "entry" in data
        entry = data["entry"]
        assert entry["amount"] == 2500
        assert entry["category_id"] == "electricity"
        assert "entry_number" in entry
        assert entry["entry_number"].startswith("EXP")

    def test_create_expense_invalid_amount(self, authenticated_client):
        """POST /api/erp/expenses/entries - should reject invalid amount"""
        expense_data = {
            "category_id": "fuel",
            "category_name": "Fuel",
            "amount": 0,
            "description": "TEST_Invalid expense",
            "payment_mode": "cash",
            "department": "Admin"
        }
        response = authenticated_client.post(f"{BASE_URL}/api/erp/expenses/entries", json=expense_data)
        assert response.status_code == 400

    def test_get_expense_entries(self, authenticated_client):
        """GET /api/erp/expenses/entries - should return list of entries"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/expenses/entries")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_get_expense_entries_with_filter(self, authenticated_client):
        """GET /api/erp/expenses/entries with status filter"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/expenses/entries", params={"status": "approved"})
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        # All entries should be approved
        for entry in data:
            assert entry["status"] == "approved"


class TestExpenseApproval:
    """Test expense approval workflow"""
    
    def test_approve_expense_entry(self, authenticated_client):
        """POST /api/erp/expenses/entries/{id}/approve - approve expense"""
        # First create a pending expense (as non-admin would)
        # Since admin auto-approves, we'll test the approval endpoint directly
        # Get a pending entry if exists
        response = authenticated_client.get(f"{BASE_URL}/api/erp/expenses/entries", params={"status": "pending"})
        entries = response.json()
        
        if entries:
            entry_id = entries[0]["id"]
            approve_response = authenticated_client.post(
                f"{BASE_URL}/api/erp/expenses/entries/{entry_id}/approve",
                json={"action": "approve", "note": "TEST_Approved by admin"}
            )
            assert approve_response.status_code == 200
            data = approve_response.json()
            assert "message" in data
            assert "new_status" in data

    def test_approve_nonexistent_entry(self, authenticated_client):
        """POST /api/erp/expenses/entries/{id}/approve - should return 404"""
        response = authenticated_client.post(
            f"{BASE_URL}/api/erp/expenses/entries/nonexistent-id/approve",
            json={"action": "approve"}
        )
        assert response.status_code == 404


# ==================== ASSET MODULE TESTS ====================

class TestAssetTypes:
    """Test asset types endpoint"""
    
    def test_get_asset_types(self, authenticated_client):
        """GET /api/erp/assets/types - should return list of asset types"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/assets/types")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) >= 7  # Default types
        # Verify structure
        if data:
            asset_type = data[0]
            assert "id" in asset_type
            assert "name" in asset_type
            assert "icon" in asset_type


class TestOwnedAssets:
    """Test company-owned asset CRUD operations"""
    
    def test_create_owned_asset(self, authenticated_client):
        """POST /api/erp/assets/owned - create company asset"""
        asset_data = {
            "name": "TEST_CNC Glass Cutting Machine",
            "asset_type": "machine",
            "description": "Automated glass cutting machine",
            "purchase_date": "2024-01-15",
            "purchase_price": 750000,
            "vendor_name": "Industrial Machines Ltd",
            "invoice_number": "INV-2024-001",
            "location": "Production Floor A",
            "department": "Production",
            "depreciation_method": "straight_line",
            "useful_life_years": 10,
            "salvage_value": 50000,
            "condition": "excellent"
        }
        response = authenticated_client.post(f"{BASE_URL}/api/erp/assets/owned", json=asset_data)
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        assert "asset" in data
        asset = data["asset"]
        assert asset["name"] == "TEST_CNC Glass Cutting Machine"
        assert asset["purchase_price"] == 750000
        assert "asset_code" in asset
        assert asset["asset_code"].startswith("AST")

    def test_create_owned_asset_wdv_depreciation(self, authenticated_client):
        """POST /api/erp/assets/owned - create asset with WDV depreciation"""
        asset_data = {
            "name": "TEST_Delivery Vehicle",
            "asset_type": "vehicle",
            "purchase_date": "2024-06-01",
            "purchase_price": 1200000,
            "depreciation_method": "wdv",
            "depreciation_rate": 15,
            "salvage_value": 100000,
            "condition": "good"
        }
        response = authenticated_client.post(f"{BASE_URL}/api/erp/assets/owned", json=asset_data)
        assert response.status_code == 200
        data = response.json()
        assert data["asset"]["depreciation_method"] == "wdv"
        assert data["asset"]["depreciation_rate"] == 15

    def test_get_owned_assets(self, authenticated_client):
        """GET /api/erp/assets/owned - should return list with depreciation"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/assets/owned")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        # Verify depreciation calculation is included
        if data:
            asset = data[0]
            assert "book_value" in asset
            assert "accumulated_depreciation" in asset

    def test_get_owned_assets_with_filter(self, authenticated_client):
        """GET /api/erp/assets/owned with type filter"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/assets/owned", params={"asset_type": "machine"})
        assert response.status_code == 200
        data = response.json()
        for asset in data:
            assert asset["asset_type"] == "machine"


class TestRentedAssets:
    """Test rented asset CRUD operations"""
    
    def test_create_rented_asset(self, authenticated_client):
        """POST /api/erp/assets/rented - create rented asset"""
        asset_data = {
            "name": "TEST_Forklift",
            "asset_type": "machine",
            "vendor_name": "Equipment Rentals Inc",
            "vendor_contact": "9876543210",
            "rent_type": "monthly",
            "rent_amount": 25000,
            "rent_start_date": "2024-01-01",
            "rent_end_date": "2024-12-31",
            "security_deposit": 50000,
            "deposit_paid": True,
            "department": "Store",
            "location": "Warehouse"
        }
        response = authenticated_client.post(f"{BASE_URL}/api/erp/assets/rented", json=asset_data)
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        assert "asset" in data
        asset = data["asset"]
        assert asset["name"] == "TEST_Forklift"
        assert asset["rent_amount"] == 25000
        assert "asset_code" in asset
        assert asset["asset_code"].startswith("RNT")

    def test_get_rented_assets(self, authenticated_client):
        """GET /api/erp/assets/rented - should return list"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/assets/rented")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        # Verify days_remaining is calculated
        if data:
            asset = data[0]
            assert "days_remaining" in asset


class TestAssetHandover:
    """Test asset handover to employee"""
    
    def test_create_handover_request(self, authenticated_client):
        """POST /api/erp/assets/handover/request - create handover request"""
        # First get an asset to handover
        assets_response = authenticated_client.get(f"{BASE_URL}/api/erp/assets/owned")
        assets = assets_response.json()
        
        if assets:
            asset = assets[0]
            handover_data = {
                "asset_id": asset["id"],
                "asset_name": asset["name"],
                "asset_code": asset["asset_code"],
                "employee_id": "TEST_EMP001",
                "employee_name": "Test Employee",
                "department": "Production",
                "purpose": "Daily operations",
                "expected_return_date": (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
            }
            response = authenticated_client.post(f"{BASE_URL}/api/erp/assets/handover/request", json=handover_data)
            assert response.status_code == 200
            data = response.json()
            assert "message" in data
            assert "request" in data
            request = data["request"]
            assert request["status"] == "pending"
            assert "request_number" in request
            assert request["request_number"].startswith("HO")

    def test_get_handover_requests(self, authenticated_client):
        """GET /api/erp/assets/handover/requests - should return list"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/assets/handover/requests")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)


class TestAssetReports:
    """Test asset reports"""
    
    def test_get_asset_register(self, authenticated_client):
        """GET /api/erp/assets/reports/register - should return asset register"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/assets/reports/register")
        assert response.status_code == 200
        data = response.json()
        assert "assets" in data
        assert "summary" in data
        summary = data["summary"]
        assert "total_assets" in summary
        assert "total_purchase_value" in summary
        assert "total_book_value" in summary
        assert "total_accumulated_depreciation" in summary


# ==================== HOLIDAY MODULE TESTS ====================

class TestHolidayTypes:
    """Test holiday types endpoint"""
    
    def test_get_holiday_types(self, authenticated_client):
        """GET /api/erp/holidays/types - should return list of holiday types"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/holidays/types")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) >= 5  # Default types
        # Verify structure
        if data:
            holiday_type = data[0]
            assert "id" in holiday_type
            assert "name" in holiday_type
            assert "color" in holiday_type
            assert "paid" in holiday_type


class TestHolidaySettings:
    """Test holiday settings"""
    
    def test_get_holiday_settings(self, authenticated_client):
        """GET /api/erp/holidays/settings - should return settings"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/holidays/settings")
        assert response.status_code == 200
        data = response.json()
        assert "weekly_offs" in data
        assert "overtime_rate" in data
        assert "comp_off_enabled" in data


class TestHolidayCalendar:
    """Test holiday calendar"""
    
    def test_create_holiday(self, authenticated_client):
        """POST /api/erp/holidays - create holiday"""
        # Use a unique future date to avoid duplicates
        import random
        holiday_date = (datetime.now() + timedelta(days=100 + random.randint(1, 100))).strftime("%Y-%m-%d")
        holiday_data = {
            "date": holiday_date,
            "name": f"TEST_Company Event {random.randint(1000, 9999)}",
            "type": "company",
            "paid": True,
            "description": "Annual company celebration"
        }
        # Note: trailing slash required for POST
        response = authenticated_client.post(f"{BASE_URL}/api/erp/holidays/", json=holiday_data)
        # May return 400 if holiday already exists
        assert response.status_code in [200, 400]
        if response.status_code == 200:
            data = response.json()
            assert "message" in data
            assert "holiday" in data
        else:
            # 400 means duplicate - that's acceptable
            data = response.json()
            assert "detail" in data

    def test_get_holidays(self, authenticated_client):
        """GET /api/erp/holidays - should return list"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/holidays")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_get_year_calendar(self, authenticated_client):
        """GET /api/erp/holidays/calendar/{year} - should return full calendar"""
        year = datetime.now().year
        response = authenticated_client.get(f"{BASE_URL}/api/erp/holidays/calendar/{year}")
        assert response.status_code == 200
        data = response.json()
        assert "year" in data
        assert data["year"] == year
        assert "summary" in data
        assert "months" in data
        assert len(data["months"]) == 12
        # Verify summary structure
        summary = data["summary"]
        assert "total_days" in summary
        assert "working_days" in summary
        assert "holidays" in summary
        assert "weekly_offs" in summary

    def test_calendar_month_structure(self, authenticated_client):
        """Verify calendar month structure"""
        year = datetime.now().year
        response = authenticated_client.get(f"{BASE_URL}/api/erp/holidays/calendar/{year}")
        data = response.json()
        month = data["months"][0]  # January
        assert "month" in month
        assert "month_name" in month
        assert "total_days" in month
        assert "working_days" in month
        assert "holidays" in month
        assert "weekly_offs" in month
        assert "days" in month


class TestSalaryImpact:
    """Test salary impact calculation"""
    
    def test_get_salary_impact(self, authenticated_client):
        """GET /api/erp/holidays/salary-impact/{empId} - should return salary impact"""
        # First create an employee if needed
        emp_response = authenticated_client.get(f"{BASE_URL}/api/erp/hr/employees")
        employees = emp_response.json()
        
        if employees:
            emp_id = employees[0]["id"]
            month = datetime.now().strftime("%Y-%m")
            response = authenticated_client.get(
                f"{BASE_URL}/api/erp/holidays/salary-impact/{emp_id}",
                params={"month": month}
            )
            assert response.status_code == 200
            data = response.json()
            assert "employee_id" in data
            assert "month" in data
            assert "days_breakdown" in data
            assert "overtime" in data
            assert "deductions" in data
            assert "net_impact" in data
        else:
            pytest.skip("No employees found for salary impact test")

    def test_salary_impact_nonexistent_employee(self, authenticated_client):
        """GET /api/erp/holidays/salary-impact/{empId} - should return 404"""
        month = datetime.now().strftime("%Y-%m")
        response = authenticated_client.get(
            f"{BASE_URL}/api/erp/holidays/salary-impact/nonexistent-emp",
            params={"month": month}
        )
        assert response.status_code == 404


# ==================== INTEGRATION TESTS ====================

class TestExpenseAssetIntegration:
    """Test integration between expense and asset modules"""
    
    def test_expense_entry_persists(self, authenticated_client):
        """Verify expense entry is persisted and retrievable"""
        # Create expense with small amount to avoid daily limit
        expense_data = {
            "category_id": "repair",
            "category_name": "Repair & Maintenance",
            "amount": 100,  # Small amount to avoid daily limit
            "description": "TEST_Small machine repair",
            "payment_mode": "cash",
            "department": "Production"
        }
        create_response = authenticated_client.post(f"{BASE_URL}/api/erp/expenses/entries", json=expense_data)
        # May fail if daily limit exceeded
        if create_response.status_code == 400:
            pytest.skip("Daily expense limit exceeded - skipping persistence test")
        assert create_response.status_code == 200
        entry_id = create_response.json()["entry"]["id"]
        
        # Retrieve and verify
        get_response = authenticated_client.get(f"{BASE_URL}/api/erp/expenses/entries/{entry_id}")
        assert get_response.status_code == 200
        entry = get_response.json()
        assert entry["amount"] == 100
        assert entry["description"] == "TEST_Small machine repair"


class TestDepreciationCalculation:
    """Test depreciation calculation for assets"""
    
    def test_straight_line_depreciation(self, authenticated_client):
        """Verify straight line depreciation calculation"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/assets/owned")
        assets = response.json()
        
        sl_assets = [a for a in assets if a.get("depreciation_method") == "straight_line"]
        if sl_assets:
            asset = sl_assets[0]
            # Book value should be less than or equal to purchase price
            assert asset["book_value"] <= asset["purchase_price"]
            # Accumulated depreciation should be non-negative
            assert asset["accumulated_depreciation"] >= 0

    def test_wdv_depreciation(self, authenticated_client):
        """Verify WDV depreciation calculation"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/assets/owned")
        assets = response.json()
        
        wdv_assets = [a for a in assets if a.get("depreciation_method") == "wdv"]
        if wdv_assets:
            asset = wdv_assets[0]
            # Book value should be less than or equal to purchase price
            assert asset["book_value"] <= asset["purchase_price"]


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
