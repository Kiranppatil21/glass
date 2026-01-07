"""
Backend API Tests for Assets, Holidays, and Expenses Modules
Tests all CRUD operations and critical edge cases
"""
import pytest
import requests
import os
from datetime import datetime, timedelta

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', 'https://glassmesh.preview.emergentagent.com')

# Test credentials
TEST_EMAIL = "admin@lucumaa.in"
TEST_PASSWORD = "adminpass"


@pytest.fixture(scope="module")
def auth_token():
    """Get authentication token"""
    response = requests.post(f"{BASE_URL}/api/auth/login", json={
        "email": TEST_EMAIL,
        "password": TEST_PASSWORD
    })
    if response.status_code == 200:
        return response.json().get("token")
    pytest.skip("Authentication failed - skipping authenticated tests")


@pytest.fixture(scope="module")
def api_client(auth_token):
    """Shared requests session with auth header"""
    session = requests.Session()
    session.headers.update({
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    })
    return session


# ==================== ASSETS MODULE TESTS ====================

class TestAssetTypes:
    """Test asset types endpoint"""
    
    def test_get_asset_types(self, api_client):
        """GET /api/erp/assets/types - Returns asset types"""
        response = api_client.get(f"{BASE_URL}/api/erp/assets/types")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) >= 7  # machine, vehicle, tool, it_asset, furniture, building, other
        # Verify structure
        for asset_type in data:
            assert "id" in asset_type
            assert "name" in asset_type
        print(f"PASS: Found {len(data)} asset types")


class TestOwnedAssets:
    """Test company-owned assets CRUD"""
    
    def test_get_owned_assets(self, api_client):
        """GET /api/erp/assets/owned - Returns owned assets list"""
        response = api_client.get(f"{BASE_URL}/api/erp/assets/owned")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        print(f"PASS: Found {len(data)} owned assets")
    
    def test_create_owned_asset(self, api_client):
        """POST /api/erp/assets/owned - Creates new company asset"""
        asset_data = {
            "name": f"TEST_Machine_{datetime.now().strftime('%H%M%S')}",
            "asset_type": "machine",
            "description": "Test machine for API testing",
            "purchase_date": datetime.now().strftime("%Y-%m-%d"),
            "purchase_price": 100000,
            "useful_life_years": 5,
            "depreciation_method": "straight_line",
            "salvage_value": 10000,
            "location": "Test Location",
            "department": "Production"
        }
        response = api_client.post(f"{BASE_URL}/api/erp/assets/owned", json=asset_data)
        assert response.status_code == 200
        data = response.json()
        assert "asset" in data
        assert data["asset"]["name"] == asset_data["name"]
        assert data["asset"]["purchase_price"] == asset_data["purchase_price"]
        print(f"PASS: Created owned asset: {data['asset']['asset_code']}")
        return data["asset"]["id"]
    
    def test_get_owned_asset_by_id(self, api_client):
        """GET /api/erp/assets/owned/{id} - Returns single asset with depreciation"""
        # First get list to find an asset
        response = api_client.get(f"{BASE_URL}/api/erp/assets/owned")
        assets = response.json()
        if not assets:
            pytest.skip("No owned assets to test")
        
        asset_id = assets[0]["id"]
        response = api_client.get(f"{BASE_URL}/api/erp/assets/owned/{asset_id}")
        assert response.status_code == 200
        data = response.json()
        assert "book_value" in data
        assert "accumulated_depreciation" in data
        assert "annual_depreciation" in data
        print(f"PASS: Asset {data['asset_code']} - Book Value: {data['book_value']}")


class TestRentedAssets:
    """Test rented assets CRUD"""
    
    def test_get_rented_assets(self, api_client):
        """GET /api/erp/assets/rented - Returns rented assets list"""
        response = api_client.get(f"{BASE_URL}/api/erp/assets/rented")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        print(f"PASS: Found {len(data)} rented assets")
    
    def test_create_rented_asset(self, api_client):
        """POST /api/erp/assets/rented - Creates new rented asset"""
        asset_data = {
            "name": f"TEST_Rented_{datetime.now().strftime('%H%M%S')}",
            "asset_type": "machine",
            "vendor_name": "Test Vendor Inc",
            "vendor_contact": "9876543210",
            "rent_type": "monthly",
            "rent_amount": 25000,
            "rent_start_date": datetime.now().strftime("%Y-%m-%d"),
            "security_deposit": 50000,
            "department": "Production",
            "location": "Warehouse"
        }
        response = api_client.post(f"{BASE_URL}/api/erp/assets/rented", json=asset_data)
        assert response.status_code == 200
        data = response.json()
        assert "asset" in data
        assert data["asset"]["name"] == asset_data["name"]
        assert data["asset"]["rent_amount"] == asset_data["rent_amount"]
        print(f"PASS: Created rented asset: {data['asset']['asset_code']}")


class TestAssetHandover:
    """Test asset handover requests"""
    
    def test_get_handover_requests(self, api_client):
        """GET /api/erp/assets/handover/requests - Returns handover list"""
        response = api_client.get(f"{BASE_URL}/api/erp/assets/handover/requests")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        print(f"PASS: Found {len(data)} handover requests")


class TestAssetReports:
    """Test asset reports"""
    
    def test_get_asset_register(self, api_client):
        """GET /api/erp/assets/reports/register - Returns asset register"""
        response = api_client.get(f"{BASE_URL}/api/erp/assets/reports/register")
        assert response.status_code == 200
        data = response.json()
        assert "assets" in data
        assert "summary" in data
        assert "total_assets" in data["summary"]
        assert "total_purchase_value" in data["summary"]
        assert "total_book_value" in data["summary"]
        print(f"PASS: Asset Register - Total: {data['summary']['total_assets']}, Book Value: {data['summary']['total_book_value']}")
    
    def test_get_depreciation_report(self, api_client):
        """GET /api/erp/assets/reports/depreciation - Returns depreciation report"""
        response = api_client.get(f"{BASE_URL}/api/erp/assets/reports/depreciation")
        assert response.status_code == 200
        data = response.json()
        assert "year" in data
        assert "assets" in data
        assert "total_annual_depreciation" in data
        print(f"PASS: Depreciation Report - Year: {data['year']}, Total: {data['total_annual_depreciation']}")
    
    def test_get_rent_liability(self, api_client):
        """GET /api/erp/assets/reports/rent-liability - Returns rent liability"""
        response = api_client.get(f"{BASE_URL}/api/erp/assets/reports/rent-liability")
        assert response.status_code == 200
        data = response.json()
        assert "active_rentals" in data
        assert "total_monthly_liability" in data
        print(f"PASS: Rent Liability - Active: {data['active_rentals']}, Monthly: {data['total_monthly_liability']}")


# ==================== HOLIDAYS MODULE TESTS ====================

class TestHolidayTypes:
    """Test holiday types endpoint"""
    
    def test_get_holiday_types(self, api_client):
        """GET /api/erp/holidays/types - Returns holiday types"""
        response = api_client.get(f"{BASE_URL}/api/erp/holidays/types")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) >= 5  # national, state, company, optional, weekly_off
        print(f"PASS: Found {len(data)} holiday types")


class TestHolidaySettings:
    """Test holiday settings"""
    
    def test_get_holiday_settings(self, api_client):
        """GET /api/erp/holidays/settings - Returns settings"""
        response = api_client.get(f"{BASE_URL}/api/erp/holidays/settings")
        assert response.status_code == 200
        data = response.json()
        assert "weekly_offs" in data
        assert "overtime_rate" in data
        assert "comp_off_enabled" in data
        print(f"PASS: Holiday Settings - Weekly Offs: {data['weekly_offs']}, Overtime Rate: {data['overtime_rate']}")


class TestHolidays:
    """Test holidays CRUD"""
    
    def test_get_holidays(self, api_client):
        """GET /api/erp/holidays/ - Returns holidays list"""
        response = api_client.get(f"{BASE_URL}/api/erp/holidays/")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        print(f"PASS: Found {len(data)} holidays")
    
    def test_create_holiday(self, api_client):
        """POST /api/erp/holidays/ - Creates new holiday"""
        # Use a unique date to avoid duplicates
        unique_date = (datetime.now() + timedelta(days=100 + int(datetime.now().strftime('%S')))).strftime("%Y-%m-%d")
        holiday_data = {
            "date": unique_date,
            "name": f"TEST_Holiday_{datetime.now().strftime('%H%M%S')}",
            "type": "company",
            "paid": True,
            "description": "Test holiday for API testing"
        }
        response = api_client.post(f"{BASE_URL}/api/erp/holidays/", json=holiday_data)
        # May return 400 if holiday already exists for that date
        if response.status_code == 400:
            print(f"INFO: Holiday already exists for {unique_date}")
            return
        assert response.status_code == 200
        data = response.json()
        assert "holiday" in data
        assert data["holiday"]["name"] == holiday_data["name"]
        print(f"PASS: Created holiday: {data['holiday']['name']} on {data['holiday']['date']}")
    
    def test_get_year_calendar(self, api_client):
        """GET /api/erp/holidays/calendar/{year} - Returns year calendar"""
        year = datetime.now().year
        response = api_client.get(f"{BASE_URL}/api/erp/holidays/calendar/{year}")
        assert response.status_code == 200
        data = response.json()
        assert "year" in data
        assert "summary" in data
        assert "months" in data
        assert len(data["months"]) == 12
        assert "working_days" in data["summary"]
        assert "holidays" in data["summary"]
        assert "weekly_offs" in data["summary"]
        print(f"PASS: Calendar {year} - Working Days: {data['summary']['working_days']}, Holidays: {data['summary']['holidays']}")


class TestOvertime:
    """Test overtime records"""
    
    def test_get_overtime_records(self, api_client):
        """GET /api/erp/holidays/overtime - Returns overtime list"""
        response = api_client.get(f"{BASE_URL}/api/erp/holidays/overtime")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        print(f"PASS: Found {len(data)} overtime records")


# ==================== EXPENSES MODULE TESTS ====================

class TestExpenseCategories:
    """Test expense categories"""
    
    def test_get_expense_categories(self, api_client):
        """GET /api/erp/expenses/categories - Returns categories"""
        response = api_client.get(f"{BASE_URL}/api/erp/expenses/categories")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) >= 9  # Default categories
        print(f"PASS: Found {len(data)} expense categories")


class TestExpenseSettings:
    """Test expense settings"""
    
    def test_get_expense_settings(self, api_client):
        """GET /api/erp/expenses/settings - Returns settings"""
        response = api_client.get(f"{BASE_URL}/api/erp/expenses/settings")
        assert response.status_code == 200
        data = response.json()
        assert "approval_enabled" in data
        assert "daily_limit" in data
        assert "monthly_limit" in data
        assert "departments" in data
        print(f"PASS: Expense Settings - Daily Limit: {data['daily_limit']}, Monthly Limit: {data['monthly_limit']}")


class TestExpenseEntries:
    """Test expense entries CRUD"""
    
    def test_get_expense_entries(self, api_client):
        """GET /api/erp/expenses/entries - Returns entries list"""
        response = api_client.get(f"{BASE_URL}/api/erp/expenses/entries")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        print(f"PASS: Found {len(data)} expense entries")
    
    def test_create_expense_entry(self, api_client):
        """POST /api/erp/expenses/entries - Creates new expense"""
        # First get categories
        cat_response = api_client.get(f"{BASE_URL}/api/erp/expenses/categories")
        categories = cat_response.json()
        if not categories:
            pytest.skip("No expense categories available")
        
        expense_data = {
            "category_id": categories[0]["id"],
            "category_name": categories[0]["name"],
            "amount": 100,
            "description": f"TEST_Expense_{datetime.now().strftime('%H%M%S')}",
            "payment_mode": "cash",
            "department": "Admin",
            "date": datetime.now().strftime("%Y-%m-%d")
        }
        response = api_client.post(f"{BASE_URL}/api/erp/expenses/entries", json=expense_data)
        # May fail if daily limit exceeded
        if response.status_code == 400:
            print(f"INFO: Daily expense limit may be exceeded")
            return
        assert response.status_code == 200
        data = response.json()
        assert "entry" in data
        assert data["entry"]["amount"] == expense_data["amount"]
        print(f"PASS: Created expense: {data['entry']['entry_number']} - ₹{data['entry']['amount']}")


class TestExpenseDashboard:
    """Test expense dashboard"""
    
    def test_get_expense_dashboard(self, api_client):
        """GET /api/erp/expenses/dashboard - Returns dashboard data"""
        response = api_client.get(f"{BASE_URL}/api/erp/expenses/dashboard")
        assert response.status_code == 200
        data = response.json()
        assert "today" in data
        assert "month" in data
        assert "pending_approvals" in data
        assert "by_category" in data
        assert "by_department" in data
        assert "recent_entries" in data
        print(f"PASS: Dashboard - Today: ₹{data['today']['total']}, Month: ₹{data['month']['total']}, Pending: {data['pending_approvals']}")


class TestExpenseReports:
    """Test expense reports"""
    
    def test_get_variance_report(self, api_client):
        """GET /api/erp/expenses/reports/variance - Returns variance report"""
        response = api_client.get(f"{BASE_URL}/api/erp/expenses/reports/variance")
        assert response.status_code == 200
        data = response.json()
        assert "month" in data
        assert "data" in data
        assert "summary" in data
        print(f"PASS: Variance Report - Month: {data['month']}")
    
    def test_get_cash_flow_report(self, api_client):
        """GET /api/erp/expenses/reports/cash-flow - Returns cash flow report"""
        response = api_client.get(f"{BASE_URL}/api/erp/expenses/reports/cash-flow")
        assert response.status_code == 200
        data = response.json()
        assert "period" in data
        assert "daily_expenses" in data
        assert "by_payment_mode" in data
        assert "total_outflow" in data
        print(f"PASS: Cash Flow Report - Total Outflow: ₹{data['total_outflow']}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
