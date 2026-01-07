"""
Comprehensive ERP Backend API Tests - Pre-deployment Verification
Tests all modules: Admin, CRM, Production, HR, Inventory, Purchase, Accounts, Payouts, Expenses, Assets, Holidays, Wallet
"""
import pytest
import requests
import os
from datetime import datetime, timedelta

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', '').rstrip('/')

class TestAuthentication:
    """Authentication tests"""
    
    def test_login_admin(self):
        """Test admin login"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": "admin@lucumaa.in",
            "password": "adminpass"
        })
        assert response.status_code == 200
        data = response.json()
        assert "token" in data
        assert "user" in data
        print(f"✅ Admin login successful - token received")
        return data["token"]
    
    def test_login_invalid_credentials(self):
        """Test login with invalid credentials"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": "invalid@test.com",
            "password": "wrongpass"
        })
        assert response.status_code in [401, 400]
        print(f"✅ Invalid login rejected correctly")


class TestAdminDashboard:
    """Admin Dashboard API tests"""
    
    def test_get_dashboard(self):
        """Test admin dashboard stats"""
        response = requests.get(f"{BASE_URL}/api/erp/admin/dashboard")
        assert response.status_code == 200
        data = response.json()
        assert "total_orders" in data or "stats" in data
        print(f"✅ Admin dashboard: {data}")
    
    def test_get_revenue_chart(self):
        """Test revenue chart data"""
        response = requests.get(f"{BASE_URL}/api/erp/admin/charts/revenue", params={"months": 6})
        assert response.status_code == 200
        data = response.json()
        print(f"✅ Revenue chart data received")
    
    def test_get_production_chart(self):
        """Test production pipeline chart"""
        response = requests.get(f"{BASE_URL}/api/erp/admin/charts/production")
        assert response.status_code == 200
        print(f"✅ Production chart data received")
    
    def test_get_expense_chart(self):
        """Test expense pie chart"""
        response = requests.get(f"{BASE_URL}/api/erp/admin/charts/expenses")
        assert response.status_code == 200
        print(f"✅ Expense chart data received")
    
    def test_get_top_customers(self):
        """Test top customers data"""
        response = requests.get(f"{BASE_URL}/api/erp/admin/charts/top-customers")
        assert response.status_code == 200
        print(f"✅ Top customers data received")


class TestCRMModule:
    """CRM Module API tests"""
    
    def test_get_leads(self):
        """Test get all leads"""
        response = requests.get(f"{BASE_URL}/api/erp/crm/leads")
        assert response.status_code == 200
        data = response.json()
        print(f"✅ CRM leads: {len(data) if isinstance(data, list) else 'data received'}")
    
    def test_create_lead(self):
        """Test create new lead"""
        lead_data = {
            "name": "TEST_Lead_Comprehensive",
            "email": f"test_lead_{datetime.now().timestamp()}@test.com",
            "phone": "+919876543210",
            "company": "Test Company",
            "source": "Website",
            "status": "new"
        }
        response = requests.post(f"{BASE_URL}/api/erp/crm/leads", json=lead_data)
        assert response.status_code in [200, 201]
        data = response.json()
        print(f"✅ Lead created: {data.get('name', data.get('_id', 'success'))}")
        return data


class TestProductionModule:
    """Production Module API tests"""
    
    def test_get_orders(self):
        """Test get production orders (job cards)"""
        response = requests.get(f"{BASE_URL}/api/erp/production/orders")
        assert response.status_code == 200
        data = response.json()
        print(f"✅ Production orders: {len(data) if isinstance(data, list) else 'data received'}")
        return data
    
    def test_create_order(self):
        """Test create new production order"""
        order_data = {
            "customer_name": "TEST_Customer_Comprehensive",
            "customer_phone": "+919876543210",
            "glass_type": "Toughened",
            "thickness": 12,
            "width": 1000,
            "height": 500,
            "quantity": 2,
            "edge_work": "Polished",
            "notes": "Test order for comprehensive testing"
        }
        response = requests.post(f"{BASE_URL}/api/erp/production/orders", json=order_data)
        assert response.status_code in [200, 201]
        data = response.json()
        print(f"✅ Production order created: {data.get('job_card_number', data.get('_id', 'success'))}")
        return data


class TestHRModule:
    """HR Module API tests"""
    
    def test_get_employees(self):
        """Test get all employees"""
        response = requests.get(f"{BASE_URL}/api/erp/hr/employees")
        assert response.status_code == 200
        data = response.json()
        print(f"✅ HR employees: {len(data) if isinstance(data, list) else 'data received'}")
        return data
    
    def test_create_employee(self):
        """Test create new employee"""
        emp_data = {
            "name": "TEST_Employee_Comprehensive",
            "email": f"test_emp_{datetime.now().timestamp()}@test.com",
            "phone": "+919876543210",
            "department": "Production",
            "designation": "Operator",
            "salary": 25000,
            "join_date": datetime.now().strftime("%Y-%m-%d")
        }
        response = requests.post(f"{BASE_URL}/api/erp/hr/employees", json=emp_data)
        assert response.status_code in [200, 201]
        data = response.json()
        print(f"✅ Employee created: {data.get('name', data.get('_id', 'success'))}")
        return data
    
    def test_get_attendance(self):
        """Test get attendance records"""
        response = requests.get(f"{BASE_URL}/api/erp/hr/attendance")
        assert response.status_code == 200
        print(f"✅ Attendance records received")
    
    def test_get_salaries(self):
        """Test get salary records"""
        response = requests.get(f"{BASE_URL}/api/erp/hr/salary")
        assert response.status_code == 200
        print(f"✅ Salary records received")


class TestInventoryModule:
    """Inventory Module API tests"""
    
    def test_get_materials(self):
        """Test get all materials"""
        response = requests.get(f"{BASE_URL}/api/erp/inventory/materials")
        assert response.status_code == 200
        data = response.json()
        print(f"✅ Inventory materials: {len(data) if isinstance(data, list) else 'data received'}")
        return data
    
    def test_create_material(self):
        """Test create new material"""
        material_data = {
            "name": "TEST_Material_Comprehensive",
            "sku": f"TEST-MAT-{int(datetime.now().timestamp())}",
            "category": "Glass",
            "unit": "sqft",
            "current_stock": 100,
            "min_stock": 10,
            "price": 500
        }
        response = requests.post(f"{BASE_URL}/api/erp/inventory/materials", json=material_data)
        assert response.status_code in [200, 201]
        data = response.json()
        print(f"✅ Material created: {data.get('name', data.get('_id', 'success'))}")
        return data
    
    def test_get_low_stock(self):
        """Test get low stock alerts"""
        response = requests.get(f"{BASE_URL}/api/erp/inventory/low-stock")
        assert response.status_code == 200
        print(f"✅ Low stock alerts received")


class TestPurchaseModule:
    """Purchase Module API tests"""
    
    def test_get_suppliers(self):
        """Test get all suppliers"""
        response = requests.get(f"{BASE_URL}/api/erp/purchase/suppliers")
        assert response.status_code == 200
        data = response.json()
        print(f"✅ Suppliers: {len(data) if isinstance(data, list) else 'data received'}")
        return data
    
    def test_get_purchase_orders(self):
        """Test get all purchase orders"""
        response = requests.get(f"{BASE_URL}/api/erp/purchase/orders")
        assert response.status_code == 200
        data = response.json()
        print(f"✅ Purchase orders: {len(data) if isinstance(data, list) else 'data received'}")
        return data
    
    def test_create_supplier(self):
        """Test create new supplier"""
        supplier_data = {
            "name": "TEST_Supplier_Comprehensive",
            "email": f"test_supplier_{datetime.now().timestamp()}@test.com",
            "phone": "+919876543210",
            "address": "Test Address",
            "gst_number": "29ABCDE1234F1Z5"
        }
        response = requests.post(f"{BASE_URL}/api/erp/purchase/suppliers", json=supplier_data)
        assert response.status_code in [200, 201]
        data = response.json()
        print(f"✅ Supplier created: {data.get('name', data.get('_id', 'success'))}")
        return data


class TestAccountsModule:
    """Accounts Module API tests"""
    
    def test_get_dashboard(self):
        """Test accounts dashboard"""
        response = requests.get(f"{BASE_URL}/api/erp/accounts/dashboard")
        assert response.status_code == 200
        data = response.json()
        print(f"✅ Accounts dashboard received")
        return data
    
    def test_get_invoices(self):
        """Test get all invoices"""
        response = requests.get(f"{BASE_URL}/api/erp/accounts/invoices")
        assert response.status_code == 200
        data = response.json()
        print(f"✅ Invoices: {len(data) if isinstance(data, list) else 'data received'}")
        return data
    
    def test_get_profit_loss(self):
        """Test P&L report"""
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        end_date = datetime.now().strftime("%Y-%m-%d")
        response = requests.get(f"{BASE_URL}/api/erp/accounts/profit-loss", params={
            "start_date": start_date,
            "end_date": end_date
        })
        assert response.status_code == 200
        print(f"✅ P&L report received")
    
    def test_get_gst_report(self):
        """Test GST report"""
        month = datetime.now().strftime("%Y-%m")
        response = requests.get(f"{BASE_URL}/api/erp/accounts/gst-report", params={"month": month})
        assert response.status_code == 200
        print(f"✅ GST report received")


class TestPayoutsModule:
    """Payouts Module API tests"""
    
    def test_get_dashboard(self):
        """Test payouts dashboard"""
        response = requests.get(f"{BASE_URL}/api/erp/payouts/dashboard")
        assert response.status_code == 200
        data = response.json()
        print(f"✅ Payouts dashboard received")
        return data
    
    def test_get_fund_accounts(self):
        """Test get fund accounts (employee bank details)"""
        response = requests.get(f"{BASE_URL}/api/erp/payouts/fund-accounts")
        assert response.status_code == 200
        print(f"✅ Fund accounts received")
    
    def test_get_payout_history(self):
        """Test get payout history"""
        response = requests.get(f"{BASE_URL}/api/erp/payouts/history")
        assert response.status_code == 200
        print(f"✅ Payout history received")


class TestExpensesModule:
    """Expenses Module API tests"""
    
    def test_get_dashboard(self):
        """Test expenses dashboard"""
        response = requests.get(f"{BASE_URL}/api/erp/expenses/dashboard")
        assert response.status_code == 200
        data = response.json()
        print(f"✅ Expenses dashboard received")
        return data
    
    def test_get_categories(self):
        """Test get expense categories"""
        response = requests.get(f"{BASE_URL}/api/erp/expenses/categories")
        assert response.status_code == 200
        data = response.json()
        print(f"✅ Expense categories: {len(data) if isinstance(data, list) else 'data received'}")
        return data
    
    def test_get_entries(self):
        """Test get expense entries"""
        response = requests.get(f"{BASE_URL}/api/erp/expenses/entries")
        assert response.status_code == 200
        print(f"✅ Expense entries received")
    
    def test_create_expense(self):
        """Test create new expense"""
        expense_data = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "category": "Office Supplies",
            "amount": 1500,
            "description": "TEST_Expense_Comprehensive",
            "payment_mode": "Cash",
            "department": "Admin"
        }
        response = requests.post(f"{BASE_URL}/api/erp/expenses/entries", json=expense_data)
        assert response.status_code in [200, 201]
        data = response.json()
        print(f"✅ Expense created: {data.get('entry_number', data.get('_id', 'success'))}")
        return data


class TestAssetsModule:
    """Assets Module API tests"""
    
    def test_get_types(self):
        """Test get asset types"""
        response = requests.get(f"{BASE_URL}/api/erp/assets/types")
        assert response.status_code == 200
        data = response.json()
        print(f"✅ Asset types: {len(data) if isinstance(data, list) else 'data received'}")
        return data
    
    def test_get_owned_assets(self):
        """Test get owned assets"""
        response = requests.get(f"{BASE_URL}/api/erp/assets/owned")
        assert response.status_code == 200
        data = response.json()
        print(f"✅ Owned assets: {len(data) if isinstance(data, list) else 'data received'}")
        return data
    
    def test_get_rented_assets(self):
        """Test get rented assets"""
        response = requests.get(f"{BASE_URL}/api/erp/assets/rented")
        assert response.status_code == 200
        data = response.json()
        print(f"✅ Rented assets: {len(data) if isinstance(data, list) else 'data received'}")
        return data
    
    def test_get_handover_requests(self):
        """Test get handover requests"""
        response = requests.get(f"{BASE_URL}/api/erp/assets/handover/requests")
        assert response.status_code == 200
        print(f"✅ Handover requests received")
    
    def test_get_asset_register(self):
        """Test get asset register report"""
        response = requests.get(f"{BASE_URL}/api/erp/assets/reports/register")
        assert response.status_code == 200
        print(f"✅ Asset register report received")


class TestHolidaysModule:
    """Holidays Module API tests"""
    
    def test_get_types(self):
        """Test get holiday types"""
        response = requests.get(f"{BASE_URL}/api/erp/holidays/types")
        assert response.status_code == 200
        data = response.json()
        print(f"✅ Holiday types: {len(data) if isinstance(data, list) else 'data received'}")
        return data
    
    def test_get_settings(self):
        """Test get holiday settings"""
        response = requests.get(f"{BASE_URL}/api/erp/holidays/settings")
        assert response.status_code == 200
        print(f"✅ Holiday settings received")
    
    def test_get_holidays(self):
        """Test get all holidays"""
        response = requests.get(f"{BASE_URL}/api/erp/holidays/")
        assert response.status_code == 200
        data = response.json()
        print(f"✅ Holidays: {len(data) if isinstance(data, list) else 'data received'}")
        return data
    
    def test_get_calendar(self):
        """Test get calendar for year"""
        year = datetime.now().year
        response = requests.get(f"{BASE_URL}/api/erp/holidays/calendar/{year}")
        assert response.status_code == 200
        print(f"✅ Calendar for {year} received")
    
    def test_get_overtime(self):
        """Test get overtime records"""
        response = requests.get(f"{BASE_URL}/api/erp/holidays/overtime")
        assert response.status_code == 200
        print(f"✅ Overtime records received")


class TestWalletModule:
    """Wallet/Referral Module API tests"""
    
    def test_get_settings(self):
        """Test get wallet settings"""
        response = requests.get(f"{BASE_URL}/api/erp/wallet/settings")
        assert response.status_code == 200
        print(f"✅ Wallet settings received")
    
    def test_get_admin_stats(self):
        """Test get admin wallet stats"""
        response = requests.get(f"{BASE_URL}/api/erp/wallet/admin/stats")
        assert response.status_code == 200
        data = response.json()
        print(f"✅ Wallet admin stats received")
        return data
    
    def test_get_all_wallets(self):
        """Test get all user wallets"""
        response = requests.get(f"{BASE_URL}/api/erp/wallet/admin/users")
        assert response.status_code == 200
        print(f"✅ All user wallets received")


class TestQRCodeModule:
    """QR Code & Print API tests"""
    
    def test_get_job_card_qr(self):
        """Test get job card QR code"""
        # First get a job card number
        orders_response = requests.get(f"{BASE_URL}/api/erp/production/orders")
        if orders_response.status_code == 200:
            orders = orders_response.json()
            if orders and len(orders) > 0:
                job_card_number = orders[0].get('job_card_number')
                if job_card_number:
                    response = requests.get(f"{BASE_URL}/api/erp/qr/job-card/{job_card_number}")
                    assert response.status_code == 200
                    print(f"✅ Job card QR for {job_card_number} received")
                    return
        print(f"⚠️ No job cards available to test QR")
    
    def test_get_job_card_print_data(self):
        """Test get job card print data"""
        orders_response = requests.get(f"{BASE_URL}/api/erp/production/orders")
        if orders_response.status_code == 200:
            orders = orders_response.json()
            if orders and len(orders) > 0:
                job_card_number = orders[0].get('job_card_number')
                if job_card_number:
                    response = requests.get(f"{BASE_URL}/api/erp/qr/job-card/{job_card_number}/print-data")
                    assert response.status_code == 200
                    print(f"✅ Job card print data for {job_card_number} received")
                    return
        print(f"⚠️ No job cards available to test print data")


class TestCustomerPortal:
    """Customer Portal API tests"""
    
    def test_get_my_orders(self):
        """Test get customer orders (requires auth)"""
        # Login first
        login_response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": "admin@lucumaa.in",
            "password": "adminpass"
        })
        if login_response.status_code == 200:
            token = login_response.json().get("token")
            headers = {"Authorization": f"Bearer {token}"}
            response = requests.get(f"{BASE_URL}/api/orders/my-orders", headers=headers)
            assert response.status_code == 200
            print(f"✅ Customer orders received")
        else:
            print(f"⚠️ Could not login to test customer portal")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
