"""
ERP API Tests for Glass Factory ERP System
Tests CRM, Production, HR modules
"""
import pytest
import requests
import os
import uuid

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', 'https://glassmesh.preview.emergentagent.com').rstrip('/')

class TestCRMModule:
    """CRM Lead Management Tests"""
    
    def test_get_leads(self):
        """Test fetching all leads"""
        response = requests.get(f"{BASE_URL}/api/erp/crm/leads")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        print(f"✓ GET /api/erp/crm/leads - Found {len(data)} leads")
    
    def test_create_lead(self):
        """Test creating a new lead"""
        lead_data = {
            "name": f"TEST_Lead_{uuid.uuid4().hex[:6]}",
            "email": f"test_{uuid.uuid4().hex[:6]}@example.com",
            "phone": "9876543210",
            "company": "Test Company",
            "customer_type": "retail",
            "source": "website",
            "enquiry_details": "Test enquiry",
            "expected_value": 50000
        }
        response = requests.post(f"{BASE_URL}/api/erp/crm/leads", json=lead_data)
        assert response.status_code == 200
        data = response.json()
        assert "lead_id" in data
        assert data["message"] == "Lead created"
        print(f"✓ POST /api/erp/crm/leads - Created lead {data['lead_id']}")
        return data["lead_id"]
    
    def test_get_leads_with_status_filter(self):
        """Test fetching leads with status filter"""
        response = requests.get(f"{BASE_URL}/api/erp/crm/leads", params={"status": "new"})
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        # All returned leads should have status 'new'
        for lead in data:
            assert lead.get("status") == "new"
        print(f"✓ GET /api/erp/crm/leads?status=new - Found {len(data)} new leads")


class TestProductionModule:
    """Production Order Management Tests"""
    
    def test_get_production_orders(self):
        """Test fetching all production orders"""
        response = requests.get(f"{BASE_URL}/api/erp/production/orders")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        print(f"✓ GET /api/erp/production/orders - Found {len(data)} orders")
    
    def test_create_production_order(self):
        """Test creating a new production order (job card)"""
        order_data = {
            "glass_type": "Toughened Glass",
            "thickness": 8,
            "width": 36,
            "height": 48,
            "quantity": 3,
            "priority": 1,
            "customer_order_id": f"TEST_ORDER_{uuid.uuid4().hex[:6]}"
        }
        response = requests.post(f"{BASE_URL}/api/erp/production/orders", json=order_data)
        assert response.status_code == 200
        data = response.json()
        assert "job_card" in data
        assert "order_id" in data
        assert data["message"] == "Production order created"
        print(f"✓ POST /api/erp/production/orders - Created job card {data['job_card']}")
        return data["order_id"]
    
    def test_update_production_stage(self):
        """Test updating production order stage"""
        # First create an order
        order_data = {
            "glass_type": "Laminated Glass",
            "thickness": 10,
            "width": 24,
            "height": 36,
            "quantity": 2,
            "priority": 2
        }
        create_response = requests.post(f"{BASE_URL}/api/erp/production/orders", json=order_data)
        assert create_response.status_code == 200
        order_id = create_response.json()["order_id"]
        
        # Update stage to cutting
        update_response = requests.patch(
            f"{BASE_URL}/api/erp/production/orders/{order_id}/stage",
            json={"stage": "cutting"}
        )
        assert update_response.status_code == 200
        assert update_response.json()["message"] == "Stage updated successfully"
        
        # Verify stage was updated
        get_response = requests.get(f"{BASE_URL}/api/erp/production/orders")
        orders = get_response.json()
        updated_order = next((o for o in orders if o["id"] == order_id), None)
        assert updated_order is not None
        assert updated_order["current_stage"] == "cutting"
        print(f"✓ PATCH /api/erp/production/orders/{order_id}/stage - Stage updated to cutting")
    
    def test_get_orders_by_stage(self):
        """Test fetching orders filtered by stage"""
        response = requests.get(f"{BASE_URL}/api/erp/production/orders", params={"stage": "cutting"})
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        for order in data:
            assert order.get("current_stage") == "cutting"
        print(f"✓ GET /api/erp/production/orders?stage=cutting - Found {len(data)} orders")
    
    def test_create_breakage_entry(self):
        """Test creating a breakage entry"""
        breakage_data = {
            "production_order_id": "test-order-id",
            "job_card_number": "JC_TEST",
            "stage": "cutting",
            "operator_id": "operator-1",
            "glass_type": "Toughened Glass",
            "size": "24x36",
            "quantity_broken": 1,
            "reason": "Machine malfunction",
            "cost_per_unit": 150
        }
        response = requests.post(f"{BASE_URL}/api/erp/production/breakage", json=breakage_data)
        assert response.status_code == 200
        data = response.json()
        assert "breakage_id" in data
        print(f"✓ POST /api/erp/production/breakage - Created breakage entry {data['breakage_id']}")


class TestHRModule:
    """HR & Payroll Management Tests"""
    
    def test_get_employees(self):
        """Test fetching all employees"""
        response = requests.get(f"{BASE_URL}/api/erp/hr/employees")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        print(f"✓ GET /api/erp/hr/employees - Found {len(data)} employees")
    
    def test_create_employee(self):
        """Test creating a new employee"""
        employee_data = {
            "emp_code": f"TEST_EMP_{uuid.uuid4().hex[:4]}",
            "name": f"Test Employee {uuid.uuid4().hex[:4]}",
            "email": f"test_{uuid.uuid4().hex[:6]}@lucumaa.in",
            "phone": "9876543210",
            "role": "operator",
            "department": "Production",
            "designation": "Glass Cutter",
            "date_of_joining": "2025-01-01",
            "salary": 15000,
            "bank_account": "1234567890",
            "bank_ifsc": "HDFC0001234"
        }
        response = requests.post(f"{BASE_URL}/api/erp/hr/employees", json=employee_data)
        assert response.status_code == 200
        data = response.json()
        assert "emp_id" in data
        assert data["message"] == "Employee created"
        print(f"✓ POST /api/erp/hr/employees - Created employee {data['emp_id']}")
        return data["emp_id"]
    
    def test_calculate_salary(self):
        """Test salary calculation for an employee"""
        # First get an employee
        employees_response = requests.get(f"{BASE_URL}/api/erp/hr/employees")
        employees = employees_response.json()
        
        if not employees:
            pytest.skip("No employees found to test salary calculation")
        
        employee_id = employees[0]["id"]
        
        salary_data = {
            "month": "2026-01",
            "overtime_pay": 1500,
            "deductions": 300
        }
        response = requests.post(
            f"{BASE_URL}/api/erp/hr/salary/calculate/{employee_id}",
            json=salary_data
        )
        assert response.status_code == 200
        data = response.json()
        assert "salary_id" in data
        assert "net_salary" in data
        assert data["message"] == "Salary calculated"
        
        # Verify calculation
        expected_net = employees[0]["salary"] + salary_data["overtime_pay"] - salary_data["deductions"]
        assert data["net_salary"] == expected_net
        print(f"✓ POST /api/erp/hr/salary/calculate/{employee_id} - Net salary: {data['net_salary']}")
    
    def test_get_salary_records(self):
        """Test fetching salary records"""
        response = requests.get(f"{BASE_URL}/api/erp/hr/salary")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        print(f"✓ GET /api/erp/hr/salary - Found {len(data)} salary records")
    
    def test_get_salary_records_by_status(self):
        """Test fetching salary records filtered by status"""
        response = requests.get(f"{BASE_URL}/api/erp/hr/salary", params={"status": "pending"})
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        for record in data:
            assert record.get("payment_status") == "pending"
        print(f"✓ GET /api/erp/hr/salary?status=pending - Found {len(data)} pending records")


class TestAdminDashboard:
    """Admin Dashboard Tests"""
    
    def test_get_admin_dashboard(self):
        """Test fetching admin dashboard metrics"""
        response = requests.get(f"{BASE_URL}/api/erp/admin/dashboard")
        assert response.status_code == 200
        data = response.json()
        
        # Verify all expected fields are present
        assert "orders_today" in data
        assert "production_stats" in data
        assert "breakage_today" in data
        assert "low_stock_items" in data
        assert "pending_pos" in data
        assert "present_employees" in data
        
        # Verify production_stats has all stages
        expected_stages = ['pending', 'cutting', 'polishing', 'grinding', 'toughening', 'quality_check', 'packing', 'dispatched']
        for stage in expected_stages:
            assert stage in data["production_stats"]
        
        print(f"✓ GET /api/erp/admin/dashboard - Dashboard loaded successfully")
        print(f"  - Orders today: {data['orders_today']}")
        print(f"  - Breakage today: ₹{data['breakage_today']}")
        print(f"  - Present employees: {data['present_employees']}")


class TestBreakageAnalytics:
    """Breakage Analytics Tests"""
    
    def test_get_breakage_analytics(self):
        """Test fetching breakage analytics"""
        response = requests.get(f"{BASE_URL}/api/erp/production/breakage/analytics")
        assert response.status_code == 200
        data = response.json()
        
        assert "by_stage" in data
        assert "by_operator" in data
        assert isinstance(data["by_stage"], list)
        assert isinstance(data["by_operator"], list)
        print(f"✓ GET /api/erp/production/breakage/analytics - Analytics loaded")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
