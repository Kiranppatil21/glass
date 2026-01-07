"""
Super Admin Panel & Audit Trail API Tests
Tests for:
- Super Admin Dashboard API
- User Management (Create, Disable, Enable, Delete)
- Audit Trail APIs (logs, daily-activity, monthly-mis)
- Role-based access control
"""
import pytest
import requests
import os
import uuid
from datetime import datetime

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', '').rstrip('/')

# Test credentials
SUPER_ADMIN_EMAIL = "admin@lucumaa.in"
SUPER_ADMIN_PASSWORD = "adminpass"


@pytest.fixture(scope="module")
def api_client():
    """Shared requests session"""
    session = requests.Session()
    session.headers.update({"Content-Type": "application/json"})
    return session


@pytest.fixture(scope="module")
def super_admin_token(api_client):
    """Get super admin authentication token"""
    response = api_client.post(f"{BASE_URL}/api/auth/login", json={
        "email": SUPER_ADMIN_EMAIL,
        "password": SUPER_ADMIN_PASSWORD
    })
    if response.status_code == 200:
        return response.json().get("token")
    pytest.skip("Super Admin authentication failed - skipping tests")


@pytest.fixture(scope="module")
def authenticated_client(api_client, super_admin_token):
    """Session with super admin auth header"""
    api_client.headers.update({"Authorization": f"Bearer {super_admin_token}"})
    return api_client


class TestSuperAdminAuthentication:
    """Test super admin login and role verification"""
    
    def test_super_admin_login(self, api_client):
        """Test super admin can login"""
        response = api_client.post(f"{BASE_URL}/api/auth/login", json={
            "email": SUPER_ADMIN_EMAIL,
            "password": SUPER_ADMIN_PASSWORD
        })
        assert response.status_code == 200
        data = response.json()
        assert "token" in data
        assert "user" in data
        assert data["user"]["role"] == "super_admin"
        print(f"✓ Super admin login successful, role: {data['user']['role']}")
    
    def test_super_admin_me_endpoint(self, authenticated_client):
        """Test /api/auth/me returns super_admin role"""
        response = authenticated_client.get(f"{BASE_URL}/api/auth/me")
        assert response.status_code == 200
        data = response.json()
        assert data["role"] == "super_admin"
        print(f"✓ /api/auth/me confirms super_admin role")


class TestSuperAdminDashboard:
    """Test Super Admin Dashboard API"""
    
    def test_dashboard_returns_data(self, authenticated_client):
        """Test /api/erp/superadmin/dashboard returns expected structure"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/superadmin/dashboard")
        assert response.status_code == 200
        data = response.json()
        
        # Verify structure
        assert "users" in data
        assert "today" in data
        assert "system" in data
        assert "recent_activity" in data
        
        # Verify users section
        assert "total" in data["users"]
        assert "active" in data["users"]
        assert "disabled" in data["users"]
        assert "by_role" in data["users"]
        
        # Verify today section
        assert "total_actions" in data["today"]
        assert "logins" in data["today"]
        assert "active_users" in data["today"]
        
        print(f"✓ Dashboard API returns correct structure")
        print(f"  - Total users: {data['users']['total']}")
        print(f"  - Active users: {data['users']['active']}")
        print(f"  - Today's actions: {data['today']['total_actions']}")
    
    def test_dashboard_role_distribution(self, authenticated_client):
        """Test dashboard returns role distribution data"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/superadmin/dashboard")
        assert response.status_code == 200
        data = response.json()
        
        by_role = data["users"]["by_role"]
        assert isinstance(by_role, dict)
        
        # Should have super_admin role at minimum
        assert "super_admin" in by_role
        assert by_role["super_admin"] >= 1
        
        print(f"✓ Role distribution: {by_role}")
    
    def test_dashboard_system_stats(self, authenticated_client):
        """Test dashboard returns system data counts"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/superadmin/dashboard")
        assert response.status_code == 200
        data = response.json()
        
        system = data["system"]
        expected_keys = ["leads", "production_orders", "invoices", "employees", 
                        "materials", "expenses", "assets", "holidays"]
        
        for key in expected_keys:
            assert key in system
            assert isinstance(system[key], int)
        
        print(f"✓ System stats: {system}")


class TestUserManagement:
    """Test User Management APIs"""
    
    def test_get_all_users(self, authenticated_client):
        """Test /api/erp/superadmin/users returns user list"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/superadmin/users")
        assert response.status_code == 200
        data = response.json()
        
        assert "users" in data
        assert "stats" in data
        assert "role_breakdown" in data
        
        assert isinstance(data["users"], list)
        assert len(data["users"]) > 0
        
        # Verify user structure
        user = data["users"][0]
        assert "id" in user
        assert "email" in user
        assert "name" in user
        assert "role" in user
        
        print(f"✓ Users API returns {len(data['users'])} users")
        print(f"  - Stats: {data['stats']}")
    
    def test_filter_users_by_role(self, authenticated_client):
        """Test filtering users by role"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/superadmin/users", 
                                           params={"role": "super_admin"})
        assert response.status_code == 200
        data = response.json()
        
        # All returned users should be super_admin
        for user in data["users"]:
            assert user["role"] == "super_admin"
        
        print(f"✓ Role filter works - found {len(data['users'])} super_admin users")
    
    def test_search_users(self, authenticated_client):
        """Test searching users by name/email"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/superadmin/users", 
                                           params={"search": "admin"})
        assert response.status_code == 200
        data = response.json()
        
        # Should find at least the admin user
        assert len(data["users"]) >= 1
        
        print(f"✓ Search works - found {len(data['users'])} users matching 'admin'")
    
    def test_create_user(self, authenticated_client):
        """Test creating a new user"""
        unique_id = str(uuid.uuid4())[:8]
        new_user = {
            "email": f"TEST_user_{unique_id}@test.com",
            "name": f"TEST User {unique_id}",
            "phone": "9876543210",
            "password": "testpass123",
            "role": "operator"
        }
        
        response = authenticated_client.post(f"{BASE_URL}/api/erp/superadmin/users", json=new_user)
        assert response.status_code == 200
        data = response.json()
        
        assert "message" in data
        assert "user" in data
        assert data["user"]["email"] == new_user["email"]
        assert data["user"]["role"] == "operator"
        
        # Store user ID for cleanup
        TestUserManagement.created_user_id = data["user"]["id"]
        TestUserManagement.created_user_email = new_user["email"]
        
        print(f"✓ User created: {new_user['email']}")
        return data["user"]["id"]
    
    def test_create_user_duplicate_email(self, authenticated_client):
        """Test creating user with duplicate email fails"""
        response = authenticated_client.post(f"{BASE_URL}/api/erp/superadmin/users", json={
            "email": SUPER_ADMIN_EMAIL,  # Already exists
            "name": "Duplicate User",
            "phone": "1234567890",
            "password": "testpass",
            "role": "operator"
        })
        assert response.status_code == 400
        assert "already registered" in response.json().get("detail", "").lower()
        
        print(f"✓ Duplicate email correctly rejected")
    
    def test_disable_user(self, authenticated_client):
        """Test disabling a user"""
        if not hasattr(TestUserManagement, 'created_user_id'):
            pytest.skip("No test user created")
        
        user_id = TestUserManagement.created_user_id
        response = authenticated_client.post(f"{BASE_URL}/api/erp/superadmin/users/{user_id}/disable")
        assert response.status_code == 200
        assert "disabled" in response.json().get("message", "").lower()
        
        # Verify user is disabled
        users_response = authenticated_client.get(f"{BASE_URL}/api/erp/superadmin/users", 
                                                  params={"status": "disabled"})
        disabled_users = users_response.json()["users"]
        disabled_ids = [u["id"] for u in disabled_users]
        assert user_id in disabled_ids
        
        print(f"✓ User disabled successfully")
    
    def test_enable_user(self, authenticated_client):
        """Test enabling a disabled user"""
        if not hasattr(TestUserManagement, 'created_user_id'):
            pytest.skip("No test user created")
        
        user_id = TestUserManagement.created_user_id
        response = authenticated_client.post(f"{BASE_URL}/api/erp/superadmin/users/{user_id}/enable")
        assert response.status_code == 200
        assert "enabled" in response.json().get("message", "").lower()
        
        print(f"✓ User enabled successfully")
    
    def test_delete_user(self, authenticated_client):
        """Test deleting a user (soft delete)"""
        if not hasattr(TestUserManagement, 'created_user_id'):
            pytest.skip("No test user created")
        
        user_id = TestUserManagement.created_user_id
        response = authenticated_client.delete(f"{BASE_URL}/api/erp/superadmin/users/{user_id}")
        assert response.status_code == 200
        assert "deleted" in response.json().get("message", "").lower()
        
        print(f"✓ User deleted successfully")
    
    def test_cannot_delete_super_admin(self, authenticated_client):
        """Test that super_admin users cannot be deleted"""
        # First get a super_admin user ID
        response = authenticated_client.get(f"{BASE_URL}/api/erp/superadmin/users", 
                                           params={"role": "super_admin"})
        super_admins = response.json()["users"]
        
        if len(super_admins) > 0:
            super_admin_id = super_admins[0]["id"]
            delete_response = authenticated_client.delete(
                f"{BASE_URL}/api/erp/superadmin/users/{super_admin_id}"
            )
            assert delete_response.status_code == 403
            print(f"✓ Super admin deletion correctly blocked")


class TestAuditTrailLogs:
    """Test Audit Trail APIs"""
    
    def test_get_audit_logs(self, authenticated_client):
        """Test /api/erp/audit/logs returns audit logs"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/audit/logs", 
                                           params={"limit": 50})
        assert response.status_code == 200
        data = response.json()
        
        assert "logs" in data
        assert "total" in data
        assert "limit" in data
        assert "skip" in data
        
        print(f"✓ Audit logs API returns {data['total']} total logs")
        
        if len(data["logs"]) > 0:
            log = data["logs"][0]
            assert "user_id" in log
            assert "user_name" in log
            assert "action" in log
            assert "module" in log
            assert "timestamp" in log
            print(f"  - Latest log: {log['user_name']} - {log['action']} - {log['module']}")
    
    def test_filter_audit_logs_by_action(self, authenticated_client):
        """Test filtering audit logs by action type"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/audit/logs", 
                                           params={"action": "create", "limit": 20})
        assert response.status_code == 200
        data = response.json()
        
        # All returned logs should have action=create
        for log in data["logs"]:
            assert log["action"] == "create"
        
        print(f"✓ Action filter works - found {len(data['logs'])} 'create' logs")
    
    def test_filter_audit_logs_by_module(self, authenticated_client):
        """Test filtering audit logs by module"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/audit/logs", 
                                           params={"module": "users", "limit": 20})
        assert response.status_code == 200
        data = response.json()
        
        # All returned logs should have module=users
        for log in data["logs"]:
            assert log["module"] == "users"
        
        print(f"✓ Module filter works - found {len(data['logs'])} 'users' module logs")


class TestDailyActivity:
    """Test Daily Activity API"""
    
    def test_get_daily_activity(self, authenticated_client):
        """Test /api/erp/audit/daily-activity returns daily breakdown"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/audit/daily-activity")
        assert response.status_code == 200
        data = response.json()
        
        assert "summary" in data
        assert "user_activity" in data
        assert "recent_logs" in data
        
        # Verify summary structure
        summary = data["summary"]
        assert "date" in summary
        assert "total_actions" in summary
        assert "active_users" in summary
        assert "actions_breakdown" in summary
        
        print(f"✓ Daily activity API returns data for {summary['date']}")
        print(f"  - Total actions: {summary['total_actions']}")
        print(f"  - Active users: {summary['active_users']}")
    
    def test_daily_activity_user_breakdown(self, authenticated_client):
        """Test daily activity includes user-wise breakdown"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/audit/daily-activity")
        assert response.status_code == 200
        data = response.json()
        
        user_activity = data["user_activity"]
        assert isinstance(user_activity, list)
        
        if len(user_activity) > 0:
            user = user_activity[0]
            assert "user_id" in user
            assert "user_name" in user
            assert "total_actions" in user
            print(f"✓ User activity breakdown: {len(user_activity)} users active")


class TestMonthlyMIS:
    """Test Monthly MIS Report API"""
    
    def test_get_monthly_mis(self, authenticated_client):
        """Test /api/erp/audit/monthly-mis returns MIS report"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/audit/monthly-mis")
        assert response.status_code == 200
        data = response.json()
        
        assert "month" in data
        assert "summary" in data
        assert "top_performers" in data
        assert "daily_breakdown" in data
        assert "module_usage" in data
        assert "action_breakdown" in data
        
        # Verify summary structure
        summary = data["summary"]
        assert "total_actions" in summary
        assert "total_users" in summary
        assert "avg_actions_per_user" in summary
        assert "avg_actions_per_day" in summary
        
        print(f"✓ Monthly MIS API returns data for {data['month']}")
        print(f"  - Total actions: {summary['total_actions']}")
        print(f"  - Total users: {summary['total_users']}")
        print(f"  - Avg per user: {summary['avg_actions_per_user']}")
    
    def test_monthly_mis_top_performers(self, authenticated_client):
        """Test MIS report includes top performers"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/audit/monthly-mis")
        assert response.status_code == 200
        data = response.json()
        
        top_performers = data["top_performers"]
        assert isinstance(top_performers, list)
        
        if len(top_performers) > 0:
            performer = top_performers[0]
            assert "user_id" in performer
            assert "user_name" in performer
            assert "total_actions" in performer
            print(f"✓ Top performers: {len(top_performers)} users listed")


class TestYearlyAudit:
    """Test Yearly Audit Trail API"""
    
    def test_get_yearly_audit(self, authenticated_client):
        """Test /api/erp/audit/yearly-audit returns yearly data"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/audit/yearly-audit")
        assert response.status_code == 200
        data = response.json()
        
        assert "year" in data
        assert "summary" in data
        assert "monthly_breakdown" in data
        assert "user_performance" in data
        assert "deleted_records" in data
        
        print(f"✓ Yearly audit API returns data for {data['year']}")
        print(f"  - Total actions: {data['summary']['total_actions']}")
        print(f"  - Months active: {data['summary']['months_active']}")


class TestApprovalHistory:
    """Test Approval History API"""
    
    def test_get_approval_history(self, authenticated_client):
        """Test /api/erp/audit/approval-history returns approvals/rejections"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/audit/approval-history")
        assert response.status_code == 200
        data = response.json()
        
        assert "summary" in data
        assert "approvals" in data
        assert "rejections" in data
        
        summary = data["summary"]
        assert "total_approvals" in summary
        assert "total_rejections" in summary
        assert "approval_rate" in summary
        
        print(f"✓ Approval history API works")
        print(f"  - Approvals: {summary['total_approvals']}")
        print(f"  - Rejections: {summary['total_rejections']}")


class TestDeletedRecords:
    """Test Deleted Records Log API"""
    
    def test_get_deleted_records(self, authenticated_client):
        """Test /api/erp/audit/deleted-records returns deleted items"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/audit/deleted-records")
        assert response.status_code == 200
        data = response.json()
        
        assert "total_deleted" in data
        assert "by_module" in data
        assert "records" in data
        
        print(f"✓ Deleted records API works")
        print(f"  - Total deleted: {data['total_deleted']}")
        print(f"  - By module: {data['by_module']}")


class TestEmployeePerformance:
    """Test Employee Performance Report API"""
    
    def test_get_employee_performance(self, authenticated_client):
        """Test /api/erp/audit/employee-performance returns performance data"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/audit/employee-performance")
        assert response.status_code == 200
        data = response.json()
        
        assert "month" in data
        assert "total_employees" in data
        assert "active_employees" in data
        assert "performance" in data
        
        print(f"✓ Employee performance API works")
        print(f"  - Month: {data['month']}")
        print(f"  - Total employees: {data['total_employees']}")
        print(f"  - Active employees: {data['active_employees']}")
        
        if len(data["performance"]) > 0:
            perf = data["performance"][0]
            assert "employee" in perf
            assert "total_actions" in perf
            assert "efficiency_score" in perf
            assert "grade" in perf


class TestAccessControl:
    """Test role-based access control"""
    
    def test_non_super_admin_cannot_access_superadmin_apis(self, api_client):
        """Test that non-super_admin users cannot access super admin APIs"""
        # Try to login as a regular user (if exists)
        # For now, test without auth
        response = api_client.get(f"{BASE_URL}/api/erp/superadmin/dashboard")
        assert response.status_code in [401, 403]
        print(f"✓ Unauthenticated access correctly blocked")
    
    def test_audit_apis_require_admin_role(self, api_client):
        """Test that audit APIs require admin/owner/super_admin role"""
        response = api_client.get(f"{BASE_URL}/api/erp/audit/logs")
        assert response.status_code in [401, 403]
        print(f"✓ Audit logs require authentication")


class TestReportsSummary:
    """Test Reports Summary API"""
    
    def test_get_reports_summary(self, authenticated_client):
        """Test /api/erp/superadmin/reports/summary returns available reports"""
        response = authenticated_client.get(f"{BASE_URL}/api/erp/superadmin/reports/summary")
        assert response.status_code == 200
        data = response.json()
        
        assert "available_reports" in data
        assert "quick_stats" in data
        
        reports = data["available_reports"]
        assert len(reports) >= 8  # Should have at least 8 report types
        
        for report in reports:
            assert "name" in report
            assert "endpoint" in report
            assert "description" in report
        
        print(f"✓ Reports summary API works")
        print(f"  - Available reports: {len(reports)}")
        print(f"  - Quick stats: {data['quick_stats']}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
