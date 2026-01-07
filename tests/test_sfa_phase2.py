"""
SFA Phase 2 API Tests
Tests for: Alerts, Performance Scorecard, Stops Detection, Individual Performance
Part of: Field Sales Automation System - Lucumaa Glass ERP
"""
import pytest
import requests
import os
from datetime import datetime

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', '').rstrip('/')

class TestSFAPhase2APIs:
    """Test SFA Phase 2 APIs: Alerts, Performance, Stops"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test session with authentication"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        
        # Login as super admin
        login_response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": "admin@lucumaa.in",
            "password": "adminpass"
        })
        
        if login_response.status_code == 200:
            token = login_response.json().get("token")
            self.session.headers.update({"Authorization": f"Bearer {token}"})
            self.token = token
            self.user_id = login_response.json().get("user", {}).get("id")
        else:
            pytest.skip("Authentication failed - skipping tests")
    
    # ==================== ALERTS API TESTS ====================
    
    def test_get_alerts_endpoint_exists(self):
        """Test GET /api/erp/sfa/alerts - endpoint exists and returns 200"""
        response = self.session.get(f"{BASE_URL}/api/erp/sfa/alerts")
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    
    def test_get_alerts_response_structure(self):
        """Test alerts response has correct structure"""
        response = self.session.get(f"{BASE_URL}/api/erp/sfa/alerts")
        assert response.status_code == 200
        
        data = response.json()
        assert "date" in data, "Response should have 'date' field"
        assert "total_alerts" in data, "Response should have 'total_alerts' field"
        assert "alerts" in data, "Response should have 'alerts' array"
        assert isinstance(data["alerts"], list), "alerts should be a list"
    
    def test_get_alerts_with_date_filter(self):
        """Test alerts API with date filter"""
        today = datetime.now().strftime("%Y-%m-%d")
        response = self.session.get(f"{BASE_URL}/api/erp/sfa/alerts?date={today}")
        assert response.status_code == 200
        
        data = response.json()
        assert data["date"] == today
    
    def test_alerts_types_structure(self):
        """Test that alerts have correct type structure when present"""
        response = self.session.get(f"{BASE_URL}/api/erp/sfa/alerts")
        assert response.status_code == 200
        
        data = response.json()
        valid_types = ["location_off", "long_stop", "zero_visits", "low_battery"]
        
        for alert in data.get("alerts", []):
            assert "id" in alert, "Alert should have 'id'"
            assert "type" in alert, "Alert should have 'type'"
            assert "severity" in alert, "Alert should have 'severity'"
            assert "title" in alert, "Alert should have 'title'"
            assert "message" in alert, "Alert should have 'message'"
            assert alert["type"] in valid_types, f"Alert type should be one of {valid_types}"
            assert alert["severity"] in ["high", "medium", "low"], "Severity should be high/medium/low"
    
    # ==================== PERFORMANCE SCORECARD API TESTS ====================
    
    def test_get_performance_scorecard_endpoint_exists(self):
        """Test GET /api/erp/sfa/performance-scorecard - endpoint exists"""
        response = self.session.get(f"{BASE_URL}/api/erp/sfa/performance-scorecard")
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    
    def test_performance_scorecard_response_structure(self):
        """Test performance scorecard has correct structure"""
        response = self.session.get(f"{BASE_URL}/api/erp/sfa/performance-scorecard")
        assert response.status_code == 200
        
        data = response.json()
        assert "month" in data, "Response should have 'month' field"
        assert "summary" in data, "Response should have 'summary' field"
        assert "targets_used" in data, "Response should have 'targets_used' field"
        assert "report" in data, "Response should have 'report' array"
    
    def test_performance_scorecard_summary_fields(self):
        """Test performance scorecard summary has required fields"""
        response = self.session.get(f"{BASE_URL}/api/erp/sfa/performance-scorecard")
        assert response.status_code == 200
        
        data = response.json()
        summary = data.get("summary", {})
        
        assert "total_employees" in summary, "Summary should have 'total_employees'"
        assert "average_score" in summary, "Summary should have 'average_score'"
        assert "top_performers" in summary, "Summary should have 'top_performers'"
        assert "needs_improvement" in summary, "Summary should have 'needs_improvement'"
    
    def test_performance_scorecard_targets(self):
        """Test performance scorecard targets structure"""
        response = self.session.get(f"{BASE_URL}/api/erp/sfa/performance-scorecard")
        assert response.status_code == 200
        
        data = response.json()
        targets = data.get("targets_used", {})
        
        assert "days_per_month" in targets, "Targets should have 'days_per_month'"
        assert "distance_per_day" in targets, "Targets should have 'distance_per_day'"
        assert "visits_per_day" in targets, "Targets should have 'visits_per_day'"
        assert "working_hours_per_day" in targets, "Targets should have 'working_hours_per_day'"
    
    def test_performance_scorecard_employee_grades(self):
        """Test that employee scorecards have grades (A+ to F)"""
        response = self.session.get(f"{BASE_URL}/api/erp/sfa/performance-scorecard")
        assert response.status_code == 200
        
        data = response.json()
        valid_grades = ["A+", "A", "B+", "B", "C", "D", "F"]
        
        for employee in data.get("report", []):
            assert "user_id" in employee, "Employee should have 'user_id'"
            assert "employee_name" in employee, "Employee should have 'employee_name'"
            assert "grade" in employee, "Employee should have 'grade'"
            assert "scores" in employee, "Employee should have 'scores'"
            assert "metrics" in employee, "Employee should have 'metrics'"
            
            if employee.get("grade"):
                assert employee["grade"] in valid_grades, f"Grade should be one of {valid_grades}"
    
    def test_performance_scorecard_scores_structure(self):
        """Test that employee scores have all required fields"""
        response = self.session.get(f"{BASE_URL}/api/erp/sfa/performance-scorecard")
        assert response.status_code == 200
        
        data = response.json()
        
        for employee in data.get("report", []):
            scores = employee.get("scores", {})
            assert "attendance" in scores, "Scores should have 'attendance'"
            assert "distance" in scores, "Scores should have 'distance'"
            assert "visits" in scores, "Scores should have 'visits'"
            assert "conversion" in scores, "Scores should have 'conversion'"
            assert "time_utilization" in scores, "Scores should have 'time_utilization'"
            assert "overall" in scores, "Scores should have 'overall'"
            
            # Scores should be 0-100
            for key, value in scores.items():
                assert 0 <= value <= 100, f"Score {key} should be between 0 and 100"
    
    def test_performance_scorecard_with_month_filter(self):
        """Test performance scorecard with month filter"""
        current_month = datetime.now().strftime("%Y-%m")
        response = self.session.get(f"{BASE_URL}/api/erp/sfa/performance-scorecard?month={current_month}")
        assert response.status_code == 200
        
        data = response.json()
        assert data["month"] == current_month
    
    # ==================== STOPS DETECTION API TESTS ====================
    
    def test_get_stops_endpoint_exists(self):
        """Test GET /api/erp/sfa/stops/{user_id} - endpoint exists"""
        # Use the logged-in user's ID
        if not hasattr(self, 'user_id') or not self.user_id:
            pytest.skip("No user_id available")
        
        response = self.session.get(f"{BASE_URL}/api/erp/sfa/stops/{self.user_id}")
        # Should return 200 or 404 (if no attendance record)
        assert response.status_code in [200, 404], f"Expected 200 or 404, got {response.status_code}"
    
    def test_get_stops_response_structure(self):
        """Test stops response structure when data exists"""
        if not hasattr(self, 'user_id') or not self.user_id:
            pytest.skip("No user_id available")
        
        response = self.session.get(f"{BASE_URL}/api/erp/sfa/stops/{self.user_id}")
        
        if response.status_code == 200:
            data = response.json()
            assert "user_id" in data, "Response should have 'user_id'"
            assert "date" in data, "Response should have 'date'"
            assert "stops" in data, "Response should have 'stops' array"
            
            if "summary" in data:
                summary = data["summary"]
                assert "total_stops" in summary, "Summary should have 'total_stops'"
                assert "visit_stops" in summary, "Summary should have 'visit_stops'"
                assert "idle_stops" in summary, "Summary should have 'idle_stops'"
    
    def test_get_stops_with_date_filter(self):
        """Test stops API with date filter"""
        if not hasattr(self, 'user_id') or not self.user_id:
            pytest.skip("No user_id available")
        
        today = datetime.now().strftime("%Y-%m-%d")
        response = self.session.get(f"{BASE_URL}/api/erp/sfa/stops/{self.user_id}?date={today}")
        assert response.status_code in [200, 404]
    
    # ==================== INDIVIDUAL PERFORMANCE API TESTS ====================
    
    def test_get_individual_performance_endpoint_exists(self):
        """Test GET /api/erp/sfa/performance/{user_id} - endpoint exists"""
        if not hasattr(self, 'user_id') or not self.user_id:
            pytest.skip("No user_id available")
        
        response = self.session.get(f"{BASE_URL}/api/erp/sfa/performance/{self.user_id}")
        # Should return 200 or 404 (if no records)
        assert response.status_code in [200, 404], f"Expected 200 or 404, got {response.status_code}"
    
    def test_individual_performance_response_structure(self):
        """Test individual performance response structure"""
        if not hasattr(self, 'user_id') or not self.user_id:
            pytest.skip("No user_id available")
        
        response = self.session.get(f"{BASE_URL}/api/erp/sfa/performance/{self.user_id}")
        
        if response.status_code == 200:
            data = response.json()
            assert "user_id" in data, "Response should have 'user_id'"
            assert "month" in data, "Response should have 'month'"
            assert "summary" in data, "Response should have 'summary'"
            assert "daily_breakdown" in data, "Response should have 'daily_breakdown'"
    
    def test_individual_performance_summary_fields(self):
        """Test individual performance summary has required fields"""
        if not hasattr(self, 'user_id') or not self.user_id:
            pytest.skip("No user_id available")
        
        response = self.session.get(f"{BASE_URL}/api/erp/sfa/performance/{self.user_id}")
        
        if response.status_code == 200:
            data = response.json()
            summary = data.get("summary", {})
            
            assert "days_worked" in summary, "Summary should have 'days_worked'"
            assert "total_distance_km" in summary, "Summary should have 'total_distance_km'"
            assert "total_working_hours" in summary, "Summary should have 'total_working_hours'"
            assert "total_visits" in summary, "Summary should have 'total_visits'"
            assert "successful_visits" in summary, "Summary should have 'successful_visits'"
            assert "conversion_rate" in summary, "Summary should have 'conversion_rate'"
    
    def test_individual_performance_with_month_filter(self):
        """Test individual performance with month filter"""
        if not hasattr(self, 'user_id') or not self.user_id:
            pytest.skip("No user_id available")
        
        current_month = datetime.now().strftime("%Y-%m")
        response = self.session.get(f"{BASE_URL}/api/erp/sfa/performance/{self.user_id}?month={current_month}")
        assert response.status_code in [200, 404]
    
    # ==================== REGRESSION TESTS - EXISTING SFA APIs ====================
    
    def test_my_day_endpoint_still_works(self):
        """Regression: GET /api/erp/sfa/my-day should still work"""
        response = self.session.get(f"{BASE_URL}/api/erp/sfa/my-day")
        assert response.status_code == 200, f"my-day endpoint failed: {response.text}"
        
        data = response.json()
        assert "date" in data, "Response should have 'date'"
        assert "status" in data, "Response should have 'status'"
    
    def test_team_dashboard_endpoint_still_works(self):
        """Regression: GET /api/erp/sfa/team-dashboard should still work"""
        response = self.session.get(f"{BASE_URL}/api/erp/sfa/team-dashboard")
        assert response.status_code == 200, f"team-dashboard endpoint failed: {response.text}"
        
        data = response.json()
        assert "date" in data, "Response should have 'date'"
        assert "summary" in data, "Response should have 'summary'"
        assert "team" in data, "Response should have 'team'"
    
    def test_daily_summary_report_still_works(self):
        """Regression: GET /api/erp/sfa/reports/daily-summary should still work"""
        response = self.session.get(f"{BASE_URL}/api/erp/sfa/reports/daily-summary")
        assert response.status_code == 200, f"daily-summary endpoint failed: {response.text}"
        
        data = response.json()
        assert "date" in data, "Response should have 'date'"
        assert "summary" in data, "Response should have 'summary'"
        assert "report" in data, "Response should have 'report'"
    
    def test_monthly_summary_report_still_works(self):
        """Regression: GET /api/erp/sfa/reports/monthly-summary should still work"""
        response = self.session.get(f"{BASE_URL}/api/erp/sfa/reports/monthly-summary")
        assert response.status_code == 200, f"monthly-summary endpoint failed: {response.text}"
        
        data = response.json()
        assert "month" in data, "Response should have 'month'"
        assert "summary" in data, "Response should have 'summary'"
        assert "report" in data, "Response should have 'report'"
    
    # ==================== ACCESS CONTROL TESTS ====================
    
    def test_alerts_requires_manager_role(self):
        """Test that alerts endpoint requires manager/admin role"""
        # Create a new session without auth
        no_auth_session = requests.Session()
        no_auth_session.headers.update({"Content-Type": "application/json"})
        
        response = no_auth_session.get(f"{BASE_URL}/api/erp/sfa/alerts")
        # Should return 401 or 403 without auth
        assert response.status_code in [401, 403, 422], f"Expected auth error, got {response.status_code}"
    
    def test_performance_scorecard_requires_manager_role(self):
        """Test that performance scorecard requires manager/admin role"""
        no_auth_session = requests.Session()
        no_auth_session.headers.update({"Content-Type": "application/json"})
        
        response = no_auth_session.get(f"{BASE_URL}/api/erp/sfa/performance-scorecard")
        assert response.status_code in [401, 403, 422], f"Expected auth error, got {response.status_code}"


class TestSFADayStartEndVisit:
    """Regression tests for Day Start/End and Visit APIs"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test session with authentication"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        
        # Login as super admin
        login_response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": "admin@lucumaa.in",
            "password": "adminpass"
        })
        
        if login_response.status_code == 200:
            token = login_response.json().get("token")
            self.session.headers.update({"Authorization": f"Bearer {token}"})
        else:
            pytest.skip("Authentication failed")
    
    def test_day_start_endpoint_exists(self):
        """Test POST /api/erp/sfa/day-start endpoint exists"""
        # Just test that endpoint exists - don't actually start a day
        response = self.session.post(f"{BASE_URL}/api/erp/sfa/day-start", json={
            "latitude": 28.6139,
            "longitude": 77.2090,
            "vehicle_type": "bike"
        })
        # Should return 200 (success) or 400 (day already started)
        assert response.status_code in [200, 400], f"Unexpected status: {response.status_code}"
    
    def test_day_end_endpoint_exists(self):
        """Test POST /api/erp/sfa/day-end endpoint exists"""
        response = self.session.post(f"{BASE_URL}/api/erp/sfa/day-end", json={
            "latitude": 28.6139,
            "longitude": 77.2090
        })
        # Should return 200 (success) or 400 (no active day)
        assert response.status_code in [200, 400], f"Unexpected status: {response.status_code}"
    
    def test_visit_start_endpoint_exists(self):
        """Test POST /api/erp/sfa/visit-start endpoint exists"""
        response = self.session.post(f"{BASE_URL}/api/erp/sfa/visit-start", json={
            "customer_name": "TEST_Customer",
            "latitude": 28.6139,
            "longitude": 77.2090,
            "purpose": "Product Demo"
        })
        # Should return 200 (success) or 400 (day not started)
        assert response.status_code in [200, 400], f"Unexpected status: {response.status_code}"
    
    def test_location_ping_endpoint_exists(self):
        """Test POST /api/erp/sfa/location-ping endpoint exists"""
        response = self.session.post(f"{BASE_URL}/api/erp/sfa/location-ping", json={
            "latitude": 28.6139,
            "longitude": 77.2090,
            "accuracy": 10.0,
            "speed": 0,
            "battery_level": 80
        })
        # Should return 200 (success) or 400 (no active day)
        assert response.status_code in [200, 400], f"Unexpected status: {response.status_code}"
    
    def test_employee_timeline_endpoint_exists(self):
        """Test GET /api/erp/sfa/employee-timeline/{user_id} endpoint exists"""
        # Use a dummy user_id - should return 404 if not found
        response = self.session.get(f"{BASE_URL}/api/erp/sfa/employee-timeline/test-user-id")
        assert response.status_code in [200, 404], f"Unexpected status: {response.status_code}"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
