"""
Test Share Configuration Features for Glass Configurator
- POST /api/erp/glass-config/share - Create shareable link
- GET /api/erp/glass-config/share/{share_id} - Retrieve shared config
- POST /api/erp/glass-config/export-pdf-multipage - Multi-page PDF export
- POST /api/erp/glass-config/quotation/{id}/send-email - Email quotation
"""
import pytest
import requests
import os
import time

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', '').rstrip('/')

class TestShareConfiguration:
    """Test Share Configuration API endpoints"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test data"""
        self.glass_config = {
            "width_mm": 900,
            "height_mm": 600,
            "thickness_mm": 8,
            "glass_type": "toughened",
            "color_name": "Clear",
            "application": "window"
        }
        self.cutouts = [
            {
                "type": "SH",
                "diameter": 50,
                "x": 200,
                "y": 300,
                "rotation": 0
            },
            {
                "type": "R",
                "width": 100,
                "height": 80,
                "x": 500,
                "y": 300,
                "rotation": 0
            }
        ]
    
    def test_create_share_link(self):
        """Test POST /api/erp/glass-config/share - Create shareable link"""
        response = requests.post(
            f"{BASE_URL}/api/erp/glass-config/share",
            json={
                "glass_config": self.glass_config,
                "cutouts": self.cutouts,
                "title": "Test Share Configuration"
            }
        )
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        data = response.json()
        
        # Verify response structure
        assert "share_id" in data, "Response should contain share_id"
        assert "share_url" in data, "Response should contain share_url"
        assert "expires_at" in data, "Response should contain expires_at"
        
        # Verify share_id format (8 characters)
        assert len(data["share_id"]) == 8, f"share_id should be 8 chars, got {len(data['share_id'])}"
        
        # Store for next test
        self.__class__.created_share_id = data["share_id"]
        print(f"✓ Created share link: {data['share_id']}")
    
    def test_get_shared_config(self):
        """Test GET /api/erp/glass-config/share/{share_id} - Retrieve shared config"""
        # First create a share link
        create_response = requests.post(
            f"{BASE_URL}/api/erp/glass-config/share",
            json={
                "glass_config": self.glass_config,
                "cutouts": self.cutouts,
                "title": "Test Retrieval"
            }
        )
        assert create_response.status_code == 200
        share_id = create_response.json()["share_id"]
        
        # Now retrieve it
        response = requests.get(f"{BASE_URL}/api/erp/glass-config/share/{share_id}")
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        data = response.json()
        
        # Verify response structure
        assert "glass_config" in data, "Response should contain glass_config"
        assert "cutouts" in data, "Response should contain cutouts"
        assert "title" in data, "Response should contain title"
        assert "view_count" in data, "Response should contain view_count"
        assert "expires_at" in data, "Response should contain expires_at"
        
        # Verify glass config data
        assert data["glass_config"]["width_mm"] == self.glass_config["width_mm"]
        assert data["glass_config"]["height_mm"] == self.glass_config["height_mm"]
        
        # Verify cutouts
        assert len(data["cutouts"]) == 2, f"Expected 2 cutouts, got {len(data['cutouts'])}"
        
        print(f"✓ Retrieved shared config: {share_id}, view_count: {data['view_count']}")
    
    def test_get_shared_config_not_found(self):
        """Test GET /api/erp/glass-config/share/{share_id} - Non-existent share"""
        response = requests.get(f"{BASE_URL}/api/erp/glass-config/share/nonexist")
        
        assert response.status_code == 404, f"Expected 404, got {response.status_code}"
        print("✓ Non-existent share returns 404")
    
    def test_view_count_increments(self):
        """Test that view_count increments on each retrieval"""
        # Create share
        create_response = requests.post(
            f"{BASE_URL}/api/erp/glass-config/share",
            json={
                "glass_config": self.glass_config,
                "cutouts": self.cutouts,
                "title": "View Count Test"
            }
        )
        share_id = create_response.json()["share_id"]
        
        # First view
        response1 = requests.get(f"{BASE_URL}/api/erp/glass-config/share/{share_id}")
        view_count_1 = response1.json()["view_count"]
        
        # Second view
        response2 = requests.get(f"{BASE_URL}/api/erp/glass-config/share/{share_id}")
        view_count_2 = response2.json()["view_count"]
        
        assert view_count_2 > view_count_1, f"View count should increment: {view_count_1} -> {view_count_2}"
        print(f"✓ View count incremented: {view_count_1} -> {view_count_2}")


class TestMultiPagePDF:
    """Test Multi-page PDF export endpoint"""
    
    @pytest.fixture
    def auth_token(self):
        """Get authentication token"""
        response = requests.post(
            f"{BASE_URL}/api/auth/login",
            json={"email": "admin@lucumaa.in", "password": "adminpass"}
        )
        if response.status_code == 200:
            return response.json().get("token")
        pytest.skip("Authentication failed")
    
    def test_export_pdf_multipage(self, auth_token):
        """Test POST /api/erp/glass-config/export-pdf-multipage"""
        # Create many cutouts to test multi-page
        cutouts = []
        for i in range(20):  # 20 cutouts to trigger multi-page
            cutouts.append({
                "number": f"H{i+1}",
                "type": "Hole",
                "diameter": 30 + (i % 5) * 10,
                "x": 100 + (i % 5) * 150,
                "y": 100 + (i // 5) * 100,
                "rotation": 0,
                "left_edge": 100 + (i % 5) * 150 - 15,
                "right_edge": 900 - (100 + (i % 5) * 150) - 15,
                "top_edge": 600 - (100 + (i // 5) * 100) - 15,
                "bottom_edge": 100 + (i // 5) * 100 - 15
            })
        
        response = requests.post(
            f"{BASE_URL}/api/erp/glass-config/export-pdf-multipage",
            headers={"Authorization": f"Bearer {auth_token}"},
            json={
                "glass_config": {
                    "width_mm": 900,
                    "height_mm": 600,
                    "thickness_mm": 8,
                    "glass_type": "toughened",
                    "color_name": "Clear",
                    "application": "window"
                },
                "cutouts": cutouts,
                "quantity": 1
            }
        )
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        assert response.headers.get("content-type") == "application/pdf", "Response should be PDF"
        assert len(response.content) > 1000, "PDF should have substantial content"
        
        print(f"✓ Multi-page PDF generated: {len(response.content)} bytes")
    
    def test_export_pdf_multipage_unauthorized(self):
        """Test multi-page PDF export without auth"""
        response = requests.post(
            f"{BASE_URL}/api/erp/glass-config/export-pdf-multipage",
            json={
                "glass_config": {
                    "width_mm": 900,
                    "height_mm": 600,
                    "thickness_mm": 8,
                    "glass_type": "toughened",
                    "color_name": "Clear",
                    "application": "window"
                },
                "cutouts": [],
                "quantity": 1
            }
        )
        
        assert response.status_code in [401, 403], f"Expected 401/403, got {response.status_code}"
        print("✓ Multi-page PDF requires authentication")


class TestEmailQuotation:
    """Test Email Quotation endpoint"""
    
    @pytest.fixture
    def auth_token(self):
        """Get authentication token"""
        response = requests.post(
            f"{BASE_URL}/api/auth/login",
            json={"email": "admin@lucumaa.in", "password": "adminpass"}
        )
        if response.status_code == 200:
            return response.json().get("token")
        pytest.skip("Authentication failed")
    
    @pytest.fixture
    def quotation_id(self, auth_token):
        """Create a quotation for testing"""
        response = requests.post(
            f"{BASE_URL}/api/erp/glass-config/quotation",
            headers={"Authorization": f"Bearer {auth_token}"},
            json={
                "config": {
                    "glass_type": "toughened",
                    "thickness_mm": 8,
                    "color_id": "clear",
                    "color_name": "Clear",
                    "application": "window",
                    "width_mm": 900,
                    "height_mm": 600,
                    "holes_cutouts": [],
                    "quantity": 1,
                    "needs_transport": False
                },
                "customer_name": "Test Customer",
                "customer_mobile": "9876543210",
                "customer_email": "test@example.com",
                "validity_days": 7
            }
        )
        if response.status_code in [200, 201]:
            return response.json().get("quotation_id")
        pytest.skip(f"Failed to create quotation: {response.text}")
    
    def test_send_email_endpoint_exists(self, auth_token, quotation_id):
        """Test POST /api/erp/glass-config/quotation/{id}/send-email endpoint exists"""
        response = requests.post(
            f"{BASE_URL}/api/erp/glass-config/quotation/{quotation_id}/send-email",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        
        # The endpoint should exist (may fail due to SMTP config, but shouldn't be 404)
        assert response.status_code != 404, "Email endpoint should exist"
        
        # If SMTP is not configured, it may return 500 with specific error
        if response.status_code == 500:
            error_msg = response.json().get("detail", "")
            assert "email" in error_msg.lower() or "smtp" in error_msg.lower(), \
                f"500 error should be email-related: {error_msg}"
            print(f"✓ Email endpoint exists (SMTP not configured: {error_msg[:50]}...)")
        else:
            print(f"✓ Email endpoint returned: {response.status_code}")
    
    def test_send_email_unauthorized(self):
        """Test email sending without auth"""
        response = requests.post(
            f"{BASE_URL}/api/erp/glass-config/quotation/test-id/send-email"
        )
        
        assert response.status_code in [401, 403], f"Expected 401/403, got {response.status_code}"
        print("✓ Email sending requires authentication")


class TestShareOrderFlow:
    """Test ordering from shared configuration"""
    
    @pytest.fixture
    def auth_token(self):
        """Get authentication token"""
        response = requests.post(
            f"{BASE_URL}/api/auth/login",
            json={"email": "admin@lucumaa.in", "password": "adminpass"}
        )
        if response.status_code == 200:
            return response.json().get("token")
        pytest.skip("Authentication failed")
    
    def test_order_from_shared_config(self, auth_token):
        """Test POST /api/erp/glass-config/share/{share_id}/order"""
        # First create a share
        create_response = requests.post(
            f"{BASE_URL}/api/erp/glass-config/share",
            json={
                "glass_config": {
                    "width_mm": 900,
                    "height_mm": 600,
                    "thickness_mm": 8,
                    "glass_type": "toughened",
                    "color_name": "Clear",
                    "application": "window"
                },
                "cutouts": [{"type": "SH", "diameter": 50, "x": 200, "y": 300, "rotation": 0}],
                "title": "Order Test"
            }
        )
        share_id = create_response.json()["share_id"]
        
        # Order from shared config
        response = requests.post(
            f"{BASE_URL}/api/erp/glass-config/share/{share_id}/order",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        data = response.json()
        
        assert "glass_config" in data, "Response should contain glass_config"
        assert "cutouts" in data, "Response should contain cutouts"
        assert "share_id" in data, "Response should contain share_id"
        
        print(f"✓ Order from shared config: {share_id}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
