"""
Test QR Code and Barcode Generation APIs
Tests for Phase 1 QR code features:
- QR code generation for job cards
- Barcode generation for job cards
- Print data API for job cards
"""
import pytest
import requests
import os

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', 'https://glassmesh.preview.emergentagent.com')


class TestQRCodeAPIs:
    """QR Code and Barcode API tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup - login and get auth token"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        
        # Login to get token
        login_response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": "admin@lucumaa.in",
            "password": "adminpass"
        })
        assert login_response.status_code == 200, f"Login failed: {login_response.text}"
        
        token = login_response.json().get("token")
        self.session.headers.update({"Authorization": f"Bearer {token}"})
        
        # Get a valid job card number for testing
        orders_response = self.session.get(f"{BASE_URL}/api/erp/production/orders")
        assert orders_response.status_code == 200
        orders = orders_response.json()
        
        if orders:
            self.job_card_number = orders[0].get("job_card_number")
        else:
            # Create a test order if none exist
            create_response = self.session.post(f"{BASE_URL}/api/erp/production/orders", json={
                "glass_type": "Toughened Glass",
                "thickness": 10,
                "width": 48,
                "height": 72,
                "quantity": 5,
                "priority": 1
            })
            assert create_response.status_code in [200, 201]
            self.job_card_number = create_response.json().get("job_card")
    
    # ==================== QR Code Image Tests ====================
    
    def test_get_job_card_qr_code(self):
        """Test QR code generation for valid job card"""
        response = self.session.get(
            f"{BASE_URL}/api/erp/qr/job-card/{self.job_card_number}"
        )
        assert response.status_code == 200
        # Check content type is image/png
        assert "image/png" in response.headers.get("content-type", "")
        # Check response has content (PNG image)
        assert len(response.content) > 1000  # PNG should be at least 1KB
    
    def test_get_job_card_qr_code_with_custom_size(self):
        """Test QR code generation with custom size parameter"""
        response = self.session.get(
            f"{BASE_URL}/api/erp/qr/job-card/{self.job_card_number}?size=300"
        )
        assert response.status_code == 200
        assert "image/png" in response.headers.get("content-type", "")
    
    def test_get_job_card_qr_code_invalid_job_card(self):
        """Test QR code generation for non-existent job card"""
        response = self.session.get(
            f"{BASE_URL}/api/erp/qr/job-card/INVALID_JOB_CARD_123"
        )
        assert response.status_code == 404
        data = response.json()
        assert "detail" in data
        assert "not found" in data["detail"].lower()
    
    # ==================== Barcode Image Tests ====================
    
    def test_get_job_card_barcode(self):
        """Test barcode generation for valid job card"""
        response = self.session.get(
            f"{BASE_URL}/api/erp/qr/job-card/{self.job_card_number}/barcode"
        )
        assert response.status_code == 200
        # Check content type is image/png
        assert "image/png" in response.headers.get("content-type", "")
        # Check response has content (PNG image)
        assert len(response.content) > 500  # Barcode should be at least 500 bytes
    
    def test_get_job_card_barcode_invalid_job_card(self):
        """Test barcode generation for non-existent job card"""
        response = self.session.get(
            f"{BASE_URL}/api/erp/qr/job-card/INVALID_JOB_CARD_456/barcode"
        )
        assert response.status_code == 404
        data = response.json()
        assert "detail" in data
        assert "not found" in data["detail"].lower()
    
    # ==================== Print Data API Tests ====================
    
    def test_get_job_card_print_data(self):
        """Test print data API returns all required fields"""
        response = self.session.get(
            f"{BASE_URL}/api/erp/qr/job-card/{self.job_card_number}/print-data"
        )
        assert response.status_code == 200
        data = response.json()
        
        # Verify all required fields are present
        required_fields = [
            "job_card_number",
            "glass_type",
            "thickness",
            "dimensions",
            "quantity",
            "current_stage",
            "priority",
            "created_at",
            "qr_code_base64",
            "barcode_base64"
        ]
        
        for field in required_fields:
            assert field in data, f"Missing field: {field}"
    
    def test_print_data_qr_code_is_base64(self):
        """Test that QR code in print data is valid base64 data URL"""
        response = self.session.get(
            f"{BASE_URL}/api/erp/qr/job-card/{self.job_card_number}/print-data"
        )
        assert response.status_code == 200
        data = response.json()
        
        qr_base64 = data.get("qr_code_base64", "")
        # Should start with data:image/png;base64,
        assert qr_base64.startswith("data:image/png;base64,"), "QR code should be base64 data URL"
        # Should have actual base64 content
        assert len(qr_base64) > 100, "QR code base64 should have content"
    
    def test_print_data_barcode_is_base64(self):
        """Test that barcode in print data is valid base64 data URL"""
        response = self.session.get(
            f"{BASE_URL}/api/erp/qr/job-card/{self.job_card_number}/print-data"
        )
        assert response.status_code == 200
        data = response.json()
        
        barcode_base64 = data.get("barcode_base64", "")
        # Should start with data:image/png;base64,
        assert barcode_base64.startswith("data:image/png;base64,"), "Barcode should be base64 data URL"
        # Should have actual base64 content
        assert len(barcode_base64) > 100, "Barcode base64 should have content"
    
    def test_print_data_job_card_number_matches(self):
        """Test that print data returns correct job card number"""
        response = self.session.get(
            f"{BASE_URL}/api/erp/qr/job-card/{self.job_card_number}/print-data"
        )
        assert response.status_code == 200
        data = response.json()
        
        assert data["job_card_number"] == self.job_card_number
    
    def test_print_data_invalid_job_card(self):
        """Test print data API for non-existent job card"""
        response = self.session.get(
            f"{BASE_URL}/api/erp/qr/job-card/INVALID_JOB_CARD_789/print-data"
        )
        assert response.status_code == 404
        data = response.json()
        assert "detail" in data
        assert "not found" in data["detail"].lower()
    
    def test_print_data_dimensions_format(self):
        """Test that dimensions are in correct format (width x height mm)"""
        response = self.session.get(
            f"{BASE_URL}/api/erp/qr/job-card/{self.job_card_number}/print-data"
        )
        assert response.status_code == 200
        data = response.json()
        
        dimensions = data.get("dimensions", "")
        # Should contain 'x' and 'mm'
        assert "x" in dimensions.lower(), "Dimensions should contain 'x'"
        assert "mm" in dimensions.lower(), "Dimensions should contain 'mm'"
    
    # Note: QR code APIs are accessible without authentication
    # This is likely intentional for QR code scanning purposes
    # The APIs still validate that the job card exists


class TestInvoiceQRCode:
    """Invoice QR Code API tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup - login and get auth token"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        
        # Login to get token
        login_response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": "admin@lucumaa.in",
            "password": "adminpass"
        })
        assert login_response.status_code == 200
        
        token = login_response.json().get("token")
        self.session.headers.update({"Authorization": f"Bearer {token}"})
        
        # Get a valid invoice number for testing
        invoices_response = self.session.get(f"{BASE_URL}/api/erp/accounts/invoices")
        if invoices_response.status_code == 200:
            invoices = invoices_response.json()
            if invoices:
                self.invoice_number = invoices[0].get("invoice_number")
            else:
                self.invoice_number = None
        else:
            self.invoice_number = None
    
    def test_get_invoice_qr_code(self):
        """Test QR code generation for valid invoice"""
        if not self.invoice_number:
            pytest.skip("No invoices available for testing")
        
        response = self.session.get(
            f"{BASE_URL}/api/erp/qr/invoice/{self.invoice_number}"
        )
        assert response.status_code == 200
        assert "image/png" in response.headers.get("content-type", "")
    
    def test_get_invoice_qr_code_invalid(self):
        """Test QR code generation for non-existent invoice"""
        response = self.session.get(
            f"{BASE_URL}/api/erp/qr/invoice/INVALID_INV_123"
        )
        assert response.status_code == 404


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
