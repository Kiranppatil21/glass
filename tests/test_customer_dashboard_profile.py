"""
Test Customer Dashboard Profile and Multi-item Order Builder Features
- Profile tab with Edit/Save functionality
- Create New Order button
- PUT /api/users/profile endpoint
- Multi-item order builder on Customize page
"""
import pytest
import requests
import os

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', 'https://glassmesh.preview.emergentagent.com')

# Test credentials
TEST_CUSTOMER_EMAIL = "test@lucumaa.com"
TEST_CUSTOMER_PASSWORD = "test123"
ADMIN_EMAIL = "admin@lucumaa.in"
ADMIN_PASSWORD = "adminpass"


class TestProfileAPI:
    """Test Profile API endpoints"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test session"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        self.token = None
    
    def login_customer(self):
        """Login as test customer and get token"""
        response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": TEST_CUSTOMER_EMAIL,
            "password": TEST_CUSTOMER_PASSWORD
        })
        if response.status_code == 200:
            self.token = response.json().get("token")
            self.session.headers.update({"Authorization": f"Bearer {self.token}"})
            return True
        return False
    
    def test_customer_login(self):
        """Test customer login with test credentials"""
        response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": TEST_CUSTOMER_EMAIL,
            "password": TEST_CUSTOMER_PASSWORD
        })
        print(f"Login response status: {response.status_code}")
        print(f"Login response: {response.text[:500]}")
        
        assert response.status_code == 200, f"Login failed: {response.text}"
        data = response.json()
        assert "token" in data, "Token not in response"
        assert "user" in data, "User not in response"
        assert data["user"]["email"] == TEST_CUSTOMER_EMAIL
    
    def test_get_profile(self):
        """Test GET /api/users/profile endpoint"""
        assert self.login_customer(), "Login failed"
        
        response = self.session.get(f"{BASE_URL}/api/users/profile")
        print(f"Get profile status: {response.status_code}")
        print(f"Get profile response: {response.text[:500]}")
        
        assert response.status_code == 200, f"Get profile failed: {response.text}"
        data = response.json()
        assert "email" in data, "Email not in profile"
        assert data["email"] == TEST_CUSTOMER_EMAIL
    
    def test_update_profile(self):
        """Test PUT /api/users/profile endpoint"""
        assert self.login_customer(), "Login failed"
        
        # Update profile with test data
        update_data = {
            "name": "Test Customer Updated",
            "phone": "9876543210",
            "company_name": "Test Company",
            "gst_number": "27AAPFU0939F1ZV",
            "address": "123 Test Street",
            "city": "Pune",
            "state": "Maharashtra",
            "pincode": "411001"
        }
        
        response = self.session.put(f"{BASE_URL}/api/users/profile", json=update_data)
        print(f"Update profile status: {response.status_code}")
        print(f"Update profile response: {response.text[:500]}")
        
        assert response.status_code == 200, f"Update profile failed: {response.text}"
        data = response.json()
        assert "message" in data, "Message not in response"
        
        # Verify update by getting profile again
        response = self.session.get(f"{BASE_URL}/api/users/profile")
        assert response.status_code == 200
        profile = response.json()
        assert profile.get("company_name") == "Test Company", "Company name not updated"
        assert profile.get("city") == "Pune", "City not updated"
    
    def test_update_profile_partial(self):
        """Test partial profile update"""
        assert self.login_customer(), "Login failed"
        
        # Update only phone
        update_data = {
            "phone": "1234567890"
        }
        
        response = self.session.put(f"{BASE_URL}/api/users/profile", json=update_data)
        print(f"Partial update status: {response.status_code}")
        
        assert response.status_code == 200, f"Partial update failed: {response.text}"
    
    def test_profile_without_auth(self):
        """Test profile endpoints without authentication"""
        # GET without auth
        response = self.session.get(f"{BASE_URL}/api/users/profile")
        print(f"Get profile without auth status: {response.status_code}")
        assert response.status_code in [401, 403], "Should require authentication"
        
        # PUT without auth
        response = self.session.put(f"{BASE_URL}/api/users/profile", json={"name": "Test"})
        print(f"Update profile without auth status: {response.status_code}")
        assert response.status_code in [401, 403], "Should require authentication"


class TestOrdersAPI:
    """Test Orders API for multi-item order builder"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test session"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        self.token = None
    
    def login_customer(self):
        """Login as test customer and get token"""
        response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": TEST_CUSTOMER_EMAIL,
            "password": TEST_CUSTOMER_PASSWORD
        })
        if response.status_code == 200:
            self.token = response.json().get("token")
            self.session.headers.update({"Authorization": f"Bearer {self.token}"})
            return True
        return False
    
    def test_get_products(self):
        """Test GET /api/products endpoint"""
        response = self.session.get(f"{BASE_URL}/api/products")
        print(f"Get products status: {response.status_code}")
        print(f"Products count: {len(response.json()) if response.status_code == 200 else 0}")
        
        assert response.status_code == 200, f"Get products failed: {response.text}"
        products = response.json()
        assert isinstance(products, list), "Products should be a list"
        assert len(products) > 0, "Should have at least one product"
        
        # Check product structure
        product = products[0]
        assert "id" in product, "Product should have id"
        assert "name" in product, "Product should have name"
        assert "thickness_options" in product, "Product should have thickness_options"
    
    def test_calculate_price(self):
        """Test POST /api/pricing/calculate endpoint"""
        # First get a product
        response = self.session.get(f"{BASE_URL}/api/products")
        assert response.status_code == 200
        products = response.json()
        assert len(products) > 0
        
        product = products[0]
        thickness = product.get("thickness_options", [5])[0] if product.get("thickness_options") else 5
        
        # Calculate price
        calc_data = {
            "product_id": product["id"],
            "thickness": thickness,
            "width": 24,
            "height": 36,
            "quantity": 2
        }
        
        response = self.session.post(f"{BASE_URL}/api/pricing/calculate", json=calc_data)
        print(f"Calculate price status: {response.status_code}")
        print(f"Price response: {response.text[:500]}")
        
        assert response.status_code == 200, f"Calculate price failed: {response.text}"
        data = response.json()
        assert "total" in data, "Total not in response"
        assert "area_sqft" in data, "Area not in response"
        assert data["total"] > 0, "Total should be positive"
    
    def test_get_my_orders(self):
        """Test GET /api/orders/my-orders endpoint"""
        assert self.login_customer(), "Login failed"
        
        response = self.session.get(f"{BASE_URL}/api/orders/my-orders")
        print(f"Get my orders status: {response.status_code}")
        
        assert response.status_code == 200, f"Get orders failed: {response.text}"
        orders = response.json()
        assert isinstance(orders, list), "Orders should be a list"
        print(f"Customer has {len(orders)} orders")
    
    def test_advance_settings_validate(self):
        """Test GET /api/settings/advance/validate-order endpoint"""
        assert self.login_customer(), "Login failed"
        
        response = self.session.get(f"{BASE_URL}/api/settings/advance/validate-order?amount=5000&advance_percent=50")
        print(f"Validate advance status: {response.status_code}")
        print(f"Validate response: {response.text[:500]}")
        
        assert response.status_code == 200, f"Validate advance failed: {response.text}"
        data = response.json()
        assert "allowed_percentages" in data, "allowed_percentages not in response"


class TestAuthMe:
    """Test auth/me endpoint"""
    
    def test_auth_me(self):
        """Test GET /api/auth/me endpoint"""
        session = requests.Session()
        session.headers.update({"Content-Type": "application/json"})
        
        # Login first
        response = session.post(f"{BASE_URL}/api/auth/login", json={
            "email": TEST_CUSTOMER_EMAIL,
            "password": TEST_CUSTOMER_PASSWORD
        })
        assert response.status_code == 200
        token = response.json().get("token")
        session.headers.update({"Authorization": f"Bearer {token}"})
        
        # Get current user
        response = session.get(f"{BASE_URL}/api/auth/me")
        print(f"Auth me status: {response.status_code}")
        print(f"Auth me response: {response.text[:500]}")
        
        assert response.status_code == 200, f"Auth me failed: {response.text}"
        data = response.json()
        assert "email" in data, "Email not in response"
        assert data["email"] == TEST_CUSTOMER_EMAIL


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
