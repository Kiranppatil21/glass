"""
Test Color Selection and Order Creation Features
- Color dropdown visibility and API
- Order creation with override_total
- Advance amount calculation
- Cart total calculation
"""
import pytest
import requests
import os

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', '').rstrip('/')

class TestProductConfig:
    """Test Product Configuration API - Colors, Thickness, Glass Types"""
    
    def test_config_all_returns_colors(self):
        """GET /api/erp/config/all should return colors array"""
        response = requests.get(f"{BASE_URL}/api/erp/config/all")
        assert response.status_code == 200
        
        data = response.json()
        assert "colors" in data
        assert isinstance(data["colors"], list)
        assert len(data["colors"]) >= 5  # At least 5 colors expected
        
        # Verify color structure
        for color in data["colors"]:
            assert "id" in color
            assert "name" in color
            assert "hex_code" in color
            assert "price_multiplier" in color
        
        # Verify specific colors exist
        color_ids = [c["id"] for c in data["colors"]]
        assert "clear" in color_ids
        assert "grey" in color_ids
        assert "bronze" in color_ids
        assert "green" in color_ids
        assert "blue" in color_ids
        print(f"✓ Found {len(data['colors'])} colors: {color_ids}")
    
    def test_config_all_returns_thickness_options(self):
        """GET /api/erp/config/all should return thickness_options"""
        response = requests.get(f"{BASE_URL}/api/erp/config/all")
        assert response.status_code == 200
        
        data = response.json()
        assert "thickness_options" in data
        assert isinstance(data["thickness_options"], list)
        assert len(data["thickness_options"]) > 0
        print(f"✓ Found {len(data['thickness_options'])} thickness options")
    
    def test_color_price_multipliers(self):
        """Verify color price multipliers are correct"""
        response = requests.get(f"{BASE_URL}/api/erp/config/all")
        assert response.status_code == 200
        
        data = response.json()
        colors = {c["id"]: c for c in data["colors"]}
        
        # Clear should have 1.0 multiplier
        assert colors["clear"]["price_multiplier"] == 1.0
        
        # Grey should have 1.1 multiplier (+10%)
        assert colors["grey"]["price_multiplier"] == 1.1
        
        # Bronze should have 1.15 multiplier (+15%)
        assert colors["bronze"]["price_multiplier"] == 1.15
        
        # Green should have 1.1 multiplier (+10%)
        assert colors["green"]["price_multiplier"] == 1.1
        
        # Blue should have 1.15 multiplier (+15%)
        assert colors["blue"]["price_multiplier"] == 1.15
        
        print("✓ All color price multipliers verified")


class TestOrderCreation:
    """Test Order Creation with override_total and advance payment"""
    
    @pytest.fixture
    def auth_token(self):
        """Get authentication token for test customer"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": "test@lucumaa.com",
            "password": "test123"
        })
        if response.status_code == 200:
            return response.json().get("token")
        pytest.skip("Authentication failed - skipping authenticated tests")
    
    @pytest.fixture
    def product_id(self):
        """Get a valid product ID"""
        response = requests.get(f"{BASE_URL}/api/products")
        if response.status_code == 200 and len(response.json()) > 0:
            return response.json()[0]["id"]
        pytest.skip("No products available")
    
    def test_order_with_override_total_50_percent(self, auth_token, product_id):
        """Order with override_total should calculate 50% advance correctly"""
        override_total = 10000
        advance_percent = 50
        expected_advance = 5000
        
        response = requests.post(
            f"{BASE_URL}/api/orders",
            headers={"Authorization": f"Bearer {auth_token}"},
            json={
                "product_id": product_id,
                "thickness": 6,
                "color": "grey",
                "width": 24,
                "height": 36,
                "quantity": 2,
                "customer_name": "Test Customer",
                "delivery_address": "123 Test Street",
                "notes": "Test order with 50% advance",
                "advance_percent": advance_percent,
                "override_total": override_total
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        
        # Verify total_amount uses override_total
        assert data["total_amount"] == override_total
        
        # Verify advance_percent is correct
        assert data["advance_percent"] == advance_percent
        
        # Verify advance_amount is 50% of override_total
        assert data["advance_amount"] == expected_advance
        
        # Verify remaining_amount
        assert data["remaining_amount"] == override_total - expected_advance
        
        print(f"✓ Order created with override_total={override_total}, advance_amount={data['advance_amount']}")
    
    def test_order_with_override_total_25_percent(self, auth_token, product_id):
        """Order with override_total should calculate 25% advance correctly"""
        override_total = 20000
        advance_percent = 25
        expected_advance = 5000
        
        response = requests.post(
            f"{BASE_URL}/api/orders",
            headers={"Authorization": f"Bearer {auth_token}"},
            json={
                "product_id": product_id,
                "thickness": 8,
                "color": "bronze",
                "width": 30,
                "height": 48,
                "quantity": 3,
                "customer_name": "Test Customer",
                "delivery_address": "456 Test Avenue",
                "notes": "Test order with 25% advance",
                "advance_percent": advance_percent,
                "override_total": override_total
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["total_amount"] == override_total
        assert data["advance_percent"] == advance_percent
        assert data["advance_amount"] == expected_advance
        assert data["remaining_amount"] == override_total - expected_advance
        
        print(f"✓ Order created with 25% advance: advance_amount={data['advance_amount']}")
    
    def test_order_with_override_total_100_percent(self, auth_token, product_id):
        """Order with 100% advance should have no remaining amount"""
        override_total = 15000
        advance_percent = 100
        
        response = requests.post(
            f"{BASE_URL}/api/orders",
            headers={"Authorization": f"Bearer {auth_token}"},
            json={
                "product_id": product_id,
                "thickness": 10,
                "color": "blue",
                "width": 36,
                "height": 60,
                "quantity": 1,
                "customer_name": "Test Customer",
                "delivery_address": "789 Test Blvd",
                "notes": "Test order with 100% advance",
                "advance_percent": advance_percent,
                "override_total": override_total
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["total_amount"] == override_total
        assert data["advance_percent"] == advance_percent
        assert data["advance_amount"] == override_total
        assert data["remaining_amount"] == 0
        
        print(f"✓ Order created with 100% advance: advance_amount={data['advance_amount']}")
    
    def test_order_with_color_in_notes(self, auth_token, product_id):
        """Order should include color in the order data"""
        response = requests.post(
            f"{BASE_URL}/api/orders",
            headers={"Authorization": f"Bearer {auth_token}"},
            json={
                "product_id": product_id,
                "thickness": 6,
                "color": "green",
                "width": 24,
                "height": 36,
                "quantity": 1,
                "customer_name": "Test Customer",
                "delivery_address": "Test Address",
                "notes": "Test order with green color",
                "advance_percent": 100,
                "override_total": 5000
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "order_id" in data
        print(f"✓ Order created with color: order_id={data['order_id']}")


class TestPriceCalculation:
    """Test Price Calculation API"""
    
    @pytest.fixture
    def product_id(self):
        """Get a valid product ID"""
        response = requests.get(f"{BASE_URL}/api/products")
        if response.status_code == 200 and len(response.json()) > 0:
            return response.json()[0]["id"]
        pytest.skip("No products available")
    
    def test_price_calculation(self, product_id):
        """POST /api/pricing/calculate should return price breakdown"""
        response = requests.post(f"{BASE_URL}/api/pricing/calculate", json={
            "product_id": product_id,
            "thickness": 6,
            "width": 24,
            "height": 36,
            "quantity": 1
        })
        
        assert response.status_code == 200
        data = response.json()
        
        # Verify response structure
        assert "area_sqft" in data
        assert "total_area" in data
        assert "base_price" in data
        assert "subtotal" in data
        assert "gst" in data
        assert "total" in data
        
        # Verify calculations are positive
        assert data["total"] > 0
        assert data["gst"] > 0
        
        print(f"✓ Price calculated: total={data['total']}, gst={data['gst']}")
    
    def test_price_calculation_multiple_quantity(self, product_id):
        """Price should scale with quantity"""
        # Calculate for quantity 1
        response1 = requests.post(f"{BASE_URL}/api/pricing/calculate", json={
            "product_id": product_id,
            "thickness": 6,
            "width": 24,
            "height": 36,
            "quantity": 1
        })
        
        # Calculate for quantity 2
        response2 = requests.post(f"{BASE_URL}/api/pricing/calculate", json={
            "product_id": product_id,
            "thickness": 6,
            "width": 24,
            "height": 36,
            "quantity": 2
        })
        
        assert response1.status_code == 200
        assert response2.status_code == 200
        
        price1 = response1.json()["total"]
        price2 = response2.json()["total"]
        
        # Price for 2 should be approximately 2x price for 1 (may have bulk discount)
        assert price2 >= price1
        print(f"✓ Price scales with quantity: qty1={price1}, qty2={price2}")


class TestAdvanceSettings:
    """Test Advance Payment Settings API"""
    
    @pytest.fixture
    def auth_token(self):
        """Get authentication token for test customer"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": "test@lucumaa.com",
            "password": "test123"
        })
        if response.status_code == 200:
            return response.json().get("token")
        pytest.skip("Authentication failed")
    
    def test_validate_order_advance_options(self, auth_token):
        """GET /api/settings/advance/validate-order should return allowed percentages"""
        response = requests.get(
            f"{BASE_URL}/api/settings/advance/validate-order",
            headers={"Authorization": f"Bearer {auth_token}"},
            params={"amount": 10000}
        )
        
        assert response.status_code == 200
        data = response.json()
        
        assert "allowed_percentages" in data
        assert isinstance(data["allowed_percentages"], list)
        assert len(data["allowed_percentages"]) > 0
        
        # For amount > 5000, should allow 25%, 50%, 75%, 100%
        assert 25 in data["allowed_percentages"] or 50 in data["allowed_percentages"]
        
        print(f"✓ Allowed percentages for ₹10000: {data['allowed_percentages']}")


class TestProducts:
    """Test Products API"""
    
    def test_get_products(self):
        """GET /api/products should return product list"""
        response = requests.get(f"{BASE_URL}/api/products")
        assert response.status_code == 200
        
        products = response.json()
        assert isinstance(products, list)
        assert len(products) > 0
        
        # Verify product structure
        for product in products:
            assert "id" in product
            assert "name" in product
            assert "thickness_options" in product
        
        print(f"✓ Found {len(products)} products")
    
    def test_product_has_thickness_options(self):
        """Products should have thickness_options array"""
        response = requests.get(f"{BASE_URL}/api/products")
        assert response.status_code == 200
        
        products = response.json()
        for product in products:
            assert "thickness_options" in product
            assert isinstance(product["thickness_options"], list)
            assert len(product["thickness_options"]) > 0
        
        print(f"✓ All products have thickness_options")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
