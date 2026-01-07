"""
Test Suite for 3D Glass Configurator and Job Work Features
- Glass Configurator (/customize) - step navigation, glass type/thickness/color/application selection
- Glass size input in mm with live price calculation
- Hole/cut-out addition (circle, square, rectangle) with position from edges
- Quotation generation with 7/15 day validity selection
- Job Work 3D Configurator (/job-work) - job type selection
- Job Work transport options - pickup and delivery with distance-based charges
- Backend pricing API - /api/erp/glass-config/pricing (public access)
- Backend price calculation - /api/erp/glass-config/calculate-price
- Backend quotation API - /api/erp/glass-config/quotation
"""
import pytest
import requests
import os
import json
from datetime import datetime

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', 'https://glassmesh.preview.emergentagent.com').rstrip('/')

# Test credentials
ADMIN_EMAIL = "admin@lucumaa.in"
ADMIN_PASSWORD = "adminpass"
TEST_CUSTOMER_EMAIL = "test@lucumaa.com"
TEST_CUSTOMER_PASSWORD = "test123"


@pytest.fixture(scope="module")
def admin_token():
    """Get admin authentication token"""
    response = requests.post(f"{BASE_URL}/api/auth/login", json={
        "email": ADMIN_EMAIL,
        "password": ADMIN_PASSWORD
    })
    if response.status_code == 200:
        return response.json().get("token")
    pytest.skip(f"Admin authentication failed: {response.status_code}")


@pytest.fixture(scope="module")
def customer_token():
    """Get customer authentication token"""
    response = requests.post(f"{BASE_URL}/api/auth/login", json={
        "email": TEST_CUSTOMER_EMAIL,
        "password": TEST_CUSTOMER_PASSWORD
    })
    if response.status_code == 200:
        return response.json().get("token")
    pytest.skip(f"Customer authentication failed: {response.status_code}")


@pytest.fixture
def api_client():
    """Shared requests session"""
    session = requests.Session()
    session.headers.update({"Content-Type": "application/json"})
    return session


class TestGlassPricingAPI:
    """Test /api/erp/glass-config/pricing endpoint"""
    
    def test_pricing_endpoint_returns_200(self, api_client, admin_token):
        """Test pricing endpoint is accessible with auth"""
        response = api_client.get(
            f"{BASE_URL}/api/erp/glass-config/pricing",
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        data = response.json()
        assert "glass_types" in data
        assert "thickness_options" in data
        assert "colors" in data
        assert "applications" in data
        print(f"✅ Pricing endpoint returns valid config with {len(data['glass_types'])} glass types")
    
    def test_pricing_has_glass_types(self, api_client, admin_token):
        """Test pricing config has glass types with required fields"""
        response = api_client.get(
            f"{BASE_URL}/api/erp/glass-config/pricing",
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        data = response.json()
        assert len(data["glass_types"]) > 0, "No glass types configured"
        
        glass_type = data["glass_types"][0]
        assert "glass_type" in glass_type
        assert "base_price_per_sqft" in glass_type
        assert "display_name" in glass_type
        assert glass_type["base_price_per_sqft"] > 0
        print(f"✅ Glass types configured: {[g['display_name'] for g in data['glass_types']]}")
    
    def test_pricing_has_thickness_options(self, api_client, admin_token):
        """Test pricing config has thickness options with multipliers"""
        response = api_client.get(
            f"{BASE_URL}/api/erp/glass-config/pricing",
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        data = response.json()
        assert len(data["thickness_options"]) > 0, "No thickness options configured"
        
        thickness = data["thickness_options"][0]
        assert "thickness_mm" in thickness
        assert "price_multiplier" in thickness
        assert "display_name" in thickness
        print(f"✅ Thickness options: {[t['display_name'] for t in data['thickness_options']]}")
    
    def test_pricing_has_colors(self, api_client, admin_token):
        """Test pricing config has colors with hex codes and price percentages"""
        response = api_client.get(
            f"{BASE_URL}/api/erp/glass-config/pricing",
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        data = response.json()
        assert len(data["colors"]) > 0, "No colors configured"
        
        color = data["colors"][0]
        assert "color_id" in color
        assert "color_name" in color
        assert "hex_code" in color
        assert "price_percentage" in color
        print(f"✅ Colors configured: {[c['color_name'] for c in data['colors']]}")
    
    def test_pricing_has_applications(self, api_client, admin_token):
        """Test pricing config has applications with multipliers"""
        response = api_client.get(
            f"{BASE_URL}/api/erp/glass-config/pricing",
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        data = response.json()
        assert len(data["applications"]) > 0, "No applications configured"
        
        app = data["applications"][0]
        assert "application_id" in app
        assert "application_name" in app
        assert "price_multiplier" in app
        print(f"✅ Applications configured: {[a['application_name'] for a in data['applications']]}")
    
    def test_pricing_has_hole_cutout_pricing(self, api_client, admin_token):
        """Test pricing config has hole/cut-out pricing with size slabs"""
        response = api_client.get(
            f"{BASE_URL}/api/erp/glass-config/pricing",
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        data = response.json()
        assert "hole_cutout_pricing" in data
        assert len(data["hole_cutout_pricing"]) > 0, "No hole pricing configured"
        
        hole_pricing = data["hole_cutout_pricing"][0]
        assert "shape" in hole_pricing
        assert "base_price" in hole_pricing
        assert "size_slabs" in hole_pricing
        print(f"✅ Hole pricing configured for shapes: {[h['shape'] for h in data['hole_cutout_pricing']]}")
    
    def test_pricing_has_transport_rates(self, api_client, admin_token):
        """Test pricing config has transport rates"""
        response = api_client.get(
            f"{BASE_URL}/api/erp/glass-config/pricing",
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        data = response.json()
        assert "transport_base_charge" in data
        assert "transport_per_km_rate" in data
        assert "gst_rate" in data
        assert data["transport_base_charge"] > 0
        assert data["transport_per_km_rate"] > 0
        assert data["gst_rate"] > 0
        print(f"✅ Transport: Base ₹{data['transport_base_charge']}, Per km ₹{data['transport_per_km_rate']}, GST {data['gst_rate']}%")


class TestPriceCalculationAPI:
    """Test /api/erp/glass-config/calculate-price endpoint"""
    
    def test_basic_price_calculation(self, api_client, admin_token):
        """Test basic price calculation with minimal config"""
        config = {
            "glass_type": "toughened",
            "thickness_mm": 5,
            "color_id": "clear",
            "color_name": "Clear",
            "application": "window",
            "width_mm": 600,
            "height_mm": 900,
            "holes_cutouts": [],
            "quantity": 1,
            "needs_transport": False
        }
        response = api_client.post(
            f"{BASE_URL}/api/erp/glass-config/calculate-price",
            json=config,
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        data = response.json()
        
        assert "price_breakdown" in data
        assert "total" in data
        breakdown = data["price_breakdown"]
        assert "area_sqft" in breakdown
        assert "glass_price" in breakdown
        assert "grand_total" in breakdown
        assert breakdown["grand_total"] > 0
        print(f"✅ Basic price calculation: ₹{breakdown['grand_total']} for {breakdown['area_sqft']} sq.ft")
    
    def test_price_calculation_with_thickness_multiplier(self, api_client, admin_token):
        """Test price increases with thicker glass"""
        base_config = {
            "glass_type": "toughened",
            "color_id": "clear",
            "color_name": "Clear",
            "application": "window",
            "width_mm": 1000,
            "height_mm": 1000,
            "holes_cutouts": [],
            "quantity": 1,
            "needs_transport": False
        }
        
        # Calculate for 5mm
        config_5mm = {**base_config, "thickness_mm": 5}
        response_5mm = api_client.post(
            f"{BASE_URL}/api/erp/glass-config/calculate-price",
            json=config_5mm,
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        price_5mm = response_5mm.json()["price_breakdown"]["grand_total"]
        
        # Calculate for 10mm
        config_10mm = {**base_config, "thickness_mm": 10}
        response_10mm = api_client.post(
            f"{BASE_URL}/api/erp/glass-config/calculate-price",
            json=config_10mm,
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        price_10mm = response_10mm.json()["price_breakdown"]["grand_total"]
        
        assert price_10mm > price_5mm, f"10mm (₹{price_10mm}) should be more expensive than 5mm (₹{price_5mm})"
        print(f"✅ Thickness multiplier working: 5mm=₹{price_5mm}, 10mm=₹{price_10mm}")
    
    def test_price_calculation_with_color_percentage(self, api_client, admin_token):
        """Test price increases with colored glass"""
        base_config = {
            "glass_type": "toughened",
            "thickness_mm": 6,
            "application": "window",
            "width_mm": 1000,
            "height_mm": 1000,
            "holes_cutouts": [],
            "quantity": 1,
            "needs_transport": False
        }
        
        # Calculate for clear
        config_clear = {**base_config, "color_id": "clear", "color_name": "Clear"}
        response_clear = api_client.post(
            f"{BASE_URL}/api/erp/glass-config/calculate-price",
            json=config_clear,
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        price_clear = response_clear.json()["price_breakdown"]["grand_total"]
        
        # Calculate for bronze (should have price_percentage > 0)
        config_bronze = {**base_config, "color_id": "bronze", "color_name": "Bronze"}
        response_bronze = api_client.post(
            f"{BASE_URL}/api/erp/glass-config/calculate-price",
            json=config_bronze,
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        price_bronze = response_bronze.json()["price_breakdown"]["grand_total"]
        
        assert price_bronze >= price_clear, f"Bronze (₹{price_bronze}) should be >= Clear (₹{price_clear})"
        print(f"✅ Color pricing working: Clear=₹{price_clear}, Bronze=₹{price_bronze}")
    
    def test_price_calculation_with_holes(self, api_client, admin_token):
        """Test price includes hole/cut-out charges"""
        config_no_holes = {
            "glass_type": "toughened",
            "thickness_mm": 6,
            "color_id": "clear",
            "color_name": "Clear",
            "application": "window",
            "width_mm": 1000,
            "height_mm": 1000,
            "holes_cutouts": [],
            "quantity": 1,
            "needs_transport": False
        }
        
        config_with_holes = {
            **config_no_holes,
            "holes_cutouts": [
                {
                    "id": "hole_1",
                    "shape": "circle",
                    "diameter_mm": 50,
                    "position_x_mm": 500,
                    "position_y_mm": 500,
                    "distance_left_mm": 475,
                    "distance_right_mm": 475,
                    "distance_top_mm": 475,
                    "distance_bottom_mm": 475
                }
            ]
        }
        
        response_no_holes = api_client.post(
            f"{BASE_URL}/api/erp/glass-config/calculate-price",
            json=config_no_holes,
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        price_no_holes = response_no_holes.json()["price_breakdown"]["grand_total"]
        
        response_with_holes = api_client.post(
            f"{BASE_URL}/api/erp/glass-config/calculate-price",
            json=config_with_holes,
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        data_with_holes = response_with_holes.json()
        price_with_holes = data_with_holes["price_breakdown"]["grand_total"]
        holes_total = data_with_holes["price_breakdown"]["holes_total"]
        
        assert price_with_holes > price_no_holes, f"With holes (₹{price_with_holes}) should be > without (₹{price_no_holes})"
        assert holes_total > 0, "Holes total should be > 0"
        print(f"✅ Hole pricing working: Without=₹{price_no_holes}, With holes=₹{price_with_holes}, Holes charge=₹{holes_total}")
    
    def test_price_calculation_with_transport(self, api_client, admin_token):
        """Test price includes transport charges"""
        config_no_transport = {
            "glass_type": "toughened",
            "thickness_mm": 6,
            "color_id": "clear",
            "color_name": "Clear",
            "application": "window",
            "width_mm": 1000,
            "height_mm": 1000,
            "holes_cutouts": [],
            "quantity": 1,
            "needs_transport": False
        }
        
        config_with_transport = {
            **config_no_transport,
            "needs_transport": True,
            "transport_address": "123 Test Street, Mumbai",
            "transport_distance_km": 20
        }
        
        response_no_transport = api_client.post(
            f"{BASE_URL}/api/erp/glass-config/calculate-price",
            json=config_no_transport,
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        price_no_transport = response_no_transport.json()["price_breakdown"]["grand_total"]
        
        response_with_transport = api_client.post(
            f"{BASE_URL}/api/erp/glass-config/calculate-price",
            json=config_with_transport,
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        data_with_transport = response_with_transport.json()
        price_with_transport = data_with_transport["price_breakdown"]["grand_total"]
        transport_charges = data_with_transport["price_breakdown"]["transport_charges"]
        
        assert price_with_transport > price_no_transport
        assert transport_charges > 0
        print(f"✅ Transport pricing working: Without=₹{price_no_transport}, With transport=₹{price_with_transport}, Transport=₹{transport_charges}")
    
    def test_price_calculation_with_quantity(self, api_client, admin_token):
        """Test price multiplies with quantity"""
        config_qty_1 = {
            "glass_type": "toughened",
            "thickness_mm": 6,
            "color_id": "clear",
            "color_name": "Clear",
            "application": "window",
            "width_mm": 1000,
            "height_mm": 1000,
            "holes_cutouts": [],
            "quantity": 1,
            "needs_transport": False
        }
        
        config_qty_3 = {**config_qty_1, "quantity": 3}
        
        response_qty_1 = api_client.post(
            f"{BASE_URL}/api/erp/glass-config/calculate-price",
            json=config_qty_1,
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        subtotal_qty_1 = response_qty_1.json()["price_breakdown"]["subtotal"]
        
        response_qty_3 = api_client.post(
            f"{BASE_URL}/api/erp/glass-config/calculate-price",
            json=config_qty_3,
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        subtotal_qty_3 = response_qty_3.json()["price_breakdown"]["subtotal"]
        
        # Subtotal for qty 3 should be approximately 3x qty 1
        ratio = subtotal_qty_3 / subtotal_qty_1
        assert 2.9 < ratio < 3.1, f"Qty 3 subtotal should be ~3x qty 1, got ratio {ratio}"
        print(f"✅ Quantity pricing working: Qty 1=₹{subtotal_qty_1}, Qty 3=₹{subtotal_qty_3}, Ratio={ratio:.2f}")


class TestQuotationAPI:
    """Test /api/erp/glass-config/quotation endpoint"""
    
    def test_create_quotation_7_days(self, api_client, admin_token):
        """Test creating quotation with 7 day validity"""
        quotation_data = {
            "config": {
                "glass_type": "toughened",
                "thickness_mm": 6,
                "color_id": "clear",
                "color_name": "Clear",
                "application": "window",
                "width_mm": 1000,
                "height_mm": 1500,
                "holes_cutouts": [],
                "quantity": 2,
                "needs_transport": False
            },
            "customer_name": "TEST_Quotation Customer",
            "customer_mobile": "9876543210",
            "customer_email": "test.quotation@example.com",
            "validity_days": 7,
            "is_job_work": False
        }
        
        response = api_client.post(
            f"{BASE_URL}/api/erp/glass-config/quotation",
            json=quotation_data,
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        data = response.json()
        
        assert "quotation_id" in data
        assert "quotation_number" in data
        assert data["quotation_number"].startswith("QT-")
        assert data["validity_days"] == 7
        assert "valid_until" in data
        assert data["status"] == "draft"
        assert data["grand_total"] > 0
        print(f"✅ Created quotation {data['quotation_number']} with 7 day validity, Total: ₹{data['grand_total']}")
        return data["quotation_id"]
    
    def test_create_quotation_15_days(self, api_client, admin_token):
        """Test creating quotation with 15 day validity"""
        quotation_data = {
            "config": {
                "glass_type": "laminated",
                "thickness_mm": 8,
                "color_id": "grey",
                "color_name": "Grey",
                "application": "partition",
                "width_mm": 1200,
                "height_mm": 2000,
                "holes_cutouts": [
                    {
                        "id": "hole_test",
                        "shape": "circle",
                        "diameter_mm": 30,
                        "position_x_mm": 600,
                        "position_y_mm": 1000,
                        "distance_left_mm": 585,
                        "distance_right_mm": 585,
                        "distance_top_mm": 985,
                        "distance_bottom_mm": 985
                    }
                ],
                "quantity": 1,
                "needs_transport": True,
                "transport_address": "456 Test Road, Delhi",
                "transport_distance_km": 15
            },
            "customer_name": "TEST_15Day Customer",
            "customer_mobile": "9876543211",
            "validity_days": 15,
            "is_job_work": False
        }
        
        response = api_client.post(
            f"{BASE_URL}/api/erp/glass-config/quotation",
            json=quotation_data,
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        assert response.status_code == 200
        data = response.json()
        
        assert data["validity_days"] == 15
        assert data["price_breakdown"]["holes_total"] > 0
        assert data["price_breakdown"]["transport_charges"] > 0
        print(f"✅ Created quotation {data['quotation_number']} with 15 day validity, Holes: ₹{data['price_breakdown']['holes_total']}, Transport: ₹{data['price_breakdown']['transport_charges']}")
    
    def test_create_job_work_quotation(self, api_client, admin_token):
        """Test creating job work quotation"""
        quotation_data = {
            "config": {
                "glass_type": "toughened",
                "thickness_mm": 10,
                "color_id": "clear",
                "color_name": "Clear",
                "application": "other",
                "width_mm": 800,
                "height_mm": 1200,
                "holes_cutouts": [],
                "quantity": 5,
                "needs_transport": True,
                "transport_address": "789 Factory Lane",
                "transport_distance_km": 25
            },
            "customer_name": "TEST_JobWork Customer",
            "customer_mobile": "9876543212",
            "validity_days": 7,
            "is_job_work": True,
            "job_work_type": "toughening"
        }
        
        response = api_client.post(
            f"{BASE_URL}/api/erp/glass-config/quotation",
            json=quotation_data,
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        assert response.status_code == 200
        data = response.json()
        
        assert data["is_job_work"] == True
        assert data["job_work_type"] == "toughening"
        print(f"✅ Created job work quotation {data['quotation_number']} for {data['job_work_type']}")
    
    def test_list_quotations(self, api_client, admin_token):
        """Test listing quotations"""
        response = api_client.get(
            f"{BASE_URL}/api/erp/glass-config/quotations",
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        assert response.status_code == 200
        data = response.json()
        
        assert "quotations" in data
        assert "total" in data
        assert data["total"] >= 0
        print(f"✅ Listed {data['total']} quotations")
    
    def test_list_job_work_quotations(self, api_client, admin_token):
        """Test filtering quotations by job work"""
        response = api_client.get(
            f"{BASE_URL}/api/erp/glass-config/quotations?is_job_work=true",
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        assert response.status_code == 200
        data = response.json()
        
        # All returned quotations should be job work
        for q in data["quotations"]:
            assert q.get("is_job_work") == True
        print(f"✅ Filtered {len(data['quotations'])} job work quotations")


class TestJobWorkLabourRatesAPI:
    """Test /api/erp/glass-config/job-work/labour-rates endpoint"""
    
    def test_get_labour_rates(self, api_client, admin_token):
        """Test getting job work labour rates"""
        response = api_client.get(
            f"{BASE_URL}/api/erp/glass-config/job-work/labour-rates",
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        assert response.status_code == 200
        data = response.json()
        
        assert "rates" in data
        assert "hole_rates" in data
        assert "transport_pickup_base" in data
        assert "transport_drop_base" in data
        assert "transport_per_km" in data
        
        # Check rates by thickness
        rates = data["rates"]
        assert len(rates) > 0
        print(f"✅ Labour rates by thickness: {rates}")
    
    def test_labour_rates_have_hole_pricing(self, api_client, admin_token):
        """Test labour rates include hole pricing"""
        response = api_client.get(
            f"{BASE_URL}/api/erp/glass-config/job-work/labour-rates",
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        data = response.json()
        
        hole_rates = data["hole_rates"]
        assert "circle" in hole_rates
        assert "square" in hole_rates
        assert "rectangle" in hole_rates
        
        for shape, rate in hole_rates.items():
            assert "base" in rate
            assert "per_mm" in rate
        print(f"✅ Hole rates: {hole_rates}")
    
    def test_labour_rates_have_transport_pricing(self, api_client, admin_token):
        """Test labour rates include transport pricing for pickup/delivery"""
        response = api_client.get(
            f"{BASE_URL}/api/erp/glass-config/job-work/labour-rates",
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        data = response.json()
        
        assert data["transport_pickup_base"] > 0
        assert data["transport_drop_base"] > 0
        assert data["transport_per_km"] > 0
        print(f"✅ Transport: Pickup base ₹{data['transport_pickup_base']}, Drop base ₹{data['transport_drop_base']}, Per km ₹{data['transport_per_km']}")


class TestQuotationWorkflow:
    """Test quotation workflow - send, view, approve, reject, convert"""
    
    @pytest.fixture
    def test_quotation(self, api_client, admin_token):
        """Create a test quotation for workflow tests"""
        quotation_data = {
            "config": {
                "glass_type": "toughened",
                "thickness_mm": 6,
                "color_id": "clear",
                "color_name": "Clear",
                "application": "window",
                "width_mm": 1000,
                "height_mm": 1000,
                "holes_cutouts": [],
                "quantity": 1,
                "needs_transport": False
            },
            "customer_name": "TEST_Workflow Customer",
            "customer_mobile": "9876543299",
            "validity_days": 7,
            "is_job_work": False
        }
        
        response = api_client.post(
            f"{BASE_URL}/api/erp/glass-config/quotation",
            json=quotation_data,
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        return response.json()
    
    def test_get_quotation_by_id(self, api_client, admin_token, test_quotation):
        """Test getting quotation by ID"""
        quotation_id = test_quotation["quotation_id"]
        response = api_client.get(
            f"{BASE_URL}/api/erp/glass-config/quotation/{quotation_id}",
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        assert response.status_code == 200
        data = response.json()
        assert data["quotation_id"] == quotation_id
        print(f"✅ Retrieved quotation {data['quotation_number']}")
    
    def test_send_quotation(self, api_client, admin_token, test_quotation):
        """Test marking quotation as sent"""
        quotation_id = test_quotation["quotation_id"]
        response = api_client.post(
            f"{BASE_URL}/api/erp/glass-config/quotation/{quotation_id}/send",
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        assert response.status_code == 200
        
        # Verify status changed
        get_response = api_client.get(
            f"{BASE_URL}/api/erp/glass-config/quotation/{quotation_id}",
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        assert get_response.json()["status"] == "sent"
        print(f"✅ Quotation marked as sent")
    
    def test_approve_quotation(self, api_client, admin_token):
        """Test approving a quotation"""
        # Create and send a quotation first
        quotation_data = {
            "config": {
                "glass_type": "toughened",
                "thickness_mm": 6,
                "color_id": "clear",
                "color_name": "Clear",
                "application": "window",
                "width_mm": 1000,
                "height_mm": 1000,
                "holes_cutouts": [],
                "quantity": 1,
                "needs_transport": False
            },
            "customer_name": "TEST_Approve Customer",
            "customer_mobile": "9876543298",
            "validity_days": 7,
            "is_job_work": False
        }
        
        create_response = api_client.post(
            f"{BASE_URL}/api/erp/glass-config/quotation",
            json=quotation_data,
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        quotation_id = create_response.json()["quotation_id"]
        
        # Send it
        api_client.post(
            f"{BASE_URL}/api/erp/glass-config/quotation/{quotation_id}/send",
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        
        # Approve it
        approve_response = api_client.post(
            f"{BASE_URL}/api/erp/glass-config/quotation/{quotation_id}/approve",
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        assert approve_response.status_code == 200
        
        # Verify status
        get_response = api_client.get(
            f"{BASE_URL}/api/erp/glass-config/quotation/{quotation_id}",
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        assert get_response.json()["status"] == "approved"
        print(f"✅ Quotation approved successfully")
    
    def test_reject_quotation(self, api_client, admin_token):
        """Test rejecting a quotation"""
        # Create a quotation
        quotation_data = {
            "config": {
                "glass_type": "toughened",
                "thickness_mm": 6,
                "color_id": "clear",
                "color_name": "Clear",
                "application": "window",
                "width_mm": 1000,
                "height_mm": 1000,
                "holes_cutouts": [],
                "quantity": 1,
                "needs_transport": False
            },
            "customer_name": "TEST_Reject Customer",
            "customer_mobile": "9876543297",
            "validity_days": 7,
            "is_job_work": False
        }
        
        create_response = api_client.post(
            f"{BASE_URL}/api/erp/glass-config/quotation",
            json=quotation_data,
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        quotation_id = create_response.json()["quotation_id"]
        
        # Reject it
        reject_response = api_client.post(
            f"{BASE_URL}/api/erp/glass-config/quotation/{quotation_id}/reject?reason=Price%20too%20high",
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        assert reject_response.status_code == 200
        
        # Verify status
        get_response = api_client.get(
            f"{BASE_URL}/api/erp/glass-config/quotation/{quotation_id}",
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        assert get_response.json()["status"] == "rejected"
        print(f"✅ Quotation rejected successfully")
    
    def test_convert_quotation_to_order(self, api_client, admin_token):
        """Test converting approved quotation to order"""
        # Create, send, and approve a quotation
        quotation_data = {
            "config": {
                "glass_type": "toughened",
                "thickness_mm": 6,
                "color_id": "clear",
                "color_name": "Clear",
                "application": "window",
                "width_mm": 1000,
                "height_mm": 1000,
                "holes_cutouts": [],
                "quantity": 1,
                "needs_transport": False
            },
            "customer_name": "TEST_Convert Customer",
            "customer_mobile": "9876543296",
            "validity_days": 7,
            "is_job_work": False
        }
        
        create_response = api_client.post(
            f"{BASE_URL}/api/erp/glass-config/quotation",
            json=quotation_data,
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        quotation_id = create_response.json()["quotation_id"]
        
        # Send and approve
        api_client.post(
            f"{BASE_URL}/api/erp/glass-config/quotation/{quotation_id}/send",
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        api_client.post(
            f"{BASE_URL}/api/erp/glass-config/quotation/{quotation_id}/approve",
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        
        # Convert to order
        convert_response = api_client.post(
            f"{BASE_URL}/api/erp/glass-config/quotation/{quotation_id}/convert-to-order",
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        assert convert_response.status_code == 200
        data = convert_response.json()
        
        assert "order" in data
        assert data["order"]["order_number"].startswith("ORD-")
        assert data["order"]["quotation_id"] == quotation_id
        print(f"✅ Quotation converted to order {data['order']['order_number']}")


class TestAuditTrail:
    """Test audit trail functionality"""
    
    def test_get_audit_trail(self, api_client, admin_token):
        """Test getting audit trail (admin only)"""
        response = api_client.get(
            f"{BASE_URL}/api/erp/glass-config/audit-trail",
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        assert response.status_code == 200
        data = response.json()
        
        assert "logs" in data
        assert "total" in data
        print(f"✅ Retrieved {data['total']} audit logs")
    
    def test_audit_trail_filter_by_action(self, api_client, admin_token):
        """Test filtering audit trail by action"""
        response = api_client.get(
            f"{BASE_URL}/api/erp/glass-config/audit-trail?action=quotation_created",
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        assert response.status_code == 200
        data = response.json()
        
        for log in data["logs"]:
            assert log["action"] == "quotation_created"
        print(f"✅ Filtered audit logs by action: {len(data['logs'])} quotation_created logs")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
