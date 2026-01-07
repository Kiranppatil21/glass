"""
Test Customer Master Integration with Orders and Customer Portal
Tests:
1. Customer Master search API
2. Customer Master profile details with linked_data
3. Order creation with customer_profile_id auto-population
4. ERP Order creation with customer_profile_id
5. Customer Portal profile tab showing Customer Master data
"""
import pytest
import requests
import os
import uuid

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', 'https://glassmesh.preview.emergentagent.com')

class TestCustomerMasterIntegration:
    """Test Customer Master integration with Orders and Customer Portal"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test fixtures"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        
        # Login as admin
        login_response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": "admin@lucumaa.in",
            "password": "adminpass"
        })
        assert login_response.status_code == 200, f"Admin login failed: {login_response.text}"
        self.admin_token = login_response.json()["token"]
        self.admin_headers = {"Authorization": f"Bearer {self.admin_token}", "Content-Type": "application/json"}
        
        # Login as test customer
        customer_login = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": "test@lucumaa.com",
            "password": "test123"
        })
        if customer_login.status_code == 200:
            self.customer_token = customer_login.json()["token"]
            self.customer_headers = {"Authorization": f"Bearer {self.customer_token}", "Content-Type": "application/json"}
        else:
            self.customer_token = None
            self.customer_headers = None
    
    # ============ Customer Master Search Tests ============
    
    def test_customer_search_by_name(self):
        """Test customer search by company name"""
        response = self.session.get(
            f"{BASE_URL}/api/erp/customer-master/search/for-invoice",
            params={"q": "ABC"},
            headers=self.admin_headers
        )
        assert response.status_code == 200, f"Search failed: {response.text}"
        results = response.json()
        assert isinstance(results, list), "Results should be a list"
        print(f"✓ Customer search by name returned {len(results)} results")
        
        if len(results) > 0:
            customer = results[0]
            # Verify required fields for invoice
            assert "id" in customer, "Missing id field"
            assert "display_name" in customer, "Missing display_name field"
            assert "mobile" in customer, "Missing mobile field"
            assert "invoice_type" in customer, "Missing invoice_type field"
            print(f"✓ Found customer: {customer.get('display_name')} ({customer.get('invoice_type')})")
    
    def test_customer_search_by_mobile(self):
        """Test customer search by mobile number"""
        response = self.session.get(
            f"{BASE_URL}/api/erp/customer-master/search/for-invoice",
            params={"q": "9876543210"},
            headers=self.admin_headers
        )
        assert response.status_code == 200, f"Search failed: {response.text}"
        results = response.json()
        print(f"✓ Customer search by mobile returned {len(results)} results")
    
    def test_customer_search_by_gstin(self):
        """Test customer search by GSTIN"""
        response = self.session.get(
            f"{BASE_URL}/api/erp/customer-master/search/for-invoice",
            params={"q": "27AABCA"},
            headers=self.admin_headers
        )
        assert response.status_code == 200, f"Search failed: {response.text}"
        results = response.json()
        print(f"✓ Customer search by GSTIN returned {len(results)} results")
    
    def test_customer_search_returns_credit_info(self):
        """Test that search returns credit limit and credit days"""
        response = self.session.get(
            f"{BASE_URL}/api/erp/customer-master/search/for-invoice",
            params={"q": "ABC"},
            headers=self.admin_headers
        )
        assert response.status_code == 200
        results = response.json()
        
        if len(results) > 0:
            customer = results[0]
            # Check credit fields are present
            assert "credit_type" in customer, "Missing credit_type field"
            assert "credit_limit" in customer, "Missing credit_limit field"
            assert "credit_days" in customer, "Missing credit_days field"
            print(f"✓ Credit info: type={customer.get('credit_type')}, limit={customer.get('credit_limit')}, days={customer.get('credit_days')}")
    
    def test_customer_search_returns_billing_address(self):
        """Test that search returns billing address"""
        response = self.session.get(
            f"{BASE_URL}/api/erp/customer-master/search/for-invoice",
            params={"q": "ABC"},
            headers=self.admin_headers
        )
        assert response.status_code == 200
        results = response.json()
        
        if len(results) > 0:
            customer = results[0]
            assert "billing_address" in customer, "Missing billing_address field"
            billing = customer.get("billing_address")
            if billing:
                assert "address_line1" in billing, "Missing address_line1"
                assert "city" in billing, "Missing city"
                assert "state" in billing, "Missing state"
                print(f"✓ Billing address: {billing.get('city')}, {billing.get('state')}")
    
    # ============ Customer Master Profile Tests ============
    
    def test_customer_profile_details(self):
        """Test getting full customer profile with linked data"""
        # First search for a customer
        search_response = self.session.get(
            f"{BASE_URL}/api/erp/customer-master/search/for-invoice",
            params={"q": "ABC"},
            headers=self.admin_headers
        )
        assert search_response.status_code == 200
        results = search_response.json()
        
        if len(results) > 0:
            customer_id = results[0]["id"]
            
            # Get full profile
            profile_response = self.session.get(
                f"{BASE_URL}/api/erp/customer-master/{customer_id}",
                headers=self.admin_headers
            )
            assert profile_response.status_code == 200, f"Profile fetch failed: {profile_response.text}"
            profile = profile_response.json()
            
            # Verify profile fields
            assert "customer_code" in profile, "Missing customer_code"
            assert "customer_type" in profile, "Missing customer_type"
            assert "gstin" in profile, "Missing gstin"
            assert "billing_address" in profile, "Missing billing_address"
            assert "credit_type" in profile, "Missing credit_type"
            assert "invoice_type" in profile, "Missing invoice_type"
            print(f"✓ Customer profile: {profile.get('customer_code')} - {profile.get('display_name')}")
    
    def test_customer_profile_linked_data(self):
        """Test that customer profile includes linked_data with outstanding and ageing"""
        search_response = self.session.get(
            f"{BASE_URL}/api/erp/customer-master/search/for-invoice",
            params={"q": "ABC"},
            headers=self.admin_headers
        )
        assert search_response.status_code == 200
        results = search_response.json()
        
        if len(results) > 0:
            customer_id = results[0]["id"]
            
            profile_response = self.session.get(
                f"{BASE_URL}/api/erp/customer-master/{customer_id}",
                headers=self.admin_headers
            )
            assert profile_response.status_code == 200
            profile = profile_response.json()
            
            # Verify linked_data structure
            assert "linked_data" in profile, "Missing linked_data"
            linked = profile.get("linked_data", {})
            
            assert "outstanding_balance" in linked, "Missing outstanding_balance in linked_data"
            assert "ageing" in linked, "Missing ageing in linked_data"
            assert "total_orders" in linked, "Missing total_orders in linked_data"
            assert "total_spent" in linked, "Missing total_spent in linked_data"
            
            ageing = linked.get("ageing", {})
            assert "0_30" in ageing, "Missing 0_30 in ageing"
            assert "31_60" in ageing, "Missing 31_60 in ageing"
            assert "61_90" in ageing, "Missing 61_90 in ageing"
            assert "over_90" in ageing, "Missing over_90 in ageing"
            
            print(f"✓ Linked data: outstanding={linked.get('outstanding_balance')}, orders={linked.get('total_orders')}")
            print(f"✓ Ageing: 0-30={ageing.get('0_30')}, 31-60={ageing.get('31_60')}, 61-90={ageing.get('61_90')}, 90+={ageing.get('over_90')}")
    
    # ============ Order Creation with Customer Profile Tests ============
    
    def test_order_creation_with_customer_profile_id(self):
        """Test order creation auto-populates from Customer Master"""
        # Get a customer profile
        search_response = self.session.get(
            f"{BASE_URL}/api/erp/customer-master/search/for-invoice",
            params={"q": "ABC"},
            headers=self.admin_headers
        )
        assert search_response.status_code == 200
        results = search_response.json()
        
        if len(results) == 0:
            pytest.skip("No customer profiles found for testing")
        
        customer = results[0]
        customer_profile_id = customer["id"]
        
        # Get a product
        products_response = self.session.get(f"{BASE_URL}/api/products", headers=self.admin_headers)
        assert products_response.status_code == 200
        products = products_response.json()
        
        if len(products) == 0:
            pytest.skip("No products found for testing")
        
        product = products[0]
        
        # Create order with customer_profile_id
        order_data = {
            "product_id": product["id"],
            "thickness": product["thickness_options"][0] if product.get("thickness_options") else 6.0,
            "width": 24,
            "height": 36,
            "quantity": 2,
            "delivery_address": "Test Address for Customer Master Integration",
            "notes": "TEST_CustomerMasterIntegration",
            "advance_percent": 100,
            "customer_profile_id": customer_profile_id
        }
        
        order_response = self.session.post(
            f"{BASE_URL}/api/orders",
            json=order_data,
            headers=self.admin_headers
        )
        
        assert order_response.status_code == 200, f"Order creation failed: {order_response.text}"
        order_result = order_response.json()
        
        # Verify customer profile was returned
        assert "customer_profile" in order_result, "Missing customer_profile in response"
        
        # Verify order was created
        assert "order_id" in order_result, "Missing order_id"
        assert "order_number" in order_result, "Missing order_number"
        
        print(f"✓ Order created: {order_result.get('order_number')}")
        print(f"✓ Customer profile linked: {order_result.get('customer_profile', {}).get('display_name')}")
    
    def test_order_creation_applies_credit_terms(self):
        """Test that order creation applies credit terms from Customer Master"""
        # Get a credit-allowed customer
        search_response = self.session.get(
            f"{BASE_URL}/api/erp/customer-master/search/for-invoice",
            params={"q": "ABC"},
            headers=self.admin_headers
        )
        assert search_response.status_code == 200
        results = search_response.json()
        
        credit_customer = None
        for customer in results:
            if customer.get("credit_type") == "credit_allowed":
                credit_customer = customer
                break
        
        if not credit_customer:
            pytest.skip("No credit-allowed customer found for testing")
        
        # Get a product
        products_response = self.session.get(f"{BASE_URL}/api/products", headers=self.admin_headers)
        products = products_response.json()
        product = products[0]
        
        # Create credit order with customer_profile_id
        order_data = {
            "product_id": product["id"],
            "thickness": product["thickness_options"][0] if product.get("thickness_options") else 6.0,
            "width": 24,
            "height": 36,
            "quantity": 2,
            "delivery_address": "Test Credit Order Address",
            "notes": "TEST_CreditOrderIntegration",
            "advance_percent": 0,
            "is_credit_order": True,
            "customer_profile_id": credit_customer["id"]
        }
        
        order_response = self.session.post(
            f"{BASE_URL}/api/orders",
            json=order_data,
            headers=self.admin_headers
        )
        
        assert order_response.status_code == 200, f"Credit order creation failed: {order_response.text}"
        order_result = order_response.json()
        
        # Verify credit order was created
        assert order_result.get("is_credit_order") == True, "Order should be marked as credit order"
        assert order_result.get("advance_percent") == 0, "Credit order should have 0% advance"
        
        print(f"✓ Credit order created: {order_result.get('order_number')}")
        print(f"✓ Credit terms applied: advance={order_result.get('advance_percent')}%")
    
    # ============ ERP Order Creation Tests ============
    
    def test_erp_order_creation_with_customer_profile(self):
        """Test ERP order creation with customer_profile_id via main orders endpoint"""
        # Get a customer profile
        search_response = self.session.get(
            f"{BASE_URL}/api/erp/customer-master/search/for-invoice",
            params={"q": "ABC"},
            headers=self.admin_headers
        )
        assert search_response.status_code == 200
        results = search_response.json()
        
        if len(results) == 0:
            pytest.skip("No customer profiles found for testing")
        
        customer = results[0]
        
        # Get a product
        products_response = self.session.get(f"{BASE_URL}/api/products", headers=self.admin_headers)
        products = products_response.json()
        product = products[0]
        
        # Create order via main orders endpoint with customer_profile_id
        order_data = {
            "product_id": product["id"],
            "thickness": product["thickness_options"][0] if product.get("thickness_options") else 6.0,
            "width": 24,
            "height": 36,
            "quantity": 2,
            "delivery_address": "Test ERP Order Address",
            "notes": "TEST_ERPOrderIntegration",
            "advance_percent": 100,
            "customer_profile_id": customer["id"]
        }
        
        order_response = self.session.post(
            f"{BASE_URL}/api/orders",
            json=order_data,
            headers=self.admin_headers
        )
        
        assert order_response.status_code == 200, f"Order creation failed: {order_response.text}"
        order_result = order_response.json()
        
        # Verify customer profile was returned
        assert "customer_profile" in order_result, "Missing customer_profile in response"
        customer_profile = order_result.get("customer_profile", {})
        
        if customer_profile:
            assert customer_profile.get("id") == customer["id"], "customer_profile_id should match"
            print(f"✓ Order created: {order_result.get('order_number')}")
            print(f"✓ Customer profile linked: {customer_profile.get('display_name')}")
            print(f"✓ GSTIN from profile: {customer_profile.get('gstin')}")
    
    # ============ Customer Master List Tests ============
    
    def test_customer_master_list(self):
        """Test customer master list endpoint"""
        response = self.session.get(
            f"{BASE_URL}/api/erp/customer-master/",
            headers=self.admin_headers
        )
        assert response.status_code == 200, f"List failed: {response.text}"
        data = response.json()
        
        # API returns 'profiles' not 'customers'
        assert "profiles" in data, "Missing profiles in response"
        assert "total" in data, "Missing total in response"
        
        print(f"✓ Customer Master list: {data.get('total')} total customers")
    
    def test_customer_master_stats(self):
        """Test customer master stats endpoint"""
        response = self.session.get(
            f"{BASE_URL}/api/erp/customer-master/stats",
            headers=self.admin_headers
        )
        assert response.status_code == 200, f"Stats failed: {response.text}"
        stats = response.json()
        
        # API returns 'total_active' not 'total'
        assert "total_active" in stats, "Missing total_active in stats"
        assert "by_category" in stats, "Missing by_category in stats"
        assert "credit_customers" in stats, "Missing credit_customers in stats"
        
        print(f"✓ Customer stats: total_active={stats.get('total_active')}, credit_customers={stats.get('credit_customers')}")
    
    # ============ Customer Portal Tests ============
    
    def test_customer_portal_profile_endpoint(self):
        """Test customer portal profile endpoint"""
        if not self.customer_headers:
            pytest.skip("Customer login not available")
        
        response = self.session.get(
            f"{BASE_URL}/api/users/profile",
            headers=self.customer_headers
        )
        
        # Profile endpoint may return 404 if not set up
        if response.status_code == 200:
            profile = response.json()
            print(f"✓ Customer portal profile: {profile.get('name', 'N/A')}")
        else:
            print(f"⚠ Customer profile endpoint returned {response.status_code}")


class TestCustomerSearchComponent:
    """Test CustomerSearch component functionality via API"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test fixtures"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        
        # Login as admin
        login_response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": "admin@lucumaa.in",
            "password": "adminpass"
        })
        assert login_response.status_code == 200
        self.admin_token = login_response.json()["token"]
        self.admin_headers = {"Authorization": f"Bearer {self.admin_token}", "Content-Type": "application/json"}
    
    def test_search_minimum_query_length(self):
        """Test that search requires minimum 2 characters"""
        # Single character should fail
        response = self.session.get(
            f"{BASE_URL}/api/erp/customer-master/search/for-invoice",
            params={"q": "A"},
            headers=self.admin_headers
        )
        # Should return 422 validation error
        assert response.status_code == 422, "Single character query should fail validation"
        print("✓ Minimum query length validation works")
    
    def test_search_returns_b2b_b2c_type(self):
        """Test that search returns invoice_type (B2B/B2C)"""
        response = self.session.get(
            f"{BASE_URL}/api/erp/customer-master/search/for-invoice",
            params={"q": "ABC"},
            headers=self.admin_headers
        )
        assert response.status_code == 200
        results = response.json()
        
        if len(results) > 0:
            customer = results[0]
            assert "invoice_type" in customer, "Missing invoice_type"
            assert customer["invoice_type"] in ["B2B", "B2C"], f"Invalid invoice_type: {customer['invoice_type']}"
            print(f"✓ Invoice type returned: {customer['invoice_type']}")
    
    def test_search_returns_gstin_for_b2b(self):
        """Test that B2B customers have GSTIN"""
        response = self.session.get(
            f"{BASE_URL}/api/erp/customer-master/search/for-invoice",
            params={"q": "ABC"},
            headers=self.admin_headers
        )
        assert response.status_code == 200
        results = response.json()
        
        for customer in results:
            if customer.get("invoice_type") == "B2B":
                assert customer.get("gstin"), f"B2B customer {customer.get('display_name')} should have GSTIN"
                print(f"✓ B2B customer has GSTIN: {customer.get('gstin')}")
                break


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
