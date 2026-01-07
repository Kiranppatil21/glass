"""
Test Customer Profile Authorization and GST Checkbox Logic
Tests:
1. Customer can save their own profile (no authorization error)
2. needs_gst_invoice field saved correctly
3. invoice_type set to B2B when needs_gst_invoice=true or company type
"""
import pytest
import requests
import os

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', '').rstrip('/')

# Test credentials
TEST_CUSTOMER_EMAIL = "test@lucumaa.com"
TEST_CUSTOMER_PASSWORD = "test123"
ADMIN_EMAIL = "admin@lucumaa.in"
ADMIN_PASSWORD = "adminpass"


class TestCustomerProfileAuthorization:
    """Test that customers can edit their own profiles"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test session"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
    
    def get_customer_token(self):
        """Login as test customer and get token"""
        response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": TEST_CUSTOMER_EMAIL,
            "password": TEST_CUSTOMER_PASSWORD
        })
        if response.status_code == 200:
            return response.json().get("token")
        return None
    
    def get_admin_token(self):
        """Login as admin and get token"""
        response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        if response.status_code == 200:
            return response.json().get("token")
        return None
    
    def test_customer_login(self):
        """Test customer can login"""
        response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": TEST_CUSTOMER_EMAIL,
            "password": TEST_CUSTOMER_PASSWORD
        })
        assert response.status_code == 200, f"Customer login failed: {response.text}"
        data = response.json()
        assert "token" in data, "No token in response"
        print(f"✓ Customer login successful, role: {data.get('user', {}).get('role')}")
    
    def test_customer_can_get_own_profile(self):
        """Test customer can get their own profile"""
        token = self.get_customer_token()
        assert token, "Failed to get customer token"
        
        # Get user profile first
        response = self.session.get(
            f"{BASE_URL}/api/users/profile",
            headers={"Authorization": f"Bearer {token}"}
        )
        assert response.status_code == 200, f"Failed to get user profile: {response.text}"
        profile = response.json()
        print(f"✓ Got user profile: {profile.get('name')}, phone: {profile.get('phone')}")
        
        # Search for customer master profile by phone
        if profile.get('phone'):
            response = self.session.get(
                f"{BASE_URL}/api/erp/customer-master/search/for-invoice?q={profile['phone']}",
                headers={"Authorization": f"Bearer {token}"}
            )
            assert response.status_code == 200, f"Failed to search customer: {response.text}"
            customers = response.json()
            if customers:
                print(f"✓ Found customer master profile: {customers[0].get('display_name')}")
                return customers[0]
        return None
    
    def test_customer_can_update_own_profile_no_auth_error(self):
        """Test customer can update their own profile without authorization error"""
        token = self.get_customer_token()
        assert token, "Failed to get customer token"
        
        # Get user profile to find phone
        response = self.session.get(
            f"{BASE_URL}/api/users/profile",
            headers={"Authorization": f"Bearer {token}"}
        )
        assert response.status_code == 200
        user_profile = response.json()
        
        # Search for customer master profile
        response = self.session.get(
            f"{BASE_URL}/api/erp/customer-master/search/for-invoice?q={user_profile.get('phone', '')}",
            headers={"Authorization": f"Bearer {token}"}
        )
        
        if response.status_code == 200 and response.json():
            customer = response.json()[0]
            customer_id = customer.get('id')
            
            # Try to update the profile - this should NOT return 403
            update_data = {
                "contact_person": "Test Update Contact"
            }
            
            response = self.session.put(
                f"{BASE_URL}/api/erp/customer-master/{customer_id}",
                json=update_data,
                headers={"Authorization": f"Bearer {token}"}
            )
            
            # Key assertion: Should NOT be 403 "Not authorized to update customers"
            assert response.status_code != 403, f"Authorization error! Customer should be able to update own profile. Response: {response.text}"
            assert response.status_code == 200, f"Update failed with status {response.status_code}: {response.text}"
            
            print(f"✓ Customer successfully updated own profile (no 403 error)")
            return True
        else:
            pytest.skip("No customer master profile found for test customer")


class TestGSTCheckboxLogic:
    """Test GST checkbox and invoice_type logic"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test session"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
    
    def get_admin_token(self):
        """Login as admin and get token"""
        response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        if response.status_code == 200:
            return response.json().get("token")
        return None
    
    def test_admin_login(self):
        """Test admin can login"""
        response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        assert response.status_code == 200, f"Admin login failed: {response.text}"
        print(f"✓ Admin login successful")
    
    def test_needs_gst_invoice_sets_b2b(self):
        """Test that needs_gst_invoice=true sets invoice_type to B2B"""
        token = self.get_admin_token()
        assert token, "Failed to get admin token"
        
        # Create a test customer with needs_gst_invoice=true
        import random
        test_mobile = f"98{random.randint(10000000, 99999999)}"
        # Generate unique GSTIN
        random_pan = f"AABCY{random.randint(1000, 9999)}R"
        test_gstin = f"27{random_pan}1ZM"
        
        customer_data = {
            "customer_type": "individual",
            "individual_name": "TEST GST Checkbox User",
            "mobile": test_mobile,
            "needs_gst_invoice": True,
            "gstin": test_gstin,
            "pan": random_pan,
            "company_name": "TEST GST Company",
            "gst_type": "regular",
            "billing_address": {
                "address_line1": "Test Address",
                "city": "Mumbai",
                "state": "Maharashtra",
                "pin_code": "400001",
                "country": "India"
            }
        }
        
        response = self.session.post(
            f"{BASE_URL}/api/erp/customer-master/",
            json=customer_data,
            headers={"Authorization": f"Bearer {token}"}
        )
        
        assert response.status_code == 200, f"Failed to create customer: {response.text}"
        created = response.json()
        
        # Verify invoice_type is B2B
        assert created.get("invoice_type") == "B2B", f"Expected B2B, got {created.get('invoice_type')}"
        assert created.get("needs_gst_invoice") == True, "needs_gst_invoice should be True"
        
        print(f"✓ needs_gst_invoice=true correctly sets invoice_type=B2B")
        
        # Cleanup - deactivate the test customer
        if created.get("id"):
            self.session.patch(
                f"{BASE_URL}/api/erp/customer-master/{created['id']}/deactivate",
                headers={"Authorization": f"Bearer {token}"}
            )
    
    def test_company_type_always_b2b(self):
        """Test that company types (pvt_ltd, ltd) always set invoice_type to B2B"""
        token = self.get_admin_token()
        assert token, "Failed to get admin token"
        
        import random
        test_mobile = f"97{random.randint(10000000, 99999999)}"
        # Generate unique GSTIN
        random_pan = f"AABCT{random.randint(1000, 9999)}R"
        test_gstin = f"27{random_pan}1ZM"
        
        customer_data = {
            "customer_type": "pvt_ltd",
            "company_name": "TEST Pvt Ltd Company",
            "mobile": test_mobile,
            "gstin": test_gstin,
            "pan": random_pan,
            "gst_type": "regular",
            "billing_address": {
                "address_line1": "Test Address",
                "city": "Mumbai",
                "state": "Maharashtra",
                "pin_code": "400001",
                "country": "India"
            }
        }
        
        response = self.session.post(
            f"{BASE_URL}/api/erp/customer-master/",
            json=customer_data,
            headers={"Authorization": f"Bearer {token}"}
        )
        
        assert response.status_code == 200, f"Failed to create customer: {response.text}"
        created = response.json()
        
        # Verify invoice_type is B2B for company type
        assert created.get("invoice_type") == "B2B", f"Expected B2B for pvt_ltd, got {created.get('invoice_type')}"
        assert created.get("needs_gst_invoice") == True, "needs_gst_invoice should be True for company type"
        
        print(f"✓ Company type (pvt_ltd) correctly sets invoice_type=B2B")
        
        # Cleanup
        if created.get("id"):
            self.session.patch(
                f"{BASE_URL}/api/erp/customer-master/{created['id']}/deactivate",
                headers={"Authorization": f"Bearer {token}"}
            )
    
    def test_individual_without_gst_is_b2c(self):
        """Test that individual without GST is B2C"""
        token = self.get_admin_token()
        assert token, "Failed to get admin token"
        
        import random
        test_mobile = f"96{random.randint(10000000, 99999999)}"
        
        customer_data = {
            "customer_type": "individual",
            "individual_name": "TEST B2C Customer",
            "mobile": test_mobile,
            "needs_gst_invoice": False,
            "billing_address": {
                "address_line1": "Test Address",
                "city": "Mumbai",
                "state": "Maharashtra",
                "pin_code": "400001",
                "country": "India"
            }
        }
        
        response = self.session.post(
            f"{BASE_URL}/api/erp/customer-master/",
            json=customer_data,
            headers={"Authorization": f"Bearer {token}"}
        )
        
        assert response.status_code == 200, f"Failed to create customer: {response.text}"
        created = response.json()
        
        # Verify invoice_type is B2C
        assert created.get("invoice_type") == "B2C", f"Expected B2C, got {created.get('invoice_type')}"
        assert created.get("needs_gst_invoice") == False, "needs_gst_invoice should be False"
        
        print(f"✓ Individual without GST correctly sets invoice_type=B2C")
        
        # Cleanup
        if created.get("id"):
            self.session.patch(
                f"{BASE_URL}/api/erp/customer-master/{created['id']}/deactivate",
                headers={"Authorization": f"Bearer {token}"}
            )
    
    def test_update_needs_gst_invoice_changes_invoice_type(self):
        """Test that updating needs_gst_invoice changes invoice_type"""
        token = self.get_admin_token()
        assert token, "Failed to get admin token"
        
        import random
        test_mobile = f"95{random.randint(10000000, 99999999)}"
        # Generate unique GSTIN for update
        random_pan = f"AABCX{random.randint(1000, 9999)}R"
        test_gstin = f"27{random_pan}1ZM"
        
        # Create B2C customer first
        customer_data = {
            "customer_type": "individual",
            "individual_name": "TEST Toggle GST Customer",
            "mobile": test_mobile,
            "needs_gst_invoice": False,
            "billing_address": {
                "address_line1": "Test Address",
                "city": "Mumbai",
                "state": "Maharashtra",
                "pin_code": "400001",
                "country": "India"
            }
        }
        
        response = self.session.post(
            f"{BASE_URL}/api/erp/customer-master/",
            json=customer_data,
            headers={"Authorization": f"Bearer {token}"}
        )
        
        assert response.status_code == 200, f"Failed to create customer: {response.text}"
        created = response.json()
        customer_id = created.get("id")
        
        assert created.get("invoice_type") == "B2C", "Initial invoice_type should be B2C"
        print(f"✓ Created B2C customer")
        
        # Update to needs_gst_invoice=true
        update_data = {
            "needs_gst_invoice": True,
            "gstin": test_gstin,
            "pan": random_pan,
            "company_name": "TEST Updated Company"
        }
        
        response = self.session.put(
            f"{BASE_URL}/api/erp/customer-master/{customer_id}",
            json=update_data,
            headers={"Authorization": f"Bearer {token}"}
        )
        
        assert response.status_code == 200, f"Failed to update customer: {response.text}"
        updated = response.json()
        
        # Verify invoice_type changed to B2B
        assert updated.get("invoice_type") == "B2B", f"Expected B2B after update, got {updated.get('invoice_type')}"
        assert updated.get("needs_gst_invoice") == True, "needs_gst_invoice should be True after update"
        
        print(f"✓ Updating needs_gst_invoice=true correctly changes invoice_type to B2B")
        
        # Cleanup
        if customer_id:
            self.session.patch(
                f"{BASE_URL}/api/erp/customer-master/{customer_id}/deactivate",
                headers={"Authorization": f"Bearer {token}"}
            )


class TestCustomerMasterStates:
    """Test states endpoint"""
    
    def test_get_states(self):
        """Test getting Indian states list"""
        session = requests.Session()
        
        # Login first
        response = session.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        assert response.status_code == 200
        token = response.json().get("token")
        
        # Get states
        response = session.get(
            f"{BASE_URL}/api/erp/customer-master/states",
            headers={"Authorization": f"Bearer {token}"}
        )
        
        assert response.status_code == 200, f"Failed to get states: {response.text}"
        states = response.json()
        assert len(states) > 0, "No states returned"
        assert any(s.get("name") == "Maharashtra" for s in states), "Maharashtra not in states list"
        
        print(f"✓ Got {len(states)} Indian states")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
