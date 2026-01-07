"""
Test suite for new pages (Industries, How It Works, Pricing, Contact) and AI Chat
Tests the features added for Glass Factory ERP website
"""
import pytest
import requests
import os

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', 'https://glassmesh.preview.emergentagent.com')

class TestAIChatAPI:
    """AI Chat endpoint tests"""
    
    def test_chat_message_success(self):
        """Test sending a message to AI chat"""
        response = requests.post(
            f"{BASE_URL}/api/chat/message",
            json={"message": "What glass types do you offer?"}
        )
        assert response.status_code == 200
        data = response.json()
        assert "response" in data
        assert "session_id" in data
        assert len(data["response"]) > 0
        print(f"✅ AI Chat response received: {data['response'][:100]}...")
    
    def test_chat_with_session_id(self):
        """Test chat with existing session ID"""
        # First message
        response1 = requests.post(
            f"{BASE_URL}/api/chat/message",
            json={"message": "Hello"}
        )
        assert response1.status_code == 200
        session_id = response1.json()["session_id"]
        
        # Second message with same session
        response2 = requests.post(
            f"{BASE_URL}/api/chat/message",
            json={"message": "What about pricing?", "session_id": session_id}
        )
        assert response2.status_code == 200
        assert response2.json()["session_id"] == session_id
        print(f"✅ Chat session maintained: {session_id}")
    
    def test_chat_clear_session(self):
        """Test clearing a chat session"""
        # Create a session
        response = requests.post(
            f"{BASE_URL}/api/chat/message",
            json={"message": "Test"}
        )
        session_id = response.json()["session_id"]
        
        # Clear the session
        delete_response = requests.delete(f"{BASE_URL}/api/chat/session/{session_id}")
        assert delete_response.status_code == 200
        print(f"✅ Chat session cleared: {session_id}")


class TestInquiryAPI:
    """Inquiry/Contact form endpoint tests"""
    
    def test_contact_form_submission(self):
        """Test contact form submission via /api/inquiry"""
        response = requests.post(
            f"{BASE_URL}/api/inquiry",
            json={
                "name": "TEST_Contact User",
                "email": "test@example.com",
                "phone": "9876543210",
                "message": "Test inquiry message",
                "subject": "general",
                "type": "contact_form"
            }
        )
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "Inquiry submitted successfully"
        print("✅ Contact form submission successful")
    
    def test_wholesale_quote_submission(self):
        """Test wholesale quote submission via /api/inquiry"""
        response = requests.post(
            f"{BASE_URL}/api/inquiry",
            json={
                "name": "TEST_Wholesale User",
                "email": "wholesale@example.com",
                "phone": "9876543210",
                "company": "Test Company",
                "requirements": "Need 500 sqft toughened glass",
                "type": "wholesale_quote",
                "calculator_data": {
                    "glassType": "toughened",
                    "thickness": 6,
                    "sqft": 500
                },
                "estimated_total": 76000
            }
        )
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "Inquiry submitted successfully"
        print("✅ Wholesale quote submission successful")
    
    def test_inquiry_missing_required_fields(self):
        """Test inquiry with missing required fields"""
        response = requests.post(
            f"{BASE_URL}/api/inquiry",
            json={
                "name": "Test"
                # Missing email and phone
            }
        )
        assert response.status_code == 422  # Validation error
        print("✅ Validation error for missing fields")
    
    def test_inquiry_invalid_email(self):
        """Test inquiry with invalid email"""
        response = requests.post(
            f"{BASE_URL}/api/inquiry",
            json={
                "name": "Test",
                "email": "invalid-email",
                "phone": "1234567890"
            }
        )
        assert response.status_code == 422  # Validation error
        print("✅ Validation error for invalid email")


class TestContactAPI:
    """Original contact endpoint tests"""
    
    def test_contact_endpoint(self):
        """Test original /api/contact endpoint"""
        response = requests.post(
            f"{BASE_URL}/api/contact",
            json={
                "name": "TEST_Original Contact",
                "email": "original@example.com",
                "phone": "9876543210",
                "message": "Test message",
                "inquiry_type": "general"
            }
        )
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "Inquiry submitted successfully"
        print("✅ Original contact endpoint working")


class TestPageEndpoints:
    """Test that page routes are accessible"""
    
    def test_homepage_loads(self):
        """Test homepage loads"""
        response = requests.get(BASE_URL)
        assert response.status_code == 200
        print("✅ Homepage loads")
    
    def test_industries_page_loads(self):
        """Test industries page loads"""
        response = requests.get(f"{BASE_URL}/industries")
        assert response.status_code == 200
        print("✅ Industries page loads")
    
    def test_how_it_works_page_loads(self):
        """Test how it works page loads"""
        response = requests.get(f"{BASE_URL}/how-it-works")
        assert response.status_code == 200
        print("✅ How It Works page loads")
    
    def test_pricing_page_loads(self):
        """Test pricing page loads"""
        response = requests.get(f"{BASE_URL}/pricing")
        assert response.status_code == 200
        print("✅ Pricing page loads")
    
    def test_contact_page_loads(self):
        """Test contact page loads"""
        response = requests.get(f"{BASE_URL}/contact")
        assert response.status_code == 200
        print("✅ Contact page loads")


class TestAPIHealth:
    """Basic API health checks"""
    
    def test_api_health(self):
        """Test API is responding"""
        response = requests.get(f"{BASE_URL}/api/products")
        assert response.status_code == 200
        print("✅ API is healthy")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
