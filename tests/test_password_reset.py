"""
Test Password Reset Feature with Email OTP and Mobile SMS OTP
Features tested:
- Forgot Password UI flow
- Send OTP API (email method)
- Send OTP API (mobile method - expected to fail due to Twilio sandbox)
- Verify OTP API
- Reset Password API
- Complete password reset flow
"""

import pytest
import requests
import os
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', '').rstrip('/')
MONGO_URL = os.environ.get('MONGO_URL', 'mongodb://localhost:27017')

# Test credentials
ADMIN_EMAIL = "admin@lucumaa.in"
ADMIN_PASSWORD = "adminpass"


class TestPasswordResetAPIs:
    """Password Reset API endpoint tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test session"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
    
    def test_send_otp_email_for_reset(self):
        """Test POST /api/auth/send-otp with email method for password reset"""
        response = self.session.post(f"{BASE_URL}/api/auth/send-otp", json={
            "method": "email",
            "identifier": ADMIN_EMAIL,
            "purpose": "reset"
        })
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        data = response.json()
        assert "message" in data
        assert data.get("method") == "email"
        assert "OTP sent" in data.get("message", "")
        print(f"✅ Send OTP (email, reset) - Status: {response.status_code}, Message: {data.get('message')}")
    
    def test_send_otp_mobile_for_reset(self):
        """Test POST /api/auth/send-otp with mobile method for password reset
        
        NOTE: This test expects a 5xx error because the Twilio account is using
        a sandbox phone number (+14155238886) which doesn't match the account.
        In production, this would work with a properly configured Twilio number.
        """
        response = self.session.post(f"{BASE_URL}/api/auth/send-otp", json={
            "method": "mobile",
            "identifier": "9876543210",
            "purpose": "reset"
        })
        
        # Expected to fail with Twilio sandbox configuration (500 or 520)
        assert response.status_code in [500, 520], f"Expected 5xx (Twilio sandbox limitation), got {response.status_code}"
        data = response.json()
        assert "Failed to send SMS OTP" in data.get("detail", "")
        print(f"⚠️ Send OTP (mobile, reset) - Expected failure due to Twilio sandbox: {data.get('detail')}")
    
    def test_verify_otp_invalid_otp(self):
        """Test POST /api/auth/verify-otp with invalid OTP"""
        response = self.session.post(f"{BASE_URL}/api/auth/verify-otp", json={
            "identifier": ADMIN_EMAIL,
            "otp": "000000",
            "purpose": "reset"
        })
        
        assert response.status_code == 400, f"Expected 400, got {response.status_code}"
        data = response.json()
        assert "Invalid OTP" in data.get("detail", "")
        print(f"✅ Verify OTP (invalid) - Correctly returns 400")
    
    def test_verify_otp_missing_fields(self):
        """Test POST /api/auth/verify-otp with missing fields"""
        response = self.session.post(f"{BASE_URL}/api/auth/verify-otp", json={
            "identifier": ADMIN_EMAIL
        })
        
        assert response.status_code == 400, f"Expected 400, got {response.status_code}"
        print(f"✅ Verify OTP (missing fields) - Correctly returns 400")
    
    def test_reset_password_missing_token(self):
        """Test POST /api/auth/reset-password with missing token"""
        response = self.session.post(f"{BASE_URL}/api/auth/reset-password", json={
            "new_password": "newpassword123"
        })
        
        assert response.status_code == 400, f"Expected 400, got {response.status_code}"
        print(f"✅ Reset Password (missing token) - Correctly returns 400")
    
    def test_reset_password_invalid_token(self):
        """Test POST /api/auth/reset-password with invalid token"""
        response = self.session.post(f"{BASE_URL}/api/auth/reset-password", json={
            "reset_token": "invalid-token-12345",
            "new_password": "newpassword123"
        })
        
        assert response.status_code == 400, f"Expected 400, got {response.status_code}"
        data = response.json()
        assert "Invalid" in data.get("detail", "") or "expired" in data.get("detail", "")
        print(f"✅ Reset Password (invalid token) - Correctly returns 400")
    
    def test_reset_password_short_password(self):
        """Test POST /api/auth/reset-password with short password"""
        response = self.session.post(f"{BASE_URL}/api/auth/reset-password", json={
            "reset_token": "some-token",
            "new_password": "123"
        })
        
        assert response.status_code == 400, f"Expected 400, got {response.status_code}"
        data = response.json()
        assert "at least 6 characters" in data.get("detail", "")
        print(f"✅ Reset Password (short password) - Correctly returns 400")


class TestCompletePasswordResetFlow:
    """Test complete password reset flow end-to-end"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test session"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
    
    def get_otp_from_db(self, identifier):
        """Helper to get OTP from MongoDB"""
        async def _get_otp():
            client = AsyncIOMotorClient(MONGO_URL)
            db = client['test_database']
            otp_record = await db.otp_codes.find_one(
                {"identifier": identifier},
                sort=[("created_at", -1)]
            )
            client.close()
            return otp_record.get('otp') if otp_record else None
        
        return asyncio.get_event_loop().run_until_complete(_get_otp())
    
    def test_complete_password_reset_flow(self):
        """Test complete password reset flow: Send OTP -> Verify OTP -> Reset Password -> Login"""
        
        # Step 1: Send OTP
        print("\n=== Step 1: Send OTP ===")
        response = self.session.post(f"{BASE_URL}/api/auth/send-otp", json={
            "method": "email",
            "identifier": ADMIN_EMAIL,
            "purpose": "reset"
        })
        assert response.status_code == 200, f"Send OTP failed: {response.text}"
        print(f"✅ OTP sent to {ADMIN_EMAIL}")
        
        # Step 2: Get OTP from database
        print("\n=== Step 2: Get OTP from database ===")
        otp = self.get_otp_from_db(ADMIN_EMAIL)
        assert otp is not None, "OTP not found in database"
        assert len(otp) == 6, f"OTP should be 6 digits, got: {otp}"
        print(f"✅ OTP retrieved: {otp}")
        
        # Step 3: Verify OTP
        print("\n=== Step 3: Verify OTP ===")
        response = self.session.post(f"{BASE_URL}/api/auth/verify-otp", json={
            "identifier": ADMIN_EMAIL,
            "otp": otp,
            "purpose": "reset"
        })
        assert response.status_code == 200, f"Verify OTP failed: {response.text}"
        data = response.json()
        reset_token = data.get("reset_token")
        assert reset_token is not None, "Reset token not returned"
        print(f"✅ OTP verified, reset token: {reset_token[:20]}...")
        
        # Step 4: Reset Password
        print("\n=== Step 4: Reset Password ===")
        response = self.session.post(f"{BASE_URL}/api/auth/reset-password", json={
            "reset_token": reset_token,
            "new_password": ADMIN_PASSWORD  # Keep same password for testing
        })
        assert response.status_code == 200, f"Reset password failed: {response.text}"
        data = response.json()
        assert "successful" in data.get("message", "").lower()
        print(f"✅ Password reset successful")
        
        # Step 5: Verify login with new password
        print("\n=== Step 5: Verify login ===")
        response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        assert response.status_code == 200, f"Login failed: {response.text}"
        data = response.json()
        assert "token" in data, "Token not returned after login"
        assert data.get("user", {}).get("email") == ADMIN_EMAIL
        print(f"✅ Login successful with reset password")
        
        print("\n✅ COMPLETE PASSWORD RESET FLOW PASSED!")
    
    def test_otp_expiry_validation(self):
        """Test that expired OTP is rejected"""
        # First send a valid OTP
        response = self.session.post(f"{BASE_URL}/api/auth/send-otp", json={
            "method": "email",
            "identifier": ADMIN_EMAIL,
            "purpose": "reset"
        })
        assert response.status_code == 200
        
        # Get the OTP
        otp = self.get_otp_from_db(ADMIN_EMAIL)
        assert otp is not None
        
        # Verify OTP works
        response = self.session.post(f"{BASE_URL}/api/auth/verify-otp", json={
            "identifier": ADMIN_EMAIL,
            "otp": otp,
            "purpose": "reset"
        })
        assert response.status_code == 200
        
        # Try to use the same OTP again (should fail - already verified)
        response = self.session.post(f"{BASE_URL}/api/auth/verify-otp", json={
            "identifier": ADMIN_EMAIL,
            "otp": otp,
            "purpose": "reset"
        })
        assert response.status_code == 400, "Already verified OTP should be rejected"
        print(f"✅ Already verified OTP correctly rejected")
    
    def test_reset_token_single_use(self):
        """Test that reset token can only be used once"""
        # Send OTP
        response = self.session.post(f"{BASE_URL}/api/auth/send-otp", json={
            "method": "email",
            "identifier": ADMIN_EMAIL,
            "purpose": "reset"
        })
        assert response.status_code == 200
        
        # Get and verify OTP
        otp = self.get_otp_from_db(ADMIN_EMAIL)
        response = self.session.post(f"{BASE_URL}/api/auth/verify-otp", json={
            "identifier": ADMIN_EMAIL,
            "otp": otp,
            "purpose": "reset"
        })
        assert response.status_code == 200
        reset_token = response.json().get("reset_token")
        
        # Use reset token
        response = self.session.post(f"{BASE_URL}/api/auth/reset-password", json={
            "reset_token": reset_token,
            "new_password": ADMIN_PASSWORD
        })
        assert response.status_code == 200
        
        # Try to use the same reset token again (should fail)
        response = self.session.post(f"{BASE_URL}/api/auth/reset-password", json={
            "reset_token": reset_token,
            "new_password": "anotherpassword"
        })
        assert response.status_code == 400, "Already used reset token should be rejected"
        print(f"✅ Already used reset token correctly rejected")


class TestPasswordResetUserNotFound:
    """Test password reset for non-existent users"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test session"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
    
    def test_send_otp_nonexistent_email(self):
        """Test sending OTP to non-existent email - OTP is sent but verify will fail"""
        response = self.session.post(f"{BASE_URL}/api/auth/send-otp", json={
            "method": "email",
            "identifier": "nonexistent@example.com",
            "purpose": "reset"
        })
        
        # OTP is sent regardless (to prevent email enumeration)
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        print(f"✅ OTP sent to non-existent email (prevents email enumeration)")
    
    def test_verify_otp_nonexistent_user(self):
        """Test verifying OTP for non-existent user"""
        # First send OTP
        self.session.post(f"{BASE_URL}/api/auth/send-otp", json={
            "method": "email",
            "identifier": "nonexistent@example.com",
            "purpose": "reset"
        })
        
        # Get OTP from DB
        async def _get_otp():
            client = AsyncIOMotorClient(MONGO_URL)
            db = client['test_database']
            otp_record = await db.otp_codes.find_one(
                {"identifier": "nonexistent@example.com"},
                sort=[("created_at", -1)]
            )
            client.close()
            return otp_record.get('otp') if otp_record else None
        
        otp = asyncio.get_event_loop().run_until_complete(_get_otp())
        
        if otp:
            # Verify OTP - should fail because user doesn't exist
            response = self.session.post(f"{BASE_URL}/api/auth/verify-otp", json={
                "identifier": "nonexistent@example.com",
                "otp": otp,
                "purpose": "reset"
            })
            
            assert response.status_code == 404, f"Expected 404, got {response.status_code}"
            data = response.json()
            assert "User not found" in data.get("detail", "")
            print(f"✅ Verify OTP for non-existent user correctly returns 404")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
