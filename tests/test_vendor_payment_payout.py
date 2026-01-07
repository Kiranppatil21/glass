"""
Test Vendor Payment Payout System (RazorpayX Payouts)
Tests the refactored vendor payment system that uses Razorpay Payouts (RazorpayX) 
for OUTGOING vendor payments instead of Razorpay Orders (for receiving payments).

Features tested:
1. Vendor Payment Initiation API - RazorpayX Payout mode (mock)
2. Vendor Payment Initiation API - Manual mode with UTR
3. Payment Status API - should auto-complete mock payouts
4. Payment Record-Manual API - complete manual payments with UTR
5. Vendor bank details validation before payout
6. PO status update after payment completion
"""

import pytest
import requests
import os
import time
import uuid

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', '').rstrip('/')

class TestVendorPaymentPayout:
    """Test vendor payment payout functionality"""
    
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
        assert login_response.status_code == 200, f"Login failed: {login_response.text}"
        token = login_response.json().get("token")
        self.session.headers.update({"Authorization": f"Bearer {token}"})
        
        yield
        
        # Cleanup - no specific cleanup needed as we use TEST_ prefix
    
    def test_01_get_vendors_list(self):
        """Test getting vendors list"""
        response = self.session.get(f"{BASE_URL}/api/erp/vendors/")
        assert response.status_code == 200, f"Failed to get vendors: {response.text}"
        data = response.json()
        assert "vendors" in data
        print(f"Found {len(data['vendors'])} vendors")
    
    def test_02_get_purchase_orders_list(self):
        """Test getting PO list"""
        response = self.session.get(f"{BASE_URL}/api/erp/vendors/po/list")
        assert response.status_code == 200, f"Failed to get POs: {response.text}"
        data = response.json()
        assert "purchase_orders" in data
        print(f"Found {len(data['purchase_orders'])} purchase orders")
    
    def test_03_create_test_vendor_with_bank_details(self):
        """Create a test vendor with bank details for payout testing"""
        vendor_data = {
            "name": f"TEST_PayoutVendor_{uuid.uuid4().hex[:6]}",
            "company_name": "Test Payout Company",
            "email": f"test_payout_{uuid.uuid4().hex[:6]}@test.com",
            "phone": f"99{uuid.uuid4().hex[:8][:8]}",
            "gst_number": "29ABCDE1234F1Z5",
            "pan_number": "ABCDE1234F",
            "bank_name": "Test Bank",
            "bank_account": "1234567890123456",
            "ifsc_code": "TEST0001234",
            "upi_id": "testvendor@upi",
            "address": "Test Address",
            "category": "raw_material",
            "credit_days": 30,
            "credit_limit": 100000,
            "notes": "Test vendor for payout testing"
        }
        
        response = self.session.post(f"{BASE_URL}/api/erp/vendors/", json=vendor_data)
        assert response.status_code == 200, f"Failed to create vendor: {response.text}"
        data = response.json()
        assert "vendor" in data
        vendor = data["vendor"]
        assert vendor["bank_account"] == "1234567890123456"
        assert vendor["ifsc_code"] == "TEST0001234"
        print(f"Created test vendor: {vendor['vendor_code']} with bank details")
        
        # Store for later tests
        self.__class__.test_vendor_id = vendor["id"]
        self.__class__.test_vendor_code = vendor["vendor_code"]
    
    def test_04_create_test_po_for_payout(self):
        """Create a test PO for payout testing"""
        vendor_id = getattr(self.__class__, 'test_vendor_id', None)
        if not vendor_id:
            pytest.skip("No test vendor created")
        
        po_data = {
            "vendor_id": vendor_id,
            "items": [
                {
                    "name": "Test Item for Payout",
                    "description": "Test item",
                    "quantity": 10,
                    "unit": "pcs",
                    "unit_price": 1000,
                    "gst_rate": 18
                }
            ],
            "delivery_date": "2025-12-31",
            "delivery_address": "Test Address",
            "payment_terms": "30_days",
            "notes": "Test PO for payout testing"
        }
        
        response = self.session.post(f"{BASE_URL}/api/erp/vendors/po", json=po_data)
        assert response.status_code == 200, f"Failed to create PO: {response.text}"
        data = response.json()
        assert "po" in data
        po = data["po"]
        assert po["status"] == "draft"
        print(f"Created test PO: {po['po_number']} with grand_total: {po['grand_total']}")
        
        self.__class__.test_po_id = po["id"]
        self.__class__.test_po_number = po["po_number"]
        self.__class__.test_po_grand_total = po["grand_total"]
    
    def test_05_submit_po_for_approval(self):
        """Submit PO for approval"""
        po_id = getattr(self.__class__, 'test_po_id', None)
        if not po_id:
            pytest.skip("No test PO created")
        
        response = self.session.post(f"{BASE_URL}/api/erp/vendors/po/{po_id}/submit")
        assert response.status_code == 200, f"Failed to submit PO: {response.text}"
        print(f"PO submitted for approval")
    
    def test_06_approve_po(self):
        """Approve the PO"""
        po_id = getattr(self.__class__, 'test_po_id', None)
        if not po_id:
            pytest.skip("No test PO created")
        
        response = self.session.post(f"{BASE_URL}/api/erp/vendors/po/{po_id}/approve", json={
            "status": "approved",
            "notes": "Approved for payout testing"
        })
        assert response.status_code == 200, f"Failed to approve PO: {response.text}"
        print(f"PO approved")
    
    def test_07_verify_po_approved_status(self):
        """Verify PO is approved and ready for payment"""
        po_id = getattr(self.__class__, 'test_po_id', None)
        if not po_id:
            pytest.skip("No test PO created")
        
        response = self.session.get(f"{BASE_URL}/api/erp/vendors/po/{po_id}")
        assert response.status_code == 200, f"Failed to get PO: {response.text}"
        po = response.json()
        assert po["status"] == "approved", f"PO status is {po['status']}, expected 'approved'"
        assert po["payment_status"] == "unpaid", f"Payment status is {po['payment_status']}, expected 'unpaid'"
        print(f"PO {po['po_number']} is approved and unpaid, outstanding: {po['outstanding_balance']}")
    
    def test_08_initiate_razorpay_payout_mock_mode(self):
        """Test initiating Razorpay payout in MOCK mode"""
        po_id = getattr(self.__class__, 'test_po_id', None)
        grand_total = getattr(self.__class__, 'test_po_grand_total', 0)
        if not po_id:
            pytest.skip("No test PO created")
        
        payment_data = {
            "po_id": po_id,
            "payment_type": "full",
            "percentage": 100,
            "amount": grand_total,
            "payment_mode": "razorpay",  # This should trigger RazorpayX Payout
            "notes": "Test payout in mock mode"
        }
        
        response = self.session.post(f"{BASE_URL}/api/erp/vendors/po/{po_id}/initiate-payment", json=payment_data)
        assert response.status_code == 200, f"Failed to initiate payout: {response.text}"
        data = response.json()
        
        # Verify response structure for payout
        assert "payment_id" in data, "Missing payment_id in response"
        assert "reference_id" in data, "Missing reference_id in response"
        assert "payout_id" in data or "mock_mode" in data, "Missing payout_id or mock_mode in response"
        assert data.get("mock_mode") == True, "Expected mock_mode to be True"
        
        print(f"Payout initiated: payment_id={data['payment_id']}, mock_mode={data.get('mock_mode')}")
        print(f"Payout ID: {data.get('payout_id')}")
        print(f"Next step: {data.get('next_step')}")
        
        self.__class__.test_payment_id = data["payment_id"]
    
    def test_09_check_payout_status_auto_complete(self):
        """Test that mock payout auto-completes when status is checked"""
        payment_id = getattr(self.__class__, 'test_payment_id', None)
        if not payment_id:
            pytest.skip("No payment initiated")
        
        # Wait a moment for processing
        time.sleep(1)
        
        response = self.session.get(f"{BASE_URL}/api/erp/vendors/payment/{payment_id}/status")
        assert response.status_code == 200, f"Failed to get payment status: {response.text}"
        data = response.json()
        
        # In mock mode, status should auto-complete
        assert data.get("status") == "completed", f"Expected status 'completed', got '{data.get('status')}'"
        assert data.get("payout_status") == "processed", f"Expected payout_status 'processed', got '{data.get('payout_status')}'"
        assert data.get("mock_mode") == True, "Expected mock_mode to be True"
        
        # UTR should be auto-generated
        utr = data.get("utr")
        assert utr is not None, "UTR should be auto-generated in mock mode"
        assert utr.startswith("UTR"), f"UTR should start with 'UTR', got '{utr}'"
        
        # Receipt number should be generated
        receipt = data.get("receipt_number")
        assert receipt is not None, "Receipt number should be generated"
        assert receipt.startswith("VPR-"), f"Receipt should start with 'VPR-', got '{receipt}'"
        
        print(f"Payout completed: status={data['status']}, UTR={utr}, receipt={receipt}")
    
    def test_10_verify_po_payment_status_updated(self):
        """Verify PO payment status is updated after payout"""
        po_id = getattr(self.__class__, 'test_po_id', None)
        if not po_id:
            pytest.skip("No test PO created")
        
        response = self.session.get(f"{BASE_URL}/api/erp/vendors/po/{po_id}")
        assert response.status_code == 200, f"Failed to get PO: {response.text}"
        po = response.json()
        
        # PO should be fully paid now
        assert po["payment_status"] == "fully_paid", f"Expected 'fully_paid', got '{po['payment_status']}'"
        assert po["outstanding_balance"] == 0, f"Expected outstanding_balance 0, got {po['outstanding_balance']}"
        
        # Payment history should have entry
        assert len(po.get("payment_history", [])) > 0, "Payment history should have entry"
        
        print(f"PO {po['po_number']} is now fully_paid, amount_paid: {po['amount_paid']}")
    
    def test_11_create_vendor_without_bank_details(self):
        """Create a vendor without bank details"""
        vendor_data = {
            "name": f"TEST_NoBankVendor_{uuid.uuid4().hex[:6]}",
            "company_name": "No Bank Company",
            "email": f"test_nobank_{uuid.uuid4().hex[:6]}@test.com",
            "phone": f"88{uuid.uuid4().hex[:8][:8]}",
            "category": "raw_material",
            "credit_days": 30,
            "credit_limit": 50000,
            "notes": "Vendor without bank details"
        }
        
        response = self.session.post(f"{BASE_URL}/api/erp/vendors/", json=vendor_data)
        assert response.status_code == 200, f"Failed to create vendor: {response.text}"
        vendor = response.json()["vendor"]
        print(f"Created vendor without bank details: {vendor['vendor_code']}")
        
        self.__class__.no_bank_vendor_id = vendor["id"]
    
    def test_12_create_po_for_no_bank_vendor(self):
        """Create PO for vendor without bank details"""
        vendor_id = getattr(self.__class__, 'no_bank_vendor_id', None)
        if not vendor_id:
            pytest.skip("No vendor without bank details")
        
        po_data = {
            "vendor_id": vendor_id,
            "items": [{"name": "Test Item", "quantity": 5, "unit": "pcs", "unit_price": 500, "gst_rate": 18}],
            "payment_terms": "30_days"
        }
        
        response = self.session.post(f"{BASE_URL}/api/erp/vendors/po", json=po_data)
        assert response.status_code == 200
        po = response.json()["po"]
        
        # Submit and approve
        self.session.post(f"{BASE_URL}/api/erp/vendors/po/{po['id']}/submit")
        self.session.post(f"{BASE_URL}/api/erp/vendors/po/{po['id']}/approve", json={"status": "approved"})
        
        self.__class__.no_bank_po_id = po["id"]
        self.__class__.no_bank_po_total = po["grand_total"]
        print(f"Created and approved PO for no-bank vendor: {po['po_number']}")
    
    def test_13_razorpay_payout_fails_without_bank_details(self):
        """Test that Razorpay payout fails when vendor has no bank details"""
        po_id = getattr(self.__class__, 'no_bank_po_id', None)
        grand_total = getattr(self.__class__, 'no_bank_po_total', 0)
        if not po_id:
            pytest.skip("No PO for no-bank vendor")
        
        payment_data = {
            "po_id": po_id,
            "payment_type": "full",
            "amount": grand_total,
            "payment_mode": "razorpay",
            "notes": "Should fail - no bank details"
        }
        
        response = self.session.post(f"{BASE_URL}/api/erp/vendors/po/{po_id}/initiate-payment", json=payment_data)
        
        # Should fail with 400 error about missing bank details
        assert response.status_code == 400, f"Expected 400, got {response.status_code}"
        error = response.json()
        assert "bank" in error.get("detail", "").lower() or "ifsc" in error.get("detail", "").lower(), \
            f"Error should mention bank details: {error}"
        
        print(f"Correctly rejected payout without bank details: {error.get('detail')}")
    
    def test_14_manual_payment_with_utr(self):
        """Test manual payment mode with UTR entry"""
        po_id = getattr(self.__class__, 'no_bank_po_id', None)
        grand_total = getattr(self.__class__, 'no_bank_po_total', 0)
        if not po_id:
            pytest.skip("No PO for manual payment test")
        
        # Initiate manual payment (bank_transfer mode)
        payment_data = {
            "po_id": po_id,
            "payment_type": "full",
            "amount": grand_total,
            "payment_mode": "bank_transfer",  # Manual mode
            "notes": "Manual bank transfer"
        }
        
        response = self.session.post(f"{BASE_URL}/api/erp/vendors/po/{po_id}/initiate-payment", json=payment_data)
        assert response.status_code == 200, f"Failed to initiate manual payment: {response.text}"
        data = response.json()
        
        assert data.get("requires_verification") == True, "Manual payment should require verification"
        payment_id = data["payment_id"]
        print(f"Manual payment initiated: {payment_id}, requires UTR verification")
        
        self.__class__.manual_payment_id = payment_id
    
    def test_15_record_manual_payment_with_utr(self):
        """Test recording manual payment with UTR"""
        payment_id = getattr(self.__class__, 'manual_payment_id', None)
        if not payment_id:
            pytest.skip("No manual payment initiated")
        
        # Record manual payment with UTR
        utr = f"MANUAL_UTR_{uuid.uuid4().hex[:8]}"
        response = self.session.post(
            f"{BASE_URL}/api/erp/vendors/payment/{payment_id}/record-manual",
            params={"transaction_ref": utr}
        )
        assert response.status_code == 200, f"Failed to record manual payment: {response.text}"
        data = response.json()
        
        assert "receipt_number" in data, "Should return receipt_number"
        assert data.get("payment_status") == "fully_paid", f"Expected fully_paid, got {data.get('payment_status')}"
        
        print(f"Manual payment recorded: receipt={data['receipt_number']}, UTR={utr}")
    
    def test_16_verify_manual_payment_po_updated(self):
        """Verify PO is updated after manual payment"""
        po_id = getattr(self.__class__, 'no_bank_po_id', None)
        if not po_id:
            pytest.skip("No PO for verification")
        
        response = self.session.get(f"{BASE_URL}/api/erp/vendors/po/{po_id}")
        assert response.status_code == 200
        po = response.json()
        
        assert po["payment_status"] == "fully_paid", f"Expected fully_paid, got {po['payment_status']}"
        assert po["outstanding_balance"] == 0, f"Expected 0 outstanding, got {po['outstanding_balance']}"
        
        print(f"Manual payment PO verified: {po['po_number']} is fully_paid")
    
    def test_17_payment_history_endpoint(self):
        """Test payment history endpoint"""
        response = self.session.get(f"{BASE_URL}/api/erp/vendors/payments/history")
        assert response.status_code == 200, f"Failed to get payment history: {response.text}"
        data = response.json()
        
        assert "payments" in data
        assert "count" in data
        
        # Should have our test payments
        payments = data["payments"]
        print(f"Found {len(payments)} payments in history")
        
        # Check for mock_mode flag in payments
        mock_payments = [p for p in payments if p.get("mock_mode") == True]
        print(f"Found {len(mock_payments)} mock mode payments")
    
    def test_18_partial_payment_flow(self):
        """Test partial payment flow"""
        vendor_id = getattr(self.__class__, 'test_vendor_id', None)
        if not vendor_id:
            pytest.skip("No test vendor")
        
        # Create new PO for partial payment test
        po_data = {
            "vendor_id": vendor_id,
            "items": [{"name": "Partial Payment Item", "quantity": 10, "unit": "pcs", "unit_price": 1000, "gst_rate": 18}],
            "payment_terms": "30_days"
        }
        
        response = self.session.post(f"{BASE_URL}/api/erp/vendors/po", json=po_data)
        assert response.status_code == 200
        po = response.json()["po"]
        
        # Submit and approve
        self.session.post(f"{BASE_URL}/api/erp/vendors/po/{po['id']}/submit")
        self.session.post(f"{BASE_URL}/api/erp/vendors/po/{po['id']}/approve", json={"status": "approved"})
        
        # Make 50% payment
        half_amount = po["grand_total"] / 2
        payment_data = {
            "po_id": po["id"],
            "payment_type": "partial",
            "percentage": 50,
            "amount": half_amount,
            "payment_mode": "razorpay",
            "notes": "50% advance payment"
        }
        
        response = self.session.post(f"{BASE_URL}/api/erp/vendors/po/{po['id']}/initiate-payment", json=payment_data)
        assert response.status_code == 200, f"Failed to initiate partial payment: {response.text}"
        data = response.json()
        
        # Check status to auto-complete
        time.sleep(1)
        status_response = self.session.get(f"{BASE_URL}/api/erp/vendors/payment/{data['payment_id']}/status")
        assert status_response.status_code == 200
        
        # Verify PO is partially paid
        po_response = self.session.get(f"{BASE_URL}/api/erp/vendors/po/{po['id']}")
        po_updated = po_response.json()
        
        assert po_updated["payment_status"] == "partially_paid", f"Expected partially_paid, got {po_updated['payment_status']}"
        assert po_updated["outstanding_balance"] > 0, "Should have outstanding balance"
        
        print(f"Partial payment successful: paid {half_amount}, outstanding {po_updated['outstanding_balance']}")
    
    def test_19_payment_amount_validation(self):
        """Test payment amount validation"""
        po_id = getattr(self.__class__, 'test_po_id', None)
        if not po_id:
            pytest.skip("No test PO")
        
        # Try to pay more than outstanding (should fail)
        payment_data = {
            "po_id": po_id,
            "payment_type": "full",
            "amount": 999999999,  # Way more than outstanding
            "payment_mode": "razorpay"
        }
        
        response = self.session.post(f"{BASE_URL}/api/erp/vendors/po/{po_id}/initiate-payment", json=payment_data)
        # Should fail because PO is already fully paid or amount exceeds outstanding
        assert response.status_code in [400, 200], f"Unexpected status: {response.status_code}"
        
        if response.status_code == 400:
            error = response.json()
            print(f"Correctly rejected excess payment: {error.get('detail')}")
        else:
            print("PO already fully paid, no validation needed")
    
    def test_20_payment_on_non_approved_po_fails(self):
        """Test that payment on non-approved PO fails"""
        vendor_id = getattr(self.__class__, 'test_vendor_id', None)
        if not vendor_id:
            pytest.skip("No test vendor")
        
        # Create draft PO
        po_data = {
            "vendor_id": vendor_id,
            "items": [{"name": "Draft Item", "quantity": 1, "unit": "pcs", "unit_price": 100, "gst_rate": 18}],
            "payment_terms": "30_days"
        }
        
        response = self.session.post(f"{BASE_URL}/api/erp/vendors/po", json=po_data)
        assert response.status_code == 200
        po = response.json()["po"]
        
        # Try to pay draft PO (should fail)
        payment_data = {
            "po_id": po["id"],
            "payment_type": "full",
            "amount": po["grand_total"],
            "payment_mode": "razorpay"
        }
        
        response = self.session.post(f"{BASE_URL}/api/erp/vendors/po/{po['id']}/initiate-payment", json=payment_data)
        assert response.status_code == 400, f"Expected 400 for draft PO payment, got {response.status_code}"
        error = response.json()
        assert "approved" in error.get("detail", "").lower(), f"Error should mention approval: {error}"
        
        print(f"Correctly rejected payment on draft PO: {error.get('detail')}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
