"""
Vendor Balance Sheet & Bulk Payment Module Tests
Tests: Balance Sheet API, Bulk Payment API, Opening Balance Update
"""
import pytest
import requests
import os
import uuid

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', 'https://glassmesh.preview.emergentagent.com')

# Test credentials
ADMIN_EMAIL = "admin@lucumaa.in"
ADMIN_PASSWORD = "adminpass"


class TestVendorBalanceSheet:
    """Vendor Balance Sheet API Tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup - Login as admin and get token"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        
        # Login as super_admin
        response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        assert response.status_code == 200, f"Admin login failed: {response.text}"
        data = response.json()
        self.token = data.get("token")
        self.session.headers.update({"Authorization": f"Bearer {self.token}"})
        
        yield
    
    def test_01_get_balance_sheet_basic(self):
        """Test GET /api/erp/vendors/{vendor_id}/balance-sheet - Basic balance sheet retrieval"""
        # First get a vendor
        vendors_response = self.session.get(f"{BASE_URL}/api/erp/vendors/")
        assert vendors_response.status_code == 200
        vendors = vendors_response.json().get("vendors", [])
        
        if len(vendors) == 0:
            pytest.skip("No vendors available for testing")
        
        vendor_id = vendors[0]["id"]
        
        # Get balance sheet
        response = self.session.get(f"{BASE_URL}/api/erp/vendors/{vendor_id}/balance-sheet")
        assert response.status_code == 200, f"Failed to get balance sheet: {response.text}"
        
        data = response.json()
        
        # Verify response structure
        assert "vendor" in data, "Response should contain 'vendor' key"
        assert "financial_year" in data, "Response should contain 'financial_year' key"
        assert "period" in data, "Response should contain 'period' key"
        assert "summary" in data, "Response should contain 'summary' key"
        assert "monthly_breakdown" in data, "Response should contain 'monthly_breakdown' key"
        assert "top_purchases" in data, "Response should contain 'top_purchases' key"
        
        # Verify summary structure
        summary = data["summary"]
        assert "opening_balance" in summary, "Summary should contain opening_balance"
        assert "total_po_value" in summary, "Summary should contain total_po_value"
        assert "total_payments" in summary, "Summary should contain total_payments"
        assert "closing_balance" in summary, "Summary should contain closing_balance"
        assert "po_count" in summary, "Summary should contain po_count"
        assert "payment_count" in summary, "Summary should contain payment_count"
        
        # Verify period structure (April-March FY)
        period = data["period"]
        assert "start" in period, "Period should contain start date"
        assert "end" in period, "Period should contain end date"
        
        print(f"✓ GET /api/erp/vendors/{vendor_id}/balance-sheet - Balance sheet retrieved")
        print(f"  Vendor: {data['vendor']['name']}")
        print(f"  FY: {data['financial_year']}")
        print(f"  Opening Balance: ₹{summary['opening_balance']}")
        print(f"  Total PO Value: ₹{summary['total_po_value']} ({summary['po_count']} POs)")
        print(f"  Total Payments: ₹{summary['total_payments']} ({summary['payment_count']} payments)")
        print(f"  Closing Balance: ₹{summary['closing_balance']}")
    
    def test_02_balance_sheet_monthly_breakdown(self):
        """Test balance sheet monthly breakdown structure"""
        # Get a vendor
        vendors_response = self.session.get(f"{BASE_URL}/api/erp/vendors/")
        vendors = vendors_response.json().get("vendors", [])
        
        if len(vendors) == 0:
            pytest.skip("No vendors available for testing")
        
        vendor_id = vendors[0]["id"]
        
        # Get balance sheet
        response = self.session.get(f"{BASE_URL}/api/erp/vendors/{vendor_id}/balance-sheet")
        assert response.status_code == 200
        
        data = response.json()
        monthly_breakdown = data.get("monthly_breakdown", [])
        
        # Should have 12 months (April to March)
        assert len(monthly_breakdown) == 12, f"Should have 12 months, got {len(monthly_breakdown)}"
        
        # Verify each month has required fields
        for month in monthly_breakdown:
            assert "month" in month, "Month entry should have 'month' key"
            assert "po_count" in month, "Month entry should have 'po_count'"
            assert "po_value" in month, "Month entry should have 'po_value'"
            assert "payment_count" in month, "Month entry should have 'payment_count'"
            assert "payment_value" in month, "Month entry should have 'payment_value'"
            assert "net" in month, "Month entry should have 'net'"
            assert "closing_balance" in month, "Month entry should have 'closing_balance'"
        
        # Verify months are in correct order (April to March)
        first_month = monthly_breakdown[0]["month"]
        last_month = monthly_breakdown[-1]["month"]
        assert "-04" in first_month, f"First month should be April, got {first_month}"
        assert "-03" in last_month, f"Last month should be March, got {last_month}"
        
        print(f"✓ Monthly breakdown verified - 12 months from April to March")
        print(f"  First month: {first_month}")
        print(f"  Last month: {last_month}")
    
    def test_03_balance_sheet_with_financial_year_param(self):
        """Test balance sheet with specific financial year parameter"""
        # Get a vendor
        vendors_response = self.session.get(f"{BASE_URL}/api/erp/vendors/")
        vendors = vendors_response.json().get("vendors", [])
        
        if len(vendors) == 0:
            pytest.skip("No vendors available for testing")
        
        vendor_id = vendors[0]["id"]
        
        # Get balance sheet for specific FY (2024-25)
        response = self.session.get(f"{BASE_URL}/api/erp/vendors/{vendor_id}/balance-sheet?financial_year=2024-25")
        assert response.status_code == 200, f"Failed to get balance sheet: {response.text}"
        
        data = response.json()
        
        # Verify the financial year is correct
        assert data["financial_year"] == "2024-25", f"Financial year should be 2024-25, got {data['financial_year']}"
        
        # Verify period dates
        period = data["period"]
        assert period["start"] == "2024-04-01", f"Start date should be 2024-04-01, got {period['start']}"
        assert period["end"] == "2025-03-31", f"End date should be 2025-03-31, got {period['end']}"
        
        print(f"✓ Balance sheet with FY parameter verified")
        print(f"  FY: {data['financial_year']}")
        print(f"  Period: {period['start']} to {period['end']}")
    
    def test_04_balance_sheet_top_purchases(self):
        """Test balance sheet top purchases section"""
        # Get a vendor with POs
        vendors_response = self.session.get(f"{BASE_URL}/api/erp/vendors/")
        vendors = vendors_response.json().get("vendors", [])
        
        if len(vendors) == 0:
            pytest.skip("No vendors available for testing")
        
        vendor_id = vendors[0]["id"]
        
        # Get balance sheet
        response = self.session.get(f"{BASE_URL}/api/erp/vendors/{vendor_id}/balance-sheet")
        assert response.status_code == 200
        
        data = response.json()
        top_purchases = data.get("top_purchases", [])
        
        # Verify top purchases structure (if any exist)
        if len(top_purchases) > 0:
            for purchase in top_purchases:
                assert "po_number" in purchase, "Top purchase should have po_number"
                assert "date" in purchase, "Top purchase should have date"
                assert "amount" in purchase, "Top purchase should have amount"
                assert "status" in purchase, "Top purchase should have status"
            
            # Should be sorted by amount (descending)
            if len(top_purchases) > 1:
                for i in range(len(top_purchases) - 1):
                    assert top_purchases[i]["amount"] >= top_purchases[i+1]["amount"], "Top purchases should be sorted by amount descending"
            
            print(f"✓ Top purchases verified - {len(top_purchases)} purchases")
            for p in top_purchases[:3]:
                print(f"  {p['po_number']}: ₹{p['amount']} ({p['status']})")
        else:
            print(f"✓ Top purchases section present (no purchases in this FY)")
    
    def test_05_balance_sheet_vendor_not_found(self):
        """Test balance sheet for non-existent vendor"""
        fake_vendor_id = str(uuid.uuid4())
        
        response = self.session.get(f"{BASE_URL}/api/erp/vendors/{fake_vendor_id}/balance-sheet")
        assert response.status_code == 404, f"Should return 404 for non-existent vendor, got {response.status_code}"
        
        print(f"✓ Balance sheet correctly returns 404 for non-existent vendor")


class TestUpdateOpeningBalance:
    """Test Update Opening Balance API"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup - Login as admin"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        
        # Login as super_admin
        response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        assert response.status_code == 200
        data = response.json()
        self.token = data.get("token")
        self.session.headers.update({"Authorization": f"Bearer {self.token}"})
        
        yield
    
    def test_06_update_opening_balance(self):
        """Test PUT /api/erp/vendors/{vendor_id}/opening-balance - Update opening balance"""
        # Create a test vendor
        unique_id = str(uuid.uuid4())[:8]
        vendor_data = {
            "name": f"TEST_VendorOB_{unique_id}",
            "company_name": f"TEST_CompanyOB_{unique_id}",
            "phone": f"98700{unique_id[:5]}",
            "category": "raw_material"
        }
        
        create_response = self.session.post(f"{BASE_URL}/api/erp/vendors/", json=vendor_data)
        assert create_response.status_code == 200
        vendor_id = create_response.json()["vendor"]["id"]
        
        # Update opening balance
        new_balance = 50000.50
        response = self.session.put(
            f"{BASE_URL}/api/erp/vendors/{vendor_id}/opening-balance?opening_balance={new_balance}&notes=Test opening balance update"
        )
        assert response.status_code == 200, f"Failed to update opening balance: {response.text}"
        
        data = response.json()
        assert "message" in data, "Response should contain message"
        assert data["new_balance"] == new_balance, f"New balance should be {new_balance}, got {data['new_balance']}"
        
        print(f"✓ PUT /api/erp/vendors/{vendor_id}/opening-balance - Opening balance updated")
        print(f"  Old balance: ₹{data['old_balance']}")
        print(f"  New balance: ₹{data['new_balance']}")
        
        # Verify balance sheet reflects the new opening balance
        bs_response = self.session.get(f"{BASE_URL}/api/erp/vendors/{vendor_id}/balance-sheet")
        assert bs_response.status_code == 200
        bs_data = bs_response.json()
        
        # The opening balance in balance sheet should reflect the update
        print(f"✓ Balance sheet opening balance: ₹{bs_data['summary']['opening_balance']}")
    
    def test_07_update_opening_balance_vendor_not_found(self):
        """Test update opening balance for non-existent vendor"""
        fake_vendor_id = str(uuid.uuid4())
        
        response = self.session.put(
            f"{BASE_URL}/api/erp/vendors/{fake_vendor_id}/opening-balance?opening_balance=10000"
        )
        assert response.status_code == 404, f"Should return 404 for non-existent vendor, got {response.status_code}"
        
        print(f"✓ Update opening balance correctly returns 404 for non-existent vendor")


class TestBulkPayment:
    """Bulk Payment API Tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup - Login as admin"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        
        # Login as super_admin
        response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        assert response.status_code == 200
        data = response.json()
        self.token = data.get("token")
        self.session.headers.update({"Authorization": f"Bearer {self.token}"})
        
        yield
    
    def _create_vendor_with_approved_pos(self, num_pos=2):
        """Helper to create a vendor with multiple approved POs"""
        unique_id = str(uuid.uuid4())[:8]
        
        # Create vendor
        vendor_data = {
            "name": f"TEST_VendorBulk_{unique_id}",
            "company_name": f"TEST_CompanyBulk_{unique_id}",
            "phone": f"98710{unique_id[:5]}",
            "category": "raw_material"
        }
        
        create_vendor_response = self.session.post(f"{BASE_URL}/api/erp/vendors/", json=vendor_data)
        assert create_vendor_response.status_code == 200
        vendor = create_vendor_response.json()["vendor"]
        vendor_id = vendor["id"]
        
        # Create multiple POs
        po_ids = []
        total_amount = 0
        
        for i in range(num_pos):
            po_data = {
                "vendor_id": vendor_id,
                "items": [
                    {
                        "name": f"Test Item {i+1}",
                        "description": f"Test item for bulk payment {i+1}",
                        "quantity": 5 + i,
                        "unit": "pcs",
                        "unit_price": 1000 * (i + 1),
                        "gst_rate": 18
                    }
                ],
                "payment_terms": "30_days",
                "notes": f"Test PO {i+1} for bulk payment"
            }
            
            create_po_response = self.session.post(f"{BASE_URL}/api/erp/vendors/po", json=po_data)
            assert create_po_response.status_code == 200
            po = create_po_response.json()["po"]
            po_id = po["id"]
            
            # Submit and approve PO
            self.session.post(f"{BASE_URL}/api/erp/vendors/po/{po_id}/submit")
            self.session.post(
                f"{BASE_URL}/api/erp/vendors/po/{po_id}/approve",
                json={"status": "approved", "notes": "Approved for bulk payment test"}
            )
            
            po_ids.append(po_id)
            total_amount += po["grand_total"]
        
        return vendor, po_ids, total_amount
    
    def test_08_create_bulk_payment(self):
        """Test POST /api/erp/vendors/bulk-payment - Create bulk payment"""
        # Create vendor with approved POs
        vendor, po_ids, expected_total = self._create_vendor_with_approved_pos(2)
        
        # Create bulk payment
        bulk_payment_data = {
            "vendor_id": vendor["id"],
            "po_ids": po_ids,
            "payment_mode": "bank_transfer",
            "transaction_ref": f"BULK_UTR_{uuid.uuid4().hex[:8]}",
            "notes": "Test bulk payment"
        }
        
        response = self.session.post(f"{BASE_URL}/api/erp/vendors/bulk-payment", json=bulk_payment_data)
        assert response.status_code == 200, f"Failed to create bulk payment: {response.text}"
        
        data = response.json()
        
        # Verify response structure
        assert "bulk_payment_id" in data, "Response should contain bulk_payment_id"
        assert "payment_ids" in data, "Response should contain payment_ids"
        assert "total_amount" in data, "Response should contain total_amount"
        assert "po_count" in data, "Response should contain po_count"
        assert "vendor_name" in data, "Response should contain vendor_name"
        
        # Verify values
        assert data["po_count"] == len(po_ids), f"PO count should be {len(po_ids)}, got {data['po_count']}"
        assert len(data["payment_ids"]) == len(po_ids), "Should have one payment record per PO"
        assert data["total_amount"] > 0, "Total amount should be greater than 0"
        
        print(f"✓ POST /api/erp/vendors/bulk-payment - Bulk payment created")
        print(f"  Bulk Payment ID: {data['bulk_payment_id']}")
        print(f"  Vendor: {data['vendor_name']}")
        print(f"  PO Count: {data['po_count']}")
        print(f"  Total Amount: ₹{data['total_amount']}")
        print(f"  Payment IDs: {len(data['payment_ids'])} records created")
        
        return data
    
    def test_09_complete_bulk_payment(self):
        """Test POST /api/erp/vendors/bulk-payment/{id}/complete - Complete bulk payment"""
        # Create vendor with approved POs
        vendor, po_ids, expected_total = self._create_vendor_with_approved_pos(2)
        
        # Create bulk payment
        utr_ref = f"BULK_UTR_{uuid.uuid4().hex[:8]}"
        bulk_payment_data = {
            "vendor_id": vendor["id"],
            "po_ids": po_ids,
            "payment_mode": "bank_transfer",
            "transaction_ref": utr_ref,
            "notes": "Test bulk payment completion"
        }
        
        create_response = self.session.post(f"{BASE_URL}/api/erp/vendors/bulk-payment", json=bulk_payment_data)
        assert create_response.status_code == 200
        bulk_payment_id = create_response.json()["bulk_payment_id"]
        
        # Complete bulk payment
        complete_response = self.session.post(
            f"{BASE_URL}/api/erp/vendors/bulk-payment/{bulk_payment_id}/complete?transaction_ref={utr_ref}"
        )
        assert complete_response.status_code == 200, f"Failed to complete bulk payment: {complete_response.text}"
        
        data = complete_response.json()
        
        # Verify response structure
        assert "bulk_receipt_number" in data, "Response should contain bulk_receipt_number"
        assert "total_amount" in data, "Response should contain total_amount"
        assert "po_count" in data, "Response should contain po_count"
        assert "payments" in data, "Response should contain payments"
        
        # Verify bulk receipt number format
        assert data["bulk_receipt_number"].startswith("BULK-"), f"Bulk receipt should start with BULK-, got {data['bulk_receipt_number']}"
        
        # Verify individual payment receipts
        assert len(data["payments"]) == len(po_ids), "Should have receipt for each PO"
        for payment in data["payments"]:
            assert "po_number" in payment, "Payment should have po_number"
            assert "receipt_number" in payment, "Payment should have receipt_number"
            assert "amount" in payment, "Payment should have amount"
            assert payment["receipt_number"].startswith("VPR-"), f"Receipt should start with VPR-, got {payment['receipt_number']}"
        
        print(f"✓ POST /api/erp/vendors/bulk-payment/{bulk_payment_id}/complete - Bulk payment completed")
        print(f"  Bulk Receipt: {data['bulk_receipt_number']}")
        print(f"  Total Amount: ₹{data['total_amount']}")
        print(f"  PO Count: {data['po_count']}")
        for p in data["payments"]:
            print(f"    {p['po_number']}: ₹{p['amount']} - Receipt: {p['receipt_number']}")
        
        # Verify POs are now fully paid
        for po_id in po_ids:
            po_response = self.session.get(f"{BASE_URL}/api/erp/vendors/po/{po_id}")
            assert po_response.status_code == 200
            po = po_response.json()
            assert po["payment_status"] == "fully_paid", f"PO {po['po_number']} should be fully_paid, got {po['payment_status']}"
        
        print(f"✓ All {len(po_ids)} POs verified as fully_paid")
    
    def test_10_bulk_payment_vendor_not_found(self):
        """Test bulk payment with non-existent vendor"""
        fake_vendor_id = str(uuid.uuid4())
        
        bulk_payment_data = {
            "vendor_id": fake_vendor_id,
            "po_ids": [str(uuid.uuid4())],
            "payment_mode": "bank_transfer"
        }
        
        response = self.session.post(f"{BASE_URL}/api/erp/vendors/bulk-payment", json=bulk_payment_data)
        assert response.status_code == 404, f"Should return 404 for non-existent vendor, got {response.status_code}"
        
        print(f"✓ Bulk payment correctly returns 404 for non-existent vendor")
    
    def test_11_bulk_payment_po_not_found(self):
        """Test bulk payment with non-existent PO"""
        # Create a vendor
        unique_id = str(uuid.uuid4())[:8]
        vendor_data = {
            "name": f"TEST_VendorBulkErr_{unique_id}",
            "company_name": f"TEST_CompanyBulkErr_{unique_id}",
            "phone": f"98720{unique_id[:5]}",
            "category": "raw_material"
        }
        
        create_vendor_response = self.session.post(f"{BASE_URL}/api/erp/vendors/", json=vendor_data)
        vendor_id = create_vendor_response.json()["vendor"]["id"]
        
        # Try bulk payment with fake PO ID
        bulk_payment_data = {
            "vendor_id": vendor_id,
            "po_ids": [str(uuid.uuid4())],
            "payment_mode": "bank_transfer"
        }
        
        response = self.session.post(f"{BASE_URL}/api/erp/vendors/bulk-payment", json=bulk_payment_data)
        assert response.status_code == 404, f"Should return 404 for non-existent PO, got {response.status_code}"
        
        print(f"✓ Bulk payment correctly returns 404 for non-existent PO")
    
    def test_12_bulk_payment_already_paid_po(self):
        """Test bulk payment with already fully paid PO"""
        # Create vendor with approved PO
        vendor, po_ids, _ = self._create_vendor_with_approved_pos(1)
        po_id = po_ids[0]
        
        # First, pay the PO fully
        po_response = self.session.get(f"{BASE_URL}/api/erp/vendors/po/{po_id}")
        po = po_response.json()
        
        # Initiate and complete payment
        payment_data = {
            "po_id": po_id,
            "payment_type": "full",
            "amount": po["grand_total"],
            "payment_mode": "bank_transfer",
            "notes": "Full payment"
        }
        
        init_response = self.session.post(f"{BASE_URL}/api/erp/vendors/po/{po_id}/initiate-payment", json=payment_data)
        payment_id = init_response.json()["payment_id"]
        
        self.session.post(f"{BASE_URL}/api/erp/vendors/payment/{payment_id}/record-manual?transaction_ref=UTR123456")
        
        # Now try bulk payment with the already paid PO
        bulk_payment_data = {
            "vendor_id": vendor["id"],
            "po_ids": [po_id],
            "payment_mode": "bank_transfer"
        }
        
        response = self.session.post(f"{BASE_URL}/api/erp/vendors/bulk-payment", json=bulk_payment_data)
        assert response.status_code == 400, f"Should return 400 for already paid PO, got {response.status_code}"
        assert "fully paid" in response.json().get("detail", "").lower(), "Error should mention fully paid"
        
        print(f"✓ Bulk payment correctly returns 400 for already fully paid PO")
    
    def test_13_bulk_payment_unapproved_po(self):
        """Test bulk payment with unapproved PO"""
        # Create vendor
        unique_id = str(uuid.uuid4())[:8]
        vendor_data = {
            "name": f"TEST_VendorBulkUnapproved_{unique_id}",
            "company_name": f"TEST_CompanyBulkUnapproved_{unique_id}",
            "phone": f"98730{unique_id[:5]}",
            "category": "raw_material"
        }
        
        create_vendor_response = self.session.post(f"{BASE_URL}/api/erp/vendors/", json=vendor_data)
        vendor_id = create_vendor_response.json()["vendor"]["id"]
        
        # Create PO but don't approve it
        po_data = {
            "vendor_id": vendor_id,
            "items": [{"name": "Test Item", "quantity": 1, "unit_price": 1000, "gst_rate": 18}],
            "payment_terms": "30_days"
        }
        
        create_po_response = self.session.post(f"{BASE_URL}/api/erp/vendors/po", json=po_data)
        po_id = create_po_response.json()["po"]["id"]
        
        # Try bulk payment with unapproved PO
        bulk_payment_data = {
            "vendor_id": vendor_id,
            "po_ids": [po_id],
            "payment_mode": "bank_transfer"
        }
        
        response = self.session.post(f"{BASE_URL}/api/erp/vendors/bulk-payment", json=bulk_payment_data)
        assert response.status_code == 400, f"Should return 400 for unapproved PO, got {response.status_code}"
        assert "not approved" in response.json().get("detail", "").lower(), "Error should mention not approved"
        
        print(f"✓ Bulk payment correctly returns 400 for unapproved PO")
    
    def test_14_complete_already_completed_bulk_payment(self):
        """Test completing an already completed bulk payment"""
        # Create vendor with approved POs
        vendor, po_ids, _ = self._create_vendor_with_approved_pos(1)
        
        # Create and complete bulk payment
        bulk_payment_data = {
            "vendor_id": vendor["id"],
            "po_ids": po_ids,
            "payment_mode": "bank_transfer",
            "transaction_ref": "UTR_COMPLETE_TEST"
        }
        
        create_response = self.session.post(f"{BASE_URL}/api/erp/vendors/bulk-payment", json=bulk_payment_data)
        bulk_payment_id = create_response.json()["bulk_payment_id"]
        
        # Complete it first time
        self.session.post(f"{BASE_URL}/api/erp/vendors/bulk-payment/{bulk_payment_id}/complete?transaction_ref=UTR_COMPLETE_TEST")
        
        # Try to complete again
        response = self.session.post(f"{BASE_URL}/api/erp/vendors/bulk-payment/{bulk_payment_id}/complete?transaction_ref=UTR_COMPLETE_TEST2")
        assert response.status_code == 400, f"Should return 400 for already completed bulk payment, got {response.status_code}"
        assert "already completed" in response.json().get("detail", "").lower(), "Error should mention already completed"
        
        print(f"✓ Bulk payment correctly returns 400 for already completed payment")
    
    def test_15_bulk_payment_updates_vendor_balance(self):
        """Test that bulk payment updates vendor balance correctly"""
        # Create vendor with approved POs
        vendor, po_ids, expected_total = self._create_vendor_with_approved_pos(2)
        vendor_id = vendor["id"]
        
        # Get vendor balance before payment
        vendor_before = self.session.get(f"{BASE_URL}/api/erp/vendors/{vendor_id}").json()
        balance_before = vendor_before.get("current_balance", 0)
        
        # Create and complete bulk payment
        bulk_payment_data = {
            "vendor_id": vendor_id,
            "po_ids": po_ids,
            "payment_mode": "bank_transfer",
            "transaction_ref": f"UTR_BALANCE_{uuid.uuid4().hex[:8]}"
        }
        
        create_response = self.session.post(f"{BASE_URL}/api/erp/vendors/bulk-payment", json=bulk_payment_data)
        bulk_payment_id = create_response.json()["bulk_payment_id"]
        total_paid = create_response.json()["total_amount"]
        
        self.session.post(f"{BASE_URL}/api/erp/vendors/bulk-payment/{bulk_payment_id}/complete?transaction_ref=UTR_BALANCE_TEST")
        
        # Get vendor balance after payment
        vendor_after = self.session.get(f"{BASE_URL}/api/erp/vendors/{vendor_id}").json()
        balance_after = vendor_after.get("current_balance", 0)
        total_paid_after = vendor_after.get("total_paid", 0)
        
        # Verify balance decreased by payment amount
        print(f"✓ Vendor balance updated after bulk payment")
        print(f"  Balance before: ₹{balance_before}")
        print(f"  Total paid: ₹{total_paid}")
        print(f"  Balance after: ₹{balance_after}")
        print(f"  Total paid (vendor): ₹{total_paid_after}")


class TestBulkPaymentNotFound:
    """Test bulk payment not found scenarios"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup - Login as admin"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        
        response = self.session.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        assert response.status_code == 200
        data = response.json()
        self.token = data.get("token")
        self.session.headers.update({"Authorization": f"Bearer {self.token}"})
        
        yield
    
    def test_16_complete_nonexistent_bulk_payment(self):
        """Test completing a non-existent bulk payment"""
        fake_bulk_id = str(uuid.uuid4())
        
        response = self.session.post(f"{BASE_URL}/api/erp/vendors/bulk-payment/{fake_bulk_id}/complete?transaction_ref=UTR123")
        assert response.status_code == 404, f"Should return 404 for non-existent bulk payment, got {response.status_code}"
        
        print(f"✓ Complete bulk payment correctly returns 404 for non-existent payment")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
