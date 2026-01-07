"""
Accounts Module and Attendance Tests for Glass Factory ERP System
Tests Invoice creation, GST calculation, Payment recording, Lead Status Update, and Attendance tracking
"""
import pytest
import requests
import os
import uuid

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', 'https://glassmesh.preview.emergentagent.com').rstrip('/')


class TestAccountsDashboard:
    """Accounts Dashboard Metrics Tests"""
    
    def test_get_accounts_dashboard(self):
        """Test fetching accounts dashboard metrics"""
        response = requests.get(f"{BASE_URL}/api/erp/accounts/dashboard")
        assert response.status_code == 200
        data = response.json()
        
        # Verify all expected fields are present
        assert "total_receivables" in data
        assert "monthly_sales" in data
        assert "monthly_gst_collected" in data
        assert "monthly_collections" in data
        assert "overdue_count" in data
        assert "overdue_amount" in data
        assert "pending_invoices" in data
        
        print(f"✓ GET /api/erp/accounts/dashboard - Dashboard loaded")
        print(f"  - Monthly Sales: ₹{data['monthly_sales']}")
        print(f"  - Receivables: ₹{data['total_receivables']}")
        print(f"  - GST Collected: ₹{data['monthly_gst_collected']}")


class TestInvoiceManagement:
    """Invoice CRUD and GST Calculation Tests"""
    
    def test_get_invoices(self):
        """Test fetching all invoices"""
        response = requests.get(f"{BASE_URL}/api/erp/accounts/invoices")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        print(f"✓ GET /api/erp/accounts/invoices - Found {len(data)} invoices")
        return data
    
    def test_create_invoice_with_gst_calculation(self):
        """Test creating invoice with correct GST calculation (CGST + SGST = 18%)"""
        invoice_data = {
            "customer_name": f"TEST_Customer_{uuid.uuid4().hex[:6]}",
            "customer_gst": "27AABCU9603R1ZX",
            "customer_address": "Test Address, Mumbai",
            "items": [
                {"description": "Toughened Glass 8mm", "quantity": 10, "unit_price": 1000, "hsn_code": "7007"}
            ],
            "due_date": "2026-01-31",
            "notes": "Test invoice for GST verification",
            "is_interstate": False
        }
        
        response = requests.post(f"{BASE_URL}/api/erp/accounts/invoices", json=invoice_data)
        assert response.status_code == 200
        data = response.json()
        assert "invoice_number" in data
        assert "invoice_id" in data
        
        # Verify GST calculation
        # Subtotal = 10 * 1000 = 10000
        # CGST = 10000 * 0.09 = 900
        # SGST = 10000 * 0.09 = 900
        # Total = 10000 + 900 + 900 = 11800
        expected_total = 10000 + (10000 * 0.18)
        assert data["total"] == expected_total
        
        print(f"✓ POST /api/erp/accounts/invoices - Created {data['invoice_number']}")
        print(f"  - Total: ₹{data['total']} (Subtotal: ₹10000 + GST: ₹1800)")
        
        # Verify invoice details
        invoice = requests.get(f"{BASE_URL}/api/erp/accounts/invoices/{data['invoice_id']}").json()
        assert invoice["subtotal"] == 10000
        assert invoice["cgst"] == 900
        assert invoice["sgst"] == 900
        assert invoice["igst"] == 0
        assert invoice["total_tax"] == 1800
        assert invoice["payment_status"] == "pending"
        print(f"✓ Verified GST breakdown: CGST=₹{invoice['cgst']}, SGST=₹{invoice['sgst']}")
        
        return data["invoice_id"]
    
    def test_create_interstate_invoice_with_igst(self):
        """Test creating interstate invoice with IGST (18%)"""
        invoice_data = {
            "customer_name": f"TEST_Interstate_{uuid.uuid4().hex[:6]}",
            "customer_gst": "09AABCU9603R1ZX",  # Different state GST
            "customer_address": "Test Address, Delhi",
            "items": [
                {"description": "Laminated Glass 12mm", "quantity": 5, "unit_price": 2000, "hsn_code": "7007"}
            ],
            "due_date": "2026-02-15",
            "notes": "Interstate sale - IGST applicable",
            "is_interstate": True
        }
        
        response = requests.post(f"{BASE_URL}/api/erp/accounts/invoices", json=invoice_data)
        assert response.status_code == 200
        data = response.json()
        
        # Verify IGST calculation
        # Subtotal = 5 * 2000 = 10000
        # IGST = 10000 * 0.18 = 1800
        # Total = 10000 + 1800 = 11800
        expected_total = 10000 + (10000 * 0.18)
        assert data["total"] == expected_total
        
        # Verify invoice has IGST, not CGST/SGST
        invoice = requests.get(f"{BASE_URL}/api/erp/accounts/invoices/{data['invoice_id']}").json()
        assert invoice["cgst"] == 0
        assert invoice["sgst"] == 0
        assert invoice["igst"] == 1800
        print(f"✓ Interstate invoice created with IGST=₹{invoice['igst']}")
        
        return data["invoice_id"]
    
    def test_get_invoices_by_status(self):
        """Test filtering invoices by payment status"""
        response = requests.get(f"{BASE_URL}/api/erp/accounts/invoices", params={"status": "pending"})
        assert response.status_code == 200
        data = response.json()
        for invoice in data:
            assert invoice["payment_status"] == "pending"
        print(f"✓ GET /api/erp/accounts/invoices?status=pending - Found {len(data)} pending invoices")
    
    def test_get_invoice_by_id(self):
        """Test fetching single invoice by ID"""
        invoices = requests.get(f"{BASE_URL}/api/erp/accounts/invoices").json()
        if not invoices:
            pytest.skip("No invoices found")
        
        invoice_id = invoices[0]["id"]
        response = requests.get(f"{BASE_URL}/api/erp/accounts/invoices/{invoice_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == invoice_id
        print(f"✓ GET /api/erp/accounts/invoices/{invoice_id} - Found: {data['invoice_number']}")
    
    def test_get_invoice_not_found(self):
        """Test fetching non-existent invoice returns 404"""
        response = requests.get(f"{BASE_URL}/api/erp/accounts/invoices/non-existent-id")
        assert response.status_code == 404
        print(f"✓ GET /api/erp/accounts/invoices/non-existent-id - Returns 404 as expected")


class TestPaymentRecording:
    """Payment Recording and Invoice Status Update Tests"""
    
    def test_record_partial_payment(self):
        """Test recording partial payment and status change to 'partial'"""
        # Create a new invoice
        invoice_data = {
            "customer_name": f"TEST_Payment_{uuid.uuid4().hex[:6]}",
            "customer_gst": "",
            "items": [
                {"description": "Test Glass", "quantity": 10, "unit_price": 1000}
            ],
            "due_date": "2026-02-28"
        }
        create_response = requests.post(f"{BASE_URL}/api/erp/accounts/invoices", json=invoice_data)
        invoice_id = create_response.json()["invoice_id"]
        invoice_total = create_response.json()["total"]  # 11800
        
        # Record partial payment (less than total)
        payment_data = {
            "amount": 5000,
            "payment_method": "bank_transfer",
            "reference": f"TXN_{uuid.uuid4().hex[:8]}",
            "notes": "Partial payment test"
        }
        payment_response = requests.post(
            f"{BASE_URL}/api/erp/accounts/invoices/{invoice_id}/payment",
            json=payment_data
        )
        assert payment_response.status_code == 200
        data = payment_response.json()
        assert data["payment_status"] == "partial"
        assert data["new_balance"] == invoice_total - 5000
        print(f"✓ Partial payment recorded: ₹5000, Balance: ₹{data['new_balance']}")
        
        # Verify invoice status
        invoice = requests.get(f"{BASE_URL}/api/erp/accounts/invoices/{invoice_id}").json()
        assert invoice["payment_status"] == "partial"
        assert invoice["amount_paid"] == 5000
        print(f"✓ Invoice status updated to 'partial'")
        
        return invoice_id
    
    def test_record_full_payment(self):
        """Test recording full payment and status change to 'paid'"""
        # Create a new invoice
        invoice_data = {
            "customer_name": f"TEST_FullPayment_{uuid.uuid4().hex[:6]}",
            "items": [
                {"description": "Test Glass", "quantity": 5, "unit_price": 500}
            ]
        }
        create_response = requests.post(f"{BASE_URL}/api/erp/accounts/invoices", json=invoice_data)
        invoice_id = create_response.json()["invoice_id"]
        invoice_total = create_response.json()["total"]  # 2950
        
        # Record full payment
        payment_data = {
            "amount": invoice_total,
            "payment_method": "cash",
            "reference": "",
            "notes": "Full payment"
        }
        payment_response = requests.post(
            f"{BASE_URL}/api/erp/accounts/invoices/{invoice_id}/payment",
            json=payment_data
        )
        assert payment_response.status_code == 200
        data = payment_response.json()
        assert data["payment_status"] == "paid"
        assert data["new_balance"] == 0
        print(f"✓ Full payment recorded: ₹{invoice_total}, Status: paid")
        
        # Verify invoice status
        invoice = requests.get(f"{BASE_URL}/api/erp/accounts/invoices/{invoice_id}").json()
        assert invoice["payment_status"] == "paid"
        assert invoice["amount_paid"] == invoice_total
        print(f"✓ Invoice status updated to 'paid'")


class TestGSTReport:
    """GST Report Generation Tests"""
    
    def test_get_gst_report(self):
        """Test fetching GST report for current month"""
        current_month = "2026-01"
        response = requests.get(f"{BASE_URL}/api/erp/accounts/gst-report", params={"month": current_month})
        assert response.status_code == 200
        data = response.json()
        
        # Verify report structure
        assert data["month"] == current_month
        assert "output_gst" in data
        assert "input_gst" in data
        assert "net_gst_liability" in data
        assert "invoice_count" in data
        
        # Verify output_gst breakdown
        output_gst = data["output_gst"]
        assert "taxable_amount" in output_gst
        assert "cgst" in output_gst
        assert "sgst" in output_gst
        assert "igst" in output_gst
        assert "total" in output_gst
        
        # Verify total calculation
        assert output_gst["total"] == output_gst["cgst"] + output_gst["sgst"] + output_gst["igst"]
        
        print(f"✓ GET /api/erp/accounts/gst-report?month={current_month}")
        print(f"  - Taxable Amount: ₹{output_gst['taxable_amount']}")
        print(f"  - Output GST: ₹{output_gst['total']} (CGST: ₹{output_gst['cgst']}, SGST: ₹{output_gst['sgst']})")
        print(f"  - Input Credit: ₹{data['input_gst']}")
        print(f"  - Net Liability: ₹{data['net_gst_liability']}")


class TestLedger:
    """Ledger Entry Tests"""
    
    def test_get_ledger_entries(self):
        """Test fetching ledger entries"""
        response = requests.get(f"{BASE_URL}/api/erp/accounts/ledger")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        print(f"✓ GET /api/erp/accounts/ledger - Found {len(data)} entries")
        
        # Verify ledger entry structure if entries exist
        if data:
            entry = data[0]
            assert "date" in entry
            assert "type" in entry
            assert "reference" in entry
            assert "description" in entry
            assert "debit" in entry
            assert "credit" in entry
            assert "account" in entry
            print(f"  - Latest entry: {entry['type']} - {entry['description']}")


class TestLeadStatusUpdate:
    """CRM Lead Status Update Tests"""
    
    def test_update_lead_status_workflow(self):
        """Test lead status update through sales pipeline"""
        # Create a new lead
        lead_data = {
            "name": f"TEST_StatusLead_{uuid.uuid4().hex[:6]}",
            "email": f"test_{uuid.uuid4().hex[:6]}@example.com",
            "phone": "9876543210",
            "company": "Test Company",
            "customer_type": "retail",
            "expected_value": 50000
        }
        create_response = requests.post(f"{BASE_URL}/api/erp/crm/leads", json=lead_data)
        assert create_response.status_code == 200
        lead_id = create_response.json()["lead_id"]
        print(f"✓ Created lead {lead_id} (status: new)")
        
        # Test status transitions: new -> contacted -> quoted -> won
        statuses = ["contacted", "quoted", "negotiation", "won"]
        for status in statuses:
            response = requests.patch(
                f"{BASE_URL}/api/erp/crm/leads/{lead_id}/status",
                json={"status": status}
            )
            assert response.status_code == 200
            assert f"updated to {status}" in response.json()["message"]
            
            # Verify status was updated
            leads = requests.get(f"{BASE_URL}/api/erp/crm/leads", params={"status": status}).json()
            updated_lead = next((l for l in leads if l["id"] == lead_id), None)
            assert updated_lead is not None
            assert updated_lead["status"] == status
            print(f"✓ Lead status updated to '{status}'")
    
    def test_update_lead_status_to_lost(self):
        """Test marking lead as lost"""
        # Create a new lead
        lead_data = {
            "name": f"TEST_LostLead_{uuid.uuid4().hex[:6]}",
            "phone": "9876543210"
        }
        create_response = requests.post(f"{BASE_URL}/api/erp/crm/leads", json=lead_data)
        lead_id = create_response.json()["lead_id"]
        
        # Update to lost
        response = requests.patch(
            f"{BASE_URL}/api/erp/crm/leads/{lead_id}/status",
            json={"status": "lost"}
        )
        assert response.status_code == 200
        print(f"✓ Lead marked as 'lost'")
    
    def test_invalid_lead_status(self):
        """Test that invalid status returns error"""
        leads = requests.get(f"{BASE_URL}/api/erp/crm/leads").json()
        if not leads:
            pytest.skip("No leads found")
        
        lead_id = leads[0]["id"]
        response = requests.patch(
            f"{BASE_URL}/api/erp/crm/leads/{lead_id}/status",
            json={"status": "invalid_status"}
        )
        assert response.status_code == 400
        assert "Invalid status" in response.json()["detail"]
        print(f"✓ Invalid status correctly rejected")
    
    def test_update_nonexistent_lead(self):
        """Test updating non-existent lead returns 404"""
        response = requests.patch(
            f"{BASE_URL}/api/erp/crm/leads/non-existent-id/status",
            json={"status": "contacted"}
        )
        assert response.status_code == 404
        print(f"✓ Non-existent lead returns 404")


class TestAttendanceTracking:
    """HR Attendance Tracking Tests"""
    
    def test_mark_attendance(self):
        """Test marking employee attendance"""
        # Get an employee
        employees = requests.get(f"{BASE_URL}/api/erp/hr/employees").json()
        if not employees:
            pytest.skip("No employees found")
        
        employee = employees[0]
        
        attendance_data = {
            "employee_id": employee["id"],
            "date": "2026-01-02",
            "status": "present",
            "check_in": "09:00",
            "check_out": "18:00",
            "overtime_hours": 0,
            "notes": "Test attendance"
        }
        
        response = requests.post(f"{BASE_URL}/api/erp/hr/attendance", json=attendance_data)
        assert response.status_code == 200
        data = response.json()
        assert "attendance_id" in data
        print(f"✓ Attendance marked for {employee['name']} on 2026-01-02")
        
        return data["attendance_id"]
    
    def test_update_existing_attendance(self):
        """Test updating attendance for same employee and date"""
        employees = requests.get(f"{BASE_URL}/api/erp/hr/employees").json()
        if not employees:
            pytest.skip("No employees found")
        
        employee = employees[0]
        
        # Mark attendance
        attendance_data = {
            "employee_id": employee["id"],
            "date": "2026-01-03",
            "status": "present",
            "check_in": "09:00",
            "check_out": "17:00"
        }
        requests.post(f"{BASE_URL}/api/erp/hr/attendance", json=attendance_data)
        
        # Update same day attendance
        updated_data = {
            "employee_id": employee["id"],
            "date": "2026-01-03",
            "status": "present",
            "check_in": "09:00",
            "check_out": "19:00",
            "overtime_hours": 2
        }
        response = requests.post(f"{BASE_URL}/api/erp/hr/attendance", json=updated_data)
        assert response.status_code == 200
        assert "updated" in response.json()["message"].lower()
        print(f"✓ Attendance updated for same date")
    
    def test_mark_half_day(self):
        """Test marking half day attendance"""
        employees = requests.get(f"{BASE_URL}/api/erp/hr/employees").json()
        if not employees:
            pytest.skip("No employees found")
        
        employee = employees[0]
        
        attendance_data = {
            "employee_id": employee["id"],
            "date": "2026-01-04",
            "status": "half_day",
            "check_in": "09:00",
            "check_out": "13:00"
        }
        
        response = requests.post(f"{BASE_URL}/api/erp/hr/attendance", json=attendance_data)
        assert response.status_code == 200
        print(f"✓ Half day attendance marked")
    
    def test_mark_leave(self):
        """Test marking leave"""
        employees = requests.get(f"{BASE_URL}/api/erp/hr/employees").json()
        if not employees:
            pytest.skip("No employees found")
        
        employee = employees[0]
        
        attendance_data = {
            "employee_id": employee["id"],
            "date": "2026-01-05",
            "status": "leave",
            "notes": "Sick leave"
        }
        
        response = requests.post(f"{BASE_URL}/api/erp/hr/attendance", json=attendance_data)
        assert response.status_code == 200
        print(f"✓ Leave marked")
    
    def test_get_attendance_records(self):
        """Test fetching attendance records"""
        response = requests.get(f"{BASE_URL}/api/erp/hr/attendance")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        print(f"✓ GET /api/erp/hr/attendance - Found {len(data)} records")
    
    def test_get_attendance_by_employee(self):
        """Test filtering attendance by employee"""
        employees = requests.get(f"{BASE_URL}/api/erp/hr/employees").json()
        if not employees:
            pytest.skip("No employees found")
        
        employee_id = employees[0]["id"]
        response = requests.get(f"{BASE_URL}/api/erp/hr/attendance", params={"employee_id": employee_id})
        assert response.status_code == 200
        data = response.json()
        for record in data:
            assert record["employee_id"] == employee_id
        print(f"✓ GET /api/erp/hr/attendance?employee_id={employee_id} - Found {len(data)} records")
    
    def test_get_attendance_by_date(self):
        """Test filtering attendance by date"""
        response = requests.get(f"{BASE_URL}/api/erp/hr/attendance", params={"date": "2026-01-01"})
        assert response.status_code == 200
        data = response.json()
        for record in data:
            assert record["date"] == "2026-01-01"
        print(f"✓ GET /api/erp/hr/attendance?date=2026-01-01 - Found {len(data)} records")
    
    def test_get_attendance_summary(self):
        """Test fetching monthly attendance summary"""
        response = requests.get(f"{BASE_URL}/api/erp/hr/attendance/summary", params={"month": "2026-01"})
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        
        # Verify summary structure
        if data:
            summary = data[0]
            assert "employee_id" in summary
            assert "employee_name" in summary
            assert "emp_code" in summary
            assert "department" in summary
            assert "present" in summary
            assert "absent" in summary
            assert "half_day" in summary
            assert "leave" in summary
            assert "total_days" in summary
            assert "overtime_hours" in summary
            
            print(f"✓ GET /api/erp/hr/attendance/summary?month=2026-01")
            print(f"  - {summary['employee_name']}: {summary['present']} present, {summary['overtime_hours']} OT hours")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
