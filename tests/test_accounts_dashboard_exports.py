"""
Accounts Dashboard and Export Tests for Glass Factory ERP System
Tests the new Accounts Dashboard features: Overview, P&L, GST Report, and Export functionality
"""
import pytest
import requests
import os

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', 'https://glassmesh.preview.emergentagent.com').rstrip('/')


class TestAccountsDashboardOverview:
    """Accounts Dashboard Overview Metrics Tests"""
    
    def test_dashboard_metrics_structure(self):
        """Test dashboard returns all required metrics"""
        response = requests.get(f"{BASE_URL}/api/erp/accounts/dashboard")
        assert response.status_code == 200
        data = response.json()
        
        required_fields = [
            "total_receivables",
            "monthly_sales",
            "monthly_gst_collected",
            "monthly_collections",
            "overdue_count",
            "overdue_amount",
            "pending_invoices"
        ]
        
        for field in required_fields:
            assert field in data, f"Missing field: {field}"
            assert isinstance(data[field], (int, float)), f"Field {field} should be numeric"
        
        print(f"✓ Dashboard metrics structure verified")
        print(f"  - Monthly Sales: ₹{data['monthly_sales']}")
        print(f"  - Receivables: ₹{data['total_receivables']}")
        print(f"  - GST Collected: ₹{data['monthly_gst_collected']}")
        print(f"  - Pending Invoices: {data['pending_invoices']}")


class TestProfitLossReport:
    """Profit & Loss Report Tests"""
    
    def test_profit_loss_report_structure(self):
        """Test P&L report returns complete structure"""
        response = requests.get(
            f"{BASE_URL}/api/erp/accounts/profit-loss",
            params={"start_date": "2026-01-01", "end_date": "2026-12-31"}
        )
        assert response.status_code == 200
        data = response.json()
        
        # Verify period
        assert "period" in data
        assert data["period"]["start_date"] == "2026-01-01"
        assert data["period"]["end_date"] == "2026-12-31"
        
        # Verify revenue section
        assert "revenue" in data
        assert "total_sales" in data["revenue"]
        assert "invoice_count" in data["revenue"]
        assert "gst_collected" in data["revenue"]
        
        # Verify cost of goods section
        assert "cost_of_goods" in data
        assert "total_purchases" in data["cost_of_goods"]
        assert "po_count" in data["cost_of_goods"]
        assert "gst_paid" in data["cost_of_goods"]
        
        # Verify profit calculations
        assert "gross_profit" in data
        assert "operating_expenses" in data
        assert "net_profit" in data
        assert "profit_margin" in data
        
        # Verify GST summary
        assert "gst_summary" in data
        assert "collected" in data["gst_summary"]
        assert "paid" in data["gst_summary"]
        assert "net_liability" in data["gst_summary"]
        
        # Verify calculations
        expected_gross = data["revenue"]["total_sales"] - data["cost_of_goods"]["total_purchases"]
        assert data["gross_profit"] == expected_gross
        
        print(f"✓ P&L Report structure verified")
        print(f"  - Revenue: ₹{data['revenue']['total_sales']}")
        print(f"  - COGS: ₹{data['cost_of_goods']['total_purchases']}")
        print(f"  - Gross Profit: ₹{data['gross_profit']}")
        print(f"  - Net Profit: ₹{data['net_profit']}")
        print(f"  - Profit Margin: {data['profit_margin']}%")
    
    def test_profit_loss_with_date_range(self):
        """Test P&L report respects date range"""
        # Test with current month
        response = requests.get(
            f"{BASE_URL}/api/erp/accounts/profit-loss",
            params={"start_date": "2026-01-01", "end_date": "2026-01-31"}
        )
        assert response.status_code == 200
        data = response.json()
        
        assert data["period"]["start_date"] == "2026-01-01"
        assert data["period"]["end_date"] == "2026-01-31"
        print(f"✓ P&L date range filter works")


class TestGSTReport:
    """GST Report Tests"""
    
    def test_gst_report_structure(self):
        """Test GST report returns complete structure"""
        response = requests.get(
            f"{BASE_URL}/api/erp/accounts/gst-report",
            params={"month": "2026-01"}
        )
        assert response.status_code == 200
        data = response.json()
        
        # Verify structure
        assert data["month"] == "2026-01"
        assert "output_gst" in data
        assert "input_gst" in data
        assert "net_gst_liability" in data
        assert "invoice_count" in data
        assert "purchase_count" in data
        
        # Verify output_gst breakdown
        output = data["output_gst"]
        assert "taxable_amount" in output
        assert "cgst" in output
        assert "sgst" in output
        assert "igst" in output
        assert "total" in output
        
        # Verify total calculation
        expected_total = output["cgst"] + output["sgst"] + output["igst"]
        assert output["total"] == expected_total
        
        # Verify net liability calculation
        expected_liability = output["total"] - data["input_gst"]
        assert data["net_gst_liability"] == expected_liability
        
        print(f"✓ GST Report structure verified")
        print(f"  - Output GST: ₹{output['total']}")
        print(f"  - Input Credit: ₹{data['input_gst']}")
        print(f"  - Net Liability: ₹{data['net_gst_liability']}")


class TestExportInvoices:
    """Invoice Export Tests"""
    
    def test_export_invoices_excel(self):
        """Test exporting invoices to Excel"""
        response = requests.get(
            f"{BASE_URL}/api/erp/reports/invoices/export",
            params={"start_date": "2026-01-01", "end_date": "2026-12-31", "format": "excel"}
        )
        assert response.status_code == 200
        
        # Verify content type
        content_type = response.headers.get("content-type", "")
        assert "spreadsheet" in content_type or "excel" in content_type or "octet-stream" in content_type
        
        # Verify content disposition
        content_disp = response.headers.get("content-disposition", "")
        assert "attachment" in content_disp
        assert ".xlsx" in content_disp
        
        # Verify file has content
        assert len(response.content) > 0
        
        print(f"✓ Invoice Excel export works")
        print(f"  - File size: {len(response.content)} bytes")
    
    def test_export_invoices_pdf(self):
        """Test exporting invoices to PDF"""
        response = requests.get(
            f"{BASE_URL}/api/erp/reports/invoices/export",
            params={"start_date": "2026-01-01", "end_date": "2026-12-31", "format": "pdf"}
        )
        assert response.status_code == 200
        
        # Verify content type
        content_type = response.headers.get("content-type", "")
        assert "pdf" in content_type or "octet-stream" in content_type
        
        # Verify content disposition
        content_disp = response.headers.get("content-disposition", "")
        assert "attachment" in content_disp
        assert ".pdf" in content_disp
        
        # Verify file has content
        assert len(response.content) > 0
        
        print(f"✓ Invoice PDF export works")
        print(f"  - File size: {len(response.content)} bytes")


class TestExportProfitLoss:
    """Profit & Loss Export Tests"""
    
    def test_export_profit_loss_excel(self):
        """Test exporting P&L to Excel"""
        response = requests.get(
            f"{BASE_URL}/api/erp/reports/profit-loss/export",
            params={"start_date": "2026-01-01", "end_date": "2026-12-31", "format": "excel"}
        )
        assert response.status_code == 200
        
        # Verify content type
        content_type = response.headers.get("content-type", "")
        assert "spreadsheet" in content_type or "excel" in content_type or "octet-stream" in content_type
        
        # Verify content disposition
        content_disp = response.headers.get("content-disposition", "")
        assert "attachment" in content_disp
        assert ".xlsx" in content_disp
        
        # Verify file has content
        assert len(response.content) > 0
        
        print(f"✓ P&L Excel export works")
        print(f"  - File size: {len(response.content)} bytes")
    
    def test_export_profit_loss_pdf(self):
        """Test exporting P&L to PDF"""
        response = requests.get(
            f"{BASE_URL}/api/erp/reports/profit-loss/export",
            params={"start_date": "2026-01-01", "end_date": "2026-12-31", "format": "pdf"}
        )
        assert response.status_code == 200
        
        # Verify content type
        content_type = response.headers.get("content-type", "")
        assert "pdf" in content_type or "octet-stream" in content_type
        
        # Verify content disposition
        content_disp = response.headers.get("content-disposition", "")
        assert "attachment" in content_disp
        assert ".pdf" in content_disp
        
        # Verify file has content
        assert len(response.content) > 0
        
        print(f"✓ P&L PDF export works")
        print(f"  - File size: {len(response.content)} bytes")


class TestExportLedger:
    """Ledger Export Tests"""
    
    def test_export_ledger_excel(self):
        """Test exporting ledger to Excel"""
        response = requests.get(
            f"{BASE_URL}/api/erp/reports/ledger/export",
            params={"start_date": "2026-01-01", "end_date": "2026-12-31", "format": "excel"}
        )
        assert response.status_code == 200
        
        # Verify content type
        content_type = response.headers.get("content-type", "")
        assert "spreadsheet" in content_type or "excel" in content_type or "octet-stream" in content_type
        
        # Verify file has content
        assert len(response.content) > 0
        
        print(f"✓ Ledger Excel export works")
        print(f"  - File size: {len(response.content)} bytes")


class TestExportPayments:
    """Payments Export Tests"""
    
    def test_export_payments_excel(self):
        """Test exporting payments to Excel"""
        response = requests.get(
            f"{BASE_URL}/api/erp/reports/payments/export",
            params={"start_date": "2026-01-01", "end_date": "2026-12-31", "format": "excel"}
        )
        assert response.status_code == 200
        
        # Verify content type
        content_type = response.headers.get("content-type", "")
        assert "spreadsheet" in content_type or "excel" in content_type or "octet-stream" in content_type
        
        # Verify file has content
        assert len(response.content) > 0
        
        print(f"✓ Payments Excel export works")
        print(f"  - File size: {len(response.content)} bytes")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
