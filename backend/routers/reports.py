"""
Reports Router - Export reports to Excel and PDF
"""
from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from typing import Dict, Any
from datetime import datetime, timezone
import io
from .base import get_erp_user, get_db
from .accounts import get_profit_loss

reports_router = APIRouter(prefix="/reports", tags=["Reports"])


@reports_router.get("/invoices/export")
async def export_invoices(
    start_date: str,
    end_date: str,
    format: str = "excel",  # excel or pdf
    current_user: dict = Depends(get_erp_user)
):
    """Export invoices to Excel or PDF"""
    db = get_db()
    invoices = await db.invoices.find({
        "created_at": {"$gte": start_date, "$lte": end_date + "T23:59:59"}
    }, {"_id": 0}).sort("created_at", -1).to_list(5000)
    
    if format == "excel":
        import xlsxwriter
        
        output = io.BytesIO()
        workbook = xlsxwriter.Workbook(output, {'in_memory': True})
        worksheet = workbook.add_worksheet("Invoices")
        
        # Formats
        header_format = workbook.add_format({'bold': True, 'bg_color': '#0d9488', 'font_color': 'white', 'border': 1})
        money_format = workbook.add_format({'num_format': '₹#,##0.00', 'border': 1})
        cell_format = workbook.add_format({'border': 1})
        
        # Headers
        headers = ['Invoice No', 'Date', 'Customer', 'GST No', 'Subtotal', 'CGST', 'SGST', 'IGST', 'Total', 'Paid', 'Balance', 'Status']
        for col, header in enumerate(headers):
            worksheet.write(0, col, header, header_format)
            worksheet.set_column(col, col, 15)
        
        # Data
        for row, inv in enumerate(invoices, 1):
            balance = inv.get('total', 0) - inv.get('amount_paid', 0)
            worksheet.write(row, 0, inv.get('invoice_number', ''), cell_format)
            worksheet.write(row, 1, inv.get('created_at', '')[:10], cell_format)
            worksheet.write(row, 2, inv.get('customer_name', ''), cell_format)
            worksheet.write(row, 3, inv.get('customer_gst', ''), cell_format)
            worksheet.write(row, 4, inv.get('subtotal', 0), money_format)
            worksheet.write(row, 5, inv.get('cgst', 0), money_format)
            worksheet.write(row, 6, inv.get('sgst', 0), money_format)
            worksheet.write(row, 7, inv.get('igst', 0), money_format)
            worksheet.write(row, 8, inv.get('total', 0), money_format)
            worksheet.write(row, 9, inv.get('amount_paid', 0), money_format)
            worksheet.write(row, 10, balance, money_format)
            worksheet.write(row, 11, inv.get('payment_status', '').upper(), cell_format)
        
        # Summary row
        summary_row = len(invoices) + 2
        worksheet.write(summary_row, 3, 'TOTAL:', header_format)
        worksheet.write(summary_row, 4, sum(i.get('subtotal', 0) for i in invoices), money_format)
        worksheet.write(summary_row, 5, sum(i.get('cgst', 0) for i in invoices), money_format)
        worksheet.write(summary_row, 6, sum(i.get('sgst', 0) for i in invoices), money_format)
        worksheet.write(summary_row, 7, sum(i.get('igst', 0) for i in invoices), money_format)
        worksheet.write(summary_row, 8, sum(i.get('total', 0) for i in invoices), money_format)
        worksheet.write(summary_row, 9, sum(i.get('amount_paid', 0) for i in invoices), money_format)
        
        workbook.close()
        output.seek(0)
        
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename=invoices_{start_date}_to_{end_date}.xlsx"}
        )
    
    elif format == "pdf":
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        
        output = io.BytesIO()
        doc = SimpleDocTemplate(output, pagesize=landscape(A4), topMargin=30, bottomMargin=30)
        elements = []
        styles = getSampleStyleSheet()
        
        # Title
        title_style = ParagraphStyle('Title', parent=styles['Heading1'], fontSize=18, textColor=colors.HexColor('#0d9488'))
        elements.append(Paragraph(f"Invoice Report: {start_date} to {end_date}", title_style))
        elements.append(Spacer(1, 20))
        
        # Table data
        data = [['Invoice No', 'Date', 'Customer', 'Subtotal', 'GST', 'Total', 'Paid', 'Status']]
        for inv in invoices:
            data.append([
                inv.get('invoice_number', ''),
                inv.get('created_at', '')[:10],
                inv.get('customer_name', '')[:30],
                f"₹{inv.get('subtotal', 0):,.2f}",
                f"₹{inv.get('total_tax', 0):,.2f}",
                f"₹{inv.get('total', 0):,.2f}",
                f"₹{inv.get('amount_paid', 0):,.2f}",
                inv.get('payment_status', '').upper()
            ])
        
        # Summary
        data.append(['', '', 'TOTAL',
            f"₹{sum(i.get('subtotal', 0) for i in invoices):,.2f}",
            f"₹{sum(i.get('total_tax', 0) for i in invoices):,.2f}",
            f"₹{sum(i.get('total', 0) for i in invoices):,.2f}",
            f"₹{sum(i.get('amount_paid', 0) for i in invoices):,.2f}",
            ''
        ])
        
        table = Table(data, repeatRows=1)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0d9488')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
            ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor('#e6f7f5')),
            ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('ALIGN', (3, 0), (-1, -1), 'RIGHT'),
        ]))
        elements.append(table)
        
        doc.build(elements)
        output.seek(0)
        
        return StreamingResponse(
            output,
            media_type="application/pdf",
            headers={"Content-Disposition": f"attachment; filename=invoices_{start_date}_to_{end_date}.pdf"}
        )


@reports_router.get("/profit-loss/export")
async def export_profit_loss(
    start_date: str,
    end_date: str,
    format: str = "excel",
    current_user: dict = Depends(get_erp_user)
):
    """Export Profit & Loss to Excel or PDF"""
    # Get P&L data
    pl_data = await get_profit_loss(start_date, end_date, current_user)
    
    if format == "excel":
        import xlsxwriter
        
        output = io.BytesIO()
        workbook = xlsxwriter.Workbook(output, {'in_memory': True})
        worksheet = workbook.add_worksheet("Profit & Loss")
        
        # Formats
        title_format = workbook.add_format({'bold': True, 'font_size': 16, 'font_color': '#0d9488'})
        header_format = workbook.add_format({'bold': True, 'bg_color': '#0d9488', 'font_color': 'white'})
        money_format = workbook.add_format({'num_format': '₹#,##0.00', 'align': 'right'})
        bold_money = workbook.add_format({'num_format': '₹#,##0.00', 'bold': True, 'align': 'right'})
        label_format = workbook.add_format({'bold': True})
        profit_format = workbook.add_format({'num_format': '₹#,##0.00', 'bold': True, 'font_color': 'green', 'align': 'right'})
        loss_format = workbook.add_format({'num_format': '₹#,##0.00', 'bold': True, 'font_color': 'red', 'align': 'right'})
        
        worksheet.set_column(0, 0, 30)
        worksheet.set_column(1, 1, 20)
        
        # Title
        worksheet.write(0, 0, f"Profit & Loss Statement", title_format)
        worksheet.write(1, 0, f"Period: {start_date} to {end_date}")
        
        row = 3
        # Revenue Section
        worksheet.write(row, 0, "REVENUE", header_format)
        worksheet.write(row, 1, "", header_format)
        row += 1
        worksheet.write(row, 0, "Total Sales")
        worksheet.write(row, 1, pl_data['revenue']['total_sales'], money_format)
        row += 1
        worksheet.write(row, 0, f"  ({pl_data['revenue']['invoice_count']} invoices)")
        row += 2
        
        # Cost of Goods
        worksheet.write(row, 0, "COST OF GOODS SOLD", header_format)
        worksheet.write(row, 1, "", header_format)
        row += 1
        worksheet.write(row, 0, "Total Purchases")
        worksheet.write(row, 1, pl_data['cost_of_goods']['total_purchases'], money_format)
        row += 2
        
        # Gross Profit
        worksheet.write(row, 0, "GROSS PROFIT", label_format)
        worksheet.write(row, 1, pl_data['gross_profit'], bold_money)
        row += 2
        
        # Operating Expenses
        worksheet.write(row, 0, "OPERATING EXPENSES", header_format)
        worksheet.write(row, 1, "", header_format)
        row += 1
        worksheet.write(row, 0, "Breakage/Wastage Loss")
        worksheet.write(row, 1, pl_data['operating_expenses']['breakage_loss'], money_format)
        row += 1
        worksheet.write(row, 0, "Salaries & Wages")
        worksheet.write(row, 1, pl_data['operating_expenses']['salaries'], money_format)
        row += 1
        worksheet.write(row, 0, "Total Operating Expenses")
        worksheet.write(row, 1, pl_data['operating_expenses']['total'], bold_money)
        row += 2
        
        # Net Profit
        worksheet.write(row, 0, "NET PROFIT / (LOSS)", label_format)
        profit_fmt = profit_format if pl_data['net_profit'] >= 0 else loss_format
        worksheet.write(row, 1, pl_data['net_profit'], profit_fmt)
        row += 1
        worksheet.write(row, 0, f"Profit Margin: {pl_data['profit_margin']}%")
        row += 2
        
        # GST Summary
        worksheet.write(row, 0, "GST SUMMARY", header_format)
        worksheet.write(row, 1, "", header_format)
        row += 1
        worksheet.write(row, 0, "GST Collected (Output)")
        worksheet.write(row, 1, pl_data['gst_summary']['collected'], money_format)
        row += 1
        worksheet.write(row, 0, "GST Paid (Input Credit)")
        worksheet.write(row, 1, pl_data['gst_summary']['paid'], money_format)
        row += 1
        worksheet.write(row, 0, "Net GST Liability")
        worksheet.write(row, 1, pl_data['gst_summary']['net_liability'], bold_money)
        
        workbook.close()
        output.seek(0)
        
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename=profit_loss_{start_date}_to_{end_date}.xlsx"}
        )
    
    elif format == "pdf":
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        
        output = io.BytesIO()
        doc = SimpleDocTemplate(output, pagesize=A4, topMargin=30, bottomMargin=30)
        elements = []
        styles = getSampleStyleSheet()
        
        # Title
        title_style = ParagraphStyle('Title', parent=styles['Heading1'], fontSize=18, textColor=colors.HexColor('#0d9488'))
        elements.append(Paragraph("Profit & Loss Statement", title_style))
        elements.append(Paragraph(f"Period: {start_date} to {end_date}", styles['Normal']))
        elements.append(Spacer(1, 20))
        
        # Build table data
        data = [
            ['REVENUE', ''],
            ['Total Sales', f"₹{pl_data['revenue']['total_sales']:,.2f}"],
            [f"({pl_data['revenue']['invoice_count']} invoices)", ''],
            ['', ''],
            ['COST OF GOODS SOLD', ''],
            ['Total Purchases', f"₹{pl_data['cost_of_goods']['total_purchases']:,.2f}"],
            ['', ''],
            ['GROSS PROFIT', f"₹{pl_data['gross_profit']:,.2f}"],
            ['', ''],
            ['OPERATING EXPENSES', ''],
            ['Breakage/Wastage Loss', f"₹{pl_data['operating_expenses']['breakage_loss']:,.2f}"],
            ['Salaries & Wages', f"₹{pl_data['operating_expenses']['salaries']:,.2f}"],
            ['Total Operating Expenses', f"₹{pl_data['operating_expenses']['total']:,.2f}"],
            ['', ''],
            ['NET PROFIT / (LOSS)', f"₹{pl_data['net_profit']:,.2f}"],
            [f"Profit Margin: {pl_data['profit_margin']}%", ''],
            ['', ''],
            ['GST SUMMARY', ''],
            ['GST Collected', f"₹{pl_data['gst_summary']['collected']:,.2f}"],
            ['GST Paid (Input)', f"₹{pl_data['gst_summary']['paid']:,.2f}"],
            ['Net GST Liability', f"₹{pl_data['gst_summary']['net_liability']:,.2f}"],
        ]
        
        table = Table(data, colWidths=[300, 150])
        table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (0, 0), 'Helvetica-Bold'),
            ('FONTNAME', (0, 4), (0, 4), 'Helvetica-Bold'),
            ('FONTNAME', (0, 7), (-1, 7), 'Helvetica-Bold'),
            ('FONTNAME', (0, 9), (0, 9), 'Helvetica-Bold'),
            ('FONTNAME', (0, 14), (-1, 14), 'Helvetica-Bold'),
            ('FONTNAME', (0, 17), (0, 17), 'Helvetica-Bold'),
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#e6f7f5')),
            ('BACKGROUND', (0, 4), (-1, 4), colors.HexColor('#e6f7f5')),
            ('BACKGROUND', (0, 9), (-1, 9), colors.HexColor('#e6f7f5')),
            ('BACKGROUND', (0, 17), (-1, 17), colors.HexColor('#e6f7f5')),
            ('TEXTCOLOR', (1, 14), (1, 14), colors.green if pl_data['net_profit'] >= 0 else colors.red),
            ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
            ('LINEBELOW', (0, 7), (-1, 7), 1, colors.black),
            ('LINEBELOW', (0, 14), (-1, 14), 2, colors.black),
        ]))
        elements.append(table)
        
        doc.build(elements)
        output.seek(0)
        
        return StreamingResponse(
            output,
            media_type="application/pdf",
            headers={"Content-Disposition": f"attachment; filename=profit_loss_{start_date}_to_{end_date}.pdf"}
        )


@reports_router.get("/ledger/export")
async def export_ledger(
    start_date: str,
    end_date: str,
    format: str = "excel",
    current_user: dict = Depends(get_erp_user)
):
    """Export ledger entries to Excel"""
    db = get_db()
    entries = await db.ledger.find({
        "date": {"$gte": start_date, "$lte": end_date}
    }, {"_id": 0}).sort("date", 1).to_list(10000)
    
    if format == "excel":
        import xlsxwriter
        
        output = io.BytesIO()
        workbook = xlsxwriter.Workbook(output, {'in_memory': True})
        worksheet = workbook.add_worksheet("Ledger")
        
        header_format = workbook.add_format({'bold': True, 'bg_color': '#0d9488', 'font_color': 'white', 'border': 1})
        money_format = workbook.add_format({'num_format': '₹#,##0.00', 'border': 1})
        cell_format = workbook.add_format({'border': 1})
        
        headers = ['Date', 'Type', 'Reference', 'Description', 'Debit', 'Credit', 'Account']
        for col, header in enumerate(headers):
            worksheet.write(0, col, header, header_format)
            worksheet.set_column(col, col, 18 if col == 3 else 12)
        
        running_balance = 0
        for row, entry in enumerate(entries, 1):
            running_balance += entry.get('debit', 0) - entry.get('credit', 0)
            worksheet.write(row, 0, entry.get('date', ''), cell_format)
            worksheet.write(row, 1, entry.get('type', '').upper(), cell_format)
            worksheet.write(row, 2, entry.get('reference', ''), cell_format)
            worksheet.write(row, 3, entry.get('description', ''), cell_format)
            worksheet.write(row, 4, entry.get('debit', 0), money_format)
            worksheet.write(row, 5, entry.get('credit', 0), money_format)
            worksheet.write(row, 6, entry.get('account', ''), cell_format)
        
        # Totals
        total_row = len(entries) + 2
        worksheet.write(total_row, 3, 'TOTAL:', header_format)
        worksheet.write(total_row, 4, sum(e.get('debit', 0) for e in entries), money_format)
        worksheet.write(total_row, 5, sum(e.get('credit', 0) for e in entries), money_format)
        
        workbook.close()
        output.seek(0)
        
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename=ledger_{start_date}_to_{end_date}.xlsx"}
        )


@reports_router.get("/payments/export")
async def export_payments(
    start_date: str,
    end_date: str,
    format: str = "excel",
    current_user: dict = Depends(get_erp_user)
):
    """Export payments to Excel"""
    db = get_db()
    payments = await db.payments.find({
        "created_at": {"$gte": start_date, "$lte": end_date + "T23:59:59"}
    }, {"_id": 0}).sort("created_at", -1).to_list(5000)
    
    if format == "excel":
        import xlsxwriter
        
        output = io.BytesIO()
        workbook = xlsxwriter.Workbook(output, {'in_memory': True})
        worksheet = workbook.add_worksheet("Payments")
        
        header_format = workbook.add_format({'bold': True, 'bg_color': '#0d9488', 'font_color': 'white', 'border': 1})
        money_format = workbook.add_format({'num_format': '₹#,##0.00', 'border': 1})
        cell_format = workbook.add_format({'border': 1})
        
        headers = ['Date', 'Invoice No', 'Amount', 'Method', 'Reference', 'Notes']
        for col, header in enumerate(headers):
            worksheet.write(0, col, header, header_format)
            worksheet.set_column(col, col, 15)
        
        for row, pmt in enumerate(payments, 1):
            worksheet.write(row, 0, pmt.get('created_at', '')[:10], cell_format)
            worksheet.write(row, 1, pmt.get('invoice_number', ''), cell_format)
            worksheet.write(row, 2, pmt.get('amount', 0), money_format)
            worksheet.write(row, 3, pmt.get('payment_method', '').upper(), cell_format)
            worksheet.write(row, 4, pmt.get('reference', ''), cell_format)
            worksheet.write(row, 5, pmt.get('notes', ''), cell_format)
        
        # Total
        total_row = len(payments) + 2
        worksheet.write(total_row, 1, 'TOTAL:', header_format)
        worksheet.write(total_row, 2, sum(p.get('amount', 0) for p in payments), money_format)
        
        workbook.close()
        output.seek(0)
        
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename=payments_{start_date}_to_{end_date}.xlsx"}
        )


@reports_router.get("/bulk-export")
async def bulk_export_all_data(
    start_date: str,
    end_date: str,
    include_invoices: bool = True,
    include_orders: bool = True,
    include_job_work: bool = True,
    include_payments: bool = True,
    include_vendors: bool = False,
    token: str = None,
    current_user: dict = Depends(get_erp_user)
):
    """Bulk export all business data to Excel with multiple sheets"""
    import xlsxwriter
    
    db = get_db()
    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {'in_memory': True})
    
    # Common formats
    header_format = workbook.add_format({'bold': True, 'bg_color': '#0d9488', 'font_color': 'white', 'border': 1, 'align': 'center'})
    money_format = workbook.add_format({'num_format': '₹#,##0.00', 'border': 1, 'align': 'right'})
    cell_format = workbook.add_format({'border': 1})
    date_format = workbook.add_format({'border': 1, 'num_format': 'dd-mm-yyyy'})
    bold_format = workbook.add_format({'bold': True, 'border': 1})
    total_format = workbook.add_format({'bold': True, 'bg_color': '#e6f7f5', 'border': 1, 'num_format': '₹#,##0.00'})
    
    date_query = {"created_at": {"$gte": start_date, "$lte": end_date + "T23:59:59"}}
    
    # Sheet 1: Invoices
    if include_invoices:
        invoices = await db.invoices.find(date_query, {"_id": 0}).sort("created_at", -1).to_list(10000)
        ws = workbook.add_worksheet("Invoices")
        
        headers = ['Invoice No', 'Date', 'Customer Name', 'Company', 'GSTIN', 'Phone', 'Subtotal', 'CGST', 'SGST', 'IGST', 'Total', 'Paid', 'Balance', 'Status']
        for col, h in enumerate(headers):
            ws.write(0, col, h, header_format)
            ws.set_column(col, col, 14)
        
        for row, inv in enumerate(invoices, 1):
            balance = inv.get('total', 0) - inv.get('amount_paid', 0)
            ws.write(row, 0, inv.get('invoice_number', ''), cell_format)
            ws.write(row, 1, inv.get('created_at', '')[:10], cell_format)
            ws.write(row, 2, inv.get('customer_name', ''), cell_format)
            ws.write(row, 3, inv.get('company_name', ''), cell_format)
            ws.write(row, 4, inv.get('customer_gst', ''), cell_format)
            ws.write(row, 5, inv.get('customer_phone', ''), cell_format)
            ws.write(row, 6, inv.get('subtotal', 0), money_format)
            ws.write(row, 7, inv.get('cgst', 0), money_format)
            ws.write(row, 8, inv.get('sgst', 0), money_format)
            ws.write(row, 9, inv.get('igst', 0), money_format)
            ws.write(row, 10, inv.get('total', 0), money_format)
            ws.write(row, 11, inv.get('amount_paid', 0), money_format)
            ws.write(row, 12, balance, money_format)
            ws.write(row, 13, inv.get('payment_status', '').upper(), cell_format)
        
        # Summary
        total_row = len(invoices) + 2
        ws.write(total_row, 5, 'TOTAL:', bold_format)
        ws.write(total_row, 6, sum(i.get('subtotal', 0) for i in invoices), total_format)
        ws.write(total_row, 7, sum(i.get('cgst', 0) for i in invoices), total_format)
        ws.write(total_row, 8, sum(i.get('sgst', 0) for i in invoices), total_format)
        ws.write(total_row, 9, sum(i.get('igst', 0) for i in invoices), total_format)
        ws.write(total_row, 10, sum(i.get('total', 0) for i in invoices), total_format)
        ws.write(total_row, 11, sum(i.get('amount_paid', 0) for i in invoices), total_format)
    
    # Sheet 2: Regular Orders
    if include_orders:
        orders = await db.orders.find(date_query, {"_id": 0}).sort("created_at", -1).to_list(10000)
        ws = workbook.add_worksheet("Regular Orders")
        
        headers = ['Order No', 'Date', 'Customer', 'Company', 'Phone', 'Items', 'Subtotal', 'Tax', 'Total', 'Advance', 'Balance', 'Status', 'Dispatch']
        for col, h in enumerate(headers):
            ws.write(0, col, h, header_format)
            ws.set_column(col, col, 14)
        
        for row, order in enumerate(orders, 1):
            total = order.get('total_price', 0)
            advance = order.get('advance_amount', 0)
            ws.write(row, 0, order.get('order_number', ''), cell_format)
            ws.write(row, 1, order.get('created_at', '')[:10], cell_format)
            ws.write(row, 2, order.get('customer_name', ''), cell_format)
            ws.write(row, 3, order.get('company_name', ''), cell_format)
            ws.write(row, 4, order.get('customer_phone', ''), cell_format)
            ws.write(row, 5, len(order.get('glass_items', [])), cell_format)
            ws.write(row, 6, order.get('subtotal', 0), money_format)
            ws.write(row, 7, order.get('total_tax', 0), money_format)
            ws.write(row, 8, total, money_format)
            ws.write(row, 9, advance, money_format)
            ws.write(row, 10, total - advance, money_format)
            ws.write(row, 11, order.get('payment_status', '').upper(), cell_format)
            ws.write(row, 12, order.get('status', '').upper(), cell_format)
        
        # Summary
        total_row = len(orders) + 2
        ws.write(total_row, 4, 'TOTAL:', bold_format)
        ws.write(total_row, 8, sum(o.get('total_price', 0) for o in orders), total_format)
        ws.write(total_row, 9, sum(o.get('advance_amount', 0) for o in orders), total_format)
    
    # Sheet 3: Job Work Orders
    if include_job_work:
        jw_orders = await db.job_work_orders.find(date_query, {"_id": 0}).sort("created_at", -1).to_list(10000)
        ws = workbook.add_worksheet("Job Work Orders")
        
        headers = ['JW No', 'Date', 'Customer', 'Company', 'Phone', 'Items', 'Total', 'Advance', 'Balance', 'Payment', 'Status']
        for col, h in enumerate(headers):
            ws.write(0, col, h, header_format)
            ws.set_column(col, col, 14)
        
        for row, order in enumerate(jw_orders, 1):
            total = order.get('summary', {}).get('grand_total', 0)
            advance = order.get('advance_paid', 0)
            ws.write(row, 0, order.get('job_work_number', ''), cell_format)
            ws.write(row, 1, order.get('created_at', '')[:10], cell_format)
            ws.write(row, 2, order.get('customer_name', ''), cell_format)
            ws.write(row, 3, order.get('company_name', ''), cell_format)
            ws.write(row, 4, order.get('phone', ''), cell_format)
            ws.write(row, 5, len(order.get('items', [])), cell_format)
            ws.write(row, 6, total, money_format)
            ws.write(row, 7, advance, money_format)
            ws.write(row, 8, total - advance, money_format)
            ws.write(row, 9, order.get('payment_status', '').upper(), cell_format)
            ws.write(row, 10, order.get('status', '').upper(), cell_format)
        
        # Summary
        total_row = len(jw_orders) + 2
        ws.write(total_row, 4, 'TOTAL:', bold_format)
        ws.write(total_row, 6, sum(o.get('summary', {}).get('grand_total', 0) for o in jw_orders), total_format)
        ws.write(total_row, 7, sum(o.get('advance_paid', 0) for o in jw_orders), total_format)
    
    # Sheet 4: Payments
    if include_payments:
        payments = await db.payments.find(date_query, {"_id": 0}).sort("created_at", -1).to_list(10000)
        ws = workbook.add_worksheet("Payments Received")
        
        headers = ['Date', 'Invoice No', 'Order No', 'Customer', 'Amount', 'Method', 'Reference', 'Notes']
        for col, h in enumerate(headers):
            ws.write(0, col, h, header_format)
            ws.set_column(col, col, 14)
        
        for row, pmt in enumerate(payments, 1):
            ws.write(row, 0, pmt.get('created_at', '')[:10], cell_format)
            ws.write(row, 1, pmt.get('invoice_number', ''), cell_format)
            ws.write(row, 2, pmt.get('order_number', ''), cell_format)
            ws.write(row, 3, pmt.get('customer_name', ''), cell_format)
            ws.write(row, 4, pmt.get('amount', 0), money_format)
            ws.write(row, 5, pmt.get('payment_method', '').upper(), cell_format)
            ws.write(row, 6, pmt.get('reference', ''), cell_format)
            ws.write(row, 7, pmt.get('notes', ''), cell_format)
        
        # Summary
        total_row = len(payments) + 2
        ws.write(total_row, 3, 'TOTAL:', bold_format)
        ws.write(total_row, 4, sum(p.get('amount', 0) for p in payments), total_format)
    
    # Sheet 5: Vendors (if requested)
    if include_vendors:
        vendor_payments = await db.vendor_payments.find({
            "status": "completed",
            "completed_at": {"$gte": start_date, "$lte": end_date + "T23:59:59"}
        }, {"_id": 0}).sort("completed_at", -1).to_list(10000)
        ws = workbook.add_worksheet("Vendor Payments")
        
        headers = ['Date', 'Receipt No', 'PO No', 'Vendor', 'Amount', 'Mode', 'UTR/Ref']
        for col, h in enumerate(headers):
            ws.write(0, col, h, header_format)
            ws.set_column(col, col, 16)
        
        for row, pmt in enumerate(vendor_payments, 1):
            ws.write(row, 0, pmt.get('completed_at', '')[:10], cell_format)
            ws.write(row, 1, pmt.get('receipt_number', ''), cell_format)
            ws.write(row, 2, pmt.get('po_number', ''), cell_format)
            ws.write(row, 3, pmt.get('vendor_name', ''), cell_format)
            ws.write(row, 4, pmt.get('amount', 0), money_format)
            ws.write(row, 5, pmt.get('payment_mode', '').upper(), cell_format)
            ws.write(row, 6, pmt.get('transaction_ref', ''), cell_format)
        
        # Summary
        total_row = len(vendor_payments) + 2
        ws.write(total_row, 3, 'TOTAL:', bold_format)
        ws.write(total_row, 4, sum(p.get('amount', 0) for p in vendor_payments), total_format)
    
    # Summary Sheet
    ws = workbook.add_worksheet("Summary")
    ws.set_column(0, 0, 30)
    ws.set_column(1, 1, 20)
    
    title_format = workbook.add_format({'bold': True, 'font_size': 16, 'font_color': '#0d9488'})
    ws.write(0, 0, "Bulk Export Summary", title_format)
    ws.write(1, 0, f"Period: {start_date} to {end_date}")
    ws.write(2, 0, f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    row = 4
    ws.write(row, 0, "Data Type", header_format)
    ws.write(row, 1, "Count", header_format)
    ws.write(row, 2, "Total Value", header_format)
    
    if include_invoices:
        invoices = await db.invoices.find(date_query, {"_id": 0}).to_list(10000)
        row += 1
        ws.write(row, 0, "Invoices", cell_format)
        ws.write(row, 1, len(invoices), cell_format)
        ws.write(row, 2, sum(i.get('total', 0) for i in invoices), money_format)
    
    if include_orders:
        orders = await db.orders.find(date_query, {"_id": 0}).to_list(10000)
        row += 1
        ws.write(row, 0, "Regular Orders", cell_format)
        ws.write(row, 1, len(orders), cell_format)
        ws.write(row, 2, sum(o.get('total_price', 0) for o in orders), money_format)
    
    if include_job_work:
        jw_orders = await db.job_work_orders.find(date_query, {"_id": 0}).to_list(10000)
        row += 1
        ws.write(row, 0, "Job Work Orders", cell_format)
        ws.write(row, 1, len(jw_orders), cell_format)
        ws.write(row, 2, sum(o.get('summary', {}).get('grand_total', 0) for o in jw_orders), money_format)
    
    if include_payments:
        payments = await db.payments.find(date_query, {"_id": 0}).to_list(10000)
        row += 1
        ws.write(row, 0, "Payments Received", cell_format)
        ws.write(row, 1, len(payments), cell_format)
        ws.write(row, 2, sum(p.get('amount', 0) for p in payments), money_format)
    
    workbook.close()
    output.seek(0)
    
    filename = f"bulk_export_{start_date}_to_{end_date}.xlsx"
    
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )
