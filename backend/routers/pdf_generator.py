"""
PDF Generation Router
Generates: Dispatch Slips, Cash Day Book, Invoices with QR codes for tracking
"""
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from datetime import datetime, timezone, timedelta
from .base import get_erp_user, get_db
from io import BytesIO
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch, mm
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.pdfgen import canvas
from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT
import qrcode
import os
import urllib.request

pdf_router = APIRouter(prefix="/pdf", tags=["PDF Generation"])

# Base URL for order tracking (will be replaced with actual domain)
BASE_URL = os.environ.get("FRONTEND_URL", "https://glassmesh.preview.emergentagent.com")

# Company Logo URL
COMPANY_LOGO_URL = "https://customer-assets.emergentagent.com/job_0aec802e-e67b-4582-8fac-1517907b7262/artifacts/752tez4i_Logo%20Cucumaa%20Glass.png"


def get_company_logo(width: int = 80, height: int = 40) -> Image:
    """Download and return company logo as reportlab Image"""
    try:
        logo_buffer = BytesIO()
        with urllib.request.urlopen(COMPANY_LOGO_URL, timeout=5) as response:
            logo_buffer.write(response.read())
        logo_buffer.seek(0)
        return Image(logo_buffer, width=width, height=height)
    except Exception as e:
        print(f"Failed to load logo: {e}")
        return None


def generate_qr_code(data: str, size: int = 100) -> Image:
    """Generate a QR code image for embedding in PDF"""
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_M,
        box_size=10,
        border=2,
    )
    qr.add_data(data)
    qr.make(fit=True)
    
    img = qr.make_image(fill_color="black", back_color="white")
    
    # Convert to bytes
    img_buffer = BytesIO()
    img.save(img_buffer, format='PNG')
    img_buffer.seek(0)
    
    # Create reportlab Image
    return Image(img_buffer, width=size, height=size)


# ================== DISPATCH SLIP PDF ==================

@pdf_router.get("/dispatch-slip/{order_id}")
async def generate_dispatch_slip(
    order_id: str,
    current_user: dict = Depends(get_erp_user)
):
    """Generate Dispatch Slip PDF for an order"""
    if current_user.get("role") not in ["super_admin", "admin", "owner", "hr", "operator", "finance", "manager", "supervisor"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    # Fetch order
    order = await db.orders.find_one({"id": order_id}, {"_id": 0})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    # Check payment is fully settled before generating dispatch slip
    payment_settled = (
        order.get('payment_status') == 'completed' or
        (order.get('advance_percent') == 100 and order.get('advance_payment_status') == 'paid') or
        (order.get('advance_payment_status') == 'paid' and order.get('remaining_payment_status') in ['paid', 'cash_received'])
    )
    if not payment_settled:
        raise HTTPException(status_code=400, detail="Cannot generate dispatch slip. Payment not fully settled.")
    
    # Generate PDF
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30)
    
    elements = []
    styles = getSampleStyleSheet()
    
    # Custom styles
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#0d9488'),
        spaceAfter=10,
        alignment=TA_CENTER
    )
    
    subtitle_style = ParagraphStyle(
        'Subtitle',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.grey,
        alignment=TA_CENTER,
        spaceAfter=20
    )
    
    heading_style = ParagraphStyle(
        'Heading',
        parent=styles['Heading2'],
        fontSize=12,
        textColor=colors.HexColor('#1e293b'),
        spaceBefore=15,
        spaceAfter=8
    )
    
    normal_style = ParagraphStyle(
        'CustomNormal',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.HexColor('#334155')
    )
    
    # Header with Logo
    logo_img = get_company_logo(width=100, height=50)
    if logo_img:
        elements.append(logo_img)
        elements.append(Spacer(1, 5))
    
    elements.append(Paragraph("LUCUMAA GLASS", title_style))
    elements.append(Paragraph("Premium Glass Manufacturing", subtitle_style))
    
    # Dispatch Slip Title
    slip_title = ParagraphStyle(
        'SlipTitle',
        parent=styles['Heading1'],
        fontSize=16,
        textColor=colors.white,
        alignment=TA_CENTER,
        backColor=colors.HexColor('#0d9488'),
        borderPadding=10
    )
    elements.append(Spacer(1, 10))
    elements.append(Paragraph("📦 DISPATCH SLIP", slip_title))
    elements.append(Spacer(1, 20))
    
    # Order Info Table
    order_number = order.get('order_number', order_id[:8]).upper()
    dispatch_slip_number = order.get('dispatch_slip_number', f"DS-{datetime.now().strftime('%Y%m%d')}-{order_number}")
    created_at = order.get('created_at', '')[:10] if order.get('created_at') else 'N/A'
    
    # Generate QR code for order tracking
    tracking_url = f"{BASE_URL}/track-order?order={order_number}"
    qr_image = generate_qr_code(tracking_url, size=80)
    
    # QR Code section with tracking info
    qr_text = ParagraphStyle(
        'QRText',
        parent=styles['Normal'],
        fontSize=8,
        textColor=colors.HexColor('#64748b'),
        alignment=TA_CENTER
    )
    
    qr_content = Table([
        [qr_image],
        [Paragraph("Scan to Track Order", qr_text)]
    ], colWidths=[90])
    qr_content.setStyle(TableStyle([
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
    ]))
    
    order_info = [
        ['Dispatch Slip #', dispatch_slip_number, 'Order #', order_number],
        ['Order Date', created_at, 'Dispatch Date', datetime.now().strftime('%Y-%m-%d')],
    ]
    
    order_table = Table(order_info, colWidths=[100, 150, 100, 150])
    order_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#f1f5f9')),
        ('BACKGROUND', (2, 0), (2, -1), colors.HexColor('#f1f5f9')),
        ('TEXTCOLOR', (0, 0), (-1, -1), colors.HexColor('#1e293b')),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTNAME', (2, 0), (2, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#e2e8f0')),
    ]))
    
    # Combine order info and QR code side by side
    header_table = Table([[order_table, qr_content]], colWidths=[420, 100])
    header_table.setStyle(TableStyle([
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ]))
    elements.append(header_table)
    elements.append(Spacer(1, 20))
    
    # Customer Details
    elements.append(Paragraph("👤 CUSTOMER DETAILS", heading_style))
    
    customer_name = order.get('customer_name', order.get('user_name', 'N/A'))
    company_name = order.get('company_name', 'N/A')
    address = order.get('delivery_address', order.get('address', 'N/A'))
    phone = order.get('customer_phone', order.get('phone', 'N/A'))
    
    customer_info = [
        ['Customer Name', customer_name],
        ['Company', company_name],
        ['Phone', phone],
        ['Delivery Address', address],
    ]
    
    customer_table = Table(customer_info, colWidths=[120, 380])
    customer_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#f1f5f9')),
        ('TEXTCOLOR', (0, 0), (-1, -1), colors.HexColor('#1e293b')),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#e2e8f0')),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ]))
    elements.append(customer_table)
    elements.append(Spacer(1, 20))
    
    # Product Details
    elements.append(Paragraph("📋 ORDER ITEMS", heading_style))
    
    items = order.get('items', [])
    if not items:
        # Single product order
        items = [{
            'name': order.get('product_name', order.get('glass_type', 'Glass Product')),
            'size': order.get('size', 'Standard'),
            'thickness': order.get('thickness', 'N/A'),
            'quantity': order.get('quantity', 1),
            'price': order.get('total_price', 0)
        }]
    
    product_data = [['#', 'Product', 'Size', 'Thickness', 'Qty', 'Amount']]
    for idx, item in enumerate(items, 1):
        product_data.append([
            str(idx),
            item.get('name', item.get('glass_type', 'Product')),
            item.get('size', 'Standard'),
            item.get('thickness', 'N/A'),
            str(item.get('quantity', 1)),
            f"₹{item.get('price', item.get('total_price', 0)):,.2f}"
        ])
    
    product_table = Table(product_data, colWidths=[30, 180, 80, 70, 40, 100])
    product_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0d9488')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#e2e8f0')),
        ('ALIGN', (0, 0), (0, -1), 'CENTER'),
        ('ALIGN', (4, 0), (4, -1), 'CENTER'),
        ('ALIGN', (5, 0), (5, -1), 'RIGHT'),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f8fafc')]),
    ]))
    elements.append(product_table)
    elements.append(Spacer(1, 20))
    
    # Payment & Transport Details
    elements.append(Paragraph("💰 PAYMENT & TRANSPORT", heading_style))
    
    total_price = order.get('total_price', 0)
    transport_charge = order.get('transport_charge', 0)
    grand_total = total_price + transport_charge
    advance_amount = order.get('advance_amount', 0)
    remaining_amount = order.get('remaining_amount', grand_total - advance_amount)
    advance_status = order.get('advance_payment_status', 'pending')
    remaining_status = order.get('remaining_payment_status', 'pending')
    
    payment_data = [
        ['Product Total', f"₹{total_price:,.2f}"],
        ['Transport Charges', f"₹{transport_charge:,.2f}"],
        ['Grand Total', f"₹{grand_total:,.2f}"],
        ['Advance Paid', f"₹{advance_amount:,.2f} ({advance_status.upper()})"],
        ['Balance Due', f"₹{remaining_amount:,.2f} ({remaining_status.upper()})"],
    ]
    
    # Transport info
    vehicle_type = order.get('transport_vehicle_type', 'N/A')
    transport_note = order.get('transport_charge_note', '')
    
    payment_table = Table(payment_data, colWidths=[150, 150])
    payment_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#f1f5f9')),
        ('BACKGROUND', (0, 2), (-1, 2), colors.HexColor('#0d9488')),
        ('TEXTCOLOR', (0, 2), (-1, 2), colors.white),
        ('TEXTCOLOR', (0, 0), (-1, -1), colors.HexColor('#1e293b')),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTNAME', (0, 2), (-1, 2), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#e2e8f0')),
        ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
    ]))
    
    # Side by side layout
    # Get driver and vehicle details from order
    driver_name = order.get('driver_name', 'Not Assigned')
    driver_phone = order.get('driver_phone', 'N/A')
    vehicle_number = order.get('vehicle_number', 'Not Assigned')
    dispatched_at = order.get('dispatched_at', '')
    
    transport_data = [
        ['Driver Name', driver_name],
        ['Driver Phone', driver_phone],
        ['Vehicle Number', vehicle_number],
        ['Vehicle Type', vehicle_type],
        ['Dispatched', dispatched_at[:16].replace('T', ' ') if dispatched_at else 'Pending'],
        ['Order Status', order.get('status', 'pending').upper()],
    ]
    
    transport_table = Table(transport_data, colWidths=[100, 200])
    transport_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#f1f5f9')),
        ('TEXTCOLOR', (0, 0), (-1, -1), colors.HexColor('#1e293b')),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#e2e8f0')),
    ]))
    
    combined_table = Table([[payment_table, transport_table]], colWidths=[310, 310])
    elements.append(combined_table)
    elements.append(Spacer(1, 30))
    
    # Signature Section
    elements.append(Paragraph("✍️ SIGNATURES", heading_style))
    
    signature_data = [
        ['Prepared By', 'Verified By', 'Received By'],
        ['\n\n\n_________________', '\n\n\n_________________', '\n\n\n_________________'],
        [f"Date: {datetime.now().strftime('%Y-%m-%d')}", 'Date: ___________', 'Date: ___________'],
    ]
    
    signature_table = Table(signature_data, colWidths=[170, 170, 170])
    signature_table.setStyle(TableStyle([
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('PADDING', (0, 0), (-1, -1), 10),
        ('LINEBELOW', (0, 0), (-1, 0), 1, colors.HexColor('#0d9488')),
    ]))
    elements.append(signature_table)
    elements.append(Spacer(1, 20))
    
    # Footer
    footer_style = ParagraphStyle(
        'Footer',
        parent=styles['Normal'],
        fontSize=8,
        textColor=colors.grey,
        alignment=TA_CENTER
    )
    elements.append(Paragraph("Thank you for choosing Lucumaa Glass!", footer_style))
    elements.append(Paragraph(f"Generated on {datetime.now().strftime('%Y-%m-%d %H:%M')} | This is a computer generated document", footer_style))
    
    # Build PDF
    doc.build(elements)
    buffer.seek(0)
    
    # Update order with dispatch slip number
    await db.orders.update_one(
        {"id": order_id},
        {"$set": {
            "dispatch_slip_number": dispatch_slip_number,
            "dispatch_slip_generated": True,
            "dispatch_created_at": datetime.now(timezone.utc).isoformat(),
            "dispatch_created_by": current_user.get("name", "Admin")
        }}
    )
    
    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=dispatch_slip_{order_number}.pdf"}
    )


# ================== CASH DAY BOOK PDF ==================

@pdf_router.get("/cash-daybook")
async def generate_cash_daybook(
    date: str = None,
    current_user: dict = Depends(get_erp_user)
):
    """Generate Cash Day Book PDF for a specific date"""
    if current_user.get("role") not in ["super_admin", "admin", "owner", "finance", "accountant", "hr"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    # Use today if no date provided
    if not date:
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    # Fetch transactions for the date
    transactions = await db.cash_transactions.find(
        {"date": date},
        {"_id": 0}
    ).sort("created_at", 1).to_list(500)
    
    # Calculate opening balance (all transactions before this date)
    prev_in = await db.cash_transactions.aggregate([
        {"$match": {"direction": "in", "date": {"$lt": date}}},
        {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
    ]).to_list(1)
    
    prev_out = await db.cash_transactions.aggregate([
        {"$match": {"direction": "out", "date": {"$lt": date}}},
        {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
    ]).to_list(1)
    
    opening_balance = (prev_in[0]["total"] if prev_in else 0) - (prev_out[0]["total"] if prev_out else 0)
    
    # Calculate day's totals
    day_in = sum(t["amount"] for t in transactions if t.get("direction") == "in")
    day_out = sum(t["amount"] for t in transactions if t.get("direction") == "out")
    closing_balance = opening_balance + day_in - day_out
    
    # Generate PDF
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30)
    
    elements = []
    styles = getSampleStyleSheet()
    
    # Custom styles
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#0d9488'),
        spaceAfter=10,
        alignment=TA_CENTER
    )
    
    subtitle_style = ParagraphStyle(
        'Subtitle',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.grey,
        alignment=TA_CENTER,
        spaceAfter=20
    )
    
    heading_style = ParagraphStyle(
        'Heading',
        parent=styles['Heading2'],
        fontSize=12,
        textColor=colors.HexColor('#1e293b'),
        spaceBefore=15,
        spaceAfter=8
    )
    
    # Header with Logo
    logo_img = get_company_logo(width=100, height=50)
    if logo_img:
        elements.append(logo_img)
        elements.append(Spacer(1, 5))
    
    elements.append(Paragraph("LUCUMAA GLASS", title_style))
    elements.append(Paragraph("Premium Glass Manufacturing", subtitle_style))
    
    # Day Book Title
    book_title = ParagraphStyle(
        'BookTitle',
        parent=styles['Heading1'],
        fontSize=16,
        textColor=colors.white,
        alignment=TA_CENTER,
        backColor=colors.HexColor('#7c3aed'),
        borderPadding=10
    )
    elements.append(Spacer(1, 10))
    elements.append(Paragraph("📖 CASH DAY BOOK", book_title))
    elements.append(Spacer(1, 20))
    
    # Date and Summary
    date_display = datetime.strptime(date, "%Y-%m-%d").strftime("%d %B %Y")
    
    summary_data = [
        ['Date', date_display, 'Generated On', datetime.now().strftime('%Y-%m-%d %H:%M')],
    ]
    
    summary_table = Table(summary_data, colWidths=[80, 170, 100, 150])
    summary_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#f1f5f9')),
        ('BACKGROUND', (2, 0), (2, -1), colors.HexColor('#f1f5f9')),
        ('TEXTCOLOR', (0, 0), (-1, -1), colors.HexColor('#1e293b')),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTNAME', (2, 0), (2, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#e2e8f0')),
    ]))
    elements.append(summary_table)
    elements.append(Spacer(1, 20))
    
    # Opening Balance
    elements.append(Paragraph("💰 BALANCE SUMMARY", heading_style))
    
    balance_data = [
        ['Opening Balance', f"₹{opening_balance:,.2f}"],
        ['Cash In (Today)', f"₹{day_in:,.2f}"],
        ['Cash Out (Today)', f"₹{day_out:,.2f}"],
        ['Closing Balance', f"₹{closing_balance:,.2f}"],
    ]
    
    balance_table = Table(balance_data, colWidths=[200, 150])
    balance_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#f1f5f9')),
        ('BACKGROUND', (0, 3), (-1, 3), colors.HexColor('#7c3aed')),
        ('TEXTCOLOR', (0, 3), (-1, 3), colors.white),
        ('TEXTCOLOR', (0, 0), (-1, 2), colors.HexColor('#1e293b')),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 11),
        ('PADDING', (0, 0), (-1, -1), 10),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#e2e8f0')),
        ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
    ]))
    elements.append(balance_table)
    elements.append(Spacer(1, 20))
    
    # Transactions
    elements.append(Paragraph("📋 TRANSACTIONS", heading_style))
    
    if transactions:
        txn_data = [['#', 'Time', 'Type', 'Category', 'Description', 'In', 'Out', 'Balance']]
        running_balance = opening_balance
        
        for idx, txn in enumerate(transactions, 1):
            amount = txn.get('amount', 0)
            direction = txn.get('direction', 'in')
            
            if direction == 'in':
                running_balance += amount
                cash_in = f"₹{amount:,.2f}"
                cash_out = "-"
            else:
                running_balance -= amount
                cash_in = "-"
                cash_out = f"₹{amount:,.2f}"
            
            time_str = txn.get('created_at', '')
            if time_str:
                try:
                    time_str = datetime.fromisoformat(time_str.replace('Z', '+00:00')).strftime('%H:%M')
                except:
                    time_str = 'N/A'
            
            txn_data.append([
                str(idx),
                time_str,
                '↑ IN' if direction == 'in' else '↓ OUT',
                txn.get('transaction_type', 'general').replace('_', ' ').title(),
                txn.get('description', '')[:30],
                cash_in,
                cash_out,
                f"₹{running_balance:,.2f}"
            ])
        
        txn_table = Table(txn_data, colWidths=[25, 45, 45, 80, 130, 65, 65, 75])
        txn_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#7c3aed')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('PADDING', (0, 0), (-1, -1), 6),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#e2e8f0')),
            ('ALIGN', (0, 0), (0, -1), 'CENTER'),
            ('ALIGN', (5, 0), (7, -1), 'RIGHT'),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f8fafc')]),
        ]))
        elements.append(txn_table)
    else:
        elements.append(Paragraph("No transactions recorded for this date.", styles['Normal']))
    
    elements.append(Spacer(1, 30))
    
    # Signature Section
    elements.append(Paragraph("✍️ VERIFICATION", heading_style))
    
    signature_data = [
        ['Prepared By', 'Verified By', 'Approved By'],
        ['\n\n\n_________________', '\n\n\n_________________', '\n\n\n_________________'],
        [current_user.get('name', 'Admin'), 'Accountant', 'Manager'],
    ]
    
    signature_table = Table(signature_data, colWidths=[170, 170, 170])
    signature_table.setStyle(TableStyle([
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('PADDING', (0, 0), (-1, -1), 10),
        ('LINEBELOW', (0, 0), (-1, 0), 1, colors.HexColor('#7c3aed')),
    ]))
    elements.append(signature_table)
    elements.append(Spacer(1, 20))
    
    # Footer
    footer_style = ParagraphStyle(
        'Footer',
        parent=styles['Normal'],
        fontSize=8,
        textColor=colors.grey,
        alignment=TA_CENTER
    )
    elements.append(Paragraph("Lucumaa Glass - Cash Management System", footer_style))
    elements.append(Paragraph(f"Generated on {datetime.now().strftime('%Y-%m-%d %H:%M')} | This is a computer generated document", footer_style))
    
    # Build PDF
    doc.build(elements)
    buffer.seek(0)
    
    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=cash_daybook_{date}.pdf"}
    )


# ================== INVOICE PDF ==================

@pdf_router.get("/invoice/{order_id}")
async def generate_invoice(
    order_id: str,
    token: str = None,
    current_user: dict = Depends(get_erp_user)
):
    """Generate Invoice PDF for an order"""
    # Allow both ERP users and regular customers to download their invoices
    db = get_db()
    
    # Fetch order
    order = await db.orders.find_one({"id": order_id}, {"_id": 0})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    # Fetch customer profile for additional details
    user_id = order.get("user_id")
    customer_profile = None
    if user_id:
        customer_profile = await db.users.find_one({"id": user_id}, {"_id": 0, "password": 0})
    
    # Generate PDF (similar structure to dispatch slip but formatted as invoice)
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30)
    
    elements = []
    styles = getSampleStyleSheet()
    
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#0d9488'),
        spaceAfter=10,
        alignment=TA_CENTER
    )
    
    subtitle_style = ParagraphStyle(
        'Subtitle',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.grey,
        alignment=TA_CENTER,
        spaceAfter=20
    )
    
    heading_style = ParagraphStyle(
        'Heading',
        parent=styles['Heading2'],
        fontSize=12,
        textColor=colors.HexColor('#1e293b'),
        spaceBefore=15,
        spaceAfter=8
    )
    
    # Header with Logo
    logo_img = get_company_logo(width=100, height=50)
    if logo_img:
        elements.append(logo_img)
        elements.append(Spacer(1, 5))
    
    elements.append(Paragraph("LUCUMAA GLASS", title_style))
    elements.append(Paragraph("Premium Glass Manufacturing", subtitle_style))
    
    # Invoice Title
    inv_title = ParagraphStyle(
        'InvTitle',
        parent=styles['Heading1'],
        fontSize=16,
        textColor=colors.white,
        alignment=TA_CENTER,
        backColor=colors.HexColor('#dc2626'),
        borderPadding=10
    )
    elements.append(Spacer(1, 10))
    elements.append(Paragraph("🧾 TAX INVOICE", inv_title))
    elements.append(Spacer(1, 20))
    
    # Invoice Info
    order_number = order.get('order_number', order_id[:8]).upper()
    invoice_number = f"INV-{datetime.now().strftime('%Y%m%d')}-{order_number}"
    created_at = order.get('created_at', '')[:10] if order.get('created_at') else 'N/A'
    
    # Generate QR code for order tracking
    tracking_url = f"{BASE_URL}/track-order?order={order_number}"
    qr_image = generate_qr_code(tracking_url, size=80)
    
    # QR Code section with tracking info
    qr_text = ParagraphStyle(
        'QRText',
        parent=styles['Normal'],
        fontSize=8,
        textColor=colors.HexColor('#64748b'),
        alignment=TA_CENTER
    )
    
    qr_content = Table([
        [qr_image],
        [Paragraph("Scan to Track", qr_text)]
    ], colWidths=[90])
    qr_content.setStyle(TableStyle([
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
    ]))
    
    invoice_info = [
        ['Invoice #', invoice_number, 'Order #', order_number],
        ['Invoice Date', datetime.now().strftime('%Y-%m-%d'), 'Order Date', created_at],
    ]
    
    invoice_table = Table(invoice_info, colWidths=[100, 150, 100, 150])
    invoice_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#f1f5f9')),
        ('BACKGROUND', (2, 0), (2, -1), colors.HexColor('#f1f5f9')),
        ('TEXTCOLOR', (0, 0), (-1, -1), colors.HexColor('#1e293b')),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTNAME', (2, 0), (2, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#e2e8f0')),
    ]))
    
    # Combine invoice info and QR code side by side
    header_table = Table([[invoice_table, qr_content]], colWidths=[420, 100])
    header_table.setStyle(TableStyle([
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ]))
    elements.append(header_table)
    elements.append(Spacer(1, 20))
    
    # Bill To - Use customer profile data if available
    elements.append(Paragraph("📍 BILL TO", heading_style))
    
    # Prefer customer profile data over order data
    customer_name = order.get('customer_name', '')
    company_name = order.get('company_name', '')
    address = order.get('delivery_address', order.get('address', ''))
    phone = order.get('customer_phone', order.get('phone', ''))
    email = ''
    gstin = order.get('customer_gstin', '')
    
    if customer_profile:
        customer_name = customer_name or customer_profile.get('name', '')
        company_name = company_name or customer_profile.get('company_name', '')
        phone = phone or customer_profile.get('phone', '')
        email = customer_profile.get('email', '')
        gstin = gstin or customer_profile.get('gstin', '')
        if not address:
            address = customer_profile.get('address', customer_profile.get('delivery_address', ''))
    
    customer_name = customer_name or 'N/A'
    company_name = company_name or 'N/A'
    address = address or 'N/A'
    phone = phone or 'N/A'
    
    bill_to = [
        ['Customer', customer_name],
        ['Company', company_name],
        ['Phone', phone],
    ]
    
    if email:
        bill_to.append(['Email', email])
    if gstin:
        bill_to.append(['GSTIN', gstin])
    
    bill_to.append(['Address', address])
    
    bill_table = Table(bill_to, colWidths=[100, 400])
    bill_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#f1f5f9')),
        ('TEXTCOLOR', (0, 0), (-1, -1), colors.HexColor('#1e293b')),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#e2e8f0')),
    ]))
    elements.append(bill_table)
    elements.append(Spacer(1, 20))
    
    # Items
    elements.append(Paragraph("📦 ITEMS", heading_style))
    
    items = order.get('items', [])
    if not items:
        items = [{
            'name': order.get('product_name', order.get('glass_type', 'Glass Product')),
            'size': order.get('size', 'Standard'),
            'thickness': order.get('thickness', 'N/A'),
            'quantity': order.get('quantity', 1),
            'price': order.get('total_price', 0)
        }]
    
    item_data = [['#', 'Description', 'Size', 'Qty', 'Rate', 'Amount']]
    subtotal = 0
    for idx, item in enumerate(items, 1):
        qty = item.get('quantity', 1)
        price = item.get('price', item.get('total_price', 0))
        rate = price / qty if qty > 0 else price
        subtotal += price
        item_data.append([
            str(idx),
            item.get('name', 'Product'),
            item.get('size', 'Standard'),
            str(qty),
            f"₹{rate:,.2f}",
            f"₹{price:,.2f}"
        ])
    
    item_table = Table(item_data, colWidths=[30, 200, 80, 50, 70, 80])
    item_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#dc2626')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#e2e8f0')),
        ('ALIGN', (3, 0), (5, -1), 'RIGHT'),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#fef2f2')]),
    ]))
    elements.append(item_table)
    elements.append(Spacer(1, 15))
    
    # Totals with GST breakdown
    transport_charge = order.get('transport_charge', 0)
    gst_info = order.get('gst_info', {})
    
    # Calculate proper totals
    total_price = order.get('total_price', subtotal)
    total_gst = gst_info.get('total_gst', 0) if gst_info else 0
    grand_total = total_price if total_price > 0 else (subtotal + transport_charge + total_gst)
    advance_paid = order.get('advance_amount', 0)
    cash_paid = order.get('cash_amount', 0) if order.get('remaining_payment_status') == 'cash_received' else 0
    total_paid = advance_paid + cash_paid
    balance_due = grand_total - total_paid
    
    totals_data = [
        ['', '', '', '', 'Subtotal', f"₹{subtotal:,.2f}"],
    ]
    
    if transport_charge > 0:
        totals_data.append(['', '', '', '', 'Transport', f"₹{transport_charge:,.2f}"])
    
    # Add GST breakdown if available
    if gst_info:
        if gst_info.get('cgst_amount', 0) > 0:
            totals_data.append(['', '', '', '', f"CGST ({gst_info.get('cgst_rate', 9)}%)", f"₹{gst_info.get('cgst_amount', 0):,.2f}"])
            totals_data.append(['', '', '', '', f"SGST ({gst_info.get('sgst_rate', 9)}%)", f"₹{gst_info.get('sgst_amount', 0):,.2f}"])
        elif gst_info.get('igst_amount', 0) > 0:
            totals_data.append(['', '', '', '', f"IGST ({gst_info.get('igst_rate', 18)}%)", f"₹{gst_info.get('igst_amount', 0):,.2f}"])
    
    totals_data.append(['', '', '', '', 'Grand Total', f"₹{grand_total:,.2f}"])
    
    if advance_paid > 0:
        totals_data.append(['', '', '', '', 'Advance Paid (Online)', f"₹{advance_paid:,.2f}"])
    if cash_paid > 0:
        totals_data.append(['', '', '', '', 'Cash Paid', f"₹{cash_paid:,.2f}"])
    
    totals_data.append(['', '', '', '', 'Balance Due', f"₹{balance_due:,.2f}"])
    
    totals_table = Table(totals_data, colWidths=[30, 200, 80, 50, 70, 80])
    totals_table.setStyle(TableStyle([
        ('FONTNAME', (4, 0), (4, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('ALIGN', (4, 0), (5, -1), 'RIGHT'),
        ('LINEABOVE', (4, 2), (5, 2), 2, colors.HexColor('#dc2626')),
        ('BACKGROUND', (4, 2), (5, 2), colors.HexColor('#dc2626')),
        ('TEXTCOLOR', (4, 2), (5, 2), colors.white),
        ('PADDING', (4, 0), (5, -1), 8),
    ]))
    elements.append(totals_table)
    elements.append(Spacer(1, 30))
    
    # Terms
    terms_style = ParagraphStyle(
        'Terms',
        parent=styles['Normal'],
        fontSize=8,
        textColor=colors.grey
    )
    elements.append(Paragraph("<b>Terms & Conditions:</b>", terms_style))
    elements.append(Paragraph("1. Payment is due as per the agreed terms.", terms_style))
    elements.append(Paragraph("2. Goods once sold will not be taken back.", terms_style))
    elements.append(Paragraph("3. Subject to local jurisdiction.", terms_style))
    elements.append(Spacer(1, 20))
    
    # Footer
    footer_style = ParagraphStyle(
        'Footer',
        parent=styles['Normal'],
        fontSize=8,
        textColor=colors.grey,
        alignment=TA_CENTER
    )
    elements.append(Paragraph("Thank you for your business!", footer_style))
    elements.append(Paragraph(f"Generated on {datetime.now().strftime('%Y-%m-%d %H:%M')}", footer_style))
    
    # Build PDF
    doc.build(elements)
    buffer.seek(0)
    
    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=invoice_{order_number}.pdf"}
    )


# ================== PAYMENT RECEIPT PDF ==================

@pdf_router.get("/payment-receipt/{order_id}")
async def generate_payment_receipt(
    order_id: str,
    token: str = None,
    current_user: dict = Depends(get_erp_user)
):
    """Generate Payment Receipt PDF (combines Online + Cash payments)"""
    db = get_db()
    
    # Check if it's a job work order or regular order
    order = await db.orders.find_one({"id": order_id}, {"_id": 0})
    is_job_work = False
    
    if not order:
        # Try job work orders
        order = await db.job_work_orders.find_one({"id": order_id}, {"_id": 0})
        is_job_work = True
        
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    # Generate PDF
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30)
    
    elements = []
    styles = getSampleStyleSheet()
    
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#0d9488'),
        spaceAfter=10,
        alignment=TA_CENTER
    )
    
    subtitle_style = ParagraphStyle(
        'Subtitle',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.grey,
        alignment=TA_CENTER,
        spaceAfter=20
    )
    
    heading_style = ParagraphStyle(
        'Heading',
        parent=styles['Heading2'],
        fontSize=12,
        textColor=colors.HexColor('#1e293b'),
        spaceBefore=15,
        spaceAfter=8
    )
    
    # Header with Logo
    logo_img = get_company_logo(width=100, height=50)
    if logo_img:
        elements.append(logo_img)
        elements.append(Spacer(1, 5))
    
    elements.append(Paragraph("LUCUMAA GLASS", title_style))
    elements.append(Paragraph("Premium Glass Manufacturing", subtitle_style))
    
    # Receipt Title
    receipt_title = ParagraphStyle(
        'ReceiptTitle',
        parent=styles['Heading1'],
        fontSize=16,
        textColor=colors.white,
        alignment=TA_CENTER,
        backColor=colors.HexColor('#10b981'),
        borderPadding=10
    )
    elements.append(Spacer(1, 10))
    elements.append(Paragraph("💳 PAYMENT RECEIPT", receipt_title))
    elements.append(Spacer(1, 20))
    
    # Order Info
    if is_job_work:
        order_number = order.get('job_work_number', order_id[:8]).upper()
        order_type = "Job Work"
        grand_total = order.get('summary', {}).get('grand_total', 0)
        advance_paid = order.get('advance_paid', 0)
        payment_status = order.get('payment_status', 'pending')
        customer_name = order.get('customer_name', 'N/A')
        phone = order.get('phone', 'N/A')
    else:
        order_number = order.get('order_number', order_id[:8]).upper()
        order_type = "Regular Order"
        grand_total = order.get('total_price', 0)
        advance_paid = order.get('advance_amount', 0)
        cash_paid = order.get('cash_amount', 0) if order.get('remaining_payment_status') == 'cash_received' else 0
        payment_status = 'paid' if (order.get('advance_payment_status') == 'paid' and 
                                    (order.get('remaining_payment_status') in ['paid', 'cash_received'] or 
                                     order.get('advance_percent') == 100)) else 'partial'
        customer_name = order.get('customer_name', 'N/A')
        phone = order.get('customer_phone', order.get('phone', 'N/A'))
    
    receipt_number = f"RCP-{datetime.now().strftime('%Y%m%d')}-{order_number}"
    created_at = order.get('created_at', '')[:10] if order.get('created_at') else 'N/A'
    
    receipt_info = [
        ['Receipt #', receipt_number, 'Order #', order_number],
        ['Receipt Date', datetime.now().strftime('%Y-%m-%d'), 'Order Date', created_at],
        ['Order Type', order_type, 'Status', payment_status.upper()],
    ]
    
    receipt_table = Table(receipt_info, colWidths=[100, 150, 100, 150])
    receipt_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#f1f5f9')),
        ('BACKGROUND', (2, 0), (2, -1), colors.HexColor('#f1f5f9')),
        ('TEXTCOLOR', (0, 0), (-1, -1), colors.HexColor('#1e293b')),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTNAME', (2, 0), (2, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#e2e8f0')),
    ]))
    elements.append(receipt_table)
    elements.append(Spacer(1, 20))
    
    # Customer Info
    elements.append(Paragraph("👤 RECEIVED FROM", heading_style))
    customer_info = [
        ['Customer', customer_name],
        ['Phone', phone],
    ]
    cust_table = Table(customer_info, colWidths=[100, 400])
    cust_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#f1f5f9')),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#e2e8f0')),
    ]))
    elements.append(cust_table)
    elements.append(Spacer(1, 20))
    
    # Payment Details
    elements.append(Paragraph("💰 PAYMENT DETAILS", heading_style))
    
    if is_job_work:
        total_paid = advance_paid
        if order.get('payment_status') == 'completed':
            total_paid = grand_total
        cash_paid = order.get('paid_amount', 0) if order.get('payment_method') == 'cash' else 0
        online_paid = order.get('paid_amount', 0) if order.get('payment_method') == 'online' else advance_paid
    else:
        cash_paid = order.get('cash_amount', 0) if order.get('remaining_payment_status') == 'cash_received' else 0
        online_paid = advance_paid
        total_paid = online_paid + cash_paid
    
    balance = grand_total - total_paid
    
    payment_data = [
        ['Description', 'Amount'],
        ['Order Total', f"₹{grand_total:,.2f}"],
    ]
    
    if online_paid > 0:
        payment_data.append(['Online Payment (Razorpay)', f"₹{online_paid:,.2f}"])
    if cash_paid > 0:
        payment_data.append(['Cash Payment', f"₹{cash_paid:,.2f}"])
    
    payment_data.append(['Total Paid', f"₹{total_paid:,.2f}"])
    
    if balance > 0:
        payment_data.append(['Balance Due', f"₹{balance:,.2f}"])
    
    payment_table = Table(payment_data, colWidths=[350, 150])
    payment_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#10b981')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 11),
        ('PADDING', (0, 0), (-1, -1), 10),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#e2e8f0')),
        ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
        ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor('#ecfdf5') if balance <= 0 else colors.HexColor('#fef2f2')),
        ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
    ]))
    elements.append(payment_table)
    elements.append(Spacer(1, 30))
    
    # Payment Status Badge
    if balance <= 0:
        status_text = "✅ PAYMENT COMPLETE"
        status_color = '#10b981'
    else:
        status_text = f"⏳ BALANCE DUE: ₹{balance:,.2f}"
        status_color = '#f59e0b'
    
    status_style = ParagraphStyle(
        'Status',
        parent=styles['Heading1'],
        fontSize=14,
        textColor=colors.white,
        alignment=TA_CENTER,
        backColor=colors.HexColor(status_color),
        borderPadding=10
    )
    elements.append(Paragraph(status_text, status_style))
    elements.append(Spacer(1, 30))
    
    # Footer
    footer_style = ParagraphStyle(
        'Footer',
        parent=styles['Normal'],
        fontSize=8,
        textColor=colors.grey,
        alignment=TA_CENTER
    )
    elements.append(Paragraph("This is a computer-generated receipt.", footer_style))
    elements.append(Paragraph(f"Generated on {datetime.now().strftime('%Y-%m-%d %H:%M')}", footer_style))
    
    # Build PDF
    doc.build(elements)
    buffer.seek(0)
    
    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=receipt_{order_number}.pdf"}
    )


# ================== JOB WORK INVOICE PDF ==================

@pdf_router.get("/job-work-invoice/{order_id}")
async def generate_job_work_invoice(
    order_id: str,
    token: str = None,
    current_user: dict = Depends(get_erp_user)
):
    """Generate Invoice PDF for a Job Work order"""
    db = get_db()
    
    order = await db.job_work_orders.find_one({"id": order_id}, {"_id": 0})
    if not order:
        raise HTTPException(status_code=404, detail="Job work order not found")
    
    # Generate PDF
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30)
    
    elements = []
    styles = getSampleStyleSheet()
    
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#ea580c'),
        spaceAfter=5,
        alignment=TA_CENTER
    )
    
    company_subtitle = ParagraphStyle(
        'CompanySubtitle',
        parent=styles['Normal'],
        fontSize=9,
        textColor=colors.HexColor('#64748b'),
        alignment=TA_CENTER,
        spaceAfter=2
    )
    
    heading_style = ParagraphStyle(
        'Heading',
        parent=styles['Heading2'],
        fontSize=11,
        textColor=colors.HexColor('#1e293b'),
        spaceBefore=12,
        spaceAfter=6
    )
    
    # ========== COMPANY HEADER WITH LOGO ==========
    # Try to add company logo
    logo_img = get_company_logo(width=100, height=50)
    if logo_img:
        elements.append(logo_img)
        elements.append(Spacer(1, 5))
    
    elements.append(Paragraph("LUCUMAA GLASS", title_style))
    elements.append(Paragraph("A Unit of Lucumaa Corporation Pvt. Ltd.", company_subtitle))
    elements.append(Paragraph("Premium Glass Toughening Services", company_subtitle))
    elements.append(Spacer(1, 5))
    
    # Company Address Box
    company_info = """
    <b>Factory:</b> Industrial Area, Sector 63, Noida, UP - 201301<br/>
    <b>Corporate Office:</b> Tower B, Logix City Centre, Noida - 201301<br/>
    <b>GST:</b> 09AABCL1234A1Z5 | <b>Phone:</b> +91-9876543210 | <b>Email:</b> info@lucumaaglass.in
    """
    company_style = ParagraphStyle(
        'CompanyInfo',
        parent=styles['Normal'],
        fontSize=8,
        textColor=colors.HexColor('#475569'),
        alignment=TA_CENTER,
        borderColor=colors.HexColor('#e2e8f0'),
        borderWidth=1,
        borderPadding=8
    )
    elements.append(Paragraph(company_info, company_style))
    elements.append(Spacer(1, 10))
    
    # Invoice Title Banner
    inv_title = ParagraphStyle(
        'InvTitle',
        parent=styles['Heading1'],
        fontSize=14,
        textColor=colors.white,
        alignment=TA_CENTER,
        backColor=colors.HexColor('#ea580c'),
        borderPadding=8
    )
    elements.append(Paragraph("JOB WORK INVOICE / CHALLAN", inv_title))
    elements.append(Spacer(1, 15))
    
    # Invoice Info
    job_work_number = order.get('job_work_number', order_id[:8]).upper()
    invoice_number = f"JWI-{datetime.now().strftime('%Y%m%d')}-{job_work_number}"
    created_at = order.get('created_at', '')[:10] if order.get('created_at') else 'N/A'
    
    invoice_info = [
        ['Invoice #', invoice_number, 'Job Work #', job_work_number],
        ['Invoice Date', datetime.now().strftime('%Y-%m-%d'), 'Order Date', created_at],
    ]
    
    invoice_table = Table(invoice_info, colWidths=[100, 150, 100, 150])
    invoice_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#fff7ed')),
        ('BACKGROUND', (2, 0), (2, -1), colors.HexColor('#fff7ed')),
        ('TEXTCOLOR', (0, 0), (-1, -1), colors.HexColor('#1e293b')),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTNAME', (2, 0), (2, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#fed7aa')),
    ]))
    elements.append(invoice_table)
    elements.append(Spacer(1, 20))
    
    # Customer Info
    elements.append(Paragraph("👤 BILL TO", heading_style))
    customer_info = [
        ['Customer', order.get('customer_name', 'N/A')],
        ['Company', order.get('company_name', 'N/A') or 'N/A'],
        ['Phone', order.get('phone', 'N/A')],
        ['Email', order.get('email', 'N/A') or 'N/A'],
        ['Address', order.get('delivery_address', 'N/A')],
    ]
    cust_table = Table(customer_info, colWidths=[100, 400])
    cust_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#fff7ed')),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#fed7aa')),
    ]))
    elements.append(cust_table)
    elements.append(Spacer(1, 20))
    
    # Items
    elements.append(Paragraph("📦 GLASS ITEMS", heading_style))
    
    item_data = [['#', 'Size (W×H)', 'Thickness', 'Qty', 'Sq.Ft', 'Labour']]
    item_details = order.get('item_details', [])
    
    for idx, item in enumerate(item_details, 1):
        size = f"{item.get('width_inch', 0)} × {item.get('height_inch', 0)} inch"
        thickness = f"{item.get('thickness_mm', 0)}mm"
        qty = str(item.get('quantity', 1))
        sqft = f"{item.get('sqft', 0):.2f}"
        labour = f"₹{item.get('labour_cost', 0):,.2f}"
        item_data.append([str(idx), size, thickness, qty, sqft, labour])
    
    item_table = Table(item_data, colWidths=[30, 150, 80, 50, 70, 100])
    item_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#ea580c')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#fed7aa')),
        ('ALIGN', (3, 0), (5, -1), 'RIGHT'),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#fff7ed')]),
    ]))
    elements.append(item_table)
    elements.append(Spacer(1, 15))
    
    # Totals
    summary = order.get('summary', {})
    labour_charges = summary.get('labour_charges', 0)
    gst_amount = summary.get('gst_amount', 0)
    gst_rate = summary.get('gst_rate', 18)
    transport_cost = summary.get('transport_cost', 0)
    grand_total = summary.get('grand_total', 0)
    advance_paid = order.get('advance_paid', 0)
    balance_due = grand_total - advance_paid
    
    totals_data = [
        ['', '', '', '', 'Labour Charges', f"₹{labour_charges:,.2f}"],
        ['', '', '', '', f'GST ({gst_rate}%)', f"₹{gst_amount:,.2f}"],
    ]
    
    if transport_cost > 0:
        totals_data.append(['', '', '', '', 'Transport', f"₹{transport_cost:,.2f}"])
    
    totals_data.extend([
        ['', '', '', '', 'Grand Total', f"₹{grand_total:,.2f}"],
        ['', '', '', '', 'Advance Paid', f"₹{advance_paid:,.2f}"],
        ['', '', '', '', 'Balance Due', f"₹{balance_due:,.2f}"],
    ])
    
    totals_table = Table(totals_data, colWidths=[30, 150, 80, 50, 70, 100])
    totals_table.setStyle(TableStyle([
        ('ALIGN', (4, 0), (5, -1), 'RIGHT'),
        ('FONTNAME', (4, 0), (5, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('PADDING', (0, 0), (-1, -1), 6),
        ('LINEABOVE', (4, 0), (5, 0), 1, colors.HexColor('#fed7aa')),
        ('BACKGROUND', (4, -3), (5, -3), colors.HexColor('#fff7ed')),
        ('BACKGROUND', (4, -1), (5, -1), colors.HexColor('#fef2f2') if balance_due > 0 else colors.HexColor('#ecfdf5')),
    ]))
    elements.append(totals_table)
    elements.append(Spacer(1, 20))
    
    # Disclaimer Note
    disclaimer_style = ParagraphStyle(
        'Disclaimer',
        parent=styles['Normal'],
        fontSize=8,
        textColor=colors.HexColor('#9a3412'),
        borderColor=colors.HexColor('#fed7aa'),
        borderWidth=1,
        borderPadding=8
    )
    elements.append(Paragraph(
        "<b>⚠️ DISCLAIMER:</b> Customer's glass material is being processed. "
        "Company is not liable for breakage during toughening process. "
        "This is standard industry practice due to inherent risks in glass toughening.",
        disclaimer_style
    ))
    elements.append(Spacer(1, 20))
    
    # Footer
    footer_style = ParagraphStyle(
        'Footer',
        parent=styles['Normal'],
        fontSize=8,
        textColor=colors.grey,
        alignment=TA_CENTER
    )
    elements.append(Paragraph("This is a computer-generated invoice.", footer_style))
    elements.append(Paragraph(f"Generated on {datetime.now().strftime('%Y-%m-%d %H:%M')}", footer_style))
    
    # Build PDF
    doc.build(elements)
    buffer.seek(0)
    
    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=job_work_invoice_{job_work_number}.pdf"}
    )


# ================== JOB WORK DELIVERY SLIP ==================

@pdf_router.get("/job-work-slip/{order_id}")
async def generate_job_work_slip(
    order_id: str,
    token: str = None,
    current_user: dict = Depends(get_erp_user)
):
    """Generate Delivery Slip for Job Work - for customer to carry when picking up glass"""
    db = get_db()
    
    order = await db.job_work_orders.find_one({"id": order_id}, {"_id": 0})
    if not order:
        raise HTTPException(status_code=404, detail="Job work order not found")
    
    # Check payment is fully settled before generating dispatch slip
    payment_status = order.get('payment_status', 'pending')
    if payment_status != 'completed':
        raise HTTPException(status_code=400, detail="Cannot generate delivery slip. Payment not fully settled. Please complete full payment first.")
    
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30)
    
    elements = []
    styles = getSampleStyleSheet()
    
    title_style = ParagraphStyle('Title', parent=styles['Heading1'], fontSize=22, textColor=colors.HexColor('#ea580c'), spaceAfter=5, alignment=TA_CENTER)
    
    # Header
    elements.append(Paragraph("LUCUMAA GLASS", title_style))
    elements.append(Paragraph("A Unit of Lucumaa Corporation Pvt. Ltd.", ParagraphStyle('Sub', parent=styles['Normal'], fontSize=9, alignment=TA_CENTER, textColor=colors.grey)))
    elements.append(Spacer(1, 15))
    
    # Slip Title
    slip_title = ParagraphStyle('SlipTitle', parent=styles['Heading1'], fontSize=14, textColor=colors.white, alignment=TA_CENTER, backColor=colors.HexColor('#16a34a'), borderPadding=10)
    elements.append(Paragraph("DELIVERY SLIP / GATE PASS", slip_title))
    elements.append(Spacer(1, 20))
    
    job_work_number = order.get('job_work_number', order_id[:8]).upper()
    
    # Slip Details
    slip_info = [
        ['Slip #', f"DS-{job_work_number}", 'Date', datetime.now().strftime('%d-%m-%Y')],
        ['Job Work #', job_work_number, 'Time', datetime.now().strftime('%H:%M')],
    ]
    
    slip_table = Table(slip_info, colWidths=[80, 170, 80, 170])
    slip_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#f0fdf4')),
        ('BACKGROUND', (2, 0), (2, -1), colors.HexColor('#f0fdf4')),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTNAME', (2, 0), (2, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 11),
        ('PADDING', (0, 0), (-1, -1), 10),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#86efac')),
    ]))
    elements.append(slip_table)
    elements.append(Spacer(1, 20))
    
    # Customer Info
    heading_style = ParagraphStyle('Heading', parent=styles['Heading2'], fontSize=11, textColor=colors.HexColor('#1e293b'), spaceBefore=10, spaceAfter=6)
    elements.append(Paragraph("CUSTOMER DETAILS", heading_style))
    
    cust_info = [
        ['Name', order.get('customer_name', 'N/A')],
        ['Phone', order.get('phone', 'N/A')],
        ['Company', order.get('company_name', '') or 'N/A'],
    ]
    cust_table = Table(cust_info, colWidths=[100, 400])
    cust_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#f0fdf4')),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#86efac')),
    ]))
    elements.append(cust_table)
    elements.append(Spacer(1, 20))
    
    # Items
    elements.append(Paragraph("GLASS ITEMS FOR DELIVERY", heading_style))
    
    item_data = [['#', 'Size (W×H)', 'Thickness', 'Qty', 'Status']]
    item_details = order.get('item_details', [])
    total_pieces = 0
    
    for idx, item in enumerate(item_details, 1):
        size = f"{item.get('width_inch', 0)} × {item.get('height_inch', 0)} inch"
        thickness = f"{item.get('thickness_mm', 0)}mm"
        qty = item.get('quantity', 1)
        total_pieces += qty
        status = "✓ Ready"
        item_data.append([str(idx), size, thickness, str(qty), status])
    
    item_table = Table(item_data, colWidths=[40, 180, 80, 60, 100])
    item_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#16a34a')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#86efac')),
        ('ALIGN', (2, 0), (-1, -1), 'CENTER'),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f0fdf4')]),
    ]))
    elements.append(item_table)
    elements.append(Spacer(1, 15))
    
    # Summary
    summary = order.get('summary', {})
    summary_data = [
        ['Total Pieces', str(total_pieces)],
        ['Total Area', f"{summary.get('total_sqft', 0):.2f} sq.ft"],
    ]
    
    summary_table = Table(summary_data, colWidths=[150, 150])
    summary_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#dcfce7')),
        ('FONTNAME', (0, 0), (-1, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 12),
        ('PADDING', (0, 0), (-1, -1), 10),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('BOX', (0, 0), (-1, -1), 2, colors.HexColor('#16a34a')),
    ]))
    elements.append(summary_table)
    elements.append(Spacer(1, 25))
    
    # Payment Status
    payment_status = order.get('payment_status', 'pending')
    advance_paid = order.get('advance_paid', 0)
    grand_total = summary.get('grand_total', 0)
    balance = grand_total - advance_paid
    
    if payment_status == 'completed' or balance <= 0:
        pay_text = "PAYMENT COMPLETE - ✓ CLEARED FOR DELIVERY"
        pay_color = '#16a34a'
    else:
        pay_text = f"BALANCE DUE: ₹{balance:,.2f} - COLLECT BEFORE DELIVERY"
        pay_color = '#dc2626'
    
    pay_style = ParagraphStyle('PayStatus', parent=styles['Heading1'], fontSize=12, textColor=colors.white, alignment=TA_CENTER, backColor=colors.HexColor(pay_color), borderPadding=10)
    elements.append(Paragraph(pay_text, pay_style))
    elements.append(Spacer(1, 25))
    
    # Signatures
    sig_data = [
        ['Customer Signature', 'Store/Warehouse Signature', 'Security Signature'],
        ['\n\n\n__________________', '\n\n\n__________________', '\n\n\n__________________'],
    ]
    sig_table = Table(sig_data, colWidths=[170, 170, 170])
    sig_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('TOPPADDING', (0, 0), (-1, -1), 10),
    ]))
    elements.append(sig_table)
    elements.append(Spacer(1, 20))
    
    # Footer
    footer_style = ParagraphStyle('Footer', parent=styles['Normal'], fontSize=8, textColor=colors.grey, alignment=TA_CENTER)
    elements.append(Paragraph("This is a system-generated delivery slip. Valid only with company seal.", footer_style))
    elements.append(Paragraph(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}", footer_style))
    
    doc.build(elements)
    buffer.seek(0)
    
    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=delivery_slip_{job_work_number}.pdf"}
    )


# ================== MERGED PDF (INVOICE + RECEIPT) ==================

@pdf_router.get("/merged/{order_id}")
async def generate_merged_pdf(
    order_id: str,
    token: str = None,
    current_user: dict = Depends(get_erp_user)
):
    """Generate Merged PDF - Invoice on Page 1, Receipt on Page 2"""
    from PyPDF2 import PdfMerger, PdfReader
    
    db = get_db()
    
    # Check if job work or regular order
    order = await db.orders.find_one({"id": order_id}, {"_id": 0})
    is_job_work = False
    
    if not order:
        order = await db.job_work_orders.find_one({"id": order_id}, {"_id": 0})
        is_job_work = True
    
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    # Generate Invoice PDF
    if is_job_work:
        invoice_response = await generate_job_work_invoice(order_id, token, current_user)
    else:
        invoice_response = await generate_invoice(order_id, token, current_user)
    
    # Generate Receipt PDF
    receipt_response = await generate_payment_receipt(order_id, token, current_user)
    
    # Read both PDFs
    invoice_buffer = BytesIO()
    async for chunk in invoice_response.body_iterator:
        invoice_buffer.write(chunk)
    invoice_buffer.seek(0)
    
    receipt_buffer = BytesIO()
    async for chunk in receipt_response.body_iterator:
        receipt_buffer.write(chunk)
    receipt_buffer.seek(0)
    
    # Merge PDFs
    merger = PdfMerger()
    merger.append(PdfReader(invoice_buffer))
    merger.append(PdfReader(receipt_buffer))
    
    output_buffer = BytesIO()
    merger.write(output_buffer)
    merger.close()
    output_buffer.seek(0)
    
    order_number = order.get('job_work_number' if is_job_work else 'order_number', order_id[:8]).upper()
    
    return StreamingResponse(
        output_buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=invoice_receipt_{order_number}.pdf"}
    )


# ================== SHARE ACTION LOGGING ==================

@pdf_router.post("/share-log")
async def log_share_action(
    order_id: str,
    document_type: str,  # invoice, receipt, merged
    share_channel: str,  # whatsapp, email, download
    current_user: dict = Depends(get_erp_user)
):
    """Log share actions for audit trail"""
    import uuid
    
    db = get_db()
    
    log_entry = {
        "id": str(uuid.uuid4()),
        "order_id": order_id,
        "document_type": document_type,
        "share_channel": share_channel,
        "shared_by": current_user.get("name", "Unknown"),
        "shared_by_id": current_user.get("id"),
        "shared_by_role": current_user.get("role", "customer"),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    
    await db.share_audit_logs.insert_one(log_entry)
    
    return {"message": "Share action logged", "log_id": log_entry["id"]}


# ================== EMAIL PDF ATTACHMENT ==================

from pydantic import BaseModel
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import aiosmtplib

class EmailPDFRequest(BaseModel):
    recipient_email: str
    document_type: str  # invoice, receipt, merged
    order_id: str
    custom_message: str = ""

@pdf_router.post("/send-email")
async def send_pdf_email(
    request: EmailPDFRequest,
    current_user: dict = Depends(get_erp_user)
):
    """Send PDF as email attachment via SMTP"""
    import uuid
    
    db = get_db()
    
    # Get order details
    order = await db.orders.find_one({"id": request.order_id}, {"_id": 0})
    is_job_work = False
    
    if not order:
        order = await db.job_work_orders.find_one({"id": request.order_id}, {"_id": 0})
        is_job_work = True
    
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    order_number = order.get('job_work_number' if is_job_work else 'order_number', request.order_id[:8]).upper()
    customer_name = order.get('customer_name', 'Customer')
    
    # Generate PDF based on document type
    pdf_buffer = BytesIO()
    
    if request.document_type == 'invoice':
        if is_job_work:
            response = await generate_job_work_invoice(request.order_id, None, current_user)
        else:
            response = await generate_invoice(request.order_id, None, current_user)
        filename = f"Invoice_{order_number}.pdf"
        subject = f"Invoice - {order_number} | Lucumaa Glass"
    elif request.document_type == 'receipt':
        response = await generate_payment_receipt(request.order_id, None, current_user)
        filename = f"Receipt_{order_number}.pdf"
        subject = f"Payment Receipt - {order_number} | Lucumaa Glass"
    elif request.document_type == 'merged':
        response = await generate_merged_pdf(request.order_id, None, current_user)
        filename = f"Invoice_Receipt_{order_number}.pdf"
        subject = f"Invoice & Receipt - {order_number} | Lucumaa Glass"
    else:
        raise HTTPException(status_code=400, detail="Invalid document type")
    
    # Read PDF content
    async for chunk in response.body_iterator:
        pdf_buffer.write(chunk)
    pdf_buffer.seek(0)
    pdf_content = pdf_buffer.read()
    
    # Create email
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = f"{os.environ.get('SENDER_NAME', 'Lucumaa Glass')} <{os.environ.get('SENDER_EMAIL', 'info@lucumaaglass.in')}>"
    msg['To'] = request.recipient_email
    
    # Custom message or default
    if request.custom_message:
        body_text = request.custom_message
    else:
        doc_name = "Invoice" if request.document_type == "invoice" else "Payment Receipt" if request.document_type == "receipt" else "Invoice & Receipt"
        body_text = f"""
Dear {customer_name},

Please find attached your {doc_name} for Order #{order_number}.

Thank you for choosing Lucumaa Glass!

Best Regards,
Lucumaa Glass Team
Premium Glass Manufacturing

---
This is a system-generated email. Please do not reply directly.
For queries, contact: info@lucumaaglass.in | +91-XXXXXXXXXX
"""
    
    msg.attach(MIMEText(body_text, 'plain'))
    
    # Attach PDF
    pdf_attachment = MIMEApplication(pdf_content, _subtype='pdf')
    pdf_attachment.add_header('Content-Disposition', 'attachment', filename=filename)
    msg.attach(pdf_attachment)
    
    # Send email via SMTP
    try:
        await aiosmtplib.send(
            msg,
            hostname=os.environ.get('SMTP_HOST', 'smtp.hostinger.com'),
            port=int(os.environ.get('SMTP_PORT', 465)),
            username=os.environ.get('SMTP_USER'),
            password=os.environ.get('SMTP_PASSWORD'),
            use_tls=True
        )
        
        # Log the share action
        log_entry = {
            "id": str(uuid.uuid4()),
            "order_id": request.order_id,
            "document_type": request.document_type,
            "share_channel": "email_smtp",
            "recipient_email": request.recipient_email,
            "shared_by": current_user.get("name", "Unknown"),
            "shared_by_id": current_user.get("id"),
            "shared_by_role": current_user.get("role", "customer"),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        await db.share_audit_logs.insert_one(log_entry)
        
        return {
            "message": f"Email sent successfully to {request.recipient_email}",
            "order_number": order_number,
            "document_type": request.document_type
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to send email: {str(e)}")


# ================== BULK EXPORT (EXCEL) ==================

@pdf_router.get("/export/orders-excel")
async def export_orders_excel(
    start_date: str = None,
    end_date: str = None,
    order_type: str = "all",  # all, regular, job_work
    token: str = None,
    current_user: dict = Depends(get_erp_user)
):
    """Export orders data to Excel format"""
    import io
    
    db = get_db()
    
    # Build query
    query = {}
    if start_date:
        query["created_at"] = {"$gte": start_date}
    if end_date:
        if "created_at" in query:
            query["created_at"]["$lte"] = end_date
        else:
            query["created_at"] = {"$lte": end_date}
    
    # Fetch orders
    regular_orders = []
    job_work_orders = []
    
    if order_type in ["all", "regular"]:
        cursor = db.orders.find(query, {"_id": 0})
        regular_orders = await cursor.to_list(length=1000)
    
    if order_type in ["all", "job_work"]:
        cursor = db.job_work_orders.find(query, {"_id": 0})
        job_work_orders = await cursor.to_list(length=1000)
    
    # Create Excel-compatible CSV
    output = io.StringIO()
    
    # Header
    output.write("Order Number,Order Type,Customer Name,Company,Phone,Total Amount,Advance Paid,Balance,Payment Status,Order Date,Status\n")
    
    # Regular orders
    for order in regular_orders:
        total = order.get('total_price', 0)
        advance = order.get('advance_amount', 0)
        balance = total - advance
        output.write(f"{order.get('order_number', '')},Regular Order,{order.get('customer_name', '')},{order.get('company_name', '')},{order.get('customer_phone', '')},{total},{advance},{balance},{order.get('payment_status', '')},{order.get('created_at', '')[:10]},{order.get('status', '')}\n")
    
    # Job work orders
    for order in job_work_orders:
        total = order.get('summary', {}).get('grand_total', 0)
        advance = order.get('advance_paid', 0)
        balance = total - advance
        output.write(f"{order.get('job_work_number', '')},Job Work,{order.get('customer_name', '')},{order.get('company_name', '')},{order.get('phone', '')},{total},{advance},{balance},{order.get('payment_status', '')},{order.get('created_at', '')[:10]},{order.get('status', '')}\n")
    
    output.seek(0)
    
    return StreamingResponse(
        io.BytesIO(output.getvalue().encode('utf-8')),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=orders_export_{datetime.now().strftime('%Y%m%d')}.csv"}
    )


# ================== SHARE AUDIT LOGS ==================

@pdf_router.get("/share-logs")
async def get_share_logs(
    order_id: str = None,
    limit: int = 100,
    current_user: dict = Depends(get_erp_user)
):
    """Get share audit logs"""
    if current_user.get("role") not in ["super_admin", "admin", "finance", "accountant"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    query = {}
    if order_id:
        query["order_id"] = order_id
    
    cursor = db.share_audit_logs.find(query, {"_id": 0}).sort("timestamp", -1).limit(limit)
    logs = await cursor.to_list(length=limit)
    
    return {"logs": logs, "count": len(logs)}


# ================== VENDOR PAYMENT RECEIPT PDF ==================

@pdf_router.get("/vendor-payment-receipt/{payment_id}")
async def generate_vendor_payment_receipt(
    payment_id: str,
    token: str = None,
    current_user: dict = Depends(get_erp_user)
):
    """Generate Vendor Payment Receipt PDF"""
    db = get_db()
    
    payment = await db.vendor_payments.find_one({"id": payment_id}, {"_id": 0})
    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found")
    
    if payment.get("status") != "completed":
        raise HTTPException(status_code=400, detail="Receipt available only for completed payments")
    
    # Get PO and Vendor details
    po = await db.purchase_orders.find_one({"id": payment.get("po_id")}, {"_id": 0})
    vendor = await db.vendors.find_one({"id": payment.get("vendor_id")}, {"_id": 0})
    
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30)
    
    elements = []
    styles = getSampleStyleSheet()
    
    title_style = ParagraphStyle('Title', parent=styles['Heading1'], fontSize=22, textColor=colors.HexColor('#0d9488'), spaceAfter=5, alignment=TA_CENTER)
    company_style = ParagraphStyle('Company', parent=styles['Normal'], fontSize=9, textColor=colors.grey, alignment=TA_CENTER, spaceAfter=3)
    heading_style = ParagraphStyle('Heading', parent=styles['Heading2'], fontSize=11, textColor=colors.HexColor('#1e293b'), spaceBefore=12, spaceAfter=6)
    
    # Header with Logo
    logo_img = get_company_logo(width=100, height=50)
    if logo_img:
        elements.append(logo_img)
        elements.append(Spacer(1, 5))
    
    elements.append(Paragraph("LUCUMAA GLASS", title_style))
    elements.append(Paragraph("A Unit of Lucumaa Corporation Pvt. Ltd.", company_style))
    elements.append(Paragraph("Premium Glass Manufacturing", company_style))
    elements.append(Spacer(1, 15))
    
    # Receipt Title
    receipt_title = ParagraphStyle('ReceiptTitle', parent=styles['Heading1'], fontSize=14, textColor=colors.white, alignment=TA_CENTER, backColor=colors.HexColor('#0d9488'), borderPadding=10)
    elements.append(Paragraph("VENDOR PAYMENT RECEIPT", receipt_title))
    elements.append(Spacer(1, 20))
    
    receipt_number = payment.get("receipt_number", f"VPR-{payment_id[:8]}")
    
    # Receipt Info
    receipt_info = [
        ['Receipt #', receipt_number, 'Date', payment.get("completed_at", "")[:10]],
        ['PO #', payment.get("po_number", ""), 'Payment Type', payment.get("payment_type", "")],
    ]
    
    receipt_table = Table(receipt_info, colWidths=[100, 150, 100, 150])
    receipt_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#f0fdfa')),
        ('BACKGROUND', (2, 0), (2, -1), colors.HexColor('#f0fdfa')),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTNAME', (2, 0), (2, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#99f6e4')),
    ]))
    elements.append(receipt_table)
    elements.append(Spacer(1, 20))
    
    # Vendor Info
    elements.append(Paragraph("PAID TO", heading_style))
    vendor_info = [
        ['Vendor Name', vendor.get('name', 'N/A') if vendor else 'N/A'],
        ['Company', vendor.get('company_name', 'N/A') if vendor else 'N/A'],
        ['Vendor Code', vendor.get('vendor_code', 'N/A') if vendor else 'N/A'],
        ['GST #', vendor.get('gst_number', 'N/A') if vendor else 'N/A'],
        ['Bank Account', vendor.get('bank_account', 'N/A') if vendor else 'N/A'],
    ]
    vendor_table = Table(vendor_info, colWidths=[120, 380])
    vendor_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#f0fdfa')),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#99f6e4')),
    ]))
    elements.append(vendor_table)
    elements.append(Spacer(1, 20))
    
    # Payment Details
    elements.append(Paragraph("PAYMENT DETAILS", heading_style))
    
    payment_details = [
        ['Description', 'Amount'],
        ['PO Total Value', f"₹{po.get('grand_total', 0):,.2f}" if po else 'N/A'],
        ['Payment Type', payment.get('payment_type', '')],
        ['This Payment', f"₹{payment.get('amount', 0):,.2f}"],
        ['Payment Mode', payment.get('payment_mode', '').upper()],
        ['Transaction Ref', payment.get('transaction_ref', 'N/A')],
    ]
    
    if po:
        payment_details.append(['Previous Payments', f"₹{(po.get('amount_paid', 0) - payment.get('amount', 0)):,.2f}"])
        payment_details.append(['Total Paid (After)', f"₹{po.get('amount_paid', 0):,.2f}"])
        payment_details.append(['Outstanding Balance', f"₹{po.get('outstanding_balance', 0):,.2f}"])
    
    payment_table = Table(payment_details, colWidths=[300, 200])
    payment_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0d9488')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#99f6e4')),
        ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
        ('BACKGROUND', (0, 3), (-1, 3), colors.HexColor('#ccfbf1')),
        ('FONTNAME', (0, 3), (-1, 3), 'Helvetica-Bold'),
        ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor('#f0fdf4') if (po and po.get('outstanding_balance', 0) == 0) else colors.HexColor('#fef2f2')),
    ]))
    elements.append(payment_table)
    elements.append(Spacer(1, 25))
    
    # Payment Status Badge
    if po and po.get('outstanding_balance', 0) <= 0:
        status_text = "PO FULLY PAID"
        status_color = '#16a34a'
    else:
        status_text = f"BALANCE REMAINING: ₹{po.get('outstanding_balance', 0):,.2f}" if po else "PARTIAL PAYMENT"
        status_color = '#f59e0b'
    
    status_style = ParagraphStyle('Status', parent=styles['Heading1'], fontSize=12, textColor=colors.white, alignment=TA_CENTER, backColor=colors.HexColor(status_color), borderPadding=10)
    elements.append(Paragraph(status_text, status_style))
    elements.append(Spacer(1, 25))
    
    # Authorized Signature
    sig_data = [
        ['Prepared By', 'Authorized Signatory'],
        [payment.get('initiated_by', ''), '\n\n\n__________________'],
    ]
    sig_table = Table(sig_data, colWidths=[250, 250])
    sig_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
    ]))
    elements.append(sig_table)
    elements.append(Spacer(1, 20))
    
    # Footer
    footer_style = ParagraphStyle('Footer', parent=styles['Normal'], fontSize=8, textColor=colors.grey, alignment=TA_CENTER)
    elements.append(Paragraph("This is a computer-generated receipt.", footer_style))
    elements.append(Paragraph(f"Generated on {datetime.now().strftime('%Y-%m-%d %H:%M')}", footer_style))
    
    doc.build(elements)
    buffer.seek(0)
    
    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=vendor_receipt_{receipt_number}.pdf"}
    )


# ================== PO PDF ==================

@pdf_router.get("/purchase-order/{po_id}")
async def generate_po_pdf(
    po_id: str,
    token: str = None,
    current_user: dict = Depends(get_erp_user)
):
    """Generate Purchase Order PDF"""
    db = get_db()
    
    po = await db.purchase_orders.find_one({"id": po_id}, {"_id": 0})
    if not po:
        raise HTTPException(status_code=404, detail="PO not found")
    
    vendor = await db.vendors.find_one({"id": po.get("vendor_id")}, {"_id": 0})
    
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30)
    
    elements = []
    styles = getSampleStyleSheet()
    
    title_style = ParagraphStyle('Title', parent=styles['Heading1'], fontSize=22, textColor=colors.HexColor('#7c3aed'), spaceAfter=5, alignment=TA_CENTER)
    company_style = ParagraphStyle('Company', parent=styles['Normal'], fontSize=9, textColor=colors.grey, alignment=TA_CENTER, spaceAfter=3)
    heading_style = ParagraphStyle('Heading', parent=styles['Heading2'], fontSize=11, textColor=colors.HexColor('#1e293b'), spaceBefore=12, spaceAfter=6)
    
    # Header with Logo
    logo_img = get_company_logo(width=100, height=50)
    if logo_img:
        elements.append(logo_img)
        elements.append(Spacer(1, 5))
    
    elements.append(Paragraph("LUCUMAA GLASS", title_style))
    elements.append(Paragraph("A Unit of Lucumaa Corporation Pvt. Ltd.", company_style))
    elements.append(Paragraph("Premium Glass Manufacturing", company_style))
    elements.append(Spacer(1, 10))
    
    # Company Address
    company_addr = """
    <b>Factory:</b> Industrial Area, Sector 63, Noida, UP - 201301 | <b>GST:</b> 09AABCL1234A1Z5
    """
    addr_style = ParagraphStyle('Addr', parent=styles['Normal'], fontSize=8, textColor=colors.HexColor('#64748b'), alignment=TA_CENTER, borderPadding=5)
    elements.append(Paragraph(company_addr, addr_style))
    elements.append(Spacer(1, 15))
    
    # PO Title
    po_title = ParagraphStyle('POTitle', parent=styles['Heading1'], fontSize=14, textColor=colors.white, alignment=TA_CENTER, backColor=colors.HexColor('#7c3aed'), borderPadding=10)
    elements.append(Paragraph("PURCHASE ORDER", po_title))
    elements.append(Spacer(1, 20))
    
    # PO Info
    po_info = [
        ['PO #', po.get('po_number', ''), 'Date', po.get('created_at', '')[:10]],
        ['Status', po.get('status', '').upper(), 'Payment Terms', po.get('payment_terms', '')],
    ]
    
    po_table = Table(po_info, colWidths=[80, 170, 80, 170])
    po_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#f5f3ff')),
        ('BACKGROUND', (2, 0), (2, -1), colors.HexColor('#f5f3ff')),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTNAME', (2, 0), (2, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#c4b5fd')),
    ]))
    elements.append(po_table)
    elements.append(Spacer(1, 15))
    
    # Vendor Info
    elements.append(Paragraph("VENDOR DETAILS", heading_style))
    vendor_info = [
        ['Vendor', vendor.get('name', 'N/A') if vendor else 'N/A'],
        ['Company', vendor.get('company_name', 'N/A') if vendor else 'N/A'],
        ['Code', vendor.get('vendor_code', 'N/A') if vendor else 'N/A'],
        ['GST', vendor.get('gst_number', 'N/A') if vendor else 'N/A'],
        ['Phone', vendor.get('phone', 'N/A') if vendor else 'N/A'],
    ]
    v_table = Table(vendor_info, colWidths=[80, 420])
    v_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#f5f3ff')),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('PADDING', (0, 0), (-1, -1), 6),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#c4b5fd')),
    ]))
    elements.append(v_table)
    elements.append(Spacer(1, 15))
    
    # Items
    elements.append(Paragraph("ORDER ITEMS", heading_style))
    
    item_data = [['#', 'Item', 'Qty', 'Unit', 'Rate', 'GST', 'Total']]
    for idx, item in enumerate(po.get('items', []), 1):
        item_data.append([
            str(idx),
            item.get('name', ''),
            str(item.get('quantity', 0)),
            item.get('unit', 'pcs'),
            f"₹{item.get('unit_price', 0):,.2f}",
            f"{item.get('gst_rate', 18)}%",
            f"₹{item.get('total', 0):,.2f}"
        ])
    
    item_table = Table(item_data, colWidths=[30, 180, 50, 50, 70, 50, 70])
    item_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#7c3aed')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('PADDING', (0, 0), (-1, -1), 6),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#c4b5fd')),
        ('ALIGN', (2, 0), (-1, -1), 'RIGHT'),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f5f3ff')]),
    ]))
    elements.append(item_table)
    elements.append(Spacer(1, 10))
    
    # Totals
    totals_data = [
        ['', '', '', '', '', 'Subtotal', f"₹{po.get('subtotal', 0):,.2f}"],
        ['', '', '', '', '', f"GST ({po.get('gst_rate', 18)}%)", f"₹{po.get('gst_amount', 0):,.2f}"],
        ['', '', '', '', '', 'Grand Total', f"₹{po.get('grand_total', 0):,.2f}"],
    ]
    
    totals_table = Table(totals_data, colWidths=[30, 180, 50, 50, 70, 50, 70])
    totals_table.setStyle(TableStyle([
        ('ALIGN', (5, 0), (-1, -1), 'RIGHT'),
        ('FONTNAME', (5, 0), (-1, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('BACKGROUND', (5, -1), (-1, -1), colors.HexColor('#f5f3ff')),
        ('LINEABOVE', (5, 0), (-1, 0), 1, colors.HexColor('#c4b5fd')),
    ]))
    elements.append(totals_table)
    elements.append(Spacer(1, 20))
    
    # Terms
    if po.get('notes'):
        elements.append(Paragraph("NOTES / TERMS", heading_style))
        elements.append(Paragraph(po.get('notes'), ParagraphStyle('Notes', parent=styles['Normal'], fontSize=9)))
        elements.append(Spacer(1, 15))
    
    # Signatures
    sig_data = [
        ['Prepared By', 'Approved By', 'Vendor Acceptance'],
        [po.get('created_by', ''), po.get('approved_by', '') or '___________', '\n\n__________________'],
    ]
    sig_table = Table(sig_data, colWidths=[170, 170, 170])
    sig_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
    ]))
    elements.append(sig_table)
    elements.append(Spacer(1, 15))
    
    # Footer
    footer_style = ParagraphStyle('Footer', parent=styles['Normal'], fontSize=8, textColor=colors.grey, alignment=TA_CENTER)
    elements.append(Paragraph("This is a system-generated Purchase Order.", footer_style))
    
    doc.build(elements)
    buffer.seek(0)
    
    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=PO_{po.get('po_number', po_id)}.pdf"}
    )
