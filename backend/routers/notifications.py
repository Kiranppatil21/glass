"""
Notifications Module - Email templates and notification triggers for ERP events
"""
import aiosmtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
import logging
from datetime import datetime

# SMTP Configuration
SMTP_HOST = os.environ.get('SMTP_HOST', 'smtp.hostinger.com')
SMTP_PORT = int(os.environ.get('SMTP_PORT', 465))
SMTP_USER = os.environ.get('SMTP_USER', '')
SMTP_PASSWORD = os.environ.get('SMTP_PASSWORD', '')
SENDER_EMAIL = os.environ.get('SENDER_EMAIL', 'info@lucumaaglass.in')
SENDER_NAME = os.environ.get('SENDER_NAME', 'Lucumaa Glass ERP')
ADMIN_EMAIL = os.environ.get('ADMIN_EMAIL', 'admin@lucumaaglass.in')


async def send_email(recipient: str, subject: str, html_content: str):
    """Send email using configured SMTP"""
    try:
        if not SMTP_PASSWORD:
            logging.warning("SMTP password not configured, skipping email")
            return False
            
        message = MIMEMultipart('alternative')
        message['Subject'] = subject
        message['From'] = f"{SENDER_NAME} <{SENDER_EMAIL}>"
        message['To'] = recipient
        
        html_part = MIMEText(html_content, 'html')
        message.attach(html_part)
        
        await aiosmtplib.send(
            message,
            hostname=SMTP_HOST,
            port=SMTP_PORT,
            username=SMTP_USER,
            password=SMTP_PASSWORD,
            use_tls=True,
        )
        logging.info(f"✅ Email sent to {recipient}: {subject}")
        return True
    except Exception as e:
        logging.error(f"❌ Email failed to {recipient}: {str(e)}")
        return False


# =============== EMAIL TEMPLATES ===============

def get_base_template(content: str, title: str = "Lucumaa Glass") -> str:
    """Base HTML email template with Lucumaa Glass branding"""
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
    </head>
    <body style="margin: 0; padding: 0; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #f4f4f4;">
        <table width="100%" cellpadding="0" cellspacing="0" style="max-width: 600px; margin: 0 auto; background-color: #ffffff;">
            <!-- Header -->
            <tr>
                <td style="background: linear-gradient(135deg, #0d9488 0%, #0f766e 100%); padding: 30px; text-align: center;">
                    <h1 style="color: #ffffff; margin: 0; font-size: 28px;">Lucumaa Glass</h1>
                    <p style="color: #a7f3d0; margin: 5px 0 0 0; font-size: 14px;">Premium Glass Solutions</p>
                </td>
            </tr>
            <!-- Content -->
            <tr>
                <td style="padding: 30px;">
                    {content}
                </td>
            </tr>
            <!-- Footer -->
            <tr>
                <td style="background-color: #1e293b; padding: 20px; text-align: center;">
                    <p style="color: #94a3b8; margin: 0; font-size: 12px;">
                        © {datetime.now().year} Lucumaa Glass. All rights reserved.
                    </p>
                    <p style="color: #64748b; margin: 10px 0 0 0; font-size: 11px;">
                        Pune, Maharashtra | +91 92847 01985
                    </p>
                </td>
            </tr>
        </table>
    </body>
    </html>
    """


# =============== ORDER NOTIFICATIONS ===============

def get_new_order_email(order_data: dict) -> tuple:
    """Email template for new production order"""
    subject = f"🆕 New Production Order: {order_data.get('job_card_number', 'N/A')}"
    content = f"""
        <h2 style="color: #0d9488; margin-top: 0;">New Production Order Received</h2>
        <div style="background-color: #f0fdfa; padding: 20px; border-radius: 8px; margin: 20px 0;">
            <table width="100%" style="font-size: 14px;">
                <tr>
                    <td style="padding: 8px 0; color: #64748b;">Job Card:</td>
                    <td style="padding: 8px 0; font-weight: bold; color: #0f172a;">{order_data.get('job_card_number', 'N/A')}</td>
                </tr>
                <tr>
                    <td style="padding: 8px 0; color: #64748b;">Glass Type:</td>
                    <td style="padding: 8px 0; color: #0f172a;">{order_data.get('glass_type', 'N/A')}</td>
                </tr>
                <tr>
                    <td style="padding: 8px 0; color: #64748b;">Size:</td>
                    <td style="padding: 8px 0; color: #0f172a;">{order_data.get('width', 0)} x {order_data.get('height', 0)} mm ({order_data.get('thickness', 0)}mm)</td>
                </tr>
                <tr>
                    <td style="padding: 8px 0; color: #64748b;">Quantity:</td>
                    <td style="padding: 8px 0; font-weight: bold; color: #0f172a;">{order_data.get('quantity', 0)} pcs</td>
                </tr>
                <tr>
                    <td style="padding: 8px 0; color: #64748b;">Priority:</td>
                    <td style="padding: 8px 0; color: #0f172a;">{'🔴 High' if order_data.get('priority', 1) >= 3 else '🟡 Normal'}</td>
                </tr>
            </table>
        </div>
        <p style="color: #64748b; font-size: 14px;">Please review and start production at your earliest convenience.</p>
        <a href="https://glassmesh.preview.emergentagent.com/erp/production" style="display: inline-block; background-color: #0d9488; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; margin-top: 15px;">View in ERP</a>
    """
    return subject, get_base_template(content)


# =============== PAYMENT NOTIFICATIONS ===============

def get_payment_confirmation_email(payment_data: dict, invoice_data: dict) -> tuple:
    """Email template for payment confirmation"""
    balance = invoice_data.get('total', 0) - (invoice_data.get('amount_paid', 0) + payment_data.get('amount', 0))
    status = "PAID IN FULL" if balance <= 0 else f"Balance: ₹{balance:,.2f}"
    
    subject = f"✅ Payment Received - {invoice_data.get('invoice_number', 'N/A')}"
    content = f"""
        <h2 style="color: #0d9488; margin-top: 0;">Payment Confirmation</h2>
        <p style="color: #475569;">Thank you for your payment. Here are the details:</p>
        
        <div style="background-color: #f0fdfa; padding: 20px; border-radius: 8px; margin: 20px 0;">
            <table width="100%" style="font-size: 14px;">
                <tr>
                    <td style="padding: 8px 0; color: #64748b;">Invoice Number:</td>
                    <td style="padding: 8px 0; font-weight: bold; color: #0f172a;">{invoice_data.get('invoice_number', 'N/A')}</td>
                </tr>
                <tr>
                    <td style="padding: 8px 0; color: #64748b;">Amount Received:</td>
                    <td style="padding: 8px 0; font-weight: bold; color: #059669; font-size: 18px;">₹{payment_data.get('amount', 0):,.2f}</td>
                </tr>
                <tr>
                    <td style="padding: 8px 0; color: #64748b;">Payment Method:</td>
                    <td style="padding: 8px 0; color: #0f172a;">{payment_data.get('payment_method', 'Cash').upper()}</td>
                </tr>
                <tr>
                    <td style="padding: 8px 0; color: #64748b;">Reference:</td>
                    <td style="padding: 8px 0; color: #0f172a;">{payment_data.get('reference', '-')}</td>
                </tr>
                <tr>
                    <td style="padding: 8px 0; color: #64748b;">Status:</td>
                    <td style="padding: 8px 0; font-weight: bold; color: {'#059669' if balance <= 0 else '#d97706'};">{status}</td>
                </tr>
            </table>
        </div>
        
        <p style="color: #64748b; font-size: 14px;">If you have any questions, please don't hesitate to contact us.</p>
    """
    return subject, get_base_template(content)


# =============== INVOICE NOTIFICATIONS ===============

def get_new_invoice_email(invoice_data: dict) -> tuple:
    """Email template for new invoice"""
    subject = f"📄 Invoice {invoice_data.get('invoice_number', 'N/A')} - Lucumaa Glass"
    
    items_html = ""
    for item in invoice_data.get('items', []):
        items_html += f"""
            <tr>
                <td style="padding: 10px; border-bottom: 1px solid #e2e8f0;">{item.get('description', 'Item')}</td>
                <td style="padding: 10px; border-bottom: 1px solid #e2e8f0; text-align: center;">{item.get('quantity', 0)}</td>
                <td style="padding: 10px; border-bottom: 1px solid #e2e8f0; text-align: right;">₹{item.get('unit_price', 0):,.2f}</td>
                <td style="padding: 10px; border-bottom: 1px solid #e2e8f0; text-align: right;">₹{item.get('quantity', 0) * item.get('unit_price', 0):,.2f}</td>
            </tr>
        """
    
    content = f"""
        <h2 style="color: #0d9488; margin-top: 0;">Invoice</h2>
        <div style="display: flex; justify-content: space-between; margin-bottom: 20px;">
            <div>
                <p style="color: #64748b; margin: 0;">Invoice To:</p>
                <p style="color: #0f172a; font-weight: bold; margin: 5px 0;">{invoice_data.get('customer_name', 'Customer')}</p>
                <p style="color: #64748b; margin: 0; font-size: 13px;">{invoice_data.get('customer_address', '')}</p>
                <p style="color: #64748b; margin: 0; font-size: 13px;">GST: {invoice_data.get('customer_gst', 'N/A')}</p>
            </div>
            <div style="text-align: right;">
                <p style="color: #64748b; margin: 0;">Invoice #:</p>
                <p style="color: #0f172a; font-weight: bold; margin: 5px 0;">{invoice_data.get('invoice_number', 'N/A')}</p>
                <p style="color: #64748b; margin: 0; font-size: 13px;">Date: {invoice_data.get('created_at', '')[:10]}</p>
                <p style="color: #64748b; margin: 0; font-size: 13px;">Due: {invoice_data.get('due_date', 'On Receipt')}</p>
            </div>
        </div>
        
        <table width="100%" style="border-collapse: collapse; margin: 20px 0; font-size: 14px;">
            <thead>
                <tr style="background-color: #0d9488; color: white;">
                    <th style="padding: 12px; text-align: left;">Description</th>
                    <th style="padding: 12px; text-align: center;">Qty</th>
                    <th style="padding: 12px; text-align: right;">Rate</th>
                    <th style="padding: 12px; text-align: right;">Amount</th>
                </tr>
            </thead>
            <tbody>
                {items_html}
            </tbody>
        </table>
        
        <div style="background-color: #f8fafc; padding: 15px; border-radius: 8px; margin-top: 20px;">
            <table width="100%" style="font-size: 14px;">
                <tr>
                    <td style="padding: 5px 0; color: #64748b;">Subtotal:</td>
                    <td style="padding: 5px 0; text-align: right;">₹{invoice_data.get('subtotal', 0):,.2f}</td>
                </tr>
                <tr>
                    <td style="padding: 5px 0; color: #64748b;">CGST (9%):</td>
                    <td style="padding: 5px 0; text-align: right;">₹{invoice_data.get('cgst', 0):,.2f}</td>
                </tr>
                <tr>
                    <td style="padding: 5px 0; color: #64748b;">SGST (9%):</td>
                    <td style="padding: 5px 0; text-align: right;">₹{invoice_data.get('sgst', 0):,.2f}</td>
                </tr>
                <tr style="border-top: 2px solid #0d9488;">
                    <td style="padding: 10px 0; font-weight: bold; color: #0f172a; font-size: 16px;">Total:</td>
                    <td style="padding: 10px 0; text-align: right; font-weight: bold; color: #0d9488; font-size: 18px;">₹{invoice_data.get('total', 0):,.2f}</td>
                </tr>
            </table>
        </div>
        
        <p style="color: #64748b; font-size: 13px; margin-top: 20px;">
            Please make payment to: <br>
            Bank: HDFC Bank | A/C: XXXXXXXXXX | IFSC: HDFC0001234
        </p>
    """
    return subject, get_base_template(content)


# =============== LOW STOCK ALERTS ===============

def get_low_stock_alert_email(low_stock_items: list) -> tuple:
    """Email template for low stock alert"""
    subject = f"⚠️ Low Stock Alert - {len(low_stock_items)} Items Need Attention"
    
    items_html = ""
    for item in low_stock_items:
        items_html += f"""
            <tr>
                <td style="padding: 10px; border-bottom: 1px solid #e2e8f0;">{item.get('name', 'Item')}</td>
                <td style="padding: 10px; border-bottom: 1px solid #e2e8f0; text-align: center;">{item.get('category', 'N/A')}</td>
                <td style="padding: 10px; border-bottom: 1px solid #e2e8f0; text-align: center; color: #dc2626; font-weight: bold;">{item.get('current_stock', 0)} {item.get('unit', 'pcs')}</td>
                <td style="padding: 10px; border-bottom: 1px solid #e2e8f0; text-align: center;">{item.get('minimum_stock', 0)} {item.get('unit', 'pcs')}</td>
            </tr>
        """
    
    content = f"""
        <h2 style="color: #dc2626; margin-top: 0;">⚠️ Low Stock Alert</h2>
        <p style="color: #475569;">The following {len(low_stock_items)} item(s) are running low on stock and need to be reordered:</p>
        
        <table width="100%" style="border-collapse: collapse; margin: 20px 0; font-size: 14px;">
            <thead>
                <tr style="background-color: #fef2f2; color: #991b1b;">
                    <th style="padding: 12px; text-align: left;">Material</th>
                    <th style="padding: 12px; text-align: center;">Category</th>
                    <th style="padding: 12px; text-align: center;">Current Stock</th>
                    <th style="padding: 12px; text-align: center;">Min. Required</th>
                </tr>
            </thead>
            <tbody>
                {items_html}
            </tbody>
        </table>
        
        <a href="https://glassmesh.preview.emergentagent.com/erp/purchase" style="display: inline-block; background-color: #dc2626; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; margin-top: 15px;">Create Purchase Order</a>
    """
    return subject, get_base_template(content)


# =============== OVERDUE INVOICE REMINDER ===============

def get_overdue_invoice_email(invoice_data: dict, days_overdue: int) -> tuple:
    """Email template for overdue invoice reminder"""
    balance = invoice_data.get('total', 0) - invoice_data.get('amount_paid', 0)
    
    subject = f"⏰ Payment Reminder - Invoice {invoice_data.get('invoice_number', 'N/A')} ({days_overdue} days overdue)"
    content = f"""
        <h2 style="color: #d97706; margin-top: 0;">Payment Reminder</h2>
        <p style="color: #475569;">This is a friendly reminder that the following invoice is overdue:</p>
        
        <div style="background-color: #fffbeb; padding: 20px; border-radius: 8px; margin: 20px 0; border-left: 4px solid #d97706;">
            <table width="100%" style="font-size: 14px;">
                <tr>
                    <td style="padding: 8px 0; color: #64748b;">Invoice Number:</td>
                    <td style="padding: 8px 0; font-weight: bold; color: #0f172a;">{invoice_data.get('invoice_number', 'N/A')}</td>
                </tr>
                <tr>
                    <td style="padding: 8px 0; color: #64748b;">Due Date:</td>
                    <td style="padding: 8px 0; color: #dc2626; font-weight: bold;">{invoice_data.get('due_date', 'N/A')}</td>
                </tr>
                <tr>
                    <td style="padding: 8px 0; color: #64748b;">Days Overdue:</td>
                    <td style="padding: 8px 0; color: #dc2626; font-weight: bold;">{days_overdue} days</td>
                </tr>
                <tr>
                    <td style="padding: 8px 0; color: #64748b;">Amount Due:</td>
                    <td style="padding: 8px 0; font-weight: bold; color: #dc2626; font-size: 18px;">₹{balance:,.2f}</td>
                </tr>
            </table>
        </div>
        
        <p style="color: #64748b; font-size: 14px;">Please arrange payment at your earliest convenience. If you have already made the payment, please ignore this reminder.</p>
        
        <p style="color: #64748b; font-size: 13px; margin-top: 20px;">
            Payment Details: <br>
            Bank: HDFC Bank | A/C: XXXXXXXXXX | IFSC: HDFC0001234
        </p>
    """
    return subject, get_base_template(content)


# =============== NOTIFICATION TRIGGERS ===============

async def notify_new_order(order_data: dict, admin_email: str = None):
    """Send notification for new production order"""
    email = admin_email or ADMIN_EMAIL
    if email:
        subject, html = get_new_order_email(order_data)
        await send_email(email, subject, html)


async def notify_payment_received(payment_data: dict, invoice_data: dict, customer_email: str = None):
    """Send payment confirmation to customer"""
    if customer_email:
        subject, html = get_payment_confirmation_email(payment_data, invoice_data)
        await send_email(customer_email, subject, html)


async def notify_new_invoice(invoice_data: dict, customer_email: str = None):
    """Send invoice to customer"""
    if customer_email:
        subject, html = get_new_invoice_email(invoice_data)
        await send_email(customer_email, subject, html)


async def notify_low_stock(low_stock_items: list, admin_email: str = None):
    """Send low stock alert to admin"""
    if low_stock_items:
        email = admin_email or ADMIN_EMAIL
        if email:
            subject, html = get_low_stock_alert_email(low_stock_items)
            await send_email(email, subject, html)


async def notify_overdue_invoice(invoice_data: dict, days_overdue: int, customer_email: str = None):
    """Send overdue reminder to customer"""
    if customer_email:
        subject, html = get_overdue_invoice_email(invoice_data, days_overdue)
        await send_email(customer_email, subject, html)
