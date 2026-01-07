"""
Notification utilities - Email, SMS, WhatsApp with fallback support
"""
import os
import aiosmtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from twilio.rest import Client as TwilioClient
import logging

logger = logging.getLogger(__name__)

# Configuration
SMTP_HOST = os.environ.get('SMTP_HOST', 'smtp.hostinger.com')
SMTP_PORT = int(os.environ.get('SMTP_PORT', 465))
SMTP_USER = os.environ.get('SMTP_USER', 'info@lucumaaGlass.in')
SMTP_PASSWORD = os.environ.get('SMTP_PASSWORD', '')
SENDER_EMAIL = os.environ.get('SENDER_EMAIL', 'info@lucumaaGlass.in')
SENDER_NAME = os.environ.get('SENDER_NAME', 'Lucumaa Glass')

TWILIO_ACCOUNT_SID = os.environ.get('TWILIO_ACCOUNT_SID', '')
TWILIO_AUTH_TOKEN = os.environ.get('TWILIO_AUTH_TOKEN', '')
TWILIO_PHONE = os.environ.get('TWILIO_PHONE_NUMBER', '')
TWILIO_WHATSAPP = os.environ.get('TWILIO_WHATSAPP_NUMBER', 'whatsapp:+14155238886')

# Initialize Twilio client if credentials available
twilio_client = None
if TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN:
    try:
        twilio_client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    except Exception as e:
        logger.error(f"Failed to initialize Twilio client: {e}")


async def send_email_notification(recipient: str, subject: str, html_content: str) -> bool:
    """Send email notification via SMTP"""
    try:
        if not SMTP_PASSWORD:
            logger.warning("SMTP password not configured, skipping email")
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
            use_tls=True,
            username=SMTP_USER,
            password=SMTP_PASSWORD
        )
        logger.info(f"Email sent to {recipient}")
        return True
    except Exception as e:
        logger.error(f"Failed to send email to {recipient}: {e}")
        return False


async def send_sms_notification(phone: str, message: str) -> bool:
    """Send SMS notification via Twilio"""
    try:
        if not twilio_client or not TWILIO_PHONE:
            logger.warning("Twilio not configured, skipping SMS")
            return False
        
        # Format phone number
        if not phone.startswith('+'):
            phone = '+91' + phone.lstrip('0')
        
        twilio_client.messages.create(
            body=message,
            from_=TWILIO_PHONE,
            to=phone
        )
        logger.info(f"SMS sent to {phone}")
        return True
    except Exception as e:
        logger.error(f"Failed to send SMS to {phone}: {e}")
        return False


async def send_whatsapp_notification(phone: str, message: str) -> bool:
    """Send WhatsApp notification via Twilio"""
    try:
        if not twilio_client:
            logger.warning("Twilio not configured, skipping WhatsApp")
            return False
        
        # Format phone number for WhatsApp
        if not phone.startswith('+'):
            phone = '+91' + phone.lstrip('0')
        
        twilio_client.messages.create(
            body=message,
            from_=TWILIO_WHATSAPP,
            to=f'whatsapp:{phone}'
        )
        logger.info(f"WhatsApp sent to {phone}")
        return True
    except Exception as e:
        logger.error(f"Failed to send WhatsApp to {phone}: {e}")
        return False


async def send_notification_with_fallback(
    phone: str, 
    message: str, 
    email: str = None,
    email_subject: str = None,
    email_html: str = None,
    prefer_whatsapp: bool = True
) -> dict:
    """
    Send notification with fallback support.
    Priority: WhatsApp -> SMS -> Email
    
    Returns dict with status for each channel attempted.
    """
    result = {
        "whatsapp": {"attempted": False, "success": False},
        "sms": {"attempted": False, "success": False},
        "email": {"attempted": False, "success": False},
        "any_success": False
    }
    
    # Try WhatsApp first if preferred
    if prefer_whatsapp:
        result["whatsapp"]["attempted"] = True
        result["whatsapp"]["success"] = await send_whatsapp_notification(phone, message)
        
        if result["whatsapp"]["success"]:
            result["any_success"] = True
            return result
    
    # Fallback to SMS
    result["sms"]["attempted"] = True
    result["sms"]["success"] = await send_sms_notification(phone, message)
    
    if result["sms"]["success"]:
        result["any_success"] = True
        return result
    
    # If WhatsApp wasn't preferred, try it now
    if not prefer_whatsapp:
        result["whatsapp"]["attempted"] = True
        result["whatsapp"]["success"] = await send_whatsapp_notification(phone, message)
        
        if result["whatsapp"]["success"]:
            result["any_success"] = True
            return result
    
    # Final fallback: Email
    if email and email_subject and email_html:
        result["email"]["attempted"] = True
        result["email"]["success"] = await send_email_notification(email, email_subject, email_html)
        
        if result["email"]["success"]:
            result["any_success"] = True
    
    return result


async def send_payment_due_alert(
    phone: str,
    customer_name: str,
    order_number: str,
    amount_due: float,
    due_date: str,
    days_overdue: int = 0,
    email: str = None
) -> dict:
    """
    Send payment due alert with all channels
    """
    if days_overdue > 0:
        message = f"""🔴 *Payment Overdue Alert*

Dear {customer_name},

Your payment of ₹{amount_due:,.2f} for order {order_number} is overdue by {days_overdue} days.

Please make the payment at your earliest convenience to avoid any service interruptions.

Thank you,
Lucumaa Glass"""
    else:
        message = f"""⏰ *Payment Reminder*

Dear {customer_name},

This is a friendly reminder that your payment of ₹{amount_due:,.2f} for order {order_number} is due on {due_date}.

Please ensure timely payment.

Thank you,
Lucumaa Glass"""
    
    email_subject = f"Payment {'Overdue' if days_overdue > 0 else 'Reminder'} - Order {order_number}"
    email_html = f"""
    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <div style="background: {'#dc2626' if days_overdue > 0 else '#f59e0b'}; color: white; padding: 20px; text-align: center;">
            <h2>{'Payment Overdue' if days_overdue > 0 else 'Payment Reminder'}</h2>
        </div>
        <div style="padding: 20px;">
            <p>Dear {customer_name},</p>
            <p>{'Your payment is overdue by ' + str(days_overdue) + ' days.' if days_overdue > 0 else 'This is a friendly reminder about your upcoming payment.'}</p>
            <div style="background: #f3f4f6; padding: 15px; border-radius: 8px; margin: 20px 0;">
                <p><strong>Order:</strong> {order_number}</p>
                <p><strong>Amount Due:</strong> ₹{amount_due:,.2f}</p>
                <p><strong>Due Date:</strong> {due_date}</p>
            </div>
            <p>Please make the payment at your earliest convenience.</p>
            <p>Thank you for your business!</p>
            <p>Best regards,<br>Lucumaa Glass</p>
        </div>
    </div>
    """
    
    return await send_notification_with_fallback(
        phone=phone,
        message=message,
        email=email,
        email_subject=email_subject,
        email_html=email_html,
        prefer_whatsapp=True
    )


async def send_vendor_payment_due_alert(
    vendor_name: str,
    po_number: str,
    amount_due: float,
    due_date: str,
    days_until_due: int,
    admin_email: str = None
) -> dict:
    """
    Send vendor payment due alert to admin/finance team
    """
    if days_until_due <= 0:
        urgency = "🔴 OVERDUE"
    elif days_until_due <= 3:
        urgency = "🟡 DUE SOON"
    else:
        urgency = "🟢 UPCOMING"
    
    message = f"""{urgency} *Vendor Payment Alert*

Vendor: {vendor_name}
PO: {po_number}
Amount: ₹{amount_due:,.2f}
Due: {due_date}
{'Overdue by ' + str(abs(days_until_due)) + ' days' if days_until_due <= 0 else 'Due in ' + str(days_until_due) + ' days'}

Please process this payment."""
    
    email_subject = f"Vendor Payment {urgency.split()[1]} - {vendor_name}"
    email_html = f"""
    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <div style="background: {'#dc2626' if days_until_due <= 0 else '#f59e0b' if days_until_due <= 3 else '#10b981'}; color: white; padding: 20px; text-align: center;">
            <h2>Vendor Payment {urgency.split()[1]}</h2>
        </div>
        <div style="padding: 20px;">
            <div style="background: #f3f4f6; padding: 15px; border-radius: 8px;">
                <p><strong>Vendor:</strong> {vendor_name}</p>
                <p><strong>PO Number:</strong> {po_number}</p>
                <p><strong>Amount:</strong> ₹{amount_due:,.2f}</p>
                <p><strong>Due Date:</strong> {due_date}</p>
            </div>
            <p style="margin-top: 20px;">Please process this vendor payment to maintain good supplier relationships.</p>
        </div>
    </div>
    """
    
    result = {"email": {"attempted": False, "success": False}, "any_success": False}
    
    if admin_email:
        result["email"]["attempted"] = True
        result["email"]["success"] = await send_email_notification(admin_email, email_subject, email_html)
        result["any_success"] = result["email"]["success"]
    
    return result
