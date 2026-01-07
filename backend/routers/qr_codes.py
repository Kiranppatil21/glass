"""
QR Code and Barcode Generation Router
Generates QR codes for job cards, invoices, and tracking
"""
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from typing import Optional
import qrcode
from qrcode.image.styledpil import StyledPilImage
from qrcode.image.styles.moduledrawers import RoundedModuleDrawer
import barcode
from barcode.writer import ImageWriter
import io
import base64
from .base import get_erp_user, get_db

qr_router = APIRouter(prefix="/qr", tags=["QR & Barcode"])


def generate_qr_code(data: str, size: int = 200) -> bytes:
    """Generate a styled QR code"""
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_H,
        box_size=10,
        border=2,
    )
    qr.add_data(data)
    qr.make(fit=True)
    
    img = qr.make_image(
        image_factory=StyledPilImage,
        module_drawer=RoundedModuleDrawer(),
        fill_color="#0d9488",
        back_color="white"
    )
    
    # Resize
    img = img.resize((size, size))
    
    # Convert to bytes
    buffer = io.BytesIO()
    img.save(buffer, format='PNG')
    buffer.seek(0)
    return buffer.getvalue()


def generate_barcode(data: str) -> bytes:
    """Generate a Code128 barcode"""
    code128 = barcode.get_barcode_class('code128')
    
    buffer = io.BytesIO()
    code128(data, writer=ImageWriter()).write(buffer, options={
        'module_width': 0.4,
        'module_height': 15,
        'font_size': 10,
        'text_distance': 5,
        'quiet_zone': 6
    })
    buffer.seek(0)
    return buffer.getvalue()


@qr_router.get("/job-card/{job_card_number}")
async def get_job_card_qr(
    job_card_number: str,
    size: int = 200,
    current_user: dict = Depends(get_erp_user)
):
    """Generate QR code for a job card"""
    db = get_db()
    
    # Verify job card exists
    order = await db.production_orders.find_one({"job_card_number": job_card_number}, {"_id": 0})
    if not order:
        raise HTTPException(status_code=404, detail="Job card not found")
    
    # QR data contains URL to track the job
    base_url = "https://glassmesh.preview.emergentagent.com"
    qr_data = f"{base_url}/track?job={job_card_number}"
    
    qr_bytes = generate_qr_code(qr_data, size)
    
    return StreamingResponse(
        io.BytesIO(qr_bytes),
        media_type="image/png",
        headers={"Content-Disposition": f"inline; filename=qr_{job_card_number}.png"}
    )


@qr_router.get("/job-card/{job_card_number}/barcode")
async def get_job_card_barcode(
    job_card_number: str,
    current_user: dict = Depends(get_erp_user)
):
    """Generate barcode for a job card"""
    db = get_db()
    
    order = await db.production_orders.find_one({"job_card_number": job_card_number}, {"_id": 0})
    if not order:
        raise HTTPException(status_code=404, detail="Job card not found")
    
    barcode_bytes = generate_barcode(job_card_number)
    
    return StreamingResponse(
        io.BytesIO(barcode_bytes),
        media_type="image/png",
        headers={"Content-Disposition": f"inline; filename=barcode_{job_card_number}.png"}
    )


@qr_router.get("/invoice/{invoice_number}")
async def get_invoice_qr(
    invoice_number: str,
    size: int = 150,
    current_user: dict = Depends(get_erp_user)
):
    """Generate QR code for an invoice"""
    db = get_db()
    
    invoice = await db.invoices.find_one({"invoice_number": invoice_number}, {"_id": 0})
    if not invoice:
        raise HTTPException(status_code=404, detail="Invoice not found")
    
    # QR contains invoice details for verification
    qr_data = f"INV:{invoice_number}|AMT:{invoice.get('total', 0)}|DATE:{invoice.get('created_at', '')[:10]}"
    
    qr_bytes = generate_qr_code(qr_data, size)
    
    return StreamingResponse(
        io.BytesIO(qr_bytes),
        media_type="image/png",
        headers={"Content-Disposition": f"inline; filename=qr_{invoice_number}.png"}
    )


@qr_router.get("/order/{order_id}")
async def get_order_tracking_qr(
    order_id: str,
    size: int = 200
):
    """Generate QR code for customer order tracking (public)"""
    db = get_db()
    
    order = await db.orders.find_one({"id": order_id}, {"_id": 0})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    base_url = "https://glassmesh.preview.emergentagent.com"
    qr_data = f"{base_url}/track-order?id={order_id}"
    
    qr_bytes = generate_qr_code(qr_data, size)
    
    return StreamingResponse(
        io.BytesIO(qr_bytes),
        media_type="image/png",
        headers={"Content-Disposition": f"inline; filename=qr_order_{order_id[:8]}.png"}
    )


@qr_router.get("/job-card/{job_card_number}/print-data")
async def get_job_card_print_data(
    job_card_number: str,
    current_user: dict = Depends(get_erp_user)
):
    """Get all data needed for printing a job card label"""
    db = get_db()
    
    order = await db.production_orders.find_one({"job_card_number": job_card_number}, {"_id": 0})
    if not order:
        raise HTTPException(status_code=404, detail="Job card not found")
    
    # Generate QR code as base64
    base_url = "https://glassmesh.preview.emergentagent.com"
    qr_data = f"{base_url}/track?job={job_card_number}"
    qr_bytes = generate_qr_code(qr_data, 150)
    qr_base64 = base64.b64encode(qr_bytes).decode('utf-8')
    
    # Generate barcode as base64
    barcode_bytes = generate_barcode(job_card_number)
    barcode_base64 = base64.b64encode(barcode_bytes).decode('utf-8')
    
    return {
        "job_card_number": job_card_number,
        "glass_type": order.get("glass_type"),
        "thickness": order.get("thickness"),
        "dimensions": f"{order.get('width')} x {order.get('height')} mm",
        "quantity": order.get("quantity"),
        "current_stage": order.get("current_stage"),
        "priority": order.get("priority"),
        "created_at": order.get("created_at", "")[:10],
        "qr_code_base64": f"data:image/png;base64,{qr_base64}",
        "barcode_base64": f"data:image/png;base64,{barcode_base64}"
    }
