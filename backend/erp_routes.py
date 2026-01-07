"""
ERP Routes - Main router that combines all modular ERP routers
This file serves as the entry point for all ERP API endpoints
"""
from fastapi import APIRouter

# Main ERP router with /api/erp prefix
erp_router = APIRouter(prefix="/api/erp")

# Import modular routers
from routers.base import init_router_dependencies, get_db, get_erp_user
from routers import (
    admin_router,
    crm_router,
    production_router,
    hr_router,
    inventory_router,
    purchase_router,
    accounts_router,
    reports_router,
    payouts_router,
    qr_router,
    sms_router,
    wallet_router,
    expense_router,
    asset_router,
    holiday_router,
    customer_router,
    audit_router,
    superadmin_router,
    sfa_router,
    sfa_expense_router,
    sfa_calls_router
)
from routers.cash_management import cash_router
from routers.pdf_generator import pdf_router
from routers.ai_forecast import forecast_router
from routers.cms import cms_router
from routers.branches import branch_router
from routers.product_config import config_router
from routers.transport import transport_router
from routers.rewards import rewards_router
from routers.gst import gst_router
from routers.job_work import job_work_router
from routers.vendor import vendor_router
from routers.alerts import alerts_router
from routers.ledger import ledger_router
from routers.customer_master import customer_master_router
from routers.glass_configurator import router as glass_config_router

def init_erp_routes(database, auth_dependency):
    """Initialize all ERP routers with database and auth dependencies"""
    # Initialize shared dependencies
    init_router_dependencies(database, auth_dependency)
    
    # Include all modular routers
    erp_router.include_router(admin_router)
    erp_router.include_router(crm_router)
    erp_router.include_router(production_router)
    erp_router.include_router(hr_router)
    erp_router.include_router(inventory_router)
    erp_router.include_router(purchase_router)
    erp_router.include_router(accounts_router)
    erp_router.include_router(reports_router)
    erp_router.include_router(payouts_router)
    erp_router.include_router(qr_router)
    erp_router.include_router(sms_router)
    erp_router.include_router(wallet_router)
    erp_router.include_router(expense_router)
    erp_router.include_router(asset_router)
    erp_router.include_router(holiday_router)
    erp_router.include_router(customer_router)
    erp_router.include_router(audit_router)
    erp_router.include_router(superadmin_router)
    erp_router.include_router(sfa_router)
    erp_router.include_router(sfa_expense_router)
    erp_router.include_router(sfa_calls_router)
    erp_router.include_router(cash_router)
    erp_router.include_router(pdf_router)
    erp_router.include_router(forecast_router)
    erp_router.include_router(cms_router)
    erp_router.include_router(branch_router)
    erp_router.include_router(config_router)
    erp_router.include_router(transport_router)
    erp_router.include_router(rewards_router)
    erp_router.include_router(gst_router)
    erp_router.include_router(job_work_router)
    erp_router.include_router(vendor_router)
    erp_router.include_router(alerts_router)
    erp_router.include_router(ledger_router)
    erp_router.include_router(customer_master_router)
    erp_router.include_router(glass_config_router)
