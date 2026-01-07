# ERP Routers Module
# Each router handles a specific domain of the ERP system

from .admin import admin_router
from .crm import crm_router
from .production import production_router
from .hr import hr_router
from .inventory import inventory_router
from .purchase import purchase_router
from .accounts import accounts_router
from .reports import reports_router
from .payouts import payouts_router
from .qr_codes import qr_router
from .wallet import wallet_router
from .sms import sms_router
from .customer import customer_router
from .expenses import expense_router
from .assets import asset_router
from .holidays import holiday_router
from .audit import audit_router
from .superadmin import superadmin_router
from .sfa import sfa_router
from .sfa_expense import sfa_expense_router
from .sfa_calls import sfa_calls_router

__all__ = [
    'admin_router',
    'crm_router', 
    'production_router',
    'hr_router',
    'inventory_router',
    'purchase_router',
    'accounts_router',
    'reports_router',
    'payouts_router',
    'qr_router',
    'wallet_router',
    'sms_router',
    'customer_router',
    'expense_router',
    'asset_router',
    'holiday_router',
    'audit_router',
    'superadmin_router',
    'sfa_router',
    'sfa_expense_router',
    'sfa_calls_router'
]
