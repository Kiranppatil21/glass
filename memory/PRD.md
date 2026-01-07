# Lucumaa Glass ERP System - PRD

## Original Problem Statement
Build a comprehensive Glass Factory ERP + CRM + Automation System for Lucumaa Glass. The system covers:
- CRM & Sales Pipeline
- Production Management with Job Cards
- HR & Payroll Management with Attendance
- Inventory Management
- Purchase Order Management
- Accounting & Finance with GST
- Real-time Admin Dashboard with Charts
- Operator Workstation Interface
- Email Notifications for key events
- **3D Glass Configurator with Interactive Booking (NEW!)**
- **Job Work 3D Configurator with Transport Options (NEW!)**

## User Personas
1. **Admin/Owner**: Full access to all modules, approvals, analytics, **pricing configuration**
2. **Sales Team**: CRM access, lead management, quotations, **3D glass configuration**
3. **Production Manager**: Production orders, job cards, breakage tracking
4. **Operators**: Station-based work queue, stage completion
5. **HR/Accountant**: Employee management, salary processing, attendance
6. **Store Manager**: Inventory management, stock tracking
7. **Customers**: **3D glass booking, quotation requests, order tracking**
8. **Marketing Team**: **Quotation generation, customer engagement**

## Core Requirements
- Real-time production tracking through stages
- Breakage/wastage management
- Role-based dashboards
- Automated salary calculation with attendance
- Inventory management with low-stock alerts
- Purchase order workflow with auto-inventory update
- Invoice generation with GST (CGST/SGST/IGST)
- Payment tracking and ledger
- **Dashboard Charts & Analytics**
- **Email Notifications**
- **3D Glass Configurator with Babylon.js (NEW!)**
- **Admin-Configurable Pricing (NEW!)**
- **Hole/Cut-out Placement with Real-time Measurements (NEW!)**
- **Quotation Generation with Validity (7/15 days) (NEW!)**

---

## What's Been Implemented

### 3D Glass Configurator (`/customize`) - COMPLETE WITH PRODUCTION MODE

**Core Features:**
- **Babylon.js 3D** live preview with interactive glass model
- **Click-to-Place Cutouts**: Select shape type (Hole, Rectangle, Triangle, Hexagon, Heart) and click on glass to place
- **Gizmo-based Interaction**: 8 resize handles + rotation handle for precise editing
- **Admin-configurable pricing** (no hardcoding):
  - Glass types: Toughened (₹85/sq.ft), Laminated (₹120/sq.ft), Frosted (₹95/sq.ft)
  - Thickness: 5mm (1.0x), 6mm (1.1x), 8mm (1.25x), 10mm (1.4x), 12mm (1.6x)
  - Colors: Clear (0%), Grey (+10%), Bronze (+15%), Blue (+15%), Green (+10%)
  - Applications: Window, Door, Partition, Railing (+10%), Shower (+15%)
  - Hole/cut-out shape + size slab pricing

**Production Mode Features (NEW!):**
- **Numbered Cutouts**: Each cutout labeled with index (H1, R2, T1, etc.)
- **Center Marks**: Red crosshair (+) at cutout center
- **Dimension Lines**: Edge distance labels (←, →, ↑, ↓) showing mm from all 4 edges
- **Inner Dimensions**: Size displayed inside cutout (Ø50 for holes, 100×80 for rectangles)
- **Grid Overlay**: Toggleable measurement grid on glass
- **High Contrast Mode**: B/W mode for production clarity
- **PDF Export**: Download technical specification sheet with 2D drawing and cutout table
- **Multi-page PDF**: Pagination for orders with >10 cutouts (NEW!)

**Share Configuration Feature (NEW!):**
- **Share Button**: Creates shareable link with 7-day expiry
- **Share Modal**: Options for Copy Link, WhatsApp, Email
- **QR Code Generation**: Scannable QR code for showroom samples (NEW!)
- **Download QR**: Save QR code as PNG for printing (NEW!)
- **Shared Config Page** (`/share/:shareId`): Public page showing 3D preview, specs, cutouts
- **Edit & Order**: Recipients can edit configuration and place orders
- **View Tracking**: Counts views for each shared link

**Fixed Glass Preview (Production Accuracy - NEW!):**
- **Fixed Visual Size**: Glass maintains same display size regardless of actual mm dimensions
- **Dimension Labels**: Width shown below glass, height shown on right side
- **Accurate Cutout Positions**: Cutouts positioned proportionally for production printing
- **No Scaling**: Glass doesn't shrink/grow when user changes dimensions

**Email Quotation (NEW!):**
- **Send via Email**: Button on quotations to email PDF to customer
- **PDF Attachment**: Quotation details with price breakdown
- **CC Sales Team**: Automatically CC sales team on emails
- **sent_at Tracking**: Records when quotation was emailed

**Advanced Features:**
- **Quick Size Presets**: Common hole sizes (Ø5, Ø8, Ø10... Ø50)
- **Template Library**: Pre-configured cutouts (Door Handle Hole, Hinge Cutout, Lock Cutout, etc.)
- **Snap-to-Grid**: Configurable grid sizes (5mm, 10mm, 20mm, 50mm)
- **Duplicate Cutout**: One-click duplication
- **Precise Dimension Inputs**: Type exact dimensions and positions
- **Corner Notch**: L-shaped cutouts at corners (TL, TR, BL, BR)
- **Freeform Polygon Drawing**: Draw custom shapes by clicking points on glass with visual preview (green first point, teal lines) (ENHANCED!)
- **3D View Rotation**: Front, Angle, Top view controls
- **Large Preview Toggle**: Increase canvas size for printing
- **Measurement Tool**: Click two points to measure exact distance in mm
- **Auto-Align Feature**: Snap selected cutout to glass edges/center - Left, Right, Top, Bottom, H-Center, V-Center, Center Both (NEW!)
- **Door Fittings Templates**: Pre-configured hardware templates categorized by type (NEW!):
  - Handles: Center, Upper, Lower (Ø35mm holes at standard positions)
  - Locks: Center Lock, Floor Lock
  - Hinges: Top, Middle, Bottom (80×25mm rectangles)
  - Floor Spring/Pivot: Center (50×180mm), Floor Pivot Hole (Ø25mm), Top Pivot Hole (Ø25mm)

**Interaction:**
- **Drag to Move**: Smooth cutout repositioning
- **Resize Handles**: 8 corner/edge handles for resizing
- **Rotation Handle**: Green handle for 0-360° rotation
- **Stable Glass**: Glass panel never blinks or re-renders during interactions

**API Endpoints:**
- `GET /api/erp/glass-config/pricing` - Public pricing configuration
- `PUT /api/erp/glass-config/pricing` - Admin pricing update
- `POST /api/erp/glass-config/calculate-price` - Live price calculation
- `POST /api/erp/glass-config/export-pdf` - Export PDF specification sheet
- `POST /api/erp/glass-config/export-pdf-multipage` - Multi-page PDF for large orders (NEW!)
- `POST /api/erp/glass-config/share` - Create shareable link (NEW!)
- `GET /api/erp/glass-config/share/{share_id}` - Get shared configuration (NEW!)
- `POST /api/erp/glass-config/share/{share_id}/order` - Order from shared config (NEW!)
- `POST /api/erp/glass-config/quotation` - Create quotation
- `GET /api/erp/glass-config/quotations` - List quotations
- `GET /api/erp/glass-config/quotation/{id}` - Get quotation details
- `POST /api/erp/glass-config/quotation/{id}/send-email` - Email quotation with PDF (NEW!)
- `POST /api/erp/glass-config/quotation/{id}/send` - Mark quotation as sent
- `POST /api/erp/glass-config/quotation/{id}/approve` - Approve quotation
- `POST /api/erp/glass-config/quotation/{id}/reject` - Reject quotation
- `POST /api/erp/glass-config/quotation/{id}/convert-to-order` - Convert to order
- `GET /api/erp/glass-config/audit-trail` - Audit trail

### Job Work 3D Configurator (`/job-work`) - COMPLETE WITH PRODUCTION MODE

**Core Features:**
- **Babylon.js 3D** live preview with orange-themed UI
- **Job types**: Glass Toughening, Lamination, Beveling, Drilling & Holes, Edge Processing
- **Labour rates by thickness** (₹8-35/sq.ft) with dropdown showing rates
- **Click-to-Place Cutouts**: Same interaction model as Glass Configurator
- **Live price calculation** with real-time updates

**Production Mode Features (Same as Glass Configurator):**
- Numbered cutouts, center marks, dimension lines, grid overlay, high contrast mode, PDF export

**Transport Options:**
- **Need Pickup**: Checkbox + distance input, Base ₹300 + ₹12/km
- **Need Delivery**: Checkbox + distance input, Base ₹300 + ₹12/km
- **Labour charges breakdown** showing job type, pickup, delivery, subtotal, GST

**API Endpoints:**
- `GET /api/erp/glass-config/job-work/labour-rates` - Get labour rates
- `PUT /api/erp/glass-config/job-work/labour-rates` - Update labour rates (Admin)

### Backend (FastAPI) - All Endpoints Working

**Admin Module:**
- `/api/erp/admin/dashboard` - Real-time metrics
- `/api/erp/admin/charts/revenue` - Monthly revenue & collections chart data (NEW!)
- `/api/erp/admin/charts/production` - Production stage distribution (NEW!)
- `/api/erp/admin/charts/expenses` - Monthly expense breakdown (NEW!)
- `/api/erp/admin/charts/top-customers` - Top 5 customers by revenue (NEW!)

**CRM Module:**
- `/api/erp/crm/leads` - CRUD operations for leads
- `/api/erp/crm/leads/{id}/status` - Lead status update

**Production Module:**
- `/api/erp/production/orders` - Production order management
- `/api/erp/production/orders/{id}/stage` - Stage transitions
- `/api/erp/production/breakage` - Breakage recording
- `/api/erp/production/breakage/analytics` - Breakage analytics

**HR Module:**
- `/api/erp/hr/employees` - Employee CRUD
- `/api/erp/hr/salary` - Salary records
- `/api/erp/hr/salary/calculate/{emp_id}` - Salary calculation
- `/api/erp/hr/salary/approve/{salary_id}` - Salary approval
- `/api/erp/hr/attendance` - Mark/Update attendance (NEW!)
- `/api/erp/hr/attendance/summary` - Monthly summary (NEW!)

**Inventory Module:**
- `/api/erp/inventory/materials` - Material CRUD
- `/api/erp/inventory/transactions` - Stock In/Out
- `/api/erp/inventory/low-stock` - Low stock alerts

**Purchase Module:**
- `/api/erp/purchase/suppliers` - Supplier CRUD
- `/api/erp/purchase/orders` - Purchase order CRUD
- `/api/erp/purchase/orders/{id}/status` - PO status workflow

**Accounts Module (ENHANCED!):**
- `/api/erp/accounts/dashboard` - Financial metrics
- `/api/erp/accounts/invoices` - Invoice CRUD
- `/api/erp/accounts/invoices/{id}/payment` - Payment recording
- `/api/erp/accounts/ledger` - Ledger entries
- `/api/erp/accounts/gst-report` - Monthly GST report
- `/api/erp/accounts/profit-loss` - P&L report with date range (NEW!)
- `/api/erp/reports/invoices/export` - Invoice export (Excel/PDF) (NEW!)
- `/api/erp/reports/profit-loss/export` - P&L export (Excel/PDF) (NEW!)
- `/api/erp/reports/ledger/export` - Ledger export (Excel) (NEW!)
- `/api/erp/reports/payments/export` - Payments export (Excel) (NEW!)

### Frontend Dashboards (14 Complete Dashboards)
1. **Admin Dashboard** (`/erp/admin`) - Real-time metrics & alerts
2. **CRM Dashboard** (`/erp/crm`) - Lead management with status dropdown
3. **Production Dashboard** (`/erp/production`) - Job card management
4. **HR Dashboard** (`/erp/hr`) - Employee, Attendance, Payroll (3 tabs)
5. **Operator Dashboard** (`/erp/operator`) - Station-based work
6. **Inventory Dashboard** (`/erp/inventory`) - Material & stock tracking
7. **Purchase Dashboard** (`/erp/purchase`) - Supplier & PO management
8. **Accounts Dashboard** (`/erp/accounts`) - Enhanced with 4 tabs
9. **Payouts Dashboard** (`/erp/payouts`) - Salary disbursement via Razorpay
10. **Wallet Dashboard** (`/erp/wallet`) - Refer & Earn management
11. **Expense Dashboard** (`/erp/expenses`) - Daily expense tracking (4 tabs)
12. **Assets Dashboard** (`/erp/assets`) - Asset management (4 tabs)
13. **Order Management** (`/erp/orders`) - Cash payment, transport, dispatch management
14. **Payment Settings** (`/erp/settings`) - Advance payment rules configuration (NEW!)
13. **Holidays Dashboard** (`/erp/holidays`) - Holiday calendar (4 tabs) (NEW!)
   - Overview: Monthly Sales, Receivables, Overdue, GST Collected, Recent Invoices
   - Invoices: Full invoice list with status filters (All/Pending/Partial/Paid)
   - Profit & Loss: Complete P&L statement with date range, export buttons
   - Reports & Export: Download reports in Excel/PDF (Invoice, P&L, Ledger, Payments)

### Key Features Implemented
- **ERP Sidebar Navigation**: Collapsible sidebar with all 8 modules, colored icons, active state highlighting
- **Role-Based Access Control**: Different user roles see only their permitted modules
  - Admin/Owner: Full access to all 8 modules
  - Manager: Dashboard, CRM, Production, Inventory, Purchase, HR
  - Sales: CRM only
  - Production Manager: Dashboard, Production, Operator, Inventory
  - Operator: Operator dashboard only
  - HR: HR & Accounts
  - Accountant: Accounts & HR
  - Store: Inventory & Purchase
- **Lead Status Update**: Click status badge on CRM card to change status
- **Attendance Tracking**: Mark P/A/H/L with monthly summary table
- **Invoice with GST**: Auto-calculates CGST/SGST (18%) or IGST for interstate
- **Payment Recording**: Tracks partial/full payments with ledger entries
- **GST Report**: Monthly output GST, input credit, net liability
- **Profit & Loss Report**: Revenue, COGS, Gross Profit, Operating Expenses, Net Profit with margin %
- **Excel/PDF Export**: Download financial reports for any date range

### Integrations Active
- **Razorpay** (LIVE keys): Payment processing
- **Hostinger SMTP**: Email notifications

---

## Test Status
- **Backend Tests**: 150/150 tests passed (100%) - Including 23 new Assets/Holidays/Expenses tests
- **Frontend**: All 12 dashboards functional and verified
- **Test files**: 
  - `/app/tests/test_erp_api.py`
  - `/app/tests/test_inventory_purchase.py`
  - `/app/tests/test_accounts_attendance.py`
  - `/app/tests/test_accounts_dashboard_exports.py`
  - `/app/tests/test_admin_charts.py`
  - `/app/tests/test_payouts.py`
  - `/app/tests/test_qr_codes.py`
  - `/app/tests/test_wallet.py`
  - `/app/tests/test_otp_customer_portal.py`
  - `/app/tests/test_assets_holidays_expenses.py` (NEW!)
- **Test Reports**: `/app/test_reports/iteration_1-11.json`

---

## Remaining Tasks (Prioritized)

### P0 - Critical (Technical Debt)
- [x] ~~Refactor `erp_routes.py` into modular routers~~ ✅ DONE (Jan 1, 2026)

### P1 - High Priority
- [x] ~~Email notifications on key events~~ ✅ DONE (Jan 1, 2026)
- [x] ~~Dashboard charts and analytics visualizations~~ ✅ DONE (Jan 1, 2026)
- [x] ~~QR code/Barcode generation for job cards~~ ✅ DONE (Jan 1, 2026)
- [x] ~~Print functionality for all dashboards~~ ✅ DONE (Jan 1, 2026)

### P2 - Medium Priority
- [x] ~~Automated salary payment via Razorpay Payouts~~ ✅ DONE (Jan 1, 2026)
- [x] ~~Refer and Earn System (Wallet, Referral Tracking, Admin Configuration)~~ ✅ DONE (Jan 1, 2026)
- [x] ~~Customer/Dealer portal with order tracking~~ ✅ DONE (Jan 1, 2026)

### P3 - Future/Backlog
- [x] ~~SMS/WhatsApp notifications (Twilio integration)~~ ✅ DONE (Jan 1, 2026)
- [x] ~~Assets Dashboard (Frontend for existing backend)~~ ✅ DONE (Jan 1, 2026)
- [x] ~~Holidays Dashboard (Frontend for existing backend)~~ ✅ DONE (Jan 1, 2026)
- [x] ~~Admin Settings UI for Advance Payment Rules~~ ✅ DONE (Jan 2, 2026)
- [x] ~~Auto Email/WhatsApp Daily P&L Reports (5 AM IST)~~ ✅ DONE (Jan 2, 2026)
- [x] ~~Weekly & Monthly P&L Reports~~ ✅ DONE (Jan 2, 2026)
- [x] ~~Dispatch Slip PDF Generation~~ ✅ DONE (Jan 2, 2026)
- [x] ~~Cash Day Book PDF Print~~ ✅ DONE (Jan 2, 2026)
- [x] ~~Invoice PDF Generation~~ ✅ DONE (Jan 2, 2026)
- [x] ~~QR Codes in PDFs for Order Tracking~~ ✅ DONE (Jan 2, 2026)
- [x] ~~Website Updates: Industries, How It Works, Wholesale Pricing, Contact~~ ✅ DONE (Jan 2, 2026)
- [x] ~~AI Chat Support (Emergent LLM Key + GPT-4o-mini)~~ ✅ DONE (Jan 2, 2026)
- [x] ~~Remove About Section~~ ✅ DONE (Jan 2, 2026)
- [x] ~~Shopping Cart functionality~~ ✅ DONE (Jan 2, 2026)
- [x] ~~AI-based demand forecasting~~ ✅ DONE (Jan 2, 2026)
- [x] ~~CMS - Content Management System~~ ✅ DONE (Jan 2, 2026)
- [x] ~~Multi-branch support~~ ✅ DONE (Jan 2, 2026)
- [x] ~~SEO (sitemap, robots.txt, RSS)~~ ✅ DONE (Jan 2, 2026)
- [x] ~~Product Configuration (Glass Types, Thickness, Colors)~~ ✅ DONE (Jan 2, 2026)
- [x] ~~Color Selection in Customer Order Builder~~ ✅ DONE (Jan 2, 2026)
- [x] ~~Razorpay Advance Payment Amount Fix~~ ✅ DONE (Jan 2, 2026)
- [x] ~~Transport Management System~~ ✅ DONE (Jan 2, 2026)
  - Transport checkbox on customer order page
  - Auto distance calculation (Factory to Delivery)
  - Load-based pricing with GST
  - Vehicle & Driver management
  - Dispatch tracking with customer notifications
  - Dispatch modal in Order Management (vehicle/driver selection)
  - Dispatch slip PDF with driver/vehicle details
- [x] ~~Rewards & Referral System~~ ✅ DONE (Jan 2, 2026)
  - Referral codes for customers (e.g., TEST4239)
  - Referrer rewards (5% of first order as credit)
  - Referee discount (10% on first order)
  - Reward points on orders (1 point/₹, 10 points = ₹1)
  - Admin credit adjustment
  - Top referrers leaderboard
  - **Customer Portal Integration:**
    - Refer & Earn section with referral code
    - Wallet balance display
    - Copy & Share referral code buttons
  - **Checkout Integration:**
    - Apply Rewards section in Step 2
    - Redeem credit/points during booking
    - "Use All" button for quick redemption
- [x] ~~Payment Validation for Dispatch~~ ✅ DONE (Jan 2, 2026)
  - Dispatch slip blocked without payment
  - Dispatch order blocked without payment
  - Clear "Payment Pending" warning in UI
- [x] ~~GST & HSN Code Integration~~ ✅ DONE (Jan 2, 2026)
  - Auto GST calculation (CGST+SGST for same state, IGST for different state)
  - HSN Code management for glass products (7003-7009)
  - Company state: Maharashtra (27), Default GST: 18%
  - Delivery State dropdown on customer order page
  - GST breakdown in order summary
  - Customer GSTIN input (optional)
  - GST Management dashboard at /erp/gst
- [ ] Mobile app version (SFA)
- [ ] Custom date range for P&L reports on demand
- [ ] Refactor server.py into smaller routers

---

## Technical Architecture

```
/app/
├── backend/
│   ├── server.py          # Main FastAPI + website routes
│   ├── erp_routes.py      # Slim entry point (~40 lines)
│   ├── routers/           # Modular ERP routers
│   │   ├── __init__.py    # Router exports
│   │   ├── base.py        # Shared dependencies
│   │   ├── admin.py       # Admin dashboard (~57 lines)
│   │   ├── crm.py         # CRM & leads (~76 lines)
│   │   ├── production.py  # Job cards & breakage (~153 lines)
│   │   ├── hr.py          # Employees, salary, attendance (~237 lines)
│   │   ├── inventory.py   # Materials & stock (~165 lines)
│   │   ├── purchase.py    # Suppliers & POs (~152 lines)
│   │   ├── accounts.py    # Invoices, payments, P&L (~369 lines)
│   │   ├── reports.py     # Excel/PDF exports (~422 lines)
│   │   ├── payouts.py     # Razorpay Payouts
│   │   ├── qr_codes.py    # QR/Barcode generation
│   │   ├── wallet.py      # Refer & Earn
│   │   ├── sms.py         # SMS/WhatsApp
│   │   ├── gst.py         # GST & HSN Management (NEW!)
│   │   └── customer.py    # Customer portal
│   └── .env               # DB, Email, Payment configs
├── frontend/
│   ├── src/
│   │   ├── pages/erp/     # 9 ERP dashboard components
│   │   ├── components/    # ERPLayout.js for sidebar
│   │   └── utils/erpApi.js # ERP API client (with QR methods)
│   └── .env               # Frontend configs
└── tests/
    ├── test_erp_api.py
    ├── test_inventory_purchase.py
    ├── test_accounts_attendance.py
    ├── test_accounts_dashboard_exports.py
    └── test_qr_codes.py   # NEW: QR/Barcode tests
```

## Database Collections
- `leads` - CRM leads
- `employees` - HR employee records
- `production_orders` - Job cards
- `breakage_entries` - Wastage records
- `salary_payments` - Payroll records
- `attendance` - Daily attendance
- `raw_materials` - Inventory items
- `inventory_transactions` - Stock movements
- `suppliers` - Vendor records
- `purchase_orders` - PO records
- `invoices` - Sales invoices
- `payments` - Payment records
- `ledger` - Accounting ledger

---

*Last updated: January 2, 2026*

### Bug Fixes (Jan 2, 2026 - Latest Session)
- **Color Dropdown Missing**: Fixed - Added color selection dropdown to CustomizeBook.js
  - Colors fetched from `/api/erp/config/all` endpoint
  - Shows 5 colors: Clear, Grey (+10%), Bronze (+15%), Green (+10%), Blue (+15%)
  - Color hex swatches displayed on buttons
  - Color included in order summary and notes
- **Razorpay Advance Amount**: Fixed - Backend correctly uses `override_total` for multi-item orders
  - Frontend sends cart total as `override_total`
  - Backend calculates `advance_amount = override_total * advance_percent / 100`
  - Verified: 50% advance on ₹40,050 = ₹20,025 correctly

### New Files Created (Jan 2, 2026)
- `/app/frontend/src/pages/erp/ForecastDashboard.js` - AI Demand Forecasting dashboard
- `/app/frontend/src/pages/erp/CMSDashboard.js` - Content Management System dashboard
- `/app/frontend/src/pages/erp/TransportDashboard.js` - Transport Management dashboard
- `/app/backend/routers/transport.py` - Transport Management backend API
- `/app/tests/test_cart_forecast_cms.py` - Test suite for new features
- `/app/tests/test_color_and_order_features.py` - Color and order creation tests
- `/app/tests/test_transport_management.py` - Transport system tests

### Updated Files (Jan 2, 2026)
- `/app/backend/erp_routes.py` - Registered transport_router
- `/app/frontend/src/App.js` - Added /erp/transport route
- `/app/frontend/src/components/ERPLayout.js` - Added Transport sidebar item
- `/app/frontend/src/utils/erpApi.js` - Added transport API methods
- `/app/frontend/src/pages/CustomizeBook.js` - Added transport option with location input

---

## Deployment Status

### Pre-Deployment Health Check (January 1, 2026)
**Status: ✅ READY FOR DEPLOYMENT**

All modules verified working:
- ✅ Authentication (JWT-based)
- ✅ Admin Dashboard
- ✅ CRM (14 leads)
- ✅ Production Orders
- ✅ HR & Payroll
- ✅ Inventory Management
- ✅ Accounts & Finance
- ✅ Super Admin Panel
- ✅ Audit Trail
- ✅ SFA Phase 1 & 2 (Alerts, Performance Scorecards, Route Maps)

Integrations Configured:
- ✅ Razorpay (Live Key)
- ✅ Twilio SMS/WhatsApp
- ✅ SMTP Email (Hostinger)

Environment:
- ✅ Backend .env configured
- ✅ Frontend .env configured
- ✅ Supervisor running
- ✅ MongoDB connected

---

## Changelog

### 2026-01-01 - Field Sales Automation (SFA) System - Phase 1
**NEW MODULE: Field Sales Attendance, Movement, Communication & Expense Intelligence System**

System designed to be: "Tamper-proof, fully automated, GPS-based, with zero manual override, and complete audit trail for every action."

**SFA Attendance & Movement (`/erp/sfa`):**
- Day Start/End with GPS location recording
- Vehicle selection: Bike (₹3.5/KM), Car (₹8/KM), Company Vehicle (₹0)
- Automatic fuel allowance calculation based on actual distance
- Location ping tracking (for continuous movement monitoring)
- Visit tracking with check-in/check-out
- Day timeline view (Home → Visits → Home)
- Team dashboard for managers (live status of all field staff)

**SFA Expense & Reimbursement (`/api/erp/sfa-expense`):**
- Self-service expense submission with bill upload (mandatory)
- Two-level approval: Manager → Accounts
- Categories: Fuel, Travel, Food, Accommodation, Mobile, Parking, Toll, Courier, Other
- Employee-wise, Category-wise, Vehicle-wise expense reports
- Complete approval history and audit trail

**SFA Call Recording & CRM (`/api/erp/sfa-calls`):**
- Call logging with auto-linking to leads/customers
- Flagged calls (calls to numbers not in CRM)
- Call notes (append-only, cannot edit/delete)
- Immutable audit trail for all call records
- Daily & Monthly call reports

**APIs Created:**
- `/api/erp/sfa/day-start` - Start day with GPS & vehicle
- `/api/erp/sfa/day-end` - End day with distance calculation
- `/api/erp/sfa/location-ping` - Continuous GPS tracking
- `/api/erp/sfa/visit-start` - Check-in at customer
- `/api/erp/sfa/visit-end` - Check-out with outcome
- `/api/erp/sfa/my-day` - Employee day summary
- `/api/erp/sfa/team-dashboard` - Manager team view
- `/api/erp/sfa/reports/*` - Daily/Monthly reports
- `/api/erp/sfa-expense/*` - Expense submission & approvals
- `/api/erp/sfa-calls/*` - Call logging & reports

Files created: sfa.py, sfa_expense.py, sfa_calls.py, SFADashboard.js
Collections: sfa_attendance, sfa_expenses, sfa_calls

**Phase 2 (Upcoming):**
- Native mobile app with background GPS tracking
- Auto call recording integration
- Route map visualization (OpenStreetMap)
- Stop duration detection
- Location OFF alerts
- Employee performance scorecard

### 2026-01-01 - Super Admin Panel & Audit Trail (NEW MODULE)
**Super Admin Dashboard** (`/erp/superadmin`):
- Dashboard with user stats, role distribution pie chart, system data counts
- Today's activity: total actions, logins, active users
- Recent activity log with color-coded action badges

**User Management:**
- Full CRUD: Create, Disable, Enable, Delete users
- Search users by name/email
- Filter by role
- Cannot delete super_admin users (safety)
- Password reset by super admin

**Audit Trail & MIS Dashboard:**
- Daily Activity Log: User-wise breakdown (Aaj kisne kya kiya)
- Monthly MIS Report: Performance trends, top performers, charts
- Yearly Audit Trail: Compliance report with all actions
- Approval History: Track all approve/reject actions
- Deleted Records Log: Recovery and compliance
- Employee Performance Report: Individual evaluation with grades

**Role-Based Access:**
- `super_admin` role added - full system control
- Super Admin menu only visible to super_admin users
- admin@lucumaa.in upgraded to super_admin role

Files created: audit.py, superadmin.py, SuperAdminDashboard.js
Test file: /app/tests/test_superadmin_audit.py (28 tests, 93% pass)
Test report: /app/test_reports/iteration_14.json

### 2026-01-01 - Password Reset Feature (P1)
**Password Reset with Email & Mobile OTP:**
- Added "Forgot Password?" link on login page
- Reset Password page with two OTP delivery methods:
  - Email OTP (via Hostinger SMTP) - ✅ Working
  - Mobile OTP (via Twilio SMS) - ⚠️ Requires proper Twilio number for production
- Complete UI flow: Forgot Password → Select Method → Enter Email/Mobile → Verify OTP → Set New Password → Success
- Backend APIs: `/api/auth/send-otp`, `/api/auth/verify-otp`, `/api/auth/reset-password`
- Security: OTP expires in 10 minutes, reset token expires in 1 hour, both are single-use
- Added Twilio API Key credentials to .env

Files updated: Login.js (new forgot password flow), .env (Twilio API keys)
Test file: /app/tests/test_password_reset.py (12 tests)
Test report: /app/test_reports/iteration_12.json

**Note:** Mobile SMS OTP requires proper Twilio phone number configuration. Current sandbox number (+14155238886) doesn't work for sending SMS.

### 2026-01-01 - Google Calendar Style Holiday View
**Holiday Calendar Enhancement:**
- Redesigned calendar to Google Calendar-style grid layout
- Full day names (Sunday, Monday, etc.) as column headers
- Holiday names displayed as colored badges inside date boxes
- Color coding: Red (National), Orange (State), Blue (Company), Purple (Optional)
- Today highlighted with teal circle
- Weekly offs marked with "OFF" badge
- Responsive design with shortened day names on mobile
- Added 7 sample Indian holidays (Republic Day, Holi, Eid, Independence Day, Dussehra, Diwali, Christmas)
- Updated legend with filled color boxes

Files updated: HolidaysDashboard.js

### 2026-01-01 - Assets & Holidays Dashboards Complete (P3)
**Assets Dashboard** (`/erp/assets`) - Full asset management UI:
- Company Assets Tab: 11 assets with asset code, purchase price, book value, depreciation %, condition badge
- Rented Assets Tab: 5 rented assets with vendor info, monthly rent, deposit, days remaining
- Handovers Tab: 4 pending handover requests with employee name, department, status
- Reports Tab: Asset Register Summary (Total Assets, Purchase Value, Book Value, Depreciation) + Rent Liability (Active Rentals, Monthly/Annual Rent, Expiring Soon)
- Stats Cards: Total Assets: 11, Book Value: ₹81.6L, Depreciation: ₹20.9L, Monthly Rent: ₹125,000
- Add Asset Modal: Create company asset with depreciation settings
- Add Rented Modal: Create rented asset with rental terms

**Holidays Dashboard** (`/erp/holidays`) - Full holiday calendar UI:
- Calendar Tab: Month grid with working days, weekly offs, holidays highlighted
- Holiday List Tab: 2 holidays with type badge, Paid badge, Delete button
- Overtime Tab: Overtime records with 2x rate configuration
- Settings Tab: Weekly Off Settings (Sunday), Overtime & Comp Off settings
- Stats Cards: Working Days: 312, Holidays: 2, Weekly Offs: 51, Total Days: 365
- Add Holiday Modal: Create holidays with name, date, type, paid checkbox
- Month Navigation: Previous/Next month buttons

**Bug Fixed:**
- Fixed FastAPI redirect_slashes issue causing Mixed Content errors
- Added Assets and Holidays routes to App.js
- Added Assets and Holidays sidebar items to ERPLayout.js
- Updated role permissions to include assets and holidays access

All 23 backend API tests passed (100%)
Files updated: App.js, ERPLayout.js, server.py, erpApi.js
Test file: /app/tests/test_assets_holidays_expenses.py
Test report: /app/test_reports/iteration_11.json

### 2026-01-01 - Phase 1 Complete (QR Codes & Print Functionality)
- Added QR code generation for job cards with styled teal color (#0d9488)
- Added barcode generation (Code128 format) for job cards
- Added print-data API returning job card details with base64-encoded QR/barcode
- Added invoice QR code generation
- Added Print Report buttons to:
  - Production Dashboard (prints all orders report)
  - Admin Dashboard (prints factory metrics report)
  - Accounts Dashboard - Invoices tab (prints invoice list)
  - Accounts Dashboard - P&L tab (prints P&L statement)
- Added QR modal in Production Dashboard showing QR code, barcode, job details
- Added Print Job Card button for printing individual job card labels
- Added Download QR button to save QR code image
- All 13 QR code API tests passed (100%)
- Files updated: qr_codes.py, ProductionDashboard.js, AdminDashboard.js, AccountsDashboard.js, erpApi.js

### 2026-01-01 - Twilio WhatsApp Integration
- Integrated Twilio WhatsApp API with user's credentials
- Added support for content templates (pre-approved messages)
- Test message sent successfully (Message SID: MM9e99acdc2cac748319c0e54f505079f1)
- Added notification logging to database
- Files updated: sms.py, erpApi.js

### 2026-01-01 - Refer & Earn System Complete
- Implemented complete wallet system with balance, transactions, referral codes
- Admin can configure: flat bonus (₹100), percentage bonus (5%), bonus cap (₹500), new user bonus (₹50)
- Admin can configure wallet usage: max 25% of order, min order ₹500
- Cashback system: 2% cashback, max ₹200, min order ₹1000
- Admin dashboard with stats, user wallets, settings tabs
- Admin can manually credit wallets
- All 19 wallet API tests passed (100%)
- Files created: WalletDashboard.js, test_wallet.py
- Files updated: wallet.py, erpApi.js, ERPLayout.js, App.js

### 2026-01-01 - 5 New ERP Modules Implemented
**1. Daily Expense Management Module**
- Expense categories (Electricity, Fuel, Repair, Labour, Consumables, Transport, Raw Material, Office, Misc)
- Flexible approval workflow (2/3/4 levels, Admin direct approve)
- Daily/Monthly expense limits (₹50,000/₹500,000)
- Attachment upload, auto posting to ledger
- Variance analysis (Planned vs Actual), Cash flow reports
- Frontend: ExpenseDashboard with 4 tabs

**2. Rented Asset Management Module**
- Rented asset master with vendor details
- Rent types: monthly, hourly, daily, unit-based
- Security deposit tracking, renewal alerts
- Auto rent expense posting, usage mapping

**3. Company Owned Asset Management Module**
- Asset master (Machine, Vehicle, Tool, IT Asset, Furniture, Building)
- Depreciation: Both Straight Line and WDV methods
- Maintenance scheduling, breakdown history
- Scrap/disposal management, asset register reports

**4. Asset Handover to Employee Module**
- Asset issue request & approval workflow
- Handover acknowledgment tracking
- Return/damage tracking with recovery calculation
- Employee asset holdings report

**5. Company Holiday & Salary Impact Module**
- Year-wise holiday calendar (National, State, Company, Optional)
- Weekly offs configuration, overtime rules (2x rate)
- Compensatory off tracking
- Salary impact preview with attendance deductions

All 33 module tests passed (100%)
Files created: expenses.py, assets.py, holidays.py, ExpenseDashboard.js
Files updated: erpApi.js, erp_routes.py, ERPLayout.js, App.js

### 2026-01-01 - Customer Portal Complete
- Customer Portal at /portal with 4 tabs: Dashboard, My Orders, Wallet, Profile
- OTP-based authentication: Send OTP via Email or SMS, 10-minute expiry
- Password reset flow: Send OTP → Verify → Reset with reset_token (1-hour expiry)
- Customer Dashboard: Total Orders (4), Total Spent, Pending Orders, Wallet Balance (₹175)
- Referral code display with Copy and Share functionality
- Order tracking with timeline view
- Profile management with saved addresses
- Support ticket creation
- All 22 Customer Portal tests passed (100%)
Files created: CustomerPortal.js, test_otp_customer_portal.py
Files updated: server.py (OTP routes), erp_routes.py, App.js

### 2026-01-01 - P2 Task Complete (Razorpay Payouts)
- Added Razorpay Payouts (RazorpayX) integration for salary disbursement
- New module: `/routers/payouts.py` with fund accounts, bulk payouts, webhook handling
- New dashboard: PayoutsDashboard with Overview, Bank Accounts, Payout History tabs
- Features:
  - Link employee bank accounts via Razorpay Fund Accounts API
  - Process individual or bulk salary payouts
  - Track payout status (pending → processing → paid)
  - Webhook support for automatic status updates
  - Ledger entries for salary payments
- All 21 tests passed (100%)
- Razorpay live integration working (fund accounts created successfully)

### 2026-01-01 - P1 Tasks Complete (Email Notifications & Charts)
- Added Email Notifications module (`/routers/notifications.py`)
  - New order alerts to admin
  - Payment confirmation to customers
  - Invoice emails to customers
  - Low stock alerts to admin
  - Overdue invoice reminders
- Added Dashboard Charts (using Recharts)
  - Revenue & Collections trend (area chart)
  - Monthly Expense Breakdown (pie chart)
  - Production Pipeline live status
  - Top 5 Customers by revenue
- All 11 new tests passed (100%)

### 2026-01-01 - Backend Refactoring (P0 Complete)
- Refactored monolithic `erp_routes.py` (1513 lines) into 10 modular router files
- New structure: `/backend/routers/` with admin, crm, production, hr, inventory, purchase, accounts, reports
- All 33 tests pass - functionality preserved
- Improved maintainability and scalability

### 2026-01-01 - Accounts Dashboard Enhancement
- Added Profit & Loss Report with date range selector
- Added GST Report with output/input tax breakdown
- Added Excel/PDF export for Invoices, P&L, Ledger, Payments
- Enhanced Accounts Dashboard with 4 tabs (Overview, Invoices, P&L, Reports)
- Fixed backend syntax errors in erp_routes.py (incomplete return statements)
- Reset test user passwords for testing
- All 33 new tests passed (100%)

### 2026-01-02 - Shopping Cart, AI Forecasting & CMS (Complete)
**Shopping Cart Functionality:**
- CartContext for state management (localStorage persistence)
- CartSidebar component for quick cart view
- Cart page (/cart) with full item management
- Add to Cart buttons on Products page
- Quantity controls, clear cart, checkout flow
- Cart badge in header showing item count

**AI Demand Forecasting Dashboard (/erp/forecast):**
- Backend: /api/erp/forecast/stats (order statistics)
- Backend: /api/erp/forecast/demand (AI-powered predictions using Emergent LLM Key + gpt-4o-mini)
- Frontend: Stats cards (Today/Week/Month/Year orders, Revenue)
- Frontend: Top Products pie chart
- Frontend: Generate AI Forecast button with loading state
- Frontend: Forecast results with trend, predictions, recommendations

**Content Management System (/erp/cms):**
- 5-tab dashboard: Pages, Banners, Menu, Blog, Contact Info
- Backend: /api/erp/cms/pages - CRUD for pages with SEO fields
- Backend: /api/erp/cms/banners - CRUD for banners
- Backend: /api/erp/cms/menu - CRUD for menu items
- Backend: /api/erp/cms/blog - CRUD for blog posts with categories
- Backend: /api/erp/cms/contact-info - GET (public) / PUT (admin)
- Backend: /api/erp/cms/sitemap - Auto-generated sitemap entries
- Frontend: Create/Edit/Delete modals for all content types
- SEO features: meta titles, descriptions, slugs

**Testing:**
- 18/19 backend tests passed (95%)
- All frontend features working (100%)
- Test file: /app/tests/test_cart_forecast_cms.py
- Test report: /app/test_reports/iteration_20.json

### 2026-01-02 - Public Blog Section & Mobile Responsiveness
**Public Blog Pages:**
- `/blog` - Blog listing page with category filter and pagination
- `/blog/:slug` - Single blog post page with full SEO support
- Social sharing buttons: Facebook, Twitter, LinkedIn, WhatsApp, Copy Link
- React-markdown for proper content rendering
- Related posts section (same category)
- View count tracking

**SEO Implementation:**
- react-helmet-async for dynamic meta tags
- Open Graph tags (og:title, og:description, og:image, og:type)
- Twitter Card tags (twitter:card, twitter:title, twitter:description, twitter:image)
- JSON-LD structured data (BlogPosting schema)
- Canonical URLs
- Updated index.html with data-rh attributes for proper helmet integration

**Public Blog API Endpoints:**
- GET /api/erp/cms/public/blog - List published posts (public)
- GET /api/erp/cms/public/blog/{slug} - Single post (public)
- GET /api/erp/cms/public/categories - Categories with post counts (public)

**Mobile Responsiveness:**
- All pages verified at 375x812 viewport
- Header navigation with Blog link in desktop and mobile menu
- Responsive blog cards, post pages, and all existing pages

**Testing:**
- Backend: 12/12 tests passed (100%)
- Frontend: All issues fixed
- Test file: /app/tests/test_public_blog.py
- Test report: /app/test_reports/iteration_21.json

### 2026-01-02 - Customer Dashboard & Multi-item Order Builder
**Customer Dashboard Enhancements:**
- **Profile Tab** - Personal Info, Business Info (GST), Address, Account Info
- Edit/Save profile with validation
- **Create New Order** button (prominently displayed at top + in Orders section)
- Order History with View (eye) and Repeat buttons
- Mobile responsive with card-based order display
- Quick Actions section on mobile

**Multi-item Order Builder (/customize):**
- Add multiple glass items with different specifications
- Each item: Glass type, Thickness, Width, Height, Quantity
- Thickness selection buttons per glass type (5mm, 6mm, 8mm, 10mm, 12mm)
- +/- quantity controls
- Add Another Glass Item button
- Cart Summary showing total items and pieces
- Calculate All & Continue button
- Order Summary before payment

**New API Endpoints:**
- GET /api/users/profile - Get current user profile
- PUT /api/users/profile - Update profile (partial updates supported)

**Testing:**
- Backend: 10/10 tests passed (100%)
- Frontend: All features working (100%)
- Test file: /app/tests/test_customer_dashboard_profile.py
- Test report: /app/test_reports/iteration_22.json

### 2026-01-02 - GST & HSN Code Integration (Complete)
**GST Management Module:**
- Backend router: `/app/backend/routers/gst.py` with full GST functionality
- Frontend dashboard: `/app/frontend/src/pages/erp/GSTDashboard.js`
- Route: `/erp/gst` - 4-tab dashboard (Company Settings, HSN Codes, Verify GSTIN, API Settings)

**GST Calculation Logic:**
- Company State: Maharashtra (27)
- Default GST Rate: 18%
- Intra-State (same state): CGST 9% + SGST 9%
- Inter-State (different state): IGST 18%
- HSN Codes: 7003-7009 for glass products

**Customer Order Integration:**
- Delivery State dropdown in Step 2 (38 Indian states)
- Customer GSTIN input (optional) for GST invoice
- GST breakdown displayed in order summary
- Grand Total includes GST
- Order creation stores GST fields (gst_type, hsn_code, cgst_amount, sgst_amount, igst_amount, total_gst)

**API Endpoints:**
- GET /api/erp/gst/states - Returns 38 Indian states with codes
- POST /api/erp/gst/calculate - Calculates GST based on delivery state
- GET /api/erp/gst/settings - Get GST settings (admin only)
- PUT /api/erp/gst/settings - Update GST settings (super admin only)
- GET /api/erp/gst/hsn-codes - Get HSN codes list
- POST /api/erp/gst/hsn-codes - Add HSN code (admin only)
- DELETE /api/erp/gst/hsn-codes/{code} - Delete HSN code (admin only)
- POST /api/erp/gst/verify - Verify GSTIN format
- GET /api/erp/gst/company-info - Get company GST info (public)

**Testing:**
- Backend: 13/13 tests passed (100%)
- Frontend: All features verified
- Test file: /app/tests/test_gst_module.py
- Test report: /app/test_reports/iteration_27.json

**Note:** GSTIN Verification API is MOCKED - uses basic regex validation when GST API not configured. For production, configure GST API key for real-time government verification.

### 2026-01-02 - Customer Portal Enhanced & Job Work Module (Complete)

**Customer Portal Enhanced (`/portal`):**
- Unified customer portal with 4 tabs: My Orders, Job Work, Wallet, Profile
- **Order Summary Single-Line Format:**
  - Sr No | Order # | QTY | Date | Amount | Advance Paid | Cash Paid | Remaining | Status | Actions
- **Payment Options for Remaining Amount:**
  - Online Payment (Razorpay)
  - Cash Payment (saves preference, admin/finance/HR collects later)
- Status badges: Order Placed, In Production, Quality Check, Ready to Dispatch, Dispatched, Delivered
- Payment badges: Payment Pending, Advance Paid, Cash Pending, Fully Paid

**Job Work Module (`/job-work`):**
- Customer brings their own glass for toughening
- Company charges only labour per sq.ft based on glass thickness:
  - 4mm: ₹8, 5mm: ₹10, 6mm: ₹12, 8mm: ₹15
  - 10mm: ₹18, 12mm: ₹22, 15mm: ₹28, 19mm: ₹35
- **CRITICAL DISCLAIMER:**
  - Company NOT responsible for glass breakage in furnace
  - NO compensation or refund for broken glass
  - Customer bears full responsibility
  - Disclaimer acceptance MANDATORY before order creation
- Status Flow: pending → accepted → material_received → in_process → completed → ready_for_delivery → delivered
- GST 18% on labour charges

**New Files:**
- `/app/frontend/src/pages/CustomerPortalEnhanced.js`
- `/app/frontend/src/pages/JobWorkPage.js`
- `/app/backend/routers/job_work.py`

**API Endpoints:**
- GET /api/erp/job-work/labour-rates - Get labour rates per thickness
- POST /api/erp/job-work/calculate - Calculate job work cost with GST
- GET /api/erp/job-work/disclaimer - Get disclaimer text
- POST /api/erp/job-work/orders - Create job work order (requires disclaimer)
- GET /api/erp/job-work/my-orders - Get customer's job work orders
- GET /api/erp/job-work/orders - Get all job work orders (admin)
- PATCH /api/erp/job-work/orders/{id}/status - Update job work status
- GET /api/erp/job-work/dashboard - Admin dashboard stats

**Testing:**
- Backend: 10/10 tests passed (100%)
- Frontend: All features verified
- Test file: /app/tests/test_job_work_customer_portal.py
- Test report: /app/test_reports/iteration_28.json

### 2026-01-02 - Admin Job Work Dashboard, Security Fix & WhatsApp Notifications (Complete)

**1. Admin Job Work Dashboard (`/erp/job-work`):**
- Stats cards: Total Orders, Pending, This Month, Total Breakage, By Status
- Search and filter by JW number, customer, phone, status
- Orders table with: JW Number, Customer, Items, Amount, Status, Date, Actions
- Status update buttons: → Job Accepted, → Material Received, → In Process, etc.
- Order details modal with items table, GST, status history, breakage record
- Record Breakage button with count and notes

**2. AI Forecast API Security Fix:**
- Fixed `/app/backend/routers/base.py` - `get_erp_user` now requires authentication
- Previously returned "system admin" for unauthenticated requests (security hole)
- Now returns 401 for unauthenticated requests
- Returns 401 for invalid/expired tokens
- Returns data only with valid admin token

**3. WhatsApp Notifications for Job Work:**
- Added message templates for all job work statuses (accepted, material_received, in_process, completed, ready_for_delivery, delivered)
- Professional formatting with order details and next steps
- Automatic notification on status update
- Uses existing Twilio integration from `/app/backend/routers/sms.py`
- Falls back to logging if Twilio not configured (MOCKED)

**New Files:**
- `/app/frontend/src/pages/erp/JobWorkDashboard.js`

**Testing:**
- Backend: 16/16 tests passed (100%)
- Frontend: All features verified
- Test file: /app/tests/test_job_work_admin_forecast.py
- Test report: /app/test_reports/iteration_29.json

**Note:** WhatsApp notifications depend on Twilio configuration. If Twilio not configured, notifications are logged but not sent.

### 2026-01-02 - Job Work 100% Payment, P&L Update, Deployment Ready (Complete)

**1. Job Work 100% Payment Required:**
- Added 4-step flow: Items → Details → **Payment** → Done
- Payment step after order creation with Online/Cash selection
- Online: Razorpay integration for instant payment
- Cash: Preference saved, admin collects at office
- Full payment required before processing begins

**2. P&L Report Updated with Job Work:**
- `/api/erp/accounts/profit-loss` now includes:
  - `revenue.job_work` - Job work labour revenue
  - `revenue.job_work_count` - Number of job work orders
  - `job_work_details` - Detailed breakdown (orders, labour_revenue, gst_collected)
- Combined revenue = Sales + Job Work

**3. Job Work Revenue Stats API:**
- New endpoint: `/api/erp/job-work/revenue-stats`
- Returns: total_revenue, total_orders, total_sqft, total_pieces, monthly_breakdown, by_payment_method

**4. Admin Dashboard Updated:**
- `/api/erp/admin/dashboard` now includes `job_work` stats:
  - `today` - Job work orders today
  - `pending` - Pending job work orders
  - `total_revenue` - Total job work revenue

**5. Deployment Check:**
- **Status: PASS ✅** - Ready for production deployment
- All environment variables properly configured
- No hardcoded URLs or secrets
- CORS, Supervisor, Database all correctly configured

**New API Endpoints:**
- POST /api/erp/job-work/orders/{id}/initiate-payment - Start Razorpay payment
- POST /api/erp/job-work/orders/{id}/verify-payment - Verify Razorpay payment
- POST /api/erp/job-work/orders/{id}/cash-payment - Admin marks cash received
- POST /api/erp/job-work/orders/{id}/set-cash-preference - Customer selects cash
- GET /api/erp/job-work/revenue-stats - Revenue stats for P&L

### 2026-01-02 - Payment Selection Flow Update (User Request)

**User Requirement:** "Pehle Cash/Online select karo, phir Pay Now button dikhe"

**Customer Portal (/portal) Updates:**
- Payment flow changed from dropdown to button selection
- Online/Cash buttons displayed for each order needing payment
- Pay button ONLY appears after selecting payment method
- Online selection → Blue highlighted button + Blue "Pay ₹X" button
- Cash selection → Green highlighted button + Green "Pay ₹X" button
- Added `processingPayment` state for loading indicator
- Updated `paymentMethod` to start as `null` (not pre-selected)

**Job Work Page (/job-work) Updates:**
- Payment step (Step 3) redesigned with two-step flow:
  - "Step 1: Select Payment Method" - Online/Cash options with descriptions
  - "Step 2: Confirm Payment" - Pay button only after selection
- Added checkmark icon on selected payment option
- Added "Please select a payment method to continue" hint when nothing selected
- Shows advance requirement info: "100% Advance Required (Single Item Order)" or "50% Advance Required (₹X)"
- Success step shows remaining amount info for partial payments

**100%/50% Advance Rule:**
- Single item (1 piece) = 100% advance payment required
- Multiple items (2+ pieces) = 50% advance payment required
- Backend logic in `/app/backend/routers/job_work.py` lines 346-357

**Files Updated:**
- `/app/frontend/src/pages/CustomerPortalEnhanced.js` - Payment selection UI
- `/app/frontend/src/pages/JobWorkPage.js` - Two-step payment flow
- `/app/backend/routers/job_work.py` - verify-payment with partial payment support

**Testing:**
- Backend: 16/16 tests passed (100%)
- Frontend: All features verified
- Test file: `/app/tests/test_payment_flow.py`
- Test report: `/app/test_reports/iteration_30.json`

### 2026-01-02 - Invoice & Receipt Downloads + Transport in Job Work

**User Requirements:**
1. Fix Invoice download ("Not Found" error)
2. Invoice should use customer profile data (business details, GSTIN)
3. Profile update reminder before order
4. Transport checkbox in Job Work (like regular orders)
5. Payment Receipt download (combines Cash + Online)
6. Every order should have Invoice & Receipt download icons before Online/Cash buttons

**PDF Download Fixes:**
- Fixed auth issue in `/app/backend/routers/base.py` - `get_erp_user` now supports both Authorization header AND query param `?token=xxx`
- Invoice download URL: `/api/erp/pdf/invoice/{order_id}?token=xxx`
- Receipt download URL: `/api/erp/pdf/payment-receipt/{order_id}?token=xxx`
- Job Work Invoice URL: `/api/erp/pdf/job-work-invoice/{order_id}?token=xxx`

**Invoice Enhancements:**
- Fetches customer profile data from `users` collection
- Includes: customer name, company, phone, email, GSTIN, address
- Shows GST breakdown: CGST/SGST or IGST
- Shows: Subtotal, Transport, GST, Grand Total, Advance Paid, Cash Paid, Balance Due

**Payment Receipt PDF (NEW):**
- Works for both regular orders and job work orders
- Combines Online + Cash payments in single receipt
- Shows: Receipt #, Order #, Customer details, Payment breakdown
- Payment status badge: "PAYMENT COMPLETE" or "BALANCE DUE: ₹X"

**Job Work Invoice PDF (NEW):**
- Orange-themed for Job Work branding
- Shows: Glass items with size, thickness, qty, sqft, labour cost
- Shows: Labour charges, GST, Transport, Grand Total
- Includes disclaimer about glass breakage liability

**Customer Portal UI Updates:**
- Every order row now has: Eye (view) | FileText (receipt) | Download (invoice) | Online | Cash
- Icons visible for ALL orders (not just paid ones)
- Green receipt icon, Red invoice icon
- `downloadInvoice(orderId, isJobWork)` function supports both order types

**Profile Update Reminder:**
- `isProfileIncomplete()` checks: name, phone, company_name, address
- Shows amber warning banner at top of orders list
- "Complete Your Profile" with "Update Profile" button
- Clicking redirects to Profile tab

**Job Work Transport Feature:**
- Added transport state variables to JobWorkPage.js
- "Transport Service" section in Step 2 with checkbox
- Location picker with GPS support
- Transport cost calculation via `/api/erp/transport/calculate`
- Transport cost added to grand total
- Backend job_work.py stores: transport_required, transport_cost, transport_distance, transport_location

**Files Updated:**
- `/app/backend/routers/base.py` - Query param token support
- `/app/backend/routers/pdf_generator.py` - Invoice, Receipt, Job Work Invoice PDFs
- `/app/backend/routers/job_work.py` - Transport fields in JobWorkCreate model
- `/app/frontend/src/pages/CustomerPortalEnhanced.js` - Download icons, profile warning
- `/app/frontend/src/pages/JobWorkPage.js` - Transport section

### 2026-01-02 - Share Invoice/Receipt System (Comprehensive)

**User Requirement:**
"Every order must have Invoice and Payment Receipt sharing functionality, including a merged PDF (invoice + receipt) available via WhatsApp, email, and download, with full audit logging and GST-compliant formatting."

**Scope - Available in ALL sections:**
- ✅ Customer Portal (`/portal`)
- ✅ Admin Order Management (`/erp/orders`)
- ✅ Job Work Dashboard (`/erp/job-work`)
- ✅ All user types: Customer, Builder/Dealer, Admin, Super Admin, Accounts

**Core Features Implemented:**

1. **Invoice Generation (GST-Compliant)**
   - Company details (Lucumaa Glass – a unit of Lucumaa Corporation Pvt. Ltd.)
   - Factory & Corporate Office address
   - Order ID, Customer details, Product details
   - Taxes (CGST/SGST or IGST), totals, payment status
   - QR code for order tracking

2. **Payment Receipt Generation**
   - Receipt number linked to Order ID
   - Payment mode (Online/Cash)
   - Amount paid, Date & time
   - Outstanding balance

3. **Share Buttons (3 Documents)**
   - 📄 Invoice (GST-compliant)
   - 🧾 Payment Receipt (Online + Cash combined)
   - 📑 Invoice + Receipt (Merged PDF - 2 pages)

4. **Sharing Options:**
   - ⬇️ Download PDF
   - 💬 WhatsApp (pre-formatted message with download link)
   - 📧 Email (opens mail client with subject/body)

5. **Merged PDF:**
   - Invoice on Page 1
   - Receipt on Page 2
   - Uses PyPDF2 for merging

6. **Audit Logging:**
   - Every share action logged to `share_audit_logs` collection
   - Stores: who, what, when, channel (whatsapp/email/download)
   - Admin can view logs via API

7. **Excel/CSV Export:**
   - Bulk export orders data
   - Filters: date range, order type (regular/job_work/all)
   - CSV format for Excel compatibility

**New Files Created:**
- `/app/frontend/src/components/ShareModal.js` - Share modal component

**Files Updated:**
- `/app/backend/routers/pdf_generator.py` - Added:
  - `GET /merged/{order_id}` - Merged Invoice+Receipt PDF
  - `POST /share-log` - Audit logging
  - `GET /export/orders-excel` - CSV export
  - `GET /share-logs` - View audit logs
- `/app/frontend/src/pages/CustomerPortalEnhanced.js` - Share button + modal
- `/app/frontend/src/pages/erp/OrderManagement.js` - Share button + modal
- `/app/frontend/src/pages/erp/JobWorkDashboard.js` - Share button + modal

**New Dependencies:**
- PyPDF2==3.0.1 (for PDF merging)

**API Endpoints:**
- `GET /api/erp/pdf/invoice/{order_id}?token=xxx` - Regular invoice
- `GET /api/erp/pdf/job-work-invoice/{order_id}?token=xxx` - Job work invoice
- `GET /api/erp/pdf/payment-receipt/{order_id}?token=xxx` - Payment receipt
- `GET /api/erp/pdf/merged/{order_id}?token=xxx` - Merged PDF (2 pages)
- `POST /api/erp/pdf/share-log` - Log share actions
- `GET /api/erp/pdf/share-logs` - View audit logs (admin only)
- `GET /api/erp/pdf/export/orders-excel` - CSV export
- `POST /api/erp/pdf/send-email` - Send PDF via SMTP with attachment

**UI Changes:**
- Every order row now shows: 👁 View | 📄 Receipt | ⬇️ Invoice | 📤 Share | Online | Cash
- Purple Share button opens ShareModal
- ShareModal shows 3 document types with Download/WhatsApp/Email options
- "System Generated • No Signature Required" footer

### 2026-01-02 - Email PDF Attachment + Job Work Graphs + P&L Date Range

**1. Direct PDF Email via SMTP:**
- Backend endpoint `POST /api/erp/pdf/send-email`
- Sends PDF as email attachment (not just link)
- Uses Hostinger SMTP configured in .env
- Fallback to mailto if SMTP fails
- ShareModal now tries SMTP first, falls back to mailto

**2. Share in AccountsDashboard:**
- Added Share button to invoices list
- Purple Share icon opens ShareModal
- Works for all invoice types

**3. Job Work Graphs in Admin Dashboard:**
- New row of 4 Job Work stat cards:
  - Job Work Orders (orange) - total + pending count
  - JW Revenue (purple) - this month total
  - JW Collected (cyan) - total received
  - In Process (amber) - currently processing
- Clickable cards navigate to Job Work dashboard
- Fetches data from `erpApi.jobWork.getDashboard()`

**4. Custom Date Range for P&L Reports:**
- Quick preset buttons:
  - Today | Last 7 Days | This Month | Last Month | Last 3 Months | This FY
- Custom date inputs retained
- Financial Year (This FY) starts April 1

**Files Updated:**
- `/app/backend/routers/pdf_generator.py` - Added send-email endpoint
- `/app/frontend/src/components/ShareModal.js` - SMTP email with fallback
- `/app/frontend/src/pages/erp/AdminDashboard.js` - Job Work stats row
- `/app/frontend/src/pages/erp/AccountsDashboard.js` - Share button + date presets

### 2026-01-02 - E2E Testing + Job Work PDF Customization

**E2E Testing Results (iteration_31):**
- Backend: 17/17 tests passed (100%)
- Frontend: All E2E flows verified
- Features tested:
  - Job Work complete flow (Items → Details → Payment → Success)
  - 100%/50% advance rule
  - Customer Portal payment flow
  - Share Modal with all options
  - PDF downloads (Invoice, Receipt, Merged)

**Job Work Invoice PDF Enhanced:**
- Company header: "LUCUMAA GLASS - A Unit of Lucumaa Corporation Pvt. Ltd."
- Factory address: Industrial Area, Sector 63, Noida, UP - 201301
- Corporate Office: Tower B, Logix City Centre, Noida - 201301
- GST number, Phone, Email displayed
- Title: "JOB WORK INVOICE / CHALLAN"

**Job Work Delivery Slip (NEW):**
- Endpoint: `GET /api/erp/pdf/job-work-slip/{order_id}`
- Title: "DELIVERY SLIP / GATE PASS"
- Green-themed (emerald color scheme)
- Contains:
  - Slip #, Job Work #, Date, Time
  - Customer details (Name, Phone, Company)
  - Glass items table with status ("✓ Ready")
  - Total pieces and area summary
  - Payment status banner (green for complete, red for balance due)
  - Signature sections: Customer, Store/Warehouse, Security
- Added to ShareModal for Job Work orders only

**ShareModal Updated:**
- 4 document options for Job Work:
  1. Invoice (red) - GST-compliant
  2. Payment Receipt (green) - Online + Cash
  3. Invoice + Receipt (purple) - Merged PDF
  4. Delivery Slip / Gate Pass (emerald) - For pickup
- Regular orders: 3 options (no delivery slip)

**Files Updated:**
- `/app/backend/routers/pdf_generator.py` - Job Work Invoice header + Delivery Slip endpoint
- `/app/frontend/src/components/ShareModal.js` - Added Delivery Slip section for Job Work


### 2026-01-03 - Vendor PO-Based Payment Module + Job Work Dispatch Restriction

**1. Vendor Management Module (COMPLETE):**
- Full vendor CRUD with categories (raw_material, glass_processing, logistics, packaging, utilities, services, other)
- Vendor code auto-generation (VND-YYYYMMDD-XXXXX)
- Bank details, GST, PAN, UPI ID storage
- Credit days and credit limit management
- Vendor ledger with all transactions

**2. Purchase Order (PO) System:**
- PO CRUD with items, quantities, rates, GST calculation
- PO workflow: Draft → Submit → Pending Approval → Approved/Rejected
- PO status tracking: unpaid, partial, paid
- Payment history per PO
- Outstanding balance calculation

**3. Vendor Payments with Razorpay:**
- Payment types: advance, partial, full
- Payment modes: razorpay, upi, net_banking, bank_transfer
- Razorpay integration for online payments
- UTR tracking for settlements
- Payment receipt PDF generation

**4. Reports & Analytics:**
- Outstanding payables report
- Payment report with date filters
- Vendor ledger view
- Audit logs for all actions

**5. Job Work Dispatch Restriction (COMPLETE):**
- Delivery Slip/Gate Pass blocked for unpaid orders
- Returns 400 error: "Cannot generate delivery slip. Payment not fully settled."
- ShareModal shows locked state for unpaid job work orders
- Only allows slip download after payment_status === 'completed'

**6. UI Enhancements:**
- Vendor & PO menu item added to sidebar (violet color)
- Scrollable sidebar navigation
- VendorManagement page with 2 tabs: Vendors, Purchase Orders
- Stats cards: Total Vendors, Active POs, Pending Approval, Outstanding
- Add Vendor modal with all fields
- Create PO modal with vendor dropdown and items
- PO approval workflow buttons

**Files Created:**
- `/app/backend/routers/vendor.py` - Full vendor module (879 lines)
- `/app/frontend/src/pages/erp/VendorManagement.js` - Frontend page (928 lines)
- `/app/tests/test_vendor_management.py` - 19 test cases

**Files Updated:**
- `/app/frontend/src/App.js` - Added /erp/vendor-management route
- `/app/frontend/src/components/ERPLayout.js` - Added Vendor & PO sidebar item, scrollable nav
- `/app/backend/routers/pdf_generator.py` - Payment validation for job work slip
- `/app/frontend/src/components/ShareModal.js` - Locked slip state for unpaid orders

**API Endpoints:**
- `GET /api/erp/vendors/` - List vendors with filtering
- `POST /api/erp/vendors/` - Create vendor
- `GET /api/erp/vendors/{id}` - Get vendor details with PO summary
- `PUT /api/erp/vendors/{id}` - Update vendor
- `GET /api/erp/vendors/po/list` - List all POs
- `POST /api/erp/vendors/po` - Create PO
- `GET /api/erp/vendors/po/{id}` - Get PO details
- `POST /api/erp/vendors/po/{id}/submit` - Submit PO for approval
- `POST /api/erp/vendors/po/{id}/approve` - Approve/reject PO
- `POST /api/erp/vendors/po/{id}/initiate-payment` - Initiate Razorpay payment
- `GET /api/erp/vendors/{id}/ledger` - Vendor ledger
- `GET /api/erp/vendors/reports/outstanding` - Outstanding payables
- `GET /api/erp/vendors/reports/payments` - Payment report
- `GET /api/erp/vendors/audit/logs` - Audit logs

**Test Results (iteration_32):**
- Backend: 18/18 tests passed, 1 skipped
- Frontend: All UI flows verified
- Access control: Customer role blocked from vendor APIs

### 2026-01-03 - Vendor Payment Flow Enhancements (Iteration 33)

**1. Razorpay Checkout Integration:**
- Frontend opens Razorpay checkout when payment mode is 'razorpay'
- Payment verification via `/api/erp/vendors/payment/{id}/verify`
- Signature verification for Razorpay payments

**2. Manual Payment with UTR Tracking:**
- Payment modal shows UTR/Transaction Reference input for manual modes
- Supported modes: UPI Transfer, Net Banking, Bank Transfer/NEFT/RTGS, Cash
- UTR stored in payment record and included in receipt
- Button text changes: "Pay Online" vs "Record Payment"
- Button disabled until UTR is entered for manual modes

**3. Payment Receipt PDF:**
- Endpoint: `GET /api/erp/pdf/vendor-payment-receipt/{payment_id}`
- Generated only for completed payments
- Includes vendor details, PO info, payment amount, UTR reference

**4. Bug Fixes:**
- Fixed syntax error (double closing brace) in VendorManagement.js
- Fixed record_manual_payment calling verify_vendor_payment with wrong params
- Added PaymentVerify Pydantic model for proper body/query param handling

**Test Results (iteration_33):**
- Backend: 23/23 tests passed, 1 skipped
- Frontend: All UI flows verified
- Payment modal UTR input working
- Receipt PDF download working

### 2026-01-03 - Vendor Balance Sheet & Bulk Payment (Iteration 34)

**1. Vendor Balance Sheet View:**
- Endpoint: `GET /api/erp/vendors/{id}/balance-sheet`
- Financial year support (April-March, FY: 2025-2026)
- Summary: Opening balance, Total PO value, Total payments, Closing balance
- Monthly breakdown table (12 months)
- Top 5 purchases sorted by amount
- UI: Modal with summary cards, monthly table, top purchases list

**2. Bulk Payment System:**
- Endpoint: `POST /api/erp/vendors/bulk-payment`
- Select multiple POs for same vendor
- Single transaction for all selected POs
- Individual receipt generated for each PO
- Bulk receipt number (BULK-YYYYMMDD-XXXX)
- Updates all POs and vendor balance atomically

**3. Opening Balance Update:**
- Endpoint: `PUT /api/erp/vendors/{id}/opening-balance`
- Audit log for balance changes

**UI Additions:**
- BookOpen icon button for Balance Sheet
- Layers icon button for Bulk Pay (purple, shown for vendors with payable balance)
- Balance Sheet modal with gradient header
- Bulk Payment modal with PO multi-select

**Test Results (iteration_34):**
- Backend: 16/16 tests passed
- Frontend: All UI flows verified

### 2026-01-03 - Bulk Export to Excel & Cleanup (Iteration 35)

**1. Bulk Export to Excel:**
- New endpoint: `GET /api/erp/reports/bulk-export`
- Single Excel file with multiple sheets:
  - Invoices (with GST breakdown)
  - Regular Orders
  - Job Work Orders
  - Payments Received
  - Summary sheet
- Optional: Include Vendor Payments sheet
- Features: Date range filter, totals per sheet

**2. UI Updates:**
- Added "Bulk Export - All Data" card in Reports & Export section
- Violet gradient design to highlight importance
- Two buttons: Download All Data, Include Vendor Payments

**3. Cleanup:**
- Deleted obsolete files:
  - `/app/frontend/src/pages/CustomerDashboard.js`
  - `/app/frontend/src/pages/CustomerPortal.js`
- Note: `/app/frontend/src/components/dashboards/CustomerDashboard.js` remains (still in use)

**Files Modified:**
- `/app/backend/routers/reports.py` - Added bulk-export endpoint
- `/app/frontend/src/pages/erp/AccountsDashboard.js` - Added Bulk Export UI

**Files Deleted:**
- `/app/frontend/src/pages/CustomerDashboard.js`
- `/app/frontend/src/pages/CustomerPortal.js`

### 2026-01-03 - Server Refactoring, SMS Fallback & Payment Alerts (Iteration 36)

**1. Server.py Refactoring - New Modules Created:**
- `/app/backend/models/__init__.py` - Pydantic models (User, Product, Order, etc.)
- `/app/backend/utils/__init__.py` - Utility functions entry point
- `/app/backend/utils/auth.py` - Password hashing, JWT token management
- `/app/backend/utils/notifications.py` - Email, SMS, WhatsApp with fallback
- `/app/backend/utils/payment_alerts.py` - Background payment due alert tasks
- `/app/backend/routers/alerts.py` - Alert management API endpoints

**2. SMS Fallback for WhatsApp:**
- New function: `send_with_sms_fallback()` in sms.py
- Priority: WhatsApp -> SMS (automatic fallback)
- New endpoint: `POST /api/erp/notifications/send-with-fallback`
- Logs which channel was used

**3. Payment Due Alerts System:**
- **Customer Alerts:**
  - 3 days before due
  - 1 day before due
  - Overdue reminders
  - Via WhatsApp/SMS/Email with fallback
- **Vendor Alerts:**
  - Sent to admin/finance team
  - Based on credit days from vendor settings
- **API Endpoints:**
  - `POST /api/erp/alerts/payment-dues/run` - Trigger alerts manually
  - `GET /api/erp/alerts/payment-dues/preview` - Preview pending alerts
  - `GET /api/erp/alerts/payment-dues/history` - Alert history log
  - `GET /api/erp/alerts/settings` - Get alert settings
  - `PUT /api/erp/alerts/settings` - Update settings
  - `POST /api/erp/alerts/test-notification` - Test notification system
- **UI:**
  - Payment Alerts card added to Admin Dashboard
  - Shows overdue and due-soon counts
  - Quick list of overdue payments
  - "View All" button to Accounts page

**Files Created:**
- `/app/backend/models/__init__.py`
- `/app/backend/utils/__init__.py`
- `/app/backend/utils/auth.py`
- `/app/backend/utils/notifications.py`
- `/app/backend/utils/payment_alerts.py`
- `/app/backend/routers/alerts.py`

**Files Modified:**
- `/app/backend/routers/sms.py` - Added send_with_sms_fallback
- `/app/backend/erp_routes.py` - Registered alerts_router
- `/app/frontend/src/pages/erp/AdminDashboard.js` - Payment Alerts card

**Note:** server.py still contains the core functionality. The new modules provide:
- Reusable notification functions
- Centralized auth utilities
- Background payment alert scheduler
- Alert management APIs

### 2026-01-03 - Server Migration & Auto-Scheduler (Iteration 37)

**1. Server.py Modular Migration - New Routers:**
- `/app/backend/routers/auth_router.py` - Auth (register, login, OTP, password reset)
- `/app/backend/routers/orders_router.py` - Order management (create, payment, dispatch)
- `/app/backend/routers/products_router.py` - Products & pricing
- `/app/backend/routers/users_router.py` - User profile, contact, inquiry

**2. Auto-Scheduler for Payment Alerts (APScheduler):**
- `/app/backend/utils/scheduler.py` - AsyncIO scheduler with cron triggers
- **Daily Payment Alerts:** 9:00 AM IST
- **Weekly Vendor Summary:** 10:00 AM IST (Mondays)
- Automatic startup with FastAPI app
- Job execution logs stored in MongoDB

**3. Scheduler API Endpoints:**
- `GET /api/erp/alerts/scheduler/jobs` - List all scheduled jobs
- `POST /api/erp/alerts/scheduler/run/{job_id}` - Run job manually
- `GET /api/erp/alerts/scheduler/logs` - View execution history

**Scheduled Jobs:**
| Job ID | Schedule | Description |
|--------|----------|-------------|
| payment_alerts_daily | 9:00 AM daily | Customer & vendor payment due alerts |
| vendor_summary_weekly | 10:00 AM Monday | Weekly vendor payment summary email |

**Files Created:**
- `/app/backend/routers/auth_router.py`
- `/app/backend/routers/orders_router.py`
- `/app/backend/routers/products_router.py`
- `/app/backend/routers/users_router.py`
- `/app/backend/utils/scheduler.py`

**Files Modified:**
- `/app/backend/routers/alerts.py` - Added scheduler endpoints
- `/app/backend/server.py` - Scheduler initialization on startup

**Note:** Original server.py endpoints still work. New routers are ready for gradual migration when needed.

### 2026-01-03 - Vendor Payment System Refactored to RazorpayX Payouts (Iteration 35)

**Critical Fix: Vendor Payments Now Use OUTGOING Transfers**

The vendor payment system was incorrectly using Razorpay Orders API (for receiving payments from customers). It has been refactored to use **Razorpay Payouts API (RazorpayX)** for sending payments to vendors.

**Key Changes:**

1. **Backend (`/app/backend/routers/vendor.py`):**
   - Added `MOCK_PAYOUT_MODE` environment flag for testing
   - `initiate_vendor_payment()` now creates payouts instead of orders
   - Added `get_payment_status()` endpoint for polling payout status
   - Mock payouts auto-complete with simulated UTR numbers
   - Bank details validation (bank_account, ifsc_code) required for Razorpay mode
   - Manual payment modes (bank_transfer, UPI, cash) work without RazorpayX

2. **Frontend (`/app/frontend/src/pages/erp/VendorManagement.js`):**
   - Removed `openRazorpayCheckout()` function (not needed for outgoing payments)
   - Added `pollPayoutStatus()` to check payout completion
   - Added `payoutProcessing` and `payoutStatus` state management
   - Payment modal now shows:
     - "Pay Vendor" title + "Outgoing Bank Transfer" subtitle
     - "RazorpayX Payout (Auto UTR)" option
     - "Send Payout ₹X" button (instead of "Pay Online")
     - Payout status indicator (Processing/Completed/Failed)
     - UTR display when payout completes

**New API Endpoints:**
- `GET /api/erp/vendors/payment/{payment_id}/status` - Check payout status and auto-complete mock payouts
- `GET /api/erp/vendors/payments/history` - Get payment history with payout status

**Mock Mode Behavior:**
- When `MOCK_PAYOUT_MODE=true`:
  - Payouts are simulated (no actual bank transfer)
  - Mock UTR generated: `UTR` + 12 random digits
  - Payout auto-completes when status is polled
- When `MOCK_PAYOUT_MODE=false`:
  - Real RazorpayX API called (requires `RAZORPAYX_ACCOUNT_NUMBER` env var)
  - UTR received via webhook when bank confirms transfer

**Test Results (iteration_35):**
- Backend: 20/20 tests passed (100%)
- Frontend: All UI elements verified (100%)
- Test file: `/app/tests/test_vendor_payment_payout.py`

**Files Modified:**
- `/app/backend/routers/vendor.py` - Payout logic, status endpoint, mock mode
- `/app/frontend/src/pages/erp/VendorManagement.js` - Payment modal UI, payout polling


### 2026-01-03 - Company Logo Implemented Everywhere

**Logo URL:** `https://customer-assets.emergentagent.com/job_0aec802e-e67b-4582-8fac-1517907b7262/artifacts/752tez4i_Logo%20Cucumaa%20Glass.png`

**Locations Updated:**
- Website Header (navigation)
- Website Footer
- Login Page
- ERP Sidebar
- Customer Portal Header
- All PDF Documents (Invoices, Receipts, Dispatch Slips, PO)
- Email Templates (OTP emails)

**Files Modified:**
- `/app/frontend/src/components/Header.js`
- `/app/frontend/src/components/Footer.js`
- `/app/frontend/src/components/ERPLayout.js`
- `/app/frontend/src/pages/Login.js`
- `/app/frontend/src/pages/CustomerPortalEnhanced.js`
- `/app/backend/routers/pdf_generator.py`
- `/app/backend/routers/auth_router.py`



### 2026-01-03 - Ledger Management System Implemented (Iteration 36)

**Comprehensive Customer, Vendor & General Ledger System with CA-ready reports**

**Features Implemented:**

1. **Customer Ledger (Sales Ledger)**
   - Opening balance entry
   - Invoice-wise sales tracking
   - Payments received tracking
   - Running balance calculation
   - Period-wise statements

2. **Vendor Ledger (Purchase Ledger)**
   - Opening balance entry
   - Purchase bills tracking
   - Payments made tracking
   - Running balance calculation
   - Period-wise statements

3. **General Ledger (GL)**
   - 13 default GL accounts (Assets, Liabilities, Income, Expense, Equity)
   - Trial Balance generation
   - Double-entry bookkeeping structure

4. **Outstanding Reports (Udhaari)**
   - Customer outstanding with ageing (0-30, 31-60, 61-90, 90+ days)
   - Vendor outstanding with ageing
   - Total outstanding summary

5. **Period Lock System**
   - Quarterly/Half-yearly/Yearly period locks
   - Role-based locking (HR, Finance, Accountant)
   - Admin/Super Admin can override or grant exceptions

6. **CA/Auditor Role**
   - New role with read-only access to all ledgers
   - Can view Trial Balance, Customer/Vendor Ledgers, Outstanding Reports

**Key API Endpoints:**
- `POST /api/erp/ledger/init-gl-accounts` - Initialize Chart of Accounts
- `POST /api/erp/ledger/opening-balance` - Set opening balance for customer/vendor
- `GET /api/erp/ledger/customer/{id}` - Get customer ledger statement
- `GET /api/erp/ledger/vendor/{id}` - Get vendor ledger statement
- `GET /api/erp/ledger/customers/outstanding` - Customer outstanding report with ageing
- `GET /api/erp/ledger/vendors/outstanding` - Vendor outstanding report with ageing
- `GET /api/erp/ledger/gl/accounts` - Get GL chart of accounts
- `GET /api/erp/ledger/gl/trial-balance` - Get trial balance
- `POST /api/erp/ledger/period-lock` - Create period lock
- `GET /api/erp/ledger/period-locks` - Get all period locks
- `GET /api/erp/ledger/audit-trail` - Get ledger audit trail

**Automation Rules:**
- NO manual ledger entries allowed
- All entries auto-generated from Sales Invoice, Purchase Bill, Payment, Receipt
- Every change logged in audit trail

**Files Created:**
- `/app/backend/routers/ledger.py` - Complete ledger management backend
- `/app/frontend/src/pages/erp/LedgerManagement.js` - Ledger UI with tabs

**Files Modified:**
- `/app/backend/erp_routes.py` - Added ledger_router
- `/app/frontend/src/components/ERPLayout.js` - Added Ledger & GL menu, CA/Auditor roles
- `/app/frontend/src/App.js` - Added /erp/ledger route

**Access Control:**
| Role | Access Level |
|------|-------------|
| super_admin, admin | Full control (read/write/lock) |
| finance, accountant | Full view + reconciliation |
| ca, auditor | Read-only (all ledgers) |
| sales_manager | Customer ledger only |
| customer | Own ledger only (read-only) |
| vendor | Own ledger only (read-only) |



### 2026-01-03 - Customer Profile / Master Module (Complete)

**Comprehensive Customer Data Management System for GST, Billing, Credit Control & CRM**

**Module Overview:**
A comprehensive customer profile system as per the user's detailed specification, storing company identity, GST, billing/shipping addresses, credit terms, and bank details, permanently linked to invoices, ledger, and payments, with auto-application across all orders and full audit trail.

**9 Data Sections Implemented:**

1. **Basic Identity Details**
   - Customer Type: Individual, Proprietor, Partnership, Pvt Ltd, Ltd, Builder, Dealer, Architect
   - Company / Firm Name (required for B2B with GSTIN)
   - Individual Name (required for B2C retail)
   - Contact Person, Mobile (validated), Email

2. **GST & Tax Details**
   - GSTIN Number with format validation (15-char: 2 digits + PAN + entity + Z + checksum)
   - PAN Number with format validation (10-char)
   - State Code auto-extracted from GSTIN
   - Place of Supply (auto from state)
   - GST Type: Regular, Composition, Unregistered

3. **Billing Address (Mandatory)**
   - Address Line 1 & 2, City, State, State Code, PIN Code, Country

4. **Shipping / Site Addresses (Multiple)**
   - Project / Site Name, Address, Contact at site
   - Multiple shipping addresses support
   - Default address flagging

5. **Business & Credit Control**
   - Customer Category: Retail, Builder, Dealer, Project
   - Credit Type: Advance Only, Credit Allowed
   - Credit Limit (₹) with value
   - Credit Days: 0, 7, 15, 30, 45, 60, 90 days

6. **Bank Details (Internal Use Only)**
   - Account Holder Name, Bank Name, Account Number
   - IFSC Code, UPI ID (optional)
   - NOT printed on invoices

7. **Invoice Preferences**
   - Language: English / Hindi
   - Email Invoice Auto-Send
   - WhatsApp Invoice Sharing
   - PO Mandatory flag

8. **Compliance & Declaration**
   - GST Declaration Checkbox
   - Data Consent Checkbox
   - Terms & Conditions Accepted
   - KYC Status: Pending / Verified / Rejected

9. **CRM & Sales Tracking**
   - Sales Person Assigned
   - Source: Reference, Google, Dealer, Website, Social Media, Walk-in
   - Notes / Remarks
   - Special Pricing (future scope)

**Auto-Linked Sections (System Generated - Read Only):**
- Sales Ledger
- Outstanding Balance
- Ageing Report (0-30, 31-60, 61-90, 90+ days)
- Order History
- Invoice History
- Payment History

**Auto-Switch Logic (B2B vs B2C):**
- If GSTIN provided → Company Name mandatory → Invoice Type = B2B
- If No GSTIN + Individual type → Individual Name mandatory → Invoice Type = B2C
- Both cannot be blank simultaneously

**API Endpoints:**
- `GET /api/erp/customer-master/states` - Get 38 Indian states with codes
- `GET /api/erp/customer-master/stats` - Customer statistics
- `POST /api/erp/customer-master/` - Create customer profile
- `GET /api/erp/customer-master/` - List with pagination, search, filters
- `GET /api/erp/customer-master/{id}` - Get single customer with linked data
- `PUT /api/erp/customer-master/{id}` - Update customer
- `PATCH /api/erp/customer-master/{id}/deactivate` - Soft delete (no hard delete)
- `PATCH /api/erp/customer-master/{id}/reactivate` - Reactivate
- `POST /api/erp/customer-master/{id}/shipping-addresses` - Add shipping address
- `PUT /api/erp/customer-master/{id}/shipping-addresses/{addr_id}` - Update address
- `DELETE /api/erp/customer-master/{id}/shipping-addresses/{addr_id}` - Delete address
- `PATCH /api/erp/customer-master/{id}/kyc` - Update KYC status (CA/Admin)
- `POST /api/erp/customer-master/migrate-existing` - Migrate from users collection
- `GET /api/erp/customer-master/search/for-invoice` - Quick search for invoicing

**Access Control:**
| Role | Access Level |
|------|-------------|
| super_admin, admin, owner | Full CRUD + deactivate |
| accounts, sales | Create, Read, Update |
| sales_executive | Read only |
| ca | KYC update + Read |

**Files Created:**
- `/app/backend/routers/customer_master.py` - Complete backend router
- `/app/frontend/src/pages/erp/CustomerManagement.js` - Full UI with modals

**Files Modified:**
- `/app/backend/erp_routes.py` - Added customer_master_router
- `/app/frontend/src/utils/erpApi.js` - Added customerMaster API methods
- `/app/frontend/src/components/ERPLayout.js` - Added Customer Master sidebar item
- `/app/frontend/src/App.js` - Added /erp/customer-master route

**MongoDB Collection:**
- `customer_profiles` - Stores comprehensive customer data

**Data Migration:**
- Existing customers from `users` collection can be migrated via `/migrate-existing` endpoint
- Migration preserves user IDs for linkage
- Skips duplicates (by mobile or email)

**Test Status:**
- Backend: All validations working (GSTIN, PAN, Mobile)
- Frontend: List, Create, View, Edit modals working
- Stats dashboard showing B2B/B2C/Credit/KYC counts



### 2026-01-03 - Customer Master Integration with Orders & Customer Portal (Complete)

**Integration Features Implemented:**

1. **Order Creation Auto-Population**
   - `customer_profile_id` field added to OrderCreate models (both server.py and orders_router.py)
   - When provided, auto-populates: customer_name, company_name, GSTIN, billing_address
   - Auto-enables credit for credit_allowed customers
   - Credit limit validation before order creation

2. **Customer Search Component**
   - New reusable component: `/app/frontend/src/components/CustomerSearch.js`
   - Searchable dropdown with debounced search
   - Shows B2B/B2C badges, credit info, billing address preview
   - Ready to integrate into any order/invoice creation form

3. **Customer Portal Enhancements**
   - Profile tab now shows Customer Master data
   - Customer Code, GSTIN, Credit Limit, Credit Days displayed
   - Account Summary section with:
     - Total Orders, Total Spent
     - Outstanding Balance
     - Overdue (90+ days)
     - Ageing breakdown (0-30, 31-60, 61-90, 90+ days)
   - Billing Address display

**Files Modified:**
- `/app/backend/server.py` - Added customer_profile_id to OrderCreate, auto-population logic
- `/app/backend/routers/orders_router.py` - ERP order creation with customer_profile_id
- `/app/frontend/src/pages/CustomerPortalEnhanced.js` - Enhanced profile tab
- `/app/frontend/src/components/CustomerSearch.js` (NEW) - Reusable search component

**Test Results:**


### 2026-01-03 - CustomerSearch Integration & Customer Portal Self-Edit (Complete)

**1. CustomerSearch Integration in Order Forms:**
- Added CustomerSearch component to `/app/frontend/src/pages/CustomizeBook.js` (Step 2 - Delivery Details)
- When customer is selected from search:
  - Auto-populates Customer Name, Company Name
  - Auto-populates GSTIN
  - Auto-populates Delivery Address from billing address
  - Auto-sets Delivery State from GSTIN
  - Shows credit info toast for credit customers
  - Enables credit order option if customer has credit allowed
- Order creation now sends `customer_profile_id` to link order with Customer Master

**2. Customer Portal Self-Edit Profile Feature:**

Customer can now edit their own profile with the following sections:

**Section 1: Basic Identity**
- Customer Type (Individual/Proprietor/Partnership/Pvt Ltd/Ltd/Builder/Dealer/Architect)
- Company / Firm Name (shown when GSTIN present or non-individual type)
- Individual Name (shown for retail without GSTIN)
- Contact Person
- Mobile Number (10-digit validation)
- Email ID

**Section 2: GST & Tax Details**
- GSTIN Number (format validation: 22AAAAA0000A1Z5)
- PAN Number (format validation: AAAAA0000A)
- GST Type (Regular/Composition/Unregistered)
- Place of Supply (state dropdown)

**Section 3: Billing Address (Mandatory)**
- Address Line 1, Address Line 2
- City, State (dropdown), PIN Code

**Section 4: Shipping / Site Addresses (Optional - Multiple)**
- Project / Site Name
- Address
- City, State, PIN Code
- Contact Person (Supervisor/Manager)
- Contact Phone
- Note: "Transport charges will be calculated based on shipping location"
- Add Address button to add multiple shipping addresses

**Files Modified:**
- `/app/frontend/src/pages/CustomizeBook.js` - Added CustomerSearch integration
- `/app/frontend/src/pages/CustomerPortalEnhanced.js` - Added Edit Profile Modal

**Test Results:**
- All features verified (100% success)
- Test report: `/app/test_reports/iteration_37.json`


- All 16 backend API tests passed (100%)
- Frontend UI verified working
- Test report: `/app/test_reports/iteration_36.json`


### 2026-01-04 - CustomerSearch Integration Final Verification (Complete)

**Full E2E Testing of CustomerSearch in Order Form:**

The CustomerSearch integration in `CustomizeBook.js` has been thoroughly tested and verified:

**Features Verified:**
1. **Customer Search** - Search by name, mobile, or GSTIN works correctly
2. **Search Dropdown** - Shows customer results with B2B/B2C badge, credit info (₹500,000 / 30d), and GSTIN
3. **Auto-Population** - All fields auto-fill correctly:
   - Customer Name: "ABC Builders Pvt Ltd"
   - Company Name: "ABC Builders Pvt Ltd"
   - GSTIN: "27AABCA1234F1ZM"
   - Delivery State: "Maharashtra (27)"
   - Delivery Address: "123 Builder Lane, Industrial Area, Mumbai, Maharashtra - 400001"
4. **Credit Customer Selection**:
   - Green banner: "Credit Customer: ₹500,000 limit, 30 days"
   - Toast notification: "Credit customer selected! Limit: ₹500,000, 30 days"
   - Credit (Pay Later) payment option enabled
5. **Clear Selection** - X button clears customer and allows re-search
6. **Order Creation** - customer_profile_id correctly linked to orders

**Test Results:**
- Backend: 16/16 tests passed (100%)
- Frontend: All CustomerSearch features verified (100%)
- Test file: `/app/tests/test_customer_master_integration.py`
- Test report: `/app/test_reports/iteration_39.json`

**Files Involved:**
- `/app/frontend/src/pages/CustomizeBook.js` - CustomerSearch at line 1319, handleCustomerSelect at line 220
- `/app/frontend/src/components/CustomerSearch.js` - Reusable search component
- `/app/backend/routers/customer_master.py` - Backend API `/api/erp/customer-master/search/for-invoice`




### 2026-01-04 - 3D Glass Configurator Drag-and-Drop Fix (Complete)

**Bug Fixed:** Cutouts were not draggable - clicking and dragging on the 3D preview did not move the cutout or update distance labels.

**Root Cause Analysis:**
1. **Stale Closure Issue:** The `moveCutout` function was using `config.cutouts.map()` which captured a stale reference to the `config` object, not the current state.
2. **Picking Issue:** The glass mesh was pickable, blocking clicks from reaching the cutout meshes behind it.
3. **Z-Index Issue:** Cutouts were positioned at z=0, same as the glass panel, making them harder to pick.
4. **earcut Import:** Triangle cutouts crashed due to incorrect import syntax for the earcut library.

**Fixes Applied:**
1. **Functional State Update:** Changed `moveCutout` to use `setGlassItems(prevItems => ...)` pattern to access the latest state
2. **Added activeItemIndexRef:** Created a ref to track the current active item index, accessible inside event handlers
3. **Glass Not Pickable:** Added `glass.isPickable = false` so clicks pass through to cutout meshes
4. **Cutout Z-Position:** Moved cutouts to `z=15` (in front of glass) for better picking
5. **Fixed earcut Import:** Changed `import * as earcut from 'earcut'` to `import earcut from 'earcut'`

**Files Modified:**
- `/app/frontend/src/pages/GlassConfigurator3D.js` - All drag-and-drop fixes

**Features Working:**
- Drag cutouts to reposition on the glass panel
- Real-time distance labels update during drag (Left, Right, Top, Bottom in mm)
- All cutout shapes: Single Hole, Double Hole, Rectangle, Triangle, Half Circle, Corner Carve, Rounded Rect, Hexagon, Heart
- Multi-item cart with individual cutouts per glass item
- Zoom In/Out and Reset View controls
- Live price calculation with cutouts

**Test Results:**
- All 12 3D configurator features tested (100% pass)
- Test report: `/app/test_reports/iteration_43.json`


### 2026-01-04 - 3D Glass Configurator - Click-to-Place & Resize Handles (Complete)

**Major Enhancement:** Complete rewrite of the 3D Glass Configurator with interactive shape placement, resizing, and rotation.

**New Features Implemented:**
1. **Click-to-Place Shapes:**
   - Click any shape button (Hole, Rectangle, Triangle, Hexagon, Heart) to enter placement mode
   - Blue highlight on selected shape button
   - "Click on glass to place" message appears
   - Click anywhere on the glass to place the shape at that position
   - Uses ray-based picking for accurate placement

2. **Default Shape Sizes (No Pre-selection Required):**
   - Hole (Circle): Ø 50mm
   - Rectangle: 100 × 80mm
   - Triangle: 100 × 80mm
   - Hexagon: Ø 60mm
   - Heart: Ø 60mm

3. **Resize Handles (8 handles):**
   - 4 corner handles (top-left, top-right, bottom-left, bottom-right)
   - 4 midpoint handles (top, bottom, left, right)
   - Blue spheres for visual identification
   - Drag to resize shape proportionally

4. **Rotation Handle:**
   - Green sphere above the shape
   - Connected by a line to the shape
   - Drag horizontally to rotate 0-360°

5. **Real-time Dimension Display:**
   - Inner label inside shape: "H Ø 50 mm" or "R 100 × 80 mm"
   - Edge distances from all 4 glass edges: ← mm, → mm, ↑ mm, ↓ mm
   - Updates instantly during drag/resize/rotate

6. **Removed Features:**
   - Predefined diameter/width/height input fields
   - "Add Cutout" button (replaced with click-to-place)

**Technical Implementation:**
- Ray-based picking using `scene.createPickingRay()` for XY plane intersection
- Functional state updates (`setGlassItems(prev => ...)`) for smooth interactions
- Refs for synchronous access to state in event handlers
- Babylon.js meshes for cutouts, handles, and rotation indicator

**Files Modified:**
- `/app/frontend/src/pages/GlassConfigurator3D.js` - Complete rewrite (~1000 lines)

**Test Results:**
- All 13 features tested - 100% pass rate
- Test report: `/app/test_reports/iteration_44.json`


### 2026-01-04 - 3D Glass Configurator Stability & UI Overhaul (Complete)

**Major Enhancement:** Complete stability refactor and UI improvement of the 3D Glass Configurator.

**Stability Improvements:**
1. **Glass Panel FIXED at Origin:**
   - Glass mesh created ONCE with unit size (1×1×1)
   - Scaled via `scaling` property - never recreated
   - Position locked at Vector3.Zero()
   - `isPickable = false` - glass never interferes with shape picking

2. **No Glass Blinking:**
   - Glass mesh never disposed during shape interactions
   - Re-render only when glass size/color explicitly changes
   - Cutout meshes recreated on each state change for reliability

3. **Smooth Shape Movement:**
   - Direct state updates during drag (no deferred commit)
   - `requestAnimationFrame` used for pointer events
   - Continuous tracking - no step-based recalculation

4. **Labels Attached to Shapes:**
   - Inner dimension label (H Ø50, R 100×80) positioned at shape center
   - Edge distance labels (←, →, ↑, ↓) positioned relative to shape bounds
   - All labels update together with shape position

5. **Zoom Safety:**
   - Zoom only affects camera ortho bounds
   - Glass mesh position and scale unaffected by zoom
   - No flickering during zoom operations

**UI Improvements:**
1. **Dropdown Menus** for:
   - Glass Type (Toughened, Laminated, Frosted)
   - Thickness (5mm, 6mm, 8mm, 10mm, 12mm)
   - Color (Clear, Grey, Bronze, Blue, Green)
   - Application (Window, Door, Partition, Railing, Shower)

2. **Compact Layout** - Space-efficient design with:
   - 5 shape buttons in a row
   - Inline zoom controls
   - Collapsible cutout info panel

**Technical Implementation:**
- Glass mesh: Unit-sized box, scaled dynamically
- Cutout meshes: Disposed and recreated on each state update
- State: React useState with functional updates
- Refs: For synchronous access in event handlers
- No deferred state commits - immediate updates

**Test Results:**
- All 11 stability features tested - 100% pass rate
- Test report: `/app/test_reports/iteration_45.json`
- Bug fixed: Price API 422 error (missing color_name)

**Files Modified:**
- `/app/frontend/src/pages/GlassConfigurator3D.js` - Complete stability refactor


### 2026-01-04 - Dimension Input Fields Enhancement (Complete)

**Enhancement:** Added dimension input fields for precise cutout sizing.

**New Features:**
1. **Diameter Input (Ø)** - For circular shapes (Hole, Hexagon, Heart)
   - Direct mm input
   - Range: 20-400mm
   - Real-time 3D update

2. **Width/Height Inputs** - For rectangular shapes (Rectangle, Triangle)
   - Separate W and H fields
   - Range: 30-400mm each
   - Independent control

3. **Position Inputs (X, Y)** - For all shapes
   - Position from bottom-left corner
   - Auto-clamped to glass boundaries
   - Alternative to drag positioning

4. **Real-time Updates** - All inputs update:
   - 3D shape size/position
   - Inner dimension labels
   - Edge distance labels
   - Instantly, no lag

**User Workflow:**
- Place shape on glass (click)
- Fine-tune with input fields OR drag/handles
- Both methods work together

**Files Modified:**
- `/app/frontend/src/pages/GlassConfigurator3D.js` - Added `updateCutoutDimension` and `updateCutoutPosition` functions, enhanced selected cutout panel UI


### 2026-01-04 - Advanced Cutout Features Enhancement (Complete)

**Major Enhancement:** Added professional glass manufacturing features.

**New Features:**

1. **Preset Hole Sizes (Quick Select):**
   - Buttons: Ø5, Ø8, Ø10, Ø12, Ø14, Ø16, Ø20, Ø25, Ø30, Ø40, Ø50mm
   - One-click size change for circular shapes
   - Current size highlighted
   - Common sizes used in glass manufacturing

2. **Snap-to-Grid Feature:**
   - Toggle button: "Snap ON/OFF"
   - Grid size options: 5mm, 10mm, 20mm, 50mm
   - Positions snap to nearest grid point during drag
   - CNC/manufacturing-ready positioning

3. **Copy/Duplicate Cutouts:**
   - Copy button next to each selected cutout
   - Duplicates shape with 30mm offset
   - Preserves all properties (size, rotation)
   - Quick creation of similar cutouts

4. **Cutout Templates (Pre-saved Configurations):**
   - **Door Handle Hole** - Ø35mm standard cutout
   - **Hinge Cutout** - 80×25mm rectangle
   - **Lock Cutout** - 25×150mm mortise lock
   - **Cable Pass-through** - Ø50mm hole
   - **Ventilation Hole** - Ø25mm small hole
   - One-click placement at glass center

**Files Modified:**
- `/app/frontend/src/pages/GlassConfigurator3D.js` - Added PRESET_HOLE_SIZES, SNAP_GRID_SIZES, CUTOUT_TEMPLATES constants and corresponding UI/functions

**Technical Implementation:**
- `snapToGrid()` function rounds positions to nearest grid value
- `duplicateCutout()` clones cutout with offset
- `placeFromTemplate()` creates cutout from template config
- `applyPresetSize()` updates dimension from preset button
- Refs for snap state to use in event handlers


### 2026-01-04 - Auto-Align, Door Fittings & Freeform Polygon (Complete)

**Major Enhancement:** Added three new professional features for glass manufacturing.

**New Features:**

1. **Auto-Align Feature:**
   - Snap selected cutout to glass edges/center
   - Buttons: ◀ Left, ↔ H-Center, ↕ V-Center, Right ▶, ▲ Top, ⊕ Center Both, ▼ Bottom
   - One-click precise positioning
   - Panel appears only when cutout is selected

2. **Door Fittings Templates (Categorized):**
   - **Handles**: Center (Ø35mm at 85%,50%), Upper, Lower
   - **Locks**: Center (25×120mm), Floor Level (30×80mm)
   - **Hinges**: Top (80×25mm at 8%,92%), Middle, Bottom
   - **Floor Spring/Pivot**: Center (50×180mm), Floor Pivot Hole (Ø25mm at 50%,3%), Top Pivot Hole
   - Each fitting placed at industry-standard positions

3. **Freeform Polygon Drawing (Enhanced):**
   - Click Freeform button to enter drawing mode
   - Click points on glass to define polygon vertices
   - **Visual Preview**: Green sphere for first point, teal spheres for others, connecting lines
   - Click near first point or click "Complete" button to finish
   - Creates custom teal polygon cutout (PG type) with full editing controls
   - Supports move, resize, rotate operations

**Files Modified:**
- `/app/frontend/src/pages/GlassConfigurator3D.js` - Added DOOR_FITTINGS with categories, autoAlignCutout(), placeDoorFitting(), enhanced handlePolygonDrawClick(), polygon preview rendering useEffect

**Technical Implementation:**
- `autoAlignCutout(alignment)` - Moves selected cutout based on alignment type
- `placeDoorFitting(fitting)` - Places door hardware at predefined position
- `getGlassCoordinates()` - Fixed for orthographic camera with NDC calculation
- `polygonPreviewMeshesRef` - Stores preview spheres and lines during drawing
- DOOR_FITTINGS array with category field for UI grouping

---

## Pending/Upcoming Tasks

**P0 - Sync JobWork3DConfigurator.js:**
- Port all new features (Auto-Align, Door Fittings, Freeform Polygon) to job work configurator

**P2 - Automatic Transport Charges:**
- Calculate and add transport charges to invoice automatically

**P3 - Razorpay Payouts Live Mode:**
- Switch mocked vendor payment system to live RazorpayX Payout API

**P4 - Native Mobile App (SFA):**
- Develop native mobile application for Sales Force Automation module



### 2026-01-04 - UI/UX Enhancements & Quick Door Setup (Complete)

**Enhancement:** Improved glass visual and added faster workflow features.

**New Features:**

1. **Quick Door Setup Button:**
   - One-click adds 5 standard door fittings: Handle + 3 Hinges + Lock
   - Orange gradient button with ⚡ icon
   - Toast confirmation: "🚪 Door Setup Complete!"

2. **Quick Size Presets:**
   - 5 preset buttons: 600×900, 900×1200, 1m×2.1m Door, 1.2m×2.4m, 500×500
   - Selected preset highlighted in blue
   - One-click dimension change

3. **Blue Transparent Glass:**
   - Glass material: Light blue tint (0.7, 0.85, 0.95 RGB)
   - Alpha: 0.4 (more transparent)
   - Specular highlights for glass effect
   - 3D border/edge bars around all 4 sides (darker blue)

4. **Clearer Dimension Labels:**
   - Bigger font size: 18px (was 14px)
   - Blue color: #0066CC
   - Format: ◀─── X mm ───▶ (width) and ▲ X mm ▼ (height)
   - Thicker white outline for visibility

**Files Modified:**
- `/app/frontend/src/pages/GlassConfigurator3D.js` - Added quickDoorSetup(), createGlassEdges(), updated glass material, enhanced dimension labels, added Quick Sizes preset buttons

