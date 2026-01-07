# Lucumaa Glass ERP - Changelog

## 2026-01-02 - Advance Payment System

### Added
- **Advance Payment Options** on order creation
  - 25%, 50%, 75%, 100% advance payment choices
  - Minimum 25% advance required
  - Remaining payment after product ready, before dispatch
  
- **6-Digit Order Numbers**
  - Auto-generated sequential order numbers (000001, 000002, etc.)
  - Replaces UUID-based order IDs in display

- **Customer Details on Orders**
  - Customer Name field (required)
  - Company Name field (optional)
  - Displayed in order sheet and customer portal

- **New Backend APIs**
  - `POST /api/orders/{id}/initiate-remaining-payment`
  - `POST /api/orders/{id}/verify-remaining-payment`

- **Updated Order Model**
  - `order_number`: 6-digit sequential number
  - `customer_name`: Customer's name
  - `company_name`: Company name (optional)
  - `advance_percent`: 25, 50, 75, or 100
  - `advance_amount`: Calculated advance payment
  - `remaining_amount`: Amount due before dispatch
  - `advance_payment_status`: pending/paid
  - `remaining_payment_status`: not_applicable/pending/paid

### Modified Files
- `/app/backend/server.py` - Order model, payment APIs
- `/app/frontend/src/pages/CustomizeBook.js` - Advance payment UI
- `/app/frontend/src/pages/CustomerPortal.js` - Orders display with payment status
- `/app/frontend/src/utils/api.js` - Track order API fix

---

## 2026-01-02 - Track Order Fix

### Fixed
- Track Order API endpoint path corrected
- Added missing fields (quantity, total_price, unit) to track response
- Frontend defensive handling for undefined values

---

## 2026-01-01 - SFA Phase 2 Complete

### Added
- **SFA Alerts API** (`GET /api/erp/sfa/alerts`)
  - Location OFF alerts (GPS signal lost for 15+ minutes)
  - Long Stop alerts (stationary 30+ minutes without visit)
  - Zero Visits alerts (2+ hours with no customer visits)
  - Low Battery alerts (device battery below 20%)

- **SFA Performance Scorecard API** (`GET /api/erp/sfa/performance-scorecard`)
  - Employee grades: A+, A, B+, B, C, D, F
  - 5 KPI scores: Attendance, Distance, Visits, Conversion, Time Utilization

- **SFA Stops Detection API** (`GET /api/erp/sfa/stops/{user_id}`)
  - Detects stops where employee stayed 5+ minutes
  - Differentiates visit stops vs idle stops

### Enhanced Frontend
- Performance tab: Score distribution chart, detailed breakdown table, grade badges
- Alerts tab: Summary cards by alert type, severity badges

---

## 2026-01-01 - SFA Phase 1

### Added
- Day Start/End with GPS tracking
- Visit tracking with check-in/check-out
- Location ping recording
- Fuel allowance calculation
- Team dashboard for managers

---

## 2026-01-01 - Super Admin Panel & Audit Trail

### Added
- Super Admin dashboard with user management
- Audit trail logging all user actions
- User enable/disable/delete functionality

---

## 2026-01-01 - Password Reset Feature

### Added
- Email OTP via Hostinger SMTP
- Mobile OTP via Twilio SMS
- Secure reset token flow

---

## Earlier Updates
- Assets & Holidays dashboards
- Customer Portal with OTP auth
- Razorpay Payouts integration
- QR Codes & Barcode generation
- Email notifications
- Dashboard charts
- Refer & Earn system
- WhatsApp notifications via Twilio
