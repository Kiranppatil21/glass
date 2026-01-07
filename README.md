# Lucumaa Glass - Factory-Direct Glass Manufacturing Website

## Overview
Lucumaa Glass is a comprehensive full-stack web application for factory-direct toughened and customized glass manufacturing. Built with modern tech stack (React + FastAPI + MongoDB), it features online customization, instant pricing, role-based dashboards, and integrated payment processing.

## Features

### Core Features
- **Product Catalog**: Browse 4+ glass types (Toughened, Laminated, Insulated, Frosted)
- **Customize & Book**: Multi-step customization workflow with instant pricing
- **Price Calculator**: Real-time pricing with bulk discounts and GST calculation
- **Order Tracking**: Track orders by Order ID
- **Role-Based Dashboards**: Customer, Dealer/Architect, and Admin dashboards
- **File Upload**: Upload drawings/sketches for custom orders
- **Payment Integration**: Razorpay payment gateway (ready to integrate)
- **Notifications**: Email (Resend), WhatsApp & SMS (Twilio) - ready to integrate

### Tech Stack
- **Frontend**: React 19, Tailwind CSS, Framer Motion, Shadcn/UI, React Router
- **Backend**: FastAPI, Python 3.11, Motor (async MongoDB)
- **Database**: MongoDB
- **Payment**: Razorpay
- **Notifications**: Resend (Email), Twilio (SMS/WhatsApp)

## Design
- **Theme**: "Crystal & Steel" - Modern Industrial Luxury
- **Color Palette**: Cyan/Teal primary colors with slate neutrals
- **Typography**: Manrope (headings), DM Sans (body)
- **Style**: Glassmorphism effects, smooth animations, clean layouts

## Setup Instructions

### Prerequisites
The application is already running in this environment. Services are managed by supervisorctl.

### Required API Keys (for full functionality)

1. **Razorpay** (Payment Processing)
   - Get from: https://dashboard.razorpay.com/
   - Add to `/app/backend/.env`:
     ```
     RAZORPAY_KEY_ID=your_key_id
     RAZORPAY_KEY_SECRET=your_key_secret
     ```
   - Add to `/app/frontend/.env`:
     ```
     REACT_APP_RAZORPAY_KEY_ID=your_key_id
     ```

2. **Resend** (Email Notifications)
   - Get from: https://resend.com/
   - Add to `/app/backend/.env`:
     ```
     RESEND_API_KEY=re_your_api_key
     SENDER_EMAIL=your_verified_email@domain.com
     ```

3. **Twilio** (SMS & WhatsApp Notifications)
   - Get from: https://www.twilio.com/console
   - Add to `/app/backend/.env`:
     ```
     TWILIO_ACCOUNT_SID=your_account_sid
     TWILIO_AUTH_TOKEN=your_auth_token
     TWILIO_PHONE_NUMBER=+1234567890
     TWILIO_WHATSAPP_NUMBER=whatsapp:+1234567890
     ```

### After Adding API Keys
```bash
# Restart backend to load new environment variables
sudo supervisorctl restart backend
```

## Application Structure

### Pages
- **Home**: Hero section, quick calculator, product showcase, why choose us
- **Products**: Product catalog with filtering
- **Product Detail**: Individual product information
- **Customize & Book**: Multi-step order customization (Configure → Details → Payment)
- **Industries**: Industries served (placeholder)
- **How It Works**: Process flow (placeholder)
- **Pricing**: Pricing calculator (placeholder)
- **Resources**: Technical resources (placeholder)
- **About**: Company information (placeholder)
- **Contact**: Contact form (placeholder)
- **Login/Register**: Authentication with role selection
- **Track Order**: Order tracking by ID
- **Customer Dashboard**: View orders, order history
- **Dealer Dashboard**: Same as customer (can be extended)
- **Admin Dashboard**: Manage all orders, update status

### API Endpoints

#### Authentication
- `POST /api/auth/register` - Register new user
- `POST /api/auth/login` - Login
- `GET /api/auth/me` - Get current user

#### Products
- `GET /api/products` - Get all products
- `GET /api/products/{id}` - Get single product

#### Pricing & Orders
- `POST /api/pricing/calculate` - Calculate price
- `POST /api/orders` - Create order
- `GET /api/orders/my-orders` - Get user's orders
- `GET /api/orders/track/{order_id}` - Track order
- `POST /api/orders/{order_id}/upload` - Upload file
- `POST /api/orders/{order_id}/payment` - Verify payment

#### Admin
- `GET /api/admin/orders` - Get all orders (admin only)
- `PATCH /api/admin/orders/{order_id}/status` - Update order status

#### Contact
- `POST /api/contact` - Submit inquiry

## Default Data
The application seeds initial data on startup:
- 4 Products with specifications
- Pricing rules for all thickness options
- Base price: ₹50/sqft + (thickness × ₹5)
- Bulk discount: 10% for orders ≥ 10 quantity

## User Roles
1. **Customer**: Place orders, track orders, view dashboard
2. **Dealer/Architect**: Same as customer + special pricing (can be extended)
3. **Admin**: Manage all orders, update production status

## URLs
- Frontend: https://glassmesh.preview.emergentagent.com
- Backend API: https://glassmesh.preview.emergentagent.com/api

## Services Management
```bash
# Check status
sudo supervisorctl status

# Restart services
sudo supervisorctl restart backend
sudo supervisorctl restart frontend

# View logs
tail -f /var/log/supervisor/backend.*.log
tail -f /var/log/supervisor/frontend.*.log
```

## Payment Flow
1. User customizes glass and enters details
2. System calculates price and creates order
3. Razorpay payment gateway opens
4. User completes payment
5. Backend verifies payment signature
6. Order status updated to "confirmed"
7. Notifications sent via email/WhatsApp

## Notification Flow
- **Order Confirmation**: Email + WhatsApp when payment is verified
- **Status Updates**: Email + WhatsApp when admin updates order status
- **Contact Inquiry**: Email acknowledgment sent to user

## Important Notes

### Currently Mocked/Incomplete
- **Payment**: Razorpay requires actual API keys for processing
- **Notifications**: Resend & Twilio require API keys for sending
- **Placeholder Pages**: Industries, How It Works, Pricing, Resources, About, Contact need content

### Working Features
✅ Product catalog with real data
✅ Price calculator with instant calculations
✅ Customize & Book workflow (3 steps)
✅ Authentication & role-based access
✅ Order creation & tracking
✅ File upload for drawings
✅ Admin dashboard for order management
✅ Responsive design
✅ Modern UI with animations

## Next Steps

1. **Add API Keys**: Add Razorpay, Resend, and Twilio keys to enable full functionality
2. **Complete Placeholder Pages**: Add content to Industries, How It Works, etc.
3. **Testing**: Test complete user flow with real payment
4. **Enhanced Features**:
   - Bulk order upload for dealers
   - Advanced pricing rules
   - Production planning dashboard
   - Stock management
   - Delivery tracking integration
   - Invoice generation with GST
   - Warranty certificate generation

## Support
For questions or issues, contact Lucumaa Glass support team.

---
Built with ❤️ using Emergent AI Platform
# glass
