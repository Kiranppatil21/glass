import React from 'react';
import { BrowserRouter, Routes, Route, useLocation } from 'react-router-dom';
import { HelmetProvider } from 'react-helmet-async';
import { AuthProvider } from './contexts/AuthContext';
import { CartProvider } from './contexts/CartContext';
import { Toaster } from './components/ui/sonner';
import Header from './components/Header';
import Footer from './components/Footer';
import ERPLayout from './components/ERPLayout';
import AIChatWidget from './components/AIChatWidget';
import CartSidebar from './components/CartSidebar';
import Home from './pages/Home';
import Products from './pages/Products';
import ProductDetail from './pages/ProductDetail';
import GlassConfigurator3D from './pages/GlassConfigurator3D';
import Industries from './pages/Industries';
import HowItWorks from './pages/HowItWorks';
import Pricing from './pages/Pricing';
import Resources from './pages/Resources';
import Contact from './pages/Contact';
import Login from './pages/Login';
import TrackOrder from './pages/TrackOrder';
import CustomerPortalEnhanced from './pages/CustomerPortalEnhanced';
import JobWorkPage from './pages/JobWorkPage';
import Cart from './pages/Cart';
import Blog from './pages/Blog';
import BlogPost from './pages/BlogPost';
import JobWork3DConfigurator from './pages/JobWork3DConfigurator';
import SharedConfig from './pages/SharedConfig';
import CustomerDashboard from './components/dashboards/CustomerDashboard';
import DealerDashboard from './components/dashboards/DealerDashboard';
import AdminDashboard from './components/dashboards/AdminDashboard';
import ERPAdminDashboard from './pages/erp/AdminDashboard';
import CRMDashboard from './pages/erp/CRMDashboard';
import ProductionDashboard from './pages/erp/ProductionDashboard';
import HRDashboard from './pages/erp/HRDashboard';
import OperatorDashboard from './pages/erp/OperatorDashboard';
import InventoryDashboard from './pages/erp/InventoryDashboard';
import PurchaseDashboard from './pages/erp/PurchaseDashboard';
import AccountsDashboard from './pages/erp/AccountsDashboard';
import PayoutsDashboard from './pages/erp/PayoutsDashboard';
import WalletDashboard from './pages/erp/WalletDashboard';
import ExpenseDashboard from './pages/erp/ExpenseDashboard';
import AssetsDashboard from './pages/erp/AssetsDashboard';
import HolidaysDashboard from './pages/erp/HolidaysDashboard';
import SuperAdminDashboard from './pages/erp/SuperAdminDashboard';
import SFADashboard from './pages/erp/SFADashboard';
import OrderManagement from './pages/erp/OrderManagement';
import SettingsDashboard from './pages/erp/SettingsDashboard';
import ForecastDashboard from './pages/erp/ForecastDashboard';
import CMSDashboard from './pages/erp/CMSDashboard';
import BranchDashboard from './pages/erp/BranchDashboard';
import ProductConfigDashboard from './pages/erp/ProductConfigDashboard';
import TransportDashboard from './pages/erp/TransportDashboard';
import RewardsDashboard from './pages/erp/RewardsDashboard';
import GSTDashboard from './pages/erp/GSTDashboard';
import JobWorkDashboard from './pages/erp/JobWorkDashboard';
import VendorManagement from './pages/erp/VendorManagement';
import LedgerManagement from './pages/erp/LedgerManagement';
import CustomerManagement from './pages/erp/CustomerManagement';
import '@/App.css';

// Layout wrapper that conditionally shows header/footer
const AppLayout = ({ children }) => {
  const location = useLocation();
  const isERPRoute = location.pathname.startsWith('/erp');
  const isCustomerPortal = location.pathname.startsWith('/portal') || location.pathname.startsWith('/job-work');
  const showChat = !isERPRoute; // Show chat on public pages
  
  if (isERPRoute || isCustomerPortal) {
    if (isCustomerPortal) return children;
    return <ERPLayout>{children}</ERPLayout>;
  }
  
  return (
    <div className="min-h-screen flex flex-col">
      <Header />
      <main className="flex-1">{children}</main>
      <Footer />
      <CartSidebar />
      {showChat && <AIChatWidget />}
    </div>
  );
};

function AppRoutes() {
  return (
    <AppLayout>
      <Routes>
        {/* Public Website Routes */}
        <Route path="/" element={<Home />} />
        <Route path="/products" element={<Products />} />
        <Route path="/products/:id" element={<ProductDetail />} />
        <Route path="/customize" element={<GlassConfigurator3D />} />
        <Route path="/industries" element={<Industries />} />
        <Route path="/how-it-works" element={<HowItWorks />} />
        <Route path="/pricing" element={<Pricing />} />
        <Route path="/resources" element={<Resources />} />
        <Route path="/contact" element={<Contact />} />
        <Route path="/cart" element={<Cart />} />
        <Route path="/blog" element={<Blog />} />
        <Route path="/blog/:slug" element={<BlogPost />} />
        <Route path="/login" element={<Login />} />
        <Route path="/track" element={<TrackOrder />} />
        <Route path="/track-order" element={<TrackOrder />} />
        <Route path="/dashboard" element={<CustomerPortalEnhanced />} />
        <Route path="/dealer-dashboard" element={<DealerDashboard />} />
        <Route path="/admin-dashboard" element={<AdminDashboard />} />
        
        {/* ERP Routes */}
        <Route path="/erp/admin" element={<ERPAdminDashboard />} />
        <Route path="/erp/crm" element={<CRMDashboard />} />
        <Route path="/erp/production" element={<ProductionDashboard />} />
        <Route path="/erp/hr" element={<HRDashboard />} />
        <Route path="/erp/operator" element={<OperatorDashboard />} />
        <Route path="/erp/inventory" element={<InventoryDashboard />} />
        <Route path="/erp/purchase" element={<PurchaseDashboard />} />
        <Route path="/erp/accounts" element={<AccountsDashboard />} />
        <Route path="/erp/payouts" element={<PayoutsDashboard />} />
        <Route path="/erp/wallet" element={<WalletDashboard />} />
        <Route path="/erp/expenses" element={<ExpenseDashboard />} />
        <Route path="/erp/assets" element={<AssetsDashboard />} />
        <Route path="/erp/holidays" element={<HolidaysDashboard />} />
        <Route path="/erp/superadmin" element={<SuperAdminDashboard />} />
        <Route path="/erp/sfa" element={<SFADashboard />} />
        <Route path="/erp/orders" element={<OrderManagement />} />
        <Route path="/erp/settings" element={<SettingsDashboard />} />
        <Route path="/erp/forecast" element={<ForecastDashboard />} />
        <Route path="/erp/cms" element={<CMSDashboard />} />
        <Route path="/erp/branches" element={<BranchDashboard />} />
        <Route path="/erp/product-config" element={<ProductConfigDashboard />} />
        <Route path="/erp/transport" element={<TransportDashboard />} />
        <Route path="/erp/rewards" element={<RewardsDashboard />} />
        <Route path="/erp/gst" element={<GSTDashboard />} />
        <Route path="/erp/job-work" element={<JobWorkDashboard />} />
        <Route path="/erp/vendor-management" element={<VendorManagement />} />
        <Route path="/erp/ledger" element={<LedgerManagement />} />
        <Route path="/erp/customer-master" element={<CustomerManagement />} />

        {/* Customer Portal */}
        <Route path="/portal" element={<CustomerPortalEnhanced />} />
        <Route path="/job-work" element={<JobWork3DConfigurator />} />
        <Route path="/share/:shareId" element={<SharedConfig />} />
      </Routes>
    </AppLayout>
  );
}

function App() {
  return (
    <HelmetProvider>
      <AuthProvider>
        <CartProvider>
          <BrowserRouter>
            <AppRoutes />
            <Toaster position="top-right" />
          </BrowserRouter>
        </CartProvider>
      </AuthProvider>
    </HelmetProvider>
  );
}

export default App;