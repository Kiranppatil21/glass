import React from 'react';
import { NavLink, useLocation, useNavigate } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import { 
  LayoutDashboard, Users, Factory, UserCog, Box, 
  ShoppingCart, Calculator, ClipboardCheck, ChevronLeft,
  ChevronRight, Home, LogOut, Lock, CreditCard, Gift, Receipt,
  Package, Calendar, Shield, Navigation, Settings, Brain, Globe, Building2, Truck, Award, Hammer, Wallet, BookOpen, UserCircle
} from 'lucide-react';

const COMPANY_LOGO = "https://customer-assets.emergentagent.com/job_0aec802e-e67b-4582-8fac-1517907b7262/artifacts/752tez4i_Logo%20Cucumaa%20Glass.png";

// Role-based access configuration
const rolePermissions = {
  super_admin: ['admin', 'crm', 'production', 'operator', 'inventory', 'purchase', 'hr', 'accounts', 'payouts', 'wallet', 'expenses', 'assets', 'holidays', 'superadmin', 'sfa', 'orders', 'settings', 'forecast', 'cms', 'branches', 'productconfig', 'transport', 'rewards', 'gst', 'jobwork', 'vendors', 'ledger', 'customermaster'],
  admin: ['admin', 'crm', 'production', 'operator', 'inventory', 'purchase', 'hr', 'accounts', 'payouts', 'wallet', 'expenses', 'assets', 'holidays', 'sfa', 'orders', 'settings', 'forecast', 'cms', 'branches', 'productconfig', 'transport', 'rewards', 'gst', 'jobwork', 'vendors', 'ledger', 'customermaster'],
  owner: ['admin', 'crm', 'production', 'operator', 'inventory', 'purchase', 'hr', 'accounts', 'payouts', 'wallet', 'expenses', 'assets', 'holidays', 'sfa', 'orders', 'settings', 'forecast', 'cms', 'branches', 'productconfig', 'transport', 'rewards', 'gst', 'jobwork', 'vendors', 'ledger', 'customermaster'],
  manager: ['admin', 'crm', 'production', 'inventory', 'purchase', 'hr', 'expenses', 'assets', 'holidays', 'sfa', 'orders', 'forecast', 'transport', 'rewards', 'jobwork', 'customermaster'],
  sales_manager: ['crm', 'sfa', 'rewards', 'ledger', 'customermaster'],
  sales: ['crm', 'sfa', 'customermaster'],
  sales_executive: ['crm', 'sfa', 'customermaster'],
  production_manager: ['admin', 'production', 'operator', 'inventory', 'transport', 'jobwork'],
  operator: ['operator', 'orders'],
  hr: ['hr', 'accounts', 'payouts', 'holidays', 'orders'],
  accountant: ['accounts', 'hr', 'payouts', 'expenses', 'assets', 'orders', 'ledger', 'customermaster'],
  finance: ['accounts', 'payouts', 'expenses', 'orders', 'forecast', 'vendors', 'ledger', 'customermaster'],
  store: ['inventory', 'purchase', 'expenses', 'assets'],
  supervisor: ['production', 'operator', 'orders', 'transport'],
  // CA/Auditor - Read-only access to all ledgers
  ca: ['ledger'],
  auditor: ['ledger'],
  // Default for customers/dealers - no ERP access
  customer: [],
  dealer: ['crm'],
};

const ERPLayout = ({ children }) => {
  const location = useLocation();
  const navigate = useNavigate();
  const { user, logout } = useAuth();
  const [collapsed, setCollapsed] = React.useState(false);

  // Get allowed modules for current user
  const userRole = user?.role || 'customer';
  const allowedModules = rolePermissions[userRole] || [];

  const menuItems = [
    { path: '/erp/superadmin', key: 'superadmin', icon: Shield, label: 'Super Admin', color: 'text-red-400' },
    { path: '/erp/admin', key: 'admin', icon: LayoutDashboard, label: 'Dashboard', color: 'text-blue-400' },
    { path: '/erp/orders', key: 'orders', icon: Package, label: 'Order Management', color: 'text-emerald-400' },
    { path: '/erp/sfa', key: 'sfa', icon: Navigation, label: 'Field Sales (SFA)', color: 'text-lime-400' },
    { path: '/erp/crm', key: 'crm', icon: Users, label: 'CRM & Sales', color: 'text-emerald-400' },
    { path: '/erp/production', key: 'production', icon: Factory, label: 'Production', color: 'text-orange-400' },
    { path: '/erp/operator', key: 'operator', icon: ClipboardCheck, label: 'Operator', color: 'text-violet-400' },
    { path: '/erp/inventory', key: 'inventory', icon: Box, label: 'Inventory', color: 'text-cyan-400' },
    { path: '/erp/purchase', key: 'purchase', icon: ShoppingCart, label: 'Purchase', color: 'text-pink-400' },
    { path: '/erp/hr', key: 'hr', icon: UserCog, label: 'HR & Payroll', color: 'text-indigo-400' },
    { path: '/erp/accounts', key: 'accounts', icon: Calculator, label: 'Accounts', color: 'text-amber-400' },
    { path: '/erp/ledger', key: 'ledger', icon: BookOpen, label: 'Ledger & GL', color: 'text-emerald-400' },
    { path: '/erp/payouts', key: 'payouts', icon: CreditCard, label: 'Payouts', color: 'text-green-400' },
    { path: '/erp/expenses', key: 'expenses', icon: Receipt, label: 'Expenses', color: 'text-red-400' },
    { path: '/erp/assets', key: 'assets', icon: Package, label: 'Assets', color: 'text-teal-400' },
    { path: '/erp/holidays', key: 'holidays', icon: Calendar, label: 'Holidays', color: 'text-yellow-400' },
    { path: '/erp/wallet', key: 'wallet', icon: Gift, label: 'Refer & Earn', color: 'text-purple-400' },
    { path: '/erp/forecast', key: 'forecast', icon: Brain, label: 'AI Forecast', color: 'text-cyan-400' },
    { path: '/erp/cms', key: 'cms', icon: Globe, label: 'CMS', color: 'text-indigo-400' },
    { path: '/erp/branches', key: 'branches', icon: Building2, label: 'Branches', color: 'text-rose-400' },
    { path: '/erp/product-config', key: 'productconfig', icon: Settings, label: 'Product Config', color: 'text-amber-400' },
    { path: '/erp/transport', key: 'transport', icon: Truck, label: 'Transport', color: 'text-orange-400' },
    { path: '/erp/rewards', key: 'rewards', icon: Award, label: 'Rewards & Referrals', color: 'text-purple-400' },
    { path: '/erp/gst', key: 'gst', icon: Receipt, label: 'GST Management', color: 'text-blue-400' },
    { path: '/erp/job-work', key: 'jobwork', icon: Hammer, label: 'Job Work', color: 'text-orange-400' },
    { path: '/erp/vendor-management', key: 'vendors', icon: Wallet, label: 'Vendor & PO', color: 'text-violet-400' },
    { path: '/erp/customer-master', key: 'customermaster', icon: UserCircle, label: 'Customer Master', color: 'text-blue-400' },
    { path: '/erp/settings', key: 'settings', icon: Settings, label: 'Payment Settings', color: 'text-gray-400' },
  ];

  // Filter menu items based on role permissions
  const visibleMenuItems = menuItems.filter(item => allowedModules.includes(item.key));

  const handleLogout = () => {
    logout();
    navigate('/login');
  };

  // If no user or no ERP access, show access denied
  if (!user) {
    return (
      <div className="min-h-screen bg-slate-900 flex items-center justify-center">
        <div className="text-center p-8">
          <Lock className="w-16 h-16 text-slate-500 mx-auto mb-4" />
          <h2 className="text-2xl font-bold text-white mb-2">Login Required</h2>
          <p className="text-slate-400 mb-6">Please login to access the ERP system</p>
          <button
            onClick={() => navigate('/login')}
            className="px-6 py-3 bg-teal-600 text-white rounded-lg hover:bg-teal-500 transition-colors"
          >
            Go to Login
          </button>
        </div>
      </div>
    );
  }

  if (allowedModules.length === 0) {
    return (
      <div className="min-h-screen bg-slate-900 flex items-center justify-center">
        <div className="text-center p-8">
          <Lock className="w-16 h-16 text-red-500 mx-auto mb-4" />
          <h2 className="text-2xl font-bold text-white mb-2">Access Denied</h2>
          <p className="text-slate-400 mb-6">Your role ({userRole}) doesn't have access to the ERP system</p>
          <button
            onClick={() => navigate('/')}
            className="px-6 py-3 bg-slate-700 text-white rounded-lg hover:bg-slate-600 transition-colors"
          >
            Back to Website
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="flex min-h-screen bg-slate-100">
      {/* Sidebar */}
      <aside 
        className={`fixed left-0 top-0 h-full bg-gradient-to-b from-slate-900 to-slate-800 text-white transition-all duration-300 z-40 shadow-xl ${
          collapsed ? 'w-20' : 'w-64'
        }`}
      >
        {/* Logo */}
        <div className="h-16 flex items-center justify-between px-4 border-b border-slate-700/50">
          {!collapsed && (
            <div className="flex items-center gap-3">
              <img 
                src={COMPANY_LOGO} 
                alt="Lucumaa Glass" 
                className="w-10 h-10 object-contain bg-white rounded-lg p-1"
              />
              <div>
                <span className="font-bold text-lg">Lucumaa</span>
                <p className="text-[10px] text-slate-400 -mt-1">ERP System</p>
              </div>
            </div>
          )}
          {collapsed && (
            <img 
              src={COMPANY_LOGO} 
              alt="LG" 
              className="w-10 h-10 object-contain bg-white rounded-lg p-1 mx-auto"
            />
          )}
        </div>

        {/* Toggle Button */}
        <button
          onClick={() => setCollapsed(!collapsed)}
          className="absolute -right-3 top-20 w-6 h-6 bg-slate-700 rounded-full flex items-center justify-center hover:bg-slate-600 transition-colors shadow-lg border border-slate-600"
        >
          {collapsed ? <ChevronRight className="w-4 h-4" /> : <ChevronLeft className="w-4 h-4" />}
        </button>

        {/* User Info */}
        {!collapsed && (
          <div className="px-4 py-3 border-b border-slate-700/50">
            <p className="text-sm font-medium text-white truncate">{user?.name || user?.email}</p>
            <p className="text-xs text-teal-400 capitalize">{userRole.replace('_', ' ')}</p>
          </div>
        )}

        {/* Navigation */}
        <nav className="p-3 mt-2 space-y-1 overflow-y-auto max-h-[calc(100vh-220px)]">
          {visibleMenuItems.map((item) => {
            const Icon = item.icon;
            const isActive = location.pathname === item.path;
            
            return (
              <NavLink
                key={item.path}
                to={item.path}
                className={`flex items-center gap-3 px-3 py-3 rounded-xl transition-all group ${
                  isActive 
                    ? 'bg-gradient-to-r from-teal-600 to-teal-500 text-white shadow-lg shadow-teal-500/20' 
                    : 'text-slate-400 hover:bg-slate-700/50 hover:text-white'
                }`}
                title={collapsed ? item.label : ''}
              >
                <div className={`flex-shrink-0 ${isActive ? 'text-white' : item.color} transition-colors`}>
                  <Icon className="w-5 h-5" />
                </div>
                {!collapsed && (
                  <span className="font-medium text-sm">{item.label}</span>
                )}
                {!collapsed && isActive && (
                  <div className="ml-auto w-2 h-2 bg-white rounded-full" />
                )}
              </NavLink>
            );
          })}
        </nav>

        {/* Bottom Section */}
        <div className="absolute bottom-0 left-0 right-0 p-3 border-t border-slate-700/50 space-y-1">
          <NavLink
            to="/"
            className="flex items-center gap-3 px-3 py-3 rounded-xl text-slate-400 hover:bg-slate-700/50 hover:text-white transition-all"
            title={collapsed ? 'Back to Website' : ''}
          >
            <Home className="w-5 h-5 flex-shrink-0" />
            {!collapsed && <span className="font-medium text-sm">Back to Website</span>}
          </NavLink>
          <button
            onClick={handleLogout}
            className="w-full flex items-center gap-3 px-3 py-3 rounded-xl text-slate-400 hover:bg-red-500/20 hover:text-red-400 transition-all"
            title={collapsed ? 'Logout' : ''}
          >
            <LogOut className="w-5 h-5 flex-shrink-0" />
            {!collapsed && <span className="font-medium text-sm">Logout</span>}
          </button>
        </div>
      </aside>

      {/* Main Content */}
      <main className={`flex-1 transition-all duration-300 ${collapsed ? 'ml-20' : 'ml-64'}`}>
        <div className="min-h-screen">
          {children}
        </div>
      </main>
    </div>
  );
};

// Export role permissions for use in other components
export { rolePermissions };
export default ERPLayout;
