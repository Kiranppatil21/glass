import React, { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import { useCart } from '../contexts/CartContext';
import { Menu, X, Phone, MessageCircle, MapPin, LogIn, Package, ChevronDown, ShoppingCart } from 'lucide-react';
import { Button } from './ui/button';

const COMPANY_LOGO = "https://customer-assets.emergentagent.com/job_0aec802e-e67b-4582-8fac-1517907b7262/artifacts/752tez4i_Logo%20Cucumaa%20Glass.png";

const Header = () => {
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const [productsOpen, setProductsOpen] = useState(false);
  const { user, logout } = useAuth();
  const { getCartCount, setIsCartOpen } = useCart();
  const navigate = useNavigate();
  const cartCount = getCartCount();

  const handleLogout = () => {
    logout();
    navigate('/');
  };

  return (
    <header className="sticky top-0 z-50 bg-white/90 backdrop-blur-xl border-b border-slate-200">
      <div className="bg-primary-700 text-white py-2">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 flex justify-between items-center text-sm">
          <div className="flex items-center gap-6">
            <a href="tel:+919284701985" className="flex items-center gap-2 hover:text-primary-200 transition-colors" data-testid="header-call-link">
              <Phone className="w-4 h-4" />
              <span className="hidden sm:inline">+91 92847 01985</span>
            </a>
            <a href="https://wa.me/919284701985" target="_blank" rel="noopener noreferrer" className="flex items-center gap-2 hover:text-primary-200 transition-colors" data-testid="header-whatsapp-link">
              <MessageCircle className="w-4 h-4" />
              <span className="hidden sm:inline">WhatsApp</span>
            </a>
            <div className="flex items-center gap-2" data-testid="header-location">
              <MapPin className="w-4 h-4" />
              <span className="hidden md:inline">Pune, Maharashtra</span>
            </div>
          </div>
          <div className="flex items-center gap-4">
            {user ? (
              <>
                <Link to="/portal" className="hover:text-primary-200" data-testid="header-dashboard-link">My Portal</Link>
                <button onClick={handleLogout} className="hover:text-primary-200" data-testid="header-logout-btn">Logout</button>
              </>
            ) : (
              <Link to="/login" className="flex items-center gap-2 hover:text-primary-200" data-testid="header-login-link">
                <LogIn className="w-4 h-4" />
                <span>Login / Register</span>
              </Link>
            )}
            <Link to="/track" className="flex items-center gap-2 hover:text-primary-200" data-testid="header-track-order-link">
              <Package className="w-4 h-4" />
              <span className="hidden sm:inline">Track Order</span>
            </Link>
          </div>
        </div>
      </div>

      <nav className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
        <div className="flex justify-between items-center">
          <Link to="/" className="flex items-center gap-3" data-testid="header-logo">
            <img 
              src={COMPANY_LOGO} 
              alt="Lucumaa Glass" 
              className="h-12 w-auto object-contain"
            />
            <div>
              <div className="text-xl font-bold text-slate-900 tracking-tight">Lucumaa Glass</div>
              <div className="text-xs text-slate-600">A Unit of Lucumaa Corporation</div>
            </div>
          </Link>

          <div className="hidden lg:flex items-center gap-8">
            <Link to="/" className="text-slate-700 hover:text-primary-700 font-medium transition-colors" data-testid="nav-home">Home</Link>
            
            <div className="relative" onMouseEnter={() => setProductsOpen(true)} onMouseLeave={() => setProductsOpen(false)}>
              <button className="flex items-center gap-1 text-slate-700 hover:text-primary-700 font-medium transition-colors" data-testid="nav-products">
                Products <ChevronDown className="w-4 h-4" />
              </button>
              {productsOpen && (
                <div className="absolute top-full left-0 mt-2 w-72 bg-white rounded-xl shadow-2xl border border-slate-200 py-2" data-testid="products-dropdown">
                  <Link to="/products" className="block px-4 py-2 hover:bg-primary-50 text-slate-700 hover:text-primary-700 font-medium border-b border-slate-100">All Products</Link>
                  <Link to="/products" className="block px-4 py-2 hover:bg-primary-50 text-slate-700 hover:text-primary-700">Toughened Glass</Link>
                  <Link to="/products" className="block px-4 py-2 hover:bg-primary-50 text-slate-700 hover:text-primary-700">Laminated Safety Glass</Link>
                  <Link to="/products" className="block px-4 py-2 hover:bg-primary-50 text-slate-700 hover:text-primary-700">Insulated Glass (DGU)</Link>
                  <Link to="/products" className="block px-4 py-2 hover:bg-primary-50 text-slate-700 hover:text-primary-700">Frosted / Acid Etched Glass</Link>
                  <Link to="/products" className="block px-4 py-2 hover:bg-primary-50 text-slate-700 hover:text-primary-700">Printed / Designer Glass</Link>
                  <Link to="/products" className="block px-4 py-2 hover:bg-primary-50 text-slate-700 hover:text-primary-700">Fire Rated Glass</Link>
                  <Link to="/products" className="block px-4 py-2 hover:bg-primary-50 text-slate-700 hover:text-primary-700 text-sm">
                    <span className="flex items-center justify-between">
                      Smart Glass (PDLC)
                      <span className="text-xs bg-primary-100 text-primary-700 px-2 py-1 rounded">Coming Soon</span>
                    </span>
                  </Link>
                </div>
              )}
            </div>

            <Link to="/customize" className="text-slate-700 hover:text-primary-700 font-medium transition-colors" data-testid="nav-customize">Customize & Book</Link>
            <Link to="/industries" className="text-slate-700 hover:text-primary-700 font-medium transition-colors" data-testid="nav-industries">Industries</Link>
            <Link to="/how-it-works" className="text-slate-700 hover:text-primary-700 font-medium transition-colors" data-testid="nav-how-it-works">How It Works</Link>
            <Link to="/pricing" className="text-slate-700 hover:text-primary-700 font-medium transition-colors" data-testid="nav-pricing">Wholesale</Link>
            <Link to="/blog" className="text-slate-700 hover:text-primary-700 font-medium transition-colors" data-testid="nav-blog">Blog</Link>
            <Link to="/contact" className="text-slate-700 hover:text-primary-700 font-medium transition-colors" data-testid="nav-contact">Contact</Link>
          </div>

          {/* Cart & CTA */}
          <div className="hidden lg:flex items-center gap-4">
            <button
              onClick={() => setIsCartOpen(true)}
              className="relative p-2 text-slate-700 hover:text-primary-700 transition-colors"
              data-testid="cart-button"
            >
              <ShoppingCart className="w-6 h-6" />
              {cartCount > 0 && (
                <span className="absolute -top-1 -right-1 w-5 h-5 bg-red-500 text-white text-xs rounded-full flex items-center justify-center font-bold">
                  {cartCount > 9 ? '9+' : cartCount}
                </span>
              )}
            </button>
            <Button asChild className="bg-primary-700 hover:bg-primary-800" data-testid="nav-get-quote-btn">
              <Link to="/customize">Get Quote</Link>
            </Button>
          </div>

          <button
            onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
            className="lg:hidden p-2"
            data-testid="mobile-menu-toggle"
          >
            {mobileMenuOpen ? <X /> : <Menu />}
          </button>
        </div>

        {mobileMenuOpen && (
          <div className="lg:hidden mt-4 pb-4 border-t border-slate-200 pt-4" data-testid="mobile-menu">
            <div className="flex flex-col gap-3">
              <Link to="/" className="text-slate-700 hover:text-primary-700 font-medium">Home</Link>
              <Link to="/products" className="text-slate-700 hover:text-primary-700 font-medium">Products</Link>
              <Link to="/customize" className="text-slate-700 hover:text-primary-700 font-medium">Customize & Book</Link>
              <Link to="/industries" className="text-slate-700 hover:text-primary-700 font-medium">Industries</Link>
              <Link to="/how-it-works" className="text-slate-700 hover:text-primary-700 font-medium">How It Works</Link>
              <Link to="/pricing" className="text-slate-700 hover:text-primary-700 font-medium">Wholesale</Link>
              <Link to="/blog" className="text-slate-700 hover:text-primary-700 font-medium">Blog</Link>
              <Link to="/contact" className="text-slate-700 hover:text-primary-700 font-medium">Contact</Link>
            </div>
          </div>
        )}
      </nav>
    </header>
  );
};

export default Header;