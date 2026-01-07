import React from 'react';
import { Link } from 'react-router-dom';
import { Phone, Mail, MapPin, Facebook, Linkedin, Instagram } from 'lucide-react';

const COMPANY_LOGO = "https://customer-assets.emergentagent.com/job_0aec802e-e67b-4582-8fac-1517907b7262/artifacts/752tez4i_Logo%20Cucumaa%20Glass.png";

const Footer = () => {
  return (
    <footer className="bg-slate-900 text-slate-300" data-testid="footer">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-16">
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-12">
          <div>
            <div className="flex items-center gap-3 mb-6">
              <img 
                src={COMPANY_LOGO} 
                alt="Lucumaa Glass" 
                className="h-12 w-auto object-contain bg-white rounded-lg p-1"
              />
              <div>
                <div className="text-lg font-bold text-white">Lucumaa Glass</div>
                <div className="text-xs text-slate-400">Premium Glass Manufacturing</div>
              </div>
            </div>
            <p className="text-sm mb-6 leading-relaxed">
              Factory-direct toughened & customized glass solutions. Trusted by architects, builders, and homeowners across India.
            </p>
            <div className="flex gap-4">
              <a href="#" className="w-10 h-10 bg-slate-800 hover:bg-primary-600 rounded-lg flex items-center justify-center transition-colors" data-testid="footer-facebook">
                <Facebook className="w-5 h-5" />
              </a>
              <a href="#" className="w-10 h-10 bg-slate-800 hover:bg-primary-600 rounded-lg flex items-center justify-center transition-colors" data-testid="footer-linkedin">
                <Linkedin className="w-5 h-5" />
              </a>
              <a href="#" className="w-10 h-10 bg-slate-800 hover:bg-primary-600 rounded-lg flex items-center justify-center transition-colors" data-testid="footer-instagram">
                <Instagram className="w-5 h-5" />
              </a>
            </div>
          </div>

          <div>
            <h3 className="text-white font-semibold mb-4">Quick Links</h3>
            <ul className="space-y-3 text-sm">
              <li><Link to="/products" className="hover:text-primary-400 transition-colors">Products</Link></li>
              <li><Link to="/customize" className="hover:text-primary-400 transition-colors">Customize & Book</Link></li>
              <li><Link to="/industries" className="hover:text-primary-400 transition-colors">Industries</Link></li>
              <li><Link to="/how-it-works" className="hover:text-primary-400 transition-colors">How It Works</Link></li>
              <li><Link to="/pricing" className="hover:text-primary-400 transition-colors">Wholesale Pricing</Link></li>
              <li><Link to="/track" className="hover:text-primary-400 transition-colors">Track Order</Link></li>
              <li><Link to="/contact" className="hover:text-primary-400 transition-colors">Contact</Link></li>
            </ul>
          </div>

          <div>
            <h3 className="text-white font-semibold mb-4">Manufacturing Unit</h3>
            <div className="space-y-3 text-sm">
              <div className="flex gap-3">
                <MapPin className="w-5 h-5 text-primary-500 flex-shrink-0" />
                <p className="leading-relaxed">
                  Ground Floor, Survey No-104/2A/1,<br />
                  Sant Nagar, Wagholi–Lohegaon Road,<br />
                  Lohegaon, Pune – 411047
                </p>
              </div>
            </div>
          </div>

          <div>
            <h3 className="text-white font-semibold mb-4">Corporate Office</h3>
            <div className="space-y-3 text-sm">
              <div className="flex gap-3">
                <MapPin className="w-5 h-5 text-primary-500 flex-shrink-0" />
                <p className="leading-relaxed">
                  Shop No. 7 & 8, D Wing,<br />
                  Dynamic Grandeura,<br />
                  Undri, Pune – 411060
                </p>
              </div>
              <div className="flex gap-3">
                <Phone className="w-5 h-5 text-primary-500 flex-shrink-0" />
                <a href="tel:+919284701985" className="hover:text-primary-400 transition-colors">+91 92847 01985</a>
              </div>
              <div className="flex gap-3">
                <Mail className="w-5 h-5 text-primary-500 flex-shrink-0" />
                <a href="mailto:info@lucumaaglass.in" className="hover:text-primary-400 transition-colors">info@lucumaaglass.in</a>
              </div>
            </div>
          </div>
        </div>

        <div className="border-t border-slate-800 mt-12 pt-8 flex flex-col md:flex-row justify-between items-center gap-4 text-sm">
          <p>© 2025 Lucumaa Glass. A Unit of Lucumaa Corporation Pvt. Ltd. All rights reserved.</p>
          <div className="flex gap-6">
            <Link to="#" className="hover:text-primary-400 transition-colors">Privacy Policy</Link>
            <Link to="#" className="hover:text-primary-400 transition-colors">Terms of Service</Link>
          </div>
        </div>
      </div>
    </footer>
  );
};

export default Footer;