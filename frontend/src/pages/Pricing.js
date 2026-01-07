import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import { 
  Package, TrendingUp, Users, Truck, Shield, Clock,
  CheckCircle, ChevronRight, Calculator, Phone, Mail
} from 'lucide-react';
import { Button } from '../components/ui/button';
import { toast } from 'sonner';

const Pricing = () => {
  const [calculatorData, setCalculatorData] = useState({
    glassType: 'toughened',
    thickness: 6,
    sqft: 100
  });
  const [showQuoteForm, setShowQuoteForm] = useState(false);
  const [quoteForm, setQuoteForm] = useState({
    name: '', company: '', phone: '', email: '', requirements: ''
  });

  // Base prices per sqft
  const basePrices = {
    toughened: { 5: 85, 6: 95, 8: 120, 10: 150, 12: 180 },
    laminated: { 6: 180, 8: 220, 10: 280, 12: 350 },
    decorative: { 6: 150, 8: 190, 10: 240 },
    mirror: { 4: 65, 5: 80, 6: 95 }
  };

  const tiers = [
    { min: 1, max: 49, discount: 0, label: 'Standard', color: 'bg-slate-100 text-slate-700' },
    { min: 50, max: 99, discount: 10, label: 'Bronze', color: 'bg-amber-100 text-amber-700' },
    { min: 100, max: 499, discount: 15, label: 'Silver', color: 'bg-slate-200 text-slate-800' },
    { min: 500, max: 999, discount: 20, label: 'Gold', color: 'bg-yellow-100 text-yellow-700' },
    { min: 1000, max: Infinity, discount: 25, label: 'Platinum', color: 'bg-purple-100 text-purple-700' }
  ];

  const calculatePrice = () => {
    const basePrice = basePrices[calculatorData.glassType]?.[calculatorData.thickness] || 100;
    const tier = tiers.find(t => calculatorData.sqft >= t.min && calculatorData.sqft <= t.max);
    const discount = tier?.discount || 0;
    const subtotal = basePrice * calculatorData.sqft;
    const discountAmount = subtotal * (discount / 100);
    const total = subtotal - discountAmount;
    return { basePrice, subtotal, discount, discountAmount, total, tier };
  };

  const priceInfo = calculatePrice();

  const handleQuoteSubmit = async (e) => {
    e.preventDefault();
    try {
      const response = await fetch(`${process.env.REACT_APP_BACKEND_URL}/api/inquiry`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ...quoteForm,
          type: 'wholesale_quote',
          calculator_data: calculatorData,
          estimated_total: priceInfo.total
        })
      });
      if (!response.ok) throw new Error('Failed to submit');
      toast.success('Quote request submitted! Our team will contact you soon.');
      setShowQuoteForm(false);
      setQuoteForm({ name: '', company: '', phone: '', email: '', requirements: '' });
    } catch (error) {
      toast.error('Failed to submit. Please call us directly.');
    }
  };

  const benefits = [
    { icon: TrendingUp, title: 'Volume Discounts', desc: 'Up to 25% off on bulk orders' },
    { icon: Users, title: 'Dedicated Support', desc: 'Personal account manager' },
    { icon: Truck, title: 'Free Delivery', desc: 'On orders above ₹50,000' },
    { icon: Shield, title: 'Extended Warranty', desc: '2-year warranty on all products' },
    { icon: Clock, title: 'Priority Production', desc: 'Faster turnaround times' },
    { icon: Package, title: 'Custom Packaging', desc: 'Branded packaging available' }
  ];

  return (
    <div className="min-h-screen bg-white" data-testid="pricing-page">
      {/* Hero Section */}
      <section className="bg-gradient-to-br from-amber-600 via-amber-700 to-orange-700 py-24">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="text-center">
            <span className="inline-block px-4 py-2 bg-white/20 text-amber-100 rounded-full text-sm font-medium mb-6">
              Wholesale & Bulk Pricing
            </span>
            <h1 className="text-4xl md:text-5xl lg:text-6xl font-bold text-white mb-6">
              Save More with <br />
              <span className="text-amber-200">Bulk Orders</span>
            </h1>
            <p className="text-lg md:text-xl text-amber-100 max-w-3xl mx-auto mb-8">
              Get factory-direct pricing with volume discounts up to 25%. 
              Perfect for contractors, architects, dealers, and large projects.
            </p>
            <div className="flex flex-wrap justify-center gap-4">
              <Button 
                onClick={() => setShowQuoteForm(true)}
                className="bg-white text-amber-700 hover:bg-amber-50 px-8 py-6 text-lg rounded-full"
              >
                Get Bulk Quote <ChevronRight className="ml-2" />
              </Button>
              <a href="tel:9284701985">
                <Button variant="outline" className="border-white/30 text-white hover:bg-white/10 px-8 py-6 text-lg rounded-full">
                  Call: 9284701985
                </Button>
              </a>
            </div>
          </div>
        </div>
      </section>

      {/* Pricing Tiers */}
      <section className="py-20">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="text-center mb-16">
            <h2 className="text-3xl md:text-4xl font-bold text-slate-900 mb-4">
              Volume-Based Discounts
            </h2>
            <p className="text-lg text-slate-600 max-w-2xl mx-auto">
              The more you order, the more you save. Our tiered pricing rewards bulk purchases.
            </p>
          </div>

          <div className="grid md:grid-cols-5 gap-4 mb-12">
            {tiers.map((tier, idx) => (
              <div 
                key={idx} 
                className={`rounded-2xl p-6 text-center border-2 transition-transform hover:scale-105 ${
                  tier.label === 'Gold' ? 'border-yellow-400 shadow-lg shadow-yellow-100' : 'border-transparent'
                } ${tier.color}`}
              >
                <div className="text-3xl font-bold mb-2">{tier.discount}%</div>
                <div className="text-lg font-semibold mb-1">{tier.label}</div>
                <div className="text-sm opacity-75">
                  {tier.max === Infinity ? `${tier.min}+ sqft` : `${tier.min}-${tier.max} sqft`}
                </div>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* Price Calculator */}
      <section className="py-20 bg-slate-50">
        <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="text-center mb-12">
            <Calculator className="w-12 h-12 text-teal-600 mx-auto mb-4" />
            <h2 className="text-3xl font-bold text-slate-900 mb-4">
              Instant Price Calculator
            </h2>
            <p className="text-slate-600">
              Get an instant estimate for your bulk order
            </p>
          </div>

          <div className="bg-white rounded-2xl shadow-xl p-8">
            <div className="grid md:grid-cols-3 gap-6 mb-8">
              {/* Glass Type */}
              <div>
                <label className="block text-sm font-medium text-slate-700 mb-2">
                  Glass Type
                </label>
                <select
                  value={calculatorData.glassType}
                  onChange={(e) => setCalculatorData({...calculatorData, glassType: e.target.value})}
                  className="w-full p-3 border border-slate-200 rounded-lg focus:ring-2 focus:ring-teal-500"
                >
                  <option value="toughened">Toughened Glass</option>
                  <option value="laminated">Laminated Glass</option>
                  <option value="decorative">Decorative Glass</option>
                  <option value="mirror">Mirror Glass</option>
                </select>
              </div>

              {/* Thickness */}
              <div>
                <label className="block text-sm font-medium text-slate-700 mb-2">
                  Thickness (mm)
                </label>
                <select
                  value={calculatorData.thickness}
                  onChange={(e) => setCalculatorData({...calculatorData, thickness: parseInt(e.target.value)})}
                  className="w-full p-3 border border-slate-200 rounded-lg focus:ring-2 focus:ring-teal-500"
                >
                  {Object.keys(basePrices[calculatorData.glassType] || {}).map(t => (
                    <option key={t} value={t}>{t}mm</option>
                  ))}
                </select>
              </div>

              {/* Quantity */}
              <div>
                <label className="block text-sm font-medium text-slate-700 mb-2">
                  Total Area (sqft)
                </label>
                <input
                  type="number"
                  value={calculatorData.sqft}
                  onChange={(e) => setCalculatorData({...calculatorData, sqft: parseInt(e.target.value) || 0})}
                  className="w-full p-3 border border-slate-200 rounded-lg focus:ring-2 focus:ring-teal-500"
                  min="1"
                />
              </div>
            </div>

            {/* Price Display */}
            <div className="bg-gradient-to-r from-teal-50 to-emerald-50 rounded-xl p-6 border border-teal-100">
              <div className="flex flex-wrap items-center justify-between gap-4">
                <div>
                  <p className="text-sm text-slate-600">
                    Base Price: ₹{priceInfo.basePrice}/sqft × {calculatorData.sqft} sqft = 
                    <span className="line-through ml-2 text-slate-400">₹{priceInfo.subtotal.toLocaleString()}</span>
                  </p>
                  {priceInfo.discount > 0 && (
                    <p className="text-sm text-emerald-600 font-medium">
                      {priceInfo.tier?.label} Discount ({priceInfo.discount}%): -₹{priceInfo.discountAmount.toLocaleString()}
                    </p>
                  )}
                </div>
                <div className="text-right">
                  <span className={`inline-block px-3 py-1 rounded-full text-sm font-medium ${priceInfo.tier?.color}`}>
                    {priceInfo.tier?.label} Tier
                  </span>
                  <p className="text-3xl font-bold text-teal-700 mt-2">
                    ₹{priceInfo.total.toLocaleString()}
                  </p>
                  <p className="text-sm text-slate-500">Estimated Total</p>
                </div>
              </div>
            </div>

            <div className="mt-6 text-center">
              <Button 
                onClick={() => setShowQuoteForm(true)}
                className="bg-teal-600 hover:bg-teal-700 text-white px-8 py-3 rounded-full"
              >
                Get Exact Quote <ChevronRight className="ml-2 w-4 h-4" />
              </Button>
              <p className="text-sm text-slate-500 mt-2">
                * Prices may vary based on specific requirements, edge processing, and customizations
              </p>
            </div>
          </div>
        </div>
      </section>

      {/* Benefits */}
      <section className="py-20">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="text-center mb-16">
            <h2 className="text-3xl font-bold text-slate-900 mb-4">
              Wholesale Partner Benefits
            </h2>
            <p className="text-lg text-slate-600">
              Join our network of successful dealers and contractors
            </p>
          </div>

          <div className="grid md:grid-cols-3 gap-8">
            {benefits.map((benefit, idx) => {
              const Icon = benefit.icon;
              return (
                <div key={idx} className="bg-white rounded-xl p-6 shadow-sm border border-slate-100 hover:shadow-lg transition-shadow">
                  <div className="w-12 h-12 rounded-xl bg-teal-100 flex items-center justify-center mb-4">
                    <Icon className="w-6 h-6 text-teal-600" />
                  </div>
                  <h3 className="text-lg font-bold text-slate-900 mb-2">{benefit.title}</h3>
                  <p className="text-slate-600">{benefit.desc}</p>
                </div>
              );
            })}
          </div>
        </div>
      </section>

      {/* Contact CTA */}
      <section className="py-16 bg-gradient-to-r from-amber-600 to-orange-600">
        <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex flex-col md:flex-row items-center justify-between gap-8">
            <div className="text-center md:text-left">
              <h2 className="text-2xl md:text-3xl font-bold text-white mb-2">
                Need a Custom Quote?
              </h2>
              <p className="text-amber-100">
                Contact our sales team for project-specific pricing
              </p>
            </div>
            <div className="flex flex-wrap gap-4">
              <a href="mailto:sales@lucumaaglass.in">
                <Button className="bg-white text-amber-700 hover:bg-amber-50 px-6 py-3 rounded-full">
                  <Mail className="w-4 h-4 mr-2" /> sales@lucumaaglass.in
                </Button>
              </a>
              <a href="tel:9284701985">
                <Button variant="outline" className="border-white text-white hover:bg-white/10 px-6 py-3 rounded-full">
                  <Phone className="w-4 h-4 mr-2" /> 9284701985
                </Button>
              </a>
            </div>
          </div>
        </div>
      </section>

      {/* Quote Modal */}
      {showQuoteForm && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-2xl max-w-lg w-full max-h-[90vh] overflow-y-auto p-6">
            <h3 className="text-2xl font-bold text-slate-900 mb-6">Request Bulk Quote</h3>
            <form onSubmit={handleQuoteSubmit} className="space-y-4">
              <input
                type="text"
                placeholder="Your Name *"
                required
                value={quoteForm.name}
                onChange={(e) => setQuoteForm({...quoteForm, name: e.target.value})}
                className="w-full p-3 border border-slate-200 rounded-lg"
              />
              <input
                type="text"
                placeholder="Company Name"
                value={quoteForm.company}
                onChange={(e) => setQuoteForm({...quoteForm, company: e.target.value})}
                className="w-full p-3 border border-slate-200 rounded-lg"
              />
              <input
                type="tel"
                placeholder="Phone Number *"
                required
                value={quoteForm.phone}
                onChange={(e) => setQuoteForm({...quoteForm, phone: e.target.value})}
                className="w-full p-3 border border-slate-200 rounded-lg"
              />
              <input
                type="email"
                placeholder="Email Address *"
                required
                value={quoteForm.email}
                onChange={(e) => setQuoteForm({...quoteForm, email: e.target.value})}
                className="w-full p-3 border border-slate-200 rounded-lg"
              />
              <textarea
                placeholder="Requirements & Specifications"
                rows={4}
                value={quoteForm.requirements}
                onChange={(e) => setQuoteForm({...quoteForm, requirements: e.target.value})}
                className="w-full p-3 border border-slate-200 rounded-lg"
              />
              <div className="flex gap-3">
                <Button type="submit" className="flex-1 bg-teal-600 hover:bg-teal-700 text-white py-3 rounded-lg">
                  Submit Request
                </Button>
                <Button 
                  type="button" 
                  variant="outline" 
                  onClick={() => setShowQuoteForm(false)}
                  className="px-6 py-3 rounded-lg"
                >
                  Cancel
                </Button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
};

export default Pricing;
