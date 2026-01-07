import React from 'react';
import { Link } from 'react-router-dom';
import { 
  Search, Ruler, CreditCard, Factory, ClipboardCheck, 
  Truck, CheckCircle, ChevronRight, Phone, Clock, Shield, Award
} from 'lucide-react';
import { Button } from '../components/ui/button';

const HowItWorks = () => {
  const steps = [
    {
      number: '01',
      title: 'Browse & Select',
      description: 'Explore our wide range of glass products. Choose from toughened glass, laminated glass, decorative options, mirrors, and more.',
      icon: Search,
      color: 'bg-blue-500',
      details: ['View product specifications', 'Compare different glass types', 'Check thickness options']
    },
    {
      number: '02',
      title: 'Customize & Measure',
      description: 'Use our online customization tool to specify exact dimensions, thickness, edge finishing, and special requirements.',
      icon: Ruler,
      color: 'bg-purple-500',
      details: ['Enter custom dimensions', 'Select edge type', 'Add holes or cutouts']
    },
    {
      number: '03',
      title: 'Get Instant Quote',
      description: 'Receive real-time pricing based on your specifications. No hidden charges - transparent pricing always.',
      icon: CreditCard,
      color: 'bg-emerald-500',
      details: ['Instant price calculation', 'Wholesale discounts applied', 'Multiple payment options']
    },
    {
      number: '04',
      title: 'Pay Advance',
      description: 'Secure your order with an advance payment. Choose from 25%, 50%, 75%, or 100% advance options based on order value.',
      icon: CreditCard,
      color: 'bg-amber-500',
      details: ['Flexible payment options', 'Secure Razorpay gateway', 'Credit available for bulk']
    },
    {
      number: '05',
      title: 'Production',
      description: 'Your glass is manufactured in our state-of-the-art facility with precision cutting, tempering, and finishing.',
      icon: Factory,
      color: 'bg-teal-500',
      details: ['3-7 days production', 'Precision CNC cutting', 'Quality tempering process']
    },
    {
      number: '06',
      title: 'Quality Check',
      description: 'Every piece undergoes rigorous quality inspection to ensure it meets our high standards and your specifications.',
      icon: ClipboardCheck,
      color: 'bg-rose-500',
      details: ['Dimension verification', 'Strength testing', 'Visual inspection']
    },
    {
      number: '07',
      title: 'Dispatch & Delivery',
      description: 'Secure packaging and safe delivery to your location. Track your order in real-time using our tracking system.',
      icon: Truck,
      color: 'bg-indigo-500',
      details: ['Safe packaging', 'Insured transport', 'Real-time tracking']
    },
    {
      number: '08',
      title: 'Installation Support',
      description: 'Get installation guidance or opt for our professional installation services for a perfect finish.',
      icon: CheckCircle,
      color: 'bg-green-500',
      details: ['Installation guides', 'Expert support', 'Professional installation']
    }
  ];

  const benefits = [
    { icon: Clock, title: 'Quick Turnaround', desc: '3-7 days production time' },
    { icon: Shield, title: 'Quality Assured', desc: 'IS 2553 certified products' },
    { icon: Award, title: 'Best Prices', desc: 'Factory direct pricing' },
    { icon: Phone, title: '24/7 Support', desc: 'Always here to help' }
  ];

  return (
    <div className="min-h-screen bg-white" data-testid="howitworks-page">
      {/* Hero Section */}
      <section className="bg-gradient-to-br from-slate-900 via-slate-800 to-purple-900 py-24">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="text-center">
            <span className="inline-block px-4 py-2 bg-purple-500/20 text-purple-300 rounded-full text-sm font-medium mb-6">
              Simple & Transparent Process
            </span>
            <h1 className="text-4xl md:text-5xl lg:text-6xl font-bold text-white mb-6">
              How It Works
            </h1>
            <p className="text-lg md:text-xl text-slate-300 max-w-3xl mx-auto">
              From selection to delivery - our streamlined process ensures you get the perfect glass products 
              with minimal hassle and maximum transparency.
            </p>
          </div>
        </div>
      </section>

      {/* Benefits Bar */}
      <section className="py-8 bg-slate-50 border-b">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-6">
            {benefits.map((benefit, idx) => {
              const Icon = benefit.icon;
              return (
                <div key={idx} className="flex items-center gap-3">
                  <div className="w-10 h-10 rounded-lg bg-teal-100 flex items-center justify-center">
                    <Icon className="w-5 h-5 text-teal-600" />
                  </div>
                  <div>
                    <p className="font-semibold text-slate-900">{benefit.title}</p>
                    <p className="text-sm text-slate-500">{benefit.desc}</p>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </section>

      {/* Steps Section */}
      <section className="py-20">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="relative">
            {/* Vertical Line */}
            <div className="absolute left-8 md:left-1/2 top-0 bottom-0 w-0.5 bg-gradient-to-b from-blue-500 via-purple-500 to-green-500 hidden md:block" />
            
            <div className="space-y-12 md:space-y-0">
              {steps.map((step, idx) => {
                const Icon = step.icon;
                const isEven = idx % 2 === 0;
                
                return (
                  <div key={step.number} className="relative md:flex md:items-center md:justify-between">
                    {/* Timeline Dot */}
                    <div className="absolute left-8 md:left-1/2 w-4 h-4 rounded-full bg-white border-4 border-slate-300 transform -translate-x-1/2 hidden md:block" />
                    
                    {/* Content */}
                    <div className={`md:w-5/12 ${isEven ? 'md:pr-16' : 'md:pl-16 md:ml-auto'}`}>
                      <div className={`bg-white rounded-2xl p-6 shadow-lg border border-slate-100 hover:shadow-xl transition-shadow ${isEven ? 'md:text-right' : ''}`}>
                        <div className={`flex items-center gap-4 mb-4 ${isEven ? 'md:flex-row-reverse' : ''}`}>
                          <div className={`w-14 h-14 rounded-xl ${step.color} flex items-center justify-center shadow-lg`}>
                            <Icon className="w-7 h-7 text-white" />
                          </div>
                          <div>
                            <span className="text-sm font-bold text-slate-400">STEP {step.number}</span>
                            <h3 className="text-xl font-bold text-slate-900">{step.title}</h3>
                          </div>
                        </div>
                        <p className="text-slate-600 mb-4">{step.description}</p>
                        <ul className={`space-y-2 ${isEven ? 'md:text-right' : ''}`}>
                          {step.details.map((detail, i) => (
                            <li key={i} className={`flex items-center gap-2 text-sm text-slate-500 ${isEven ? 'md:flex-row-reverse' : ''}`}>
                              <CheckCircle className="w-4 h-4 text-teal-500 flex-shrink-0" />
                              {detail}
                            </li>
                          ))}
                        </ul>
                      </div>
                    </div>
                    
                    {/* Spacer for timeline */}
                    <div className="hidden md:block md:w-5/12" />
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      </section>

      {/* Video/Demo Section */}
      <section className="py-20 bg-slate-50">
        <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 text-center">
          <h2 className="text-3xl md:text-4xl font-bold text-slate-900 mb-6">
            Ready to Get Started?
          </h2>
          <p className="text-lg text-slate-600 mb-8">
            Experience our easy customization tool and get your glass order in just a few clicks.
          </p>
          <div className="flex flex-wrap justify-center gap-4">
            <Link to="/customize">
              <Button className="bg-teal-600 hover:bg-teal-700 text-white px-8 py-6 text-lg rounded-full">
                Start Customizing <ChevronRight className="ml-2" />
              </Button>
            </Link>
            <Link to="/products">
              <Button variant="outline" className="border-slate-300 px-8 py-6 text-lg rounded-full">
                View Products
              </Button>
            </Link>
          </div>
        </div>
      </section>

      {/* FAQ Section */}
      <section className="py-20">
        <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8">
          <h2 className="text-3xl font-bold text-slate-900 mb-12 text-center">
            Frequently Asked Questions
          </h2>
          
          <div className="space-y-6">
            {[
              { q: 'How long does production take?', a: 'Standard orders are completed in 3-7 working days. Rush orders may be available for urgent requirements.' },
              { q: 'What payment methods do you accept?', a: 'We accept UPI, credit/debit cards, net banking, and bank transfers through our secure Razorpay gateway.' },
              { q: 'Do you provide installation services?', a: 'Yes, we offer professional installation services in select cities. Contact us for availability in your area.' },
              { q: 'What is your return policy?', a: 'Custom glass products are made to order and cannot be returned. However, we replace any products with manufacturing defects.' },
              { q: 'Do you deliver across India?', a: 'Yes, we deliver to all major cities across India. Delivery charges vary based on location and order size.' }
            ].map((faq, idx) => (
              <div key={idx} className="bg-white rounded-xl p-6 shadow-sm border border-slate-100">
                <h3 className="font-bold text-slate-900 mb-2">{faq.q}</h3>
                <p className="text-slate-600">{faq.a}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* Contact CTA */}
      <section className="py-16 bg-gradient-to-r from-teal-600 to-emerald-600">
        <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 text-center">
          <h2 className="text-2xl md:text-3xl font-bold text-white mb-4">
            Have Questions? We're Here to Help!
          </h2>
          <p className="text-teal-100 mb-6">
            Call us at <a href="tel:9284701985" className="font-bold underline">9284701985</a> or email <a href="mailto:info@lucumaaglass.in" className="font-bold underline">info@lucumaaglass.in</a>
          </p>
          <Link to="/contact">
            <Button className="bg-white text-teal-700 hover:bg-teal-50 px-8 py-3 rounded-full">
              Contact Us
            </Button>
          </Link>
        </div>
      </section>
    </div>
  );
};

export default HowItWorks;
