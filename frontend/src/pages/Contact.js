import React, { useState } from 'react';
import { 
  Phone, Mail, MapPin, Clock, Send, MessageSquare,
  Building2, Truck, CreditCard, CheckCircle
} from 'lucide-react';
import { Button } from '../components/ui/button';
import { toast } from 'sonner';

const Contact = () => {
  const [formData, setFormData] = useState({
    name: '',
    email: '',
    phone: '',
    subject: 'general',
    message: ''
  });
  const [submitting, setSubmitting] = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setSubmitting(true);
    try {
      const response = await fetch(`${process.env.REACT_APP_BACKEND_URL}/api/inquiry`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ...formData,
          type: 'contact_form'
        })
      });
      if (!response.ok) throw new Error('Failed to submit');
      toast.success('Message sent successfully! We will get back to you soon.');
      setFormData({ name: '', email: '', phone: '', subject: 'general', message: '' });
    } catch (error) {
      toast.error('Failed to send message. Please try calling us directly.');
    } finally {
      setSubmitting(false);
    }
  };

  const contactInfo = [
    {
      icon: Phone,
      title: 'Phone',
      details: ['+91 9284701985'],
      action: 'tel:+919284701985',
      actionText: 'Call Now',
      color: 'bg-emerald-500'
    },
    {
      icon: Mail,
      title: 'Email',
      details: [
        'book@lucumaaglass.in (Bookings)',
        'info@lucumaaglass.in (Information)',
        'sales@lucumaaglass.in (Sales)'
      ],
      action: 'mailto:info@lucumaaglass.in',
      actionText: 'Send Email',
      color: 'bg-blue-500'
    },
    {
      icon: MapPin,
      title: 'Factory & Corporate Office',
      details: [
        'Lucumaa Glass Manufacturing',
        'Industrial Area, Pune',
        'Maharashtra, India'
      ],
      action: 'https://maps.google.com',
      actionText: 'Get Directions',
      color: 'bg-rose-500'
    },
    {
      icon: Clock,
      title: 'Business Hours',
      details: [
        'Monday - Saturday',
        '9:00 AM - 7:00 PM',
        'Sunday: Closed'
      ],
      color: 'bg-amber-500'
    }
  ];

  const departments = [
    { value: 'general', label: 'General Inquiry' },
    { value: 'sales', label: 'Sales & Quotations' },
    { value: 'support', label: 'Customer Support' },
    { value: 'booking', label: 'Order Booking' },
    { value: 'bulk', label: 'Bulk/Wholesale Orders' },
    { value: 'complaint', label: 'Complaints & Feedback' }
  ];

  return (
    <div className="min-h-screen bg-white" data-testid="contact-page">
      {/* Hero Section */}
      <section className="bg-gradient-to-br from-teal-600 via-teal-700 to-emerald-700 py-20">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="text-center">
            <h1 className="text-4xl md:text-5xl font-bold text-white mb-4">
              Get in Touch
            </h1>
            <p className="text-lg md:text-xl text-teal-100 max-w-2xl mx-auto">
              Have questions? We'd love to hear from you. Send us a message 
              and we'll respond as soon as possible.
            </p>
          </div>
        </div>
      </section>

      {/* Contact Cards */}
      <section className="py-16 -mt-8">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-6">
            {contactInfo.map((info, idx) => {
              const Icon = info.icon;
              return (
                <div key={idx} className="bg-white rounded-2xl shadow-xl p-6 border border-slate-100">
                  <div className={`w-12 h-12 rounded-xl ${info.color} flex items-center justify-center mb-4`}>
                    <Icon className="w-6 h-6 text-white" />
                  </div>
                  <h3 className="text-lg font-bold text-slate-900 mb-3">{info.title}</h3>
                  <div className="space-y-1 mb-4">
                    {info.details.map((detail, i) => (
                      <p key={i} className="text-slate-600 text-sm">{detail}</p>
                    ))}
                  </div>
                  {info.action && (
                    <a 
                      href={info.action}
                      target={info.action.startsWith('http') ? '_blank' : undefined}
                      rel={info.action.startsWith('http') ? 'noopener noreferrer' : undefined}
                      className="text-teal-600 font-medium text-sm hover:underline"
                    >
                      {info.actionText} →
                    </a>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      </section>

      {/* Contact Form & Map */}
      <section className="py-16 bg-slate-50">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="grid lg:grid-cols-2 gap-12">
            {/* Contact Form */}
            <div>
              <h2 className="text-2xl font-bold text-slate-900 mb-6">
                Send Us a Message
              </h2>
              <form onSubmit={handleSubmit} className="space-y-5">
                <div className="grid md:grid-cols-2 gap-5">
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-2">
                      Your Name *
                    </label>
                    <input
                      type="text"
                      required
                      value={formData.name}
                      onChange={(e) => setFormData({...formData, name: e.target.value})}
                      className="w-full p-3 border border-slate-200 rounded-lg focus:ring-2 focus:ring-teal-500 focus:border-teal-500"
                      placeholder="John Doe"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-2">
                      Phone Number *
                    </label>
                    <input
                      type="tel"
                      required
                      value={formData.phone}
                      onChange={(e) => setFormData({...formData, phone: e.target.value})}
                      className="w-full p-3 border border-slate-200 rounded-lg focus:ring-2 focus:ring-teal-500 focus:border-teal-500"
                      placeholder="+91 9876543210"
                    />
                  </div>
                </div>

                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-2">
                    Email Address *
                  </label>
                  <input
                    type="email"
                    required
                    value={formData.email}
                    onChange={(e) => setFormData({...formData, email: e.target.value})}
                    className="w-full p-3 border border-slate-200 rounded-lg focus:ring-2 focus:ring-teal-500 focus:border-teal-500"
                    placeholder="you@example.com"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-2">
                    Department
                  </label>
                  <select
                    value={formData.subject}
                    onChange={(e) => setFormData({...formData, subject: e.target.value})}
                    className="w-full p-3 border border-slate-200 rounded-lg focus:ring-2 focus:ring-teal-500 focus:border-teal-500"
                  >
                    {departments.map(dept => (
                      <option key={dept.value} value={dept.value}>{dept.label}</option>
                    ))}
                  </select>
                </div>

                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-2">
                    Your Message *
                  </label>
                  <textarea
                    required
                    rows={5}
                    value={formData.message}
                    onChange={(e) => setFormData({...formData, message: e.target.value})}
                    className="w-full p-3 border border-slate-200 rounded-lg focus:ring-2 focus:ring-teal-500 focus:border-teal-500"
                    placeholder="Tell us about your requirements..."
                  />
                </div>

                <Button 
                  type="submit" 
                  disabled={submitting}
                  className="w-full bg-teal-600 hover:bg-teal-700 text-white py-4 rounded-lg font-medium"
                >
                  {submitting ? (
                    <>Sending...</>
                  ) : (
                    <>
                      <Send className="w-4 h-4 mr-2" /> Send Message
                    </>
                  )}
                </Button>
              </form>
            </div>

            {/* Info Panel */}
            <div className="space-y-8">
              {/* Quick Contact */}
              <div className="bg-white rounded-2xl p-6 shadow-sm border border-slate-100">
                <h3 className="text-lg font-bold text-slate-900 mb-4">Quick Contact</h3>
                <div className="space-y-4">
                  <a href="tel:+919284701985" className="flex items-center gap-4 p-4 bg-emerald-50 rounded-xl hover:bg-emerald-100 transition-colors">
                    <div className="w-12 h-12 rounded-full bg-emerald-500 flex items-center justify-center">
                      <Phone className="w-5 h-5 text-white" />
                    </div>
                    <div>
                      <p className="font-medium text-slate-900">Call Us</p>
                      <p className="text-emerald-600 font-bold">+91 9284701985</p>
                    </div>
                  </a>
                  <a href="https://wa.me/919284701985" target="_blank" rel="noopener noreferrer" className="flex items-center gap-4 p-4 bg-green-50 rounded-xl hover:bg-green-100 transition-colors">
                    <div className="w-12 h-12 rounded-full bg-green-500 flex items-center justify-center">
                      <MessageSquare className="w-5 h-5 text-white" />
                    </div>
                    <div>
                      <p className="font-medium text-slate-900">WhatsApp</p>
                      <p className="text-green-600 font-bold">Message Us</p>
                    </div>
                  </a>
                </div>
              </div>

              {/* Why Choose Us */}
              <div className="bg-gradient-to-br from-teal-600 to-emerald-600 rounded-2xl p-6 text-white">
                <h3 className="text-lg font-bold mb-4">Why Choose Lucumaa Glass?</h3>
                <div className="space-y-3">
                  {[
                    { icon: Building2, text: 'State-of-the-art manufacturing facility' },
                    { icon: CheckCircle, text: 'IS 2553 certified products' },
                    { icon: Truck, text: 'Pan-India delivery network' },
                    { icon: CreditCard, text: 'Flexible payment options' }
                  ].map((item, idx) => {
                    const Icon = item.icon;
                    return (
                      <div key={idx} className="flex items-center gap-3">
                        <Icon className="w-5 h-5 text-teal-200" />
                        <span className="text-teal-50">{item.text}</span>
                      </div>
                    );
                  })}
                </div>
              </div>

              {/* Map Placeholder */}
              <div className="bg-slate-200 rounded-2xl h-64 flex items-center justify-center">
                <div className="text-center">
                  <MapPin className="w-12 h-12 text-slate-400 mx-auto mb-2" />
                  <p className="text-slate-500">Factory & Corporate Office</p>
                  <p className="text-slate-700 font-medium">Pune, Maharashtra</p>
                  <a 
                    href="https://maps.google.com/?q=Pune+Maharashtra+India" 
                    target="_blank" 
                    rel="noopener noreferrer"
                    className="text-teal-600 text-sm mt-2 inline-block hover:underline"
                  >
                    Open in Google Maps →
                  </a>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* CTA */}
      <section className="py-12 bg-slate-900">
        <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 text-center">
          <h2 className="text-2xl font-bold text-white mb-4">
            Prefer Email for Specific Needs?
          </h2>
          <div className="flex flex-wrap justify-center gap-4 text-sm">
            <a href="mailto:book@lucumaaglass.in" className="text-teal-400 hover:underline">
              book@lucumaaglass.in - Bookings
            </a>
            <span className="text-slate-500">|</span>
            <a href="mailto:info@lucumaaglass.in" className="text-teal-400 hover:underline">
              info@lucumaaglass.in - Information
            </a>
            <span className="text-slate-500">|</span>
            <a href="mailto:sales@lucumaaglass.in" className="text-teal-400 hover:underline">
              sales@lucumaaglass.in - Sales
            </a>
          </div>
        </div>
      </section>
    </div>
  );
};

export default Contact;
