import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { motion, AnimatePresence } from 'framer-motion';
import { ArrowRight, Calculator, Shield, TrendingUp, Building, Users, Award, CheckCircle, ChevronLeft, ChevronRight } from 'lucide-react';
import { Button } from '../components/ui/button';
import { Card, CardContent } from '../components/ui/card';
import api from '../utils/api';

const Home = () => {
  const [width, setWidth] = useState('');
  const [height, setHeight] = useState('');
  const [thickness, setThickness] = useState('6');
  const [quantity, setQuantity] = useState('1');
  const [estimatedPrice, setEstimatedPrice] = useState(null);
  const [products, setProducts] = useState([]);
  const [currentSlide, setCurrentSlide] = useState(0);

  const heroSlides = [
    {
      title: "Factory-Direct Toughened Glass",
      subtitle: "Customized & Delivered",
      description: "Premium quality glass manufactured in-house. Calculate price instantly, customize online, and get doorstep delivery across India.",
      image: "https://images.unsplash.com/photo-1721433730939-34917128c2bd?crop=entropy&cs=srgb&fm=jpg&q=85",
      cta1: "Customize & Book Now",
      cta2: "Explore Products"
    },
    {
      title: "50,000+ Satisfied Customers",
      subtitle: "Trusted Nationwide",
      description: "Join thousands of architects, builders, and homeowners who trust Lucumaa Glass for their premium glass requirements.",
      image: "https://images.unsplash.com/photo-1754780960162-839cda44d736?crop=entropy&cs=srgb&fm=jpg&q=85",
      cta1: "View Projects",
      cta2: "Get Started"
    },
    {
      title: "IS 2553 Certified Quality",
      subtitle: "5X Stronger Than Standard Glass",
      description: "Advanced manufacturing with international standards. Every piece undergoes rigorous quality testing before dispatch.",
      image: "https://images.unsplash.com/photo-1697281679290-ad7be1b10682?crop=entropy&cs=srgb&fm=jpg&q=85",
      cta1: "Our Certifications",
      cta2: "Quality Promise"
    },
    {
      title: "7-14 Days Fast Delivery",
      subtitle: "From Order to Installation",
      description: "State-of-the-art facility with 50K+ sq ft monthly capacity. Quick turnaround without compromising quality.",
      image: "https://images.unsplash.com/photo-1763209230598-3efb61fc36bb?crop=entropy&cs=srgb&fm=jpg&q=85",
      cta1: "Track Order",
      cta2: "Calculate Price"
    }
  ];

  useEffect(() => {
    fetchProducts();
    const interval = setInterval(() => {
      setCurrentSlide((prev) => (prev + 1) % heroSlides.length);
    }, 5000);
    return () => clearInterval(interval);
  }, []);

  const nextSlide = () => {
    setCurrentSlide((prev) => (prev + 1) % heroSlides.length);
  };

  const prevSlide = () => {
    setCurrentSlide((prev) => (prev - 1 + heroSlides.length) % heroSlides.length);
  };

  const fetchProducts = async () => {
    try {
      const response = await api.products.getAll();
      setProducts(response.data.slice(0, 4));
    } catch (error) {
      console.error('Failed to fetch products:', error);
    }
  };

  const calculateQuickPrice = () => {
    const w = parseFloat(width) || 0;
    const h = parseFloat(height) || 0;
    const t = parseFloat(thickness) || 0;
    const q = parseInt(quantity) || 1;

    if (w > 0 && h > 0) {
      const areaSqFt = (w * h) / 144;
      const totalArea = areaSqFt * q;
      const pricePerSqFt = 50 + t * 5;
      const basePrice = totalArea * pricePerSqFt;
      const gst = basePrice * 0.18;
      setEstimatedPrice(Math.round(basePrice + gst));
    }
  };

  return (
    <div className="min-h-screen" data-testid="home-page">
      <section className="relative h-[90vh] overflow-hidden" data-testid="hero-section">
        <AnimatePresence mode="wait">
          <motion.div
            key={currentSlide}
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.5 }}
            className="absolute inset-0"
          >
            <img
              src={heroSlides[currentSlide].image}
              alt={heroSlides[currentSlide].title}
              className="w-full h-full object-cover"
            />
            <div className="absolute inset-0 bg-gradient-to-b from-slate-900/60 via-slate-900/40 to-white"></div>
          </motion.div>
        </AnimatePresence>

        <div className="relative max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-full flex items-center">
          <div className="max-w-3xl">
            <AnimatePresence mode="wait">
              <motion.div
                key={currentSlide}
                initial={{ opacity: 0, y: 30 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -30 }}
                transition={{ duration: 0.5 }}
              >
                <h1 className="text-5xl sm:text-6xl lg:text-7xl font-bold text-white mb-6 tracking-tight leading-none" data-testid="hero-title">
                  {heroSlides[currentSlide].title}<br />
                  <span className="text-primary-400">{heroSlides[currentSlide].subtitle}</span>
                </h1>
                <p className="text-xl text-white/90 mb-8 max-w-2xl leading-relaxed" data-testid="hero-subtitle">
                  {heroSlides[currentSlide].description}
                </p>
                <div className="flex flex-wrap gap-4">
                  <Button asChild size="lg" className="bg-primary-600 hover:bg-primary-700 text-lg px-8 py-6" data-testid="hero-customize-btn">
                    <Link to="/customize">{heroSlides[currentSlide].cta1} <ArrowRight className="ml-2" /></Link>
                  </Button>
                  <Button asChild variant="outline" size="lg" className="text-lg px-8 py-6 border-white text-white hover:bg-white/10" data-testid="hero-products-btn">
                    <Link to="/products">{heroSlides[currentSlide].cta2}</Link>
                  </Button>
                </div>
              </motion.div>
            </AnimatePresence>
          </div>
        </div>

        <button
          onClick={prevSlide}
          className="absolute left-4 top-1/2 -translate-y-1/2 w-12 h-12 bg-white/20 hover:bg-white/30 backdrop-blur-lg rounded-full flex items-center justify-center text-white transition-all z-10"
          data-testid="prev-slide"
        >
          <ChevronLeft className="w-6 h-6" />
        </button>
        <button
          onClick={nextSlide}
          className="absolute right-4 top-1/2 -translate-y-1/2 w-12 h-12 bg-white/20 hover:bg-white/30 backdrop-blur-lg rounded-full flex items-center justify-center text-white transition-all z-10"
          data-testid="next-slide"
        >
          <ChevronRight className="w-6 h-6" />
        </button>

        <div className="absolute bottom-8 left-1/2 -translate-x-1/2 flex gap-3 z-10">
          {heroSlides.map((_, index) => (
            <button
              key={index}
              onClick={() => setCurrentSlide(index)}
              className={`h-2 rounded-full transition-all ${
                index === currentSlide ? 'w-12 bg-white' : 'w-2 bg-white/50'
              }`}
              data-testid={`slide-indicator-${index}`}
            />
          ))}
        </div>
      </section>

      <section className="py-20 bg-gradient-to-br from-primary-600 to-primary-800 text-white" data-testid="stats-section">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="text-center mb-16">
            <motion.h2 
              initial={{ opacity: 0, y: 20 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true }}
              className="text-4xl font-bold mb-4 tracking-tight"
            >
              Trusted by Thousands Across India
            </motion.h2>
            <p className="text-xl text-primary-100">Numbers that speak for our quality and service</p>
          </div>

          <div className="grid grid-cols-2 md:grid-cols-4 gap-8">
            {[
              { number: '50,000+', label: 'Satisfied Customers', icon: Users },
              { number: '75,000+', label: 'Sq Ft Glass Delivered', icon: TrendingUp },
              { number: '1,000+', label: 'Projects Completed', icon: Building },
              { number: '100%', label: 'Quality Certified', icon: Award }
            ].map((stat, index) => (
              <motion.div
                key={index}
                initial={{ opacity: 0, scale: 0.9 }}
                whileInView={{ opacity: 1, scale: 1 }}
                viewport={{ once: true }}
                transition={{ delay: index * 0.1 }}
                className="text-center"
              >
                <div className="w-20 h-20 bg-white/10 backdrop-blur-lg rounded-2xl flex items-center justify-center mx-auto mb-4 border border-white/20">
                  <stat.icon className="w-10 h-10 text-white" />
                </div>
                <div className="text-5xl font-bold mb-2">{stat.number}</div>
                <div className="text-lg text-primary-100">{stat.label}</div>
              </motion.div>
            ))}
          </div>

          <div className="mt-16 text-center">
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true }}
              transition={{ delay: 0.4 }}
            >
              <p className="text-xl mb-6">Join the growing community of satisfied customers</p>
              <Button asChild size="lg" className="bg-white text-primary-700 hover:bg-primary-50 text-lg px-8 py-6">
                <Link to="/customize">Start Your Order Now <ArrowRight className="ml-2" /></Link>
              </Button>
            </motion.div>
          </div>
        </div>
      </section>

      <section className="py-20 bg-white" data-testid="calculator-section">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="text-center mb-12">
            <h2 className="text-4xl font-bold text-slate-900 mb-4 tracking-tight" data-testid="calculator-title">Quick Glass Price Calculator</h2>
            <p className="text-lg text-slate-600">Get an instant estimate for your glass requirements</p>
          </div>

          <Card className="max-w-4xl mx-auto shadow-xl border-slate-200" data-testid="price-calculator-card">
            <CardContent className="p-8">
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-6">
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-2">Width (inches)</label>
                  <input
                    type="number"
                    value={width}
                    onChange={(e) => setWidth(e.target.value)}
                    className="w-full h-12 rounded-lg border-slate-300 px-4 focus:border-primary-500 focus:ring-2 focus:ring-primary-500/20 transition-all"
                    placeholder="24"
                    data-testid="calc-width-input"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-2">Height (inches)</label>
                  <input
                    type="number"
                    value={height}
                    onChange={(e) => setHeight(e.target.value)}
                    className="w-full h-12 rounded-lg border-slate-300 px-4 focus:border-primary-500 focus:ring-2 focus:ring-primary-500/20 transition-all"
                    placeholder="36"
                    data-testid="calc-height-input"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-2">Thickness (mm)</label>
                  <select
                    value={thickness}
                    onChange={(e) => setThickness(e.target.value)}
                    className="w-full h-12 rounded-lg border-slate-300 px-4 focus:border-primary-500 focus:ring-2 focus:ring-primary-500/20 transition-all"
                    data-testid="calc-thickness-select"
                  >
                    <option value="5">5mm</option>
                    <option value="6">6mm</option>
                    <option value="8">8mm</option>
                    <option value="10">10mm</option>
                    <option value="12">12mm</option>
                  </select>
                </div>
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-2">Quantity</label>
                  <input
                    type="number"
                    value={quantity}
                    onChange={(e) => setQuantity(e.target.value)}
                    className="w-full h-12 rounded-lg border-slate-300 px-4 focus:border-primary-500 focus:ring-2 focus:ring-primary-500/20 transition-all"
                    placeholder="1"
                    data-testid="calc-quantity-input"
                  />
                </div>
              </div>

              <Button onClick={calculateQuickPrice} className="w-full bg-primary-700 hover:bg-primary-800 h-12 text-lg" data-testid="calc-calculate-btn">
                <Calculator className="mr-2" /> Calculate Price
              </Button>

              {estimatedPrice && (
                <div className="mt-6 p-6 bg-primary-50 rounded-lg border border-primary-200" data-testid="calc-result">
                  <p className="text-sm text-slate-600 mb-1">Estimated Price (incl. GST)</p>
                  <p className="text-4xl font-bold text-primary-700">₹{estimatedPrice.toLocaleString()}</p>
                  <p className="text-sm text-slate-500 mt-2">*Final price may vary based on customization & delivery location</p>
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      </section>

      <section className="py-20 bg-slate-50" data-testid="products-section">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="text-center mb-12">
            <h2 className="text-4xl font-bold text-slate-900 mb-4 tracking-tight">Our Product Range</h2>
            <p className="text-lg text-slate-600">Premium quality glass for every application</p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-8">
            {products.map((product, index) => (
              <motion.div
                key={product.id}
                initial={{ opacity: 0, y: 20 }}
                whileInView={{ opacity: 1, y: 0 }}
                transition={{ delay: index * 0.1 }}
                viewport={{ once: true }}
              >
                <Card className="group hover:shadow-2xl transition-all duration-500 hover:-translate-y-2 border-slate-200" data-testid={`product-card-${index}`}>
                  <div className="aspect-[4/3] overflow-hidden rounded-t-xl">
                    <img src={product.image_url} alt={product.name} className="w-full h-full object-cover group-hover:scale-110 transition-transform duration-500" />
                  </div>
                  <CardContent className="p-6">
                    <h3 className="text-xl font-semibold text-slate-900 mb-2">{product.name}</h3>
                    <p className="text-slate-600 text-sm mb-4 line-clamp-2">{product.description}</p>
                    <Button asChild variant="ghost" className="text-primary-700 hover:text-primary-800 p-0" data-testid={`product-learn-more-${index}`}>
                      <Link to={`/products/${product.id}`}>Learn More <ArrowRight className="ml-2 w-4 h-4" /></Link>
                    </Button>
                  </CardContent>
                </Card>
              </motion.div>
            ))}
          </div>

          <div className="text-center mt-12">
            <Button asChild size="lg" variant="outline" className="border-slate-300" data-testid="view-all-products-btn">
              <Link to="/products">View All Products</Link>
            </Button>
          </div>
        </div>
      </section>

      <section className="py-20 bg-white" data-testid="why-lucumaa-section">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="text-center mb-12">
            <h2 className="text-4xl font-bold text-slate-900 mb-4 tracking-tight">Why Lucumaa Glass?</h2>
            <p className="text-lg text-slate-600">Industry-leading manufacturing & service excellence</p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-8">
            <Card className="text-center p-8 border-slate-200 hover:border-primary-500 transition-colors" data-testid="feature-factory">
              <div className="w-16 h-16 bg-primary-100 rounded-2xl flex items-center justify-center mx-auto mb-4">
                <Building className="w-8 h-8 text-primary-700" />
              </div>
              <h3 className="text-xl font-semibold text-slate-900 mb-2">Factory Direct</h3>
              <p className="text-slate-600">No middlemen. Best prices guaranteed from our own manufacturing unit.</p>
            </Card>

            <Card className="text-center p-8 border-slate-200 hover:border-primary-500 transition-colors" data-testid="feature-quality">
              <div className="w-16 h-16 bg-primary-100 rounded-2xl flex items-center justify-center mx-auto mb-4">
                <Shield className="w-8 h-8 text-primary-700" />
              </div>
              <h3 className="text-xl font-semibold text-slate-900 mb-2">Premium Quality</h3>
              <p className="text-slate-600">IS 2553 certified. 5-7x stronger than standard glass.</p>
            </Card>

            <Card className="text-center p-8 border-slate-200 hover:border-primary-500 transition-colors" data-testid="feature-customization">
              <div className="w-16 h-16 bg-primary-100 rounded-2xl flex items-center justify-center mx-auto mb-4">
                <TrendingUp className="w-8 h-8 text-primary-700" />
              </div>
              <h3 className="text-xl font-semibold text-slate-900 mb-2">Full Customization</h3>
              <p className="text-slate-600">Any size, any thickness. Upload your drawings for perfect fit.</p>
            </Card>

            <Card className="text-center p-8 border-slate-200 hover:border-primary-500 transition-colors" data-testid="feature-delivery">
              <div className="w-16 h-16 bg-primary-100 rounded-2xl flex items-center justify-center mx-auto mb-4">
                <Award className="w-8 h-8 text-primary-700" />
              </div>
              <h3 className="text-xl font-semibold text-slate-900 mb-2">Fast Delivery</h3>
              <p className="text-slate-600">7-14 days production. Safe delivery across India.</p>
            </Card>
          </div>
        </div>
      </section>

      <section className="py-20 bg-primary-700 text-white" data-testid="cta-section">
        <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 text-center">
          <h2 className="text-4xl font-bold mb-6 tracking-tight">Ready to Start Your Custom Order?</h2>
          <p className="text-xl mb-8 text-primary-100">Get instant pricing, customize your requirements, and place your order online in minutes.</p>
          <Button asChild size="lg" className="bg-white text-primary-700 hover:bg-primary-50 text-lg px-8 py-6" data-testid="cta-start-order-btn">
            <Link to="/customize">Start Custom Order <ArrowRight className="ml-2" /></Link>
          </Button>
        </div>
      </section>

      <section className="relative py-32 overflow-hidden" data-testid="manufacturing-showcase">
        <div className="absolute inset-0">
          <img
            src="https://images.unsplash.com/photo-1697281679290-ad7be1b10682?crop=entropy&cs=srgb&fm=jpg&q=85"
            alt="Manufacturing facility"
            className="w-full h-full object-cover"
          />
          <div className="absolute inset-0 bg-slate-900/80"></div>
        </div>

        <div className="relative max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-12 items-center">
            <motion.div
              initial={{ opacity: 0, x: -30 }}
              whileInView={{ opacity: 1, x: 0 }}
              viewport={{ once: true }}
            >
              <h2 className="text-5xl font-bold text-white mb-6 tracking-tight">
                State-of-the-Art Manufacturing
              </h2>
              <p className="text-xl text-slate-300 mb-8 leading-relaxed">
                Our advanced facility in Pune is equipped with cutting-edge machinery and follows stringent quality control processes to deliver premium glass products.
              </p>
              <div className="grid grid-cols-2 gap-6">
                <div className="bg-white/10 backdrop-blur-lg rounded-xl p-6 border border-white/20">
                  <div className="text-4xl font-bold text-primary-400 mb-2">100K+</div>
                  <div className="text-slate-300">Sq Ft Capacity/Month</div>
                </div>
                <div className="bg-white/10 backdrop-blur-lg rounded-xl p-6 border border-white/20">
                  <div className="text-4xl font-bold text-primary-400 mb-2">7-14</div>
                  <div className="text-slate-300">Days Delivery</div>
                </div>
                <div className="bg-white/10 backdrop-blur-lg rounded-xl p-6 border border-white/20">
                  <div className="text-4xl font-bold text-primary-400 mb-2">1000+</div>
                  <div className="text-slate-300">Projects Completed</div>
                </div>
                <div className="bg-white/10 backdrop-blur-lg rounded-xl p-6 border border-white/20">
                  <div className="text-4xl font-bold text-primary-400 mb-2">15+</div>
                  <div className="text-slate-300">Years Experience</div>
                </div>
              </div>
            </motion.div>

            <motion.div
              initial={{ opacity: 0, x: 30 }}
              whileInView={{ opacity: 1, x: 0 }}
              viewport={{ once: true }}
              className="hidden lg:block"
            >
              <div className="bg-white/10 backdrop-blur-lg rounded-2xl p-8 border border-white/20">
                <h3 className="text-2xl font-bold text-white mb-6">Our Certifications</h3>
                <div className="space-y-4">
                  <div className="flex items-center gap-3">
                    <CheckCircle className="w-6 h-6 text-primary-400" />
                    <span className="text-slate-200">IS 2553 Certified</span>
                  </div>
                  <div className="flex items-center gap-3">
                    <CheckCircle className="w-6 h-6 text-primary-400" />
                    <span className="text-slate-200">ASTM C1048 Compliant</span>
                  </div>
                  <div className="flex items-center gap-3">
                    <CheckCircle className="w-6 h-6 text-primary-400" />
                    <span className="text-slate-200">EN 12543 Standards</span>
                  </div>
                  <div className="flex items-center gap-3">
                    <CheckCircle className="w-6 h-6 text-primary-400" />
                    <span className="text-slate-200">ISO 9001:2015</span>
                  </div>
                </div>
              </div>
            </motion.div>
          </div>
        </div>
      </section>

      <section className="py-20 bg-white" data-testid="industries-preview">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="text-center mb-16">
            <h2 className="text-4xl font-bold text-slate-900 mb-4 tracking-tight">Industries We Serve</h2>
            <p className="text-lg text-slate-600 max-w-3xl mx-auto">
              Trusted by leading architects, builders, and developers across residential, commercial, and industrial sectors
            </p>
          </div>

          <div className="grid grid-cols-2 md:grid-cols-4 gap-8">
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true }}
              className="text-center"
            >
              <div className="w-20 h-20 bg-primary-100 rounded-2xl flex items-center justify-center mx-auto mb-4">
                <Building className="w-10 h-10 text-primary-700" />
              </div>
              <h3 className="font-semibold text-slate-900 mb-2">Builders & Developers</h3>
              <p className="text-sm text-slate-600">Residential & commercial projects</p>
            </motion.div>

            <motion.div
              initial={{ opacity: 0, y: 20 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true }}
              transition={{ delay: 0.1 }}
              className="text-center"
            >
              <div className="w-20 h-20 bg-primary-100 rounded-2xl flex items-center justify-center mx-auto mb-4">
                <Users className="w-10 h-10 text-primary-700" />
              </div>
              <h3 className="font-semibold text-slate-900 mb-2">Architects & Designers</h3>
              <p className="text-sm text-slate-600">Custom design solutions</p>
            </motion.div>

            <motion.div
              initial={{ opacity: 0, y: 20 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true }}
              transition={{ delay: 0.2 }}
              className="text-center"
            >
              <div className="w-20 h-20 bg-primary-100 rounded-2xl flex items-center justify-center mx-auto mb-4">
                <Shield className="w-10 h-10 text-primary-700" />
              </div>
              <h3 className="font-semibold text-slate-900 mb-2">Hotels & Hospitals</h3>
              <p className="text-sm text-slate-600">Safety-first installations</p>
            </motion.div>

            <motion.div
              initial={{ opacity: 0, y: 20 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true }}
              transition={{ delay: 0.3 }}
              className="text-center"
            >
              <div className="w-20 h-20 bg-primary-100 rounded-2xl flex items-center justify-center mx-auto mb-4">
                <TrendingUp className="w-10 h-10 text-primary-700" />
              </div>
              <h3 className="font-semibold text-slate-900 mb-2">Retail Showrooms</h3>
              <p className="text-sm text-slate-600">Premium display solutions</p>
            </motion.div>
          </div>

          <div className="text-center mt-12">
            <Button asChild variant="outline" size="lg" className="border-slate-300">
              <Link to="/industries">View All Industries</Link>
            </Button>
          </div>
        </div>
      </section>

      <section className="relative py-32 overflow-hidden bg-slate-900" data-testid="process-showcase">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="text-center mb-16">
            <h2 className="text-5xl font-bold text-white mb-4 tracking-tight">How It Works</h2>
            <p className="text-xl text-slate-400">From order to delivery in 7 simple steps</p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-4 gap-8">
            {[
              { step: '01', title: 'Choose Glass', desc: 'Select from our product range' },
              { step: '02', title: 'Customize', desc: 'Enter dimensions & specifications' },
              { step: '03', title: 'Get Quote', desc: 'Instant price calculation' },
              { step: '04', title: 'Confirm Order', desc: 'Secure online payment' },
              { step: '05', title: 'Production', desc: 'Manufactured with precision' },
              { step: '06', title: 'Quality Check', desc: 'Rigorous testing standards' },
              { step: '07', title: 'Dispatch', desc: 'Safe packaging & delivery' },
              { step: '08', title: 'Installation', desc: 'Expert support available' }
            ].map((item, index) => (
              <motion.div
                key={index}
                initial={{ opacity: 0, y: 30 }}
                whileInView={{ opacity: 1, y: 0 }}
                viewport={{ once: true }}
                transition={{ delay: index * 0.1 }}
                className="relative"
              >
                <div className="bg-white/5 backdrop-blur-lg rounded-xl p-6 border border-white/10 hover:border-primary-500/50 transition-all">
                  <div className="text-6xl font-bold text-primary-500/20 mb-4">{item.step}</div>
                  <h3 className="text-xl font-semibold text-white mb-2">{item.title}</h3>
                  <p className="text-slate-400 text-sm">{item.desc}</p>
                </div>
                {index < 7 && (
                  <div className="hidden lg:block absolute top-1/2 -right-4 w-8 h-0.5 bg-primary-500/30"></div>
                )}
              </motion.div>
            ))}
          </div>

          <div className="text-center mt-12">
            <Button asChild size="lg" className="bg-primary-600 hover:bg-primary-700 text-lg px-8 py-6">
              <Link to="/how-it-works">Learn More About Our Process</Link>
            </Button>
          </div>
        </div>
      </section>

      <section className="py-20 bg-gradient-to-br from-slate-50 to-primary-50" data-testid="testimonials">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="text-center mb-16">
            <h2 className="text-4xl font-bold text-slate-900 mb-4 tracking-tight">What Our Clients Say</h2>
            <p className="text-lg text-slate-600">Trusted by hundreds of satisfied customers</p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
            {[
              {
                name: 'Rajesh Sharma',
                role: 'Architect, RS Designs',
                text: 'Exceptional quality and timely delivery. The customization options and instant pricing made our project planning so much easier.',
                rating: 5
              },
              {
                name: 'Priya Deshmukh',
                role: 'Builder, Deshmukh Constructions',
                text: 'Factory-direct pricing saved us 30% on our last project. Professional service from order to installation.',
                rating: 5
              },
              {
                name: 'Amit Patel',
                role: 'Homeowner',
                text: 'Ordered custom glass for my home renovation. The quality is outstanding and the process was seamless.',
                rating: 5
              }
            ].map((testimonial, index) => (
              <motion.div
                key={index}
                initial={{ opacity: 0, scale: 0.95 }}
                whileInView={{ opacity: 1, scale: 1 }}
                viewport={{ once: true }}
                transition={{ delay: index * 0.1 }}
              >
                <Card className="h-full hover:shadow-xl transition-shadow">
                  <CardContent className="p-8">
                    <div className="flex mb-4">
                      {[...Array(testimonial.rating)].map((_, i) => (
                        <svg key={i} className="w-5 h-5 text-yellow-400 fill-current" viewBox="0 0 20 20">
                          <path d="M10 15l-5.878 3.09 1.123-6.545L.489 6.91l6.572-.955L10 0l2.939 5.955 6.572.955-4.756 4.635 1.123 6.545z" />
                        </svg>
                      ))}
                    </div>
                    <p className="text-slate-600 mb-6 leading-relaxed">"{testimonial.text}"</p>
                    <div>
                      <div className="font-semibold text-slate-900">{testimonial.name}</div>
                      <div className="text-sm text-slate-500">{testimonial.role}</div>
                    </div>
                  </CardContent>
                </Card>
              </motion.div>
            ))}
          </div>
        </div>
      </section>
    </div>
  );
};

export default Home;