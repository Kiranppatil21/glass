import React from 'react';
import { Link } from 'react-router-dom';
import { Building2, Home, Car, Store, Hospital, Hotel, Factory, ChevronRight, CheckCircle } from 'lucide-react';
import { Button } from '../components/ui/button';

const Industries = () => {
  const industries = [
    {
      id: 'commercial',
      title: 'Commercial Buildings',
      description: 'Elevate your office spaces, malls, and corporate buildings with our premium glass solutions. From stunning facades to energy-efficient glazing.',
      image: 'https://images.unsplash.com/photo-1553601581-8a1f1010efbe?crop=entropy&cs=srgb&fm=jpg&q=85&w=800',
      applications: ['Glass Facades', 'Curtain Walls', 'Partition Walls', 'Revolving Doors', 'Skylights'],
      icon: Building2,
      color: 'from-blue-600 to-blue-800'
    },
    {
      id: 'residential',
      title: 'Residential & Homes',
      description: 'Transform your home with elegant glass installations. Modern windows, shower enclosures, railings, and decorative glass panels.',
      image: 'https://images.unsplash.com/photo-1760067537877-dd4d2722c649?crop=entropy&cs=srgb&fm=jpg&q=85&w=800',
      applications: ['Shower Enclosures', 'Balcony Railings', 'Windows & Doors', 'Kitchen Backsplash', 'Mirrors'],
      icon: Home,
      color: 'from-emerald-600 to-emerald-800'
    },
    {
      id: 'automotive',
      title: 'Automotive & Transport',
      description: 'High-quality automotive glass for windshields, side windows, and sunroofs. Meeting international safety standards.',
      image: 'https://images.unsplash.com/photo-1503376780353-7e6692767b70?crop=entropy&cs=srgb&fm=jpg&q=85&w=800',
      applications: ['Windshields', 'Side Windows', 'Rear Windows', 'Sunroofs', 'Bus & Truck Glass'],
      icon: Car,
      color: 'from-slate-600 to-slate-800'
    },
    {
      id: 'retail',
      title: 'Retail & Showrooms',
      description: 'Create stunning retail experiences with glass storefronts, display cases, and interior partitions that showcase your products beautifully.',
      image: 'https://images.unsplash.com/photo-1630048911157-66276d027a31?crop=entropy&cs=srgb&fm=jpg&q=85&w=800',
      applications: ['Shop Fronts', 'Display Cases', 'Security Glass', 'Interior Partitions', 'Signage Glass'],
      icon: Store,
      color: 'from-purple-600 to-purple-800'
    },
    {
      id: 'healthcare',
      title: 'Healthcare & Hospitals',
      description: 'Specialized glass solutions for hospitals, clinics, and laboratories. Hygienic, safe, and easy to maintain.',
      image: 'https://images.unsplash.com/photo-1519494026892-80bbd2d6fd0d?crop=entropy&cs=srgb&fm=jpg&q=85&w=800',
      applications: ['Clean Room Glass', 'X-Ray Protection', 'Lab Partitions', 'Privacy Glass', 'Antimicrobial Glass'],
      icon: Hospital,
      color: 'from-teal-600 to-teal-800'
    },
    {
      id: 'hospitality',
      title: 'Hotels & Hospitality',
      description: 'Create luxurious guest experiences with premium glass in lobbies, rooms, restaurants, and spa facilities.',
      image: 'https://images.unsplash.com/photo-1763485956293-873ea83bf095?crop=entropy&cs=srgb&fm=jpg&q=85&w=800',
      applications: ['Lobby Glass', 'Room Partitions', 'Bathroom Enclosures', 'Pool Fencing', 'Restaurant Fronts'],
      icon: Hotel,
      color: 'from-amber-600 to-amber-800'
    }
  ];

  const stats = [
    { value: '500+', label: 'Projects Completed' },
    { value: '15+', label: 'Years Experience' },
    { value: '200+', label: 'Happy Clients' },
    { value: '50+', label: 'Cities Served' }
  ];

  return (
    <div className="min-h-screen bg-white" data-testid="industries-page">
      {/* Hero Section */}
      <section className="relative bg-gradient-to-br from-slate-900 via-slate-800 to-teal-900 py-24 overflow-hidden">
        <div className="absolute inset-0 opacity-20">
          <div className="absolute inset-0" style={{
            backgroundImage: 'url("https://images.unsplash.com/photo-1762340748868-753b4c304d60?crop=entropy&cs=srgb&fm=jpg&q=85&w=1920")',
            backgroundSize: 'cover',
            backgroundPosition: 'center'
          }} />
        </div>
        <div className="relative max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="text-center">
            <span className="inline-block px-4 py-2 bg-teal-500/20 text-teal-300 rounded-full text-sm font-medium mb-6">
              Industries We Serve
            </span>
            <h1 className="text-4xl md:text-5xl lg:text-6xl font-bold text-white mb-6">
              Glass Solutions for <br />
              <span className="text-transparent bg-clip-text bg-gradient-to-r from-teal-400 to-emerald-400">
                Every Industry
              </span>
            </h1>
            <p className="text-lg md:text-xl text-slate-300 max-w-3xl mx-auto mb-8">
              From towering commercial buildings to cozy homes, we provide premium glass products 
              tailored to meet the unique requirements of diverse industries.
            </p>
            <div className="flex flex-wrap justify-center gap-4">
              <Link to="/customize">
                <Button className="bg-teal-500 hover:bg-teal-600 text-white px-8 py-6 text-lg rounded-full">
                  Get Custom Quote <ChevronRight className="ml-2" />
                </Button>
              </Link>
              <Link to="/contact">
                <Button variant="outline" className="border-white/30 text-white hover:bg-white/10 px-8 py-6 text-lg rounded-full">
                  Contact Sales
                </Button>
              </Link>
            </div>
          </div>
        </div>
      </section>

      {/* Stats Section */}
      <section className="py-12 bg-slate-50 border-y border-slate-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-8">
            {stats.map((stat, idx) => (
              <div key={idx} className="text-center">
                <p className="text-3xl md:text-4xl font-bold text-teal-600">{stat.value}</p>
                <p className="text-slate-600 mt-1">{stat.label}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* Industries Grid */}
      <section className="py-20">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="text-center mb-16">
            <h2 className="text-3xl md:text-4xl font-bold text-slate-900 mb-4">
              Specialized Solutions for Every Sector
            </h2>
            <p className="text-lg text-slate-600 max-w-2xl mx-auto">
              We understand the unique requirements of each industry and deliver customized glass solutions that exceed expectations.
            </p>
          </div>

          <div className="space-y-16">
            {industries.map((industry, idx) => {
              const Icon = industry.icon;
              const isEven = idx % 2 === 0;
              
              return (
                <div 
                  key={industry.id}
                  className={`flex flex-col ${isEven ? 'lg:flex-row' : 'lg:flex-row-reverse'} gap-8 lg:gap-12 items-center`}
                >
                  {/* Image */}
                  <div className="w-full lg:w-1/2">
                    <div className="relative rounded-2xl overflow-hidden shadow-2xl group">
                      <img 
                        src={industry.image} 
                        alt={industry.title}
                        className="w-full h-80 object-cover transition-transform duration-500 group-hover:scale-105"
                      />
                      <div className={`absolute inset-0 bg-gradient-to-t ${industry.color} opacity-30`} />
                      <div className="absolute top-4 left-4">
                        <div className={`w-12 h-12 rounded-xl bg-gradient-to-br ${industry.color} flex items-center justify-center shadow-lg`}>
                          <Icon className="w-6 h-6 text-white" />
                        </div>
                      </div>
                    </div>
                  </div>

                  {/* Content */}
                  <div className="w-full lg:w-1/2 space-y-6">
                    <h3 className="text-2xl md:text-3xl font-bold text-slate-900">
                      {industry.title}
                    </h3>
                    <p className="text-lg text-slate-600">
                      {industry.description}
                    </p>
                    
                    <div className="grid grid-cols-2 gap-3">
                      {industry.applications.map((app, i) => (
                        <div key={i} className="flex items-center gap-2">
                          <CheckCircle className="w-5 h-5 text-teal-500 flex-shrink-0" />
                          <span className="text-slate-700">{app}</span>
                        </div>
                      ))}
                    </div>

                    <Link to="/customize">
                      <Button className={`bg-gradient-to-r ${industry.color} text-white px-6 py-3 rounded-full hover:opacity-90 transition-opacity`}>
                        Get Quote for {industry.title.split(' ')[0]} <ChevronRight className="ml-1 w-4 h-4" />
                      </Button>
                    </Link>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </section>

      {/* CTA Section */}
      <section className="py-20 bg-gradient-to-br from-teal-600 to-teal-800">
        <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 text-center">
          <Factory className="w-16 h-16 text-teal-200 mx-auto mb-6" />
          <h2 className="text-3xl md:text-4xl font-bold text-white mb-6">
            Can't Find Your Industry?
          </h2>
          <p className="text-lg text-teal-100 mb-8">
            We provide custom glass solutions for any industry. Contact our team to discuss 
            your specific requirements and get a tailored solution.
          </p>
          <div className="flex flex-wrap justify-center gap-4">
            <Link to="/contact">
              <Button className="bg-white text-teal-700 hover:bg-teal-50 px-8 py-6 text-lg rounded-full">
                Contact Our Team
              </Button>
            </Link>
            <a href="tel:9284701985">
              <Button variant="outline" className="border-white/30 text-white hover:bg-white/10 px-8 py-6 text-lg rounded-full">
                Call: 9284701985
              </Button>
            </a>
          </div>
        </div>
      </section>
    </div>
  );
};

export default Industries;
