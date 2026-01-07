import React, { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { motion } from 'framer-motion';
import { CheckCircle, ArrowRight } from 'lucide-react';
import { Button } from '../components/ui/button';
import { Card, CardContent } from '../components/ui/card';
import api from '../utils/api';

const ProductDetail = () => {
  const { id } = useParams();
  const navigate = useNavigate();
  const [product, setProduct] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchProduct();
  }, [id]);

  const fetchProduct = async () => {
    try {
      const response = await api.products.getOne(id);
      setProduct(response.data);
    } catch (error) {
      console.error('Failed to fetch product:', error);
    } finally {
      setLoading(false);
    }
  };

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center" data-testid="product-detail-loading">
        <div className="text-xl text-slate-600">Loading product...</div>
      </div>
    );
  }

  if (!product) {
    return (
      <div className="min-h-screen flex items-center justify-center" data-testid="product-not-found">
        <div className="text-center">
          <h2 className="text-2xl font-bold text-slate-900 mb-4">Product Not Found</h2>
          <Button onClick={() => navigate('/products')}>Back to Products</Button>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen py-20" data-testid="product-detail-page">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-12">
          <motion.div
            initial={{ opacity: 0, x: -30 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ duration: 0.5 }}
          >
            <div className="aspect-[4/3] rounded-2xl overflow-hidden shadow-2xl">
              <img src={product.image_url} alt={product.name} className="w-full h-full object-cover" />
            </div>
          </motion.div>

          <motion.div
            initial={{ opacity: 0, x: 30 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ duration: 0.5 }}
          >
            <h1 className="text-4xl font-bold text-slate-900 mb-4 tracking-tight" data-testid="product-name">{product.name}</h1>
            <p className="text-lg text-slate-600 mb-8 leading-relaxed">{product.description}</p>

            <Card className="mb-8">
              <CardContent className="p-6 space-y-4">
                <div>
                  <h3 className="text-lg font-semibold text-slate-900 mb-2">Strength & Safety</h3>
                  <p className="text-slate-600">{product.strength_info}</p>
                </div>
                <div>
                  <h3 className="text-lg font-semibold text-slate-900 mb-2">Standards Compliance</h3>
                  <p className="text-slate-600">{product.standards}</p>
                </div>
              </CardContent>
            </Card>

            <div className="mb-8">
              <h3 className="text-lg font-semibold text-slate-900 mb-4">Applications</h3>
              <div className="grid grid-cols-2 gap-3">
                {product.applications.map((app, index) => (
                  <div key={index} className="flex items-center gap-2">
                    <CheckCircle className="w-5 h-5 text-primary-600" />
                    <span className="text-slate-700">{app}</span>
                  </div>
                ))}
              </div>
            </div>

            <div className="mb-8">
              <h3 className="text-lg font-semibold text-slate-900 mb-4">Available Thickness Options</h3>
              <div className="flex flex-wrap gap-3">
                {product.thickness_options.map((thickness, index) => (
                  <div key={index} className="px-6 py-3 bg-slate-100 rounded-lg font-medium text-slate-900">
                    {thickness}mm
                  </div>
                ))}
              </div>
            </div>

            <div className="flex gap-4">
              <Button
                onClick={() => navigate('/customize')}
                size="lg"
                className="flex-1 bg-primary-700 hover:bg-primary-800"
                data-testid="customize-book-btn"
              >
                Customize & Book <ArrowRight className="ml-2" />
              </Button>
              <Button
                onClick={() => navigate('/contact')}
                size="lg"
                variant="outline"
                className="flex-1"
                data-testid="contact-btn"
              >
                Contact Us
              </Button>
            </div>
          </motion.div>
        </div>
      </div>
    </div>
  );
};

export default ProductDetail;