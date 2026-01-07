import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { motion } from 'framer-motion';
import { Card, CardContent } from '../components/ui/card';
import { ShoppingCart, Eye } from 'lucide-react';
import { useCart } from '../contexts/CartContext';
import { toast } from 'sonner';
import api from '../utils/api';

const Products = () => {
  const [products, setProducts] = useState([]);
  const [loading, setLoading] = useState(true);
  const { addToCart } = useCart();

  useEffect(() => {
    fetchProducts();
  }, []);

  const fetchProducts = async () => {
    try {
      const response = await api.products.getAll();
      setProducts(response.data);
    } catch (error) {
      console.error('Failed to fetch products:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleAddToCart = (product) => {
    addToCart({
      id: product.id,
      name: product.name,
      image: product.image_url,
      price: product.base_price || 100,
      glassType: product.name,
      thickness: product.thickness_options?.[0] || 6
    }, {
      quantity: 1,
      totalPrice: product.base_price || 100
    });
    toast.success(`${product.name} added to cart!`);
  };

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center" data-testid="products-loading">
        <div className="text-xl text-slate-600">Loading products...</div>
      </div>
    );
  }

  return (
    <div className="min-h-screen py-20" data-testid="products-page">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="text-center mb-16">
          <h1 className="text-5xl font-bold text-slate-900 mb-4 tracking-tight" data-testid="products-title">Our Product Range</h1>
          <p className="text-xl text-slate-600 max-w-3xl mx-auto leading-relaxed">
            Premium quality glass manufactured with advanced technology, complying with international standards.
          </p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-8">
          {products.map((product, index) => (
            <motion.div
              key={product.id}
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: index * 0.1 }}
            >
              <Card className="group hover:shadow-2xl transition-all duration-500 hover:-translate-y-2 border-slate-200 h-full" data-testid={`product-item-${index}`}>
                <div className="aspect-[4/3] overflow-hidden rounded-t-xl">
                  <img
                    src={product.image_url}
                    alt={product.name}
                    className="w-full h-full object-cover group-hover:scale-110 transition-transform duration-500"
                  />
                </div>
                <CardContent className="p-6">
                  <h3 className="text-2xl font-semibold text-slate-900 mb-3">{product.name}</h3>
                  <p className="text-slate-600 mb-4 leading-relaxed">{product.description}</p>
                  
                  <div className="space-y-3 mb-6">
                    <div>
                      <p className="text-sm font-medium text-slate-700 mb-1">Applications:</p>
                      <p className="text-sm text-slate-600">{product.applications.join(', ')}</p>
                    </div>
                    <div>
                      <p className="text-sm font-medium text-slate-700 mb-1">Available Thickness:</p>
                      <p className="text-sm text-slate-600">{product.thickness_options.join('mm, ')}mm</p>
                    </div>
                    <div>
                      <p className="text-sm font-medium text-slate-700 mb-1">Standards:</p>
                      <p className="text-sm text-slate-600">{product.standards}</p>
                    </div>
                  </div>

                  <div className="flex gap-3">
                    <Link
                      to={`/products/${product.id}`}
                      className="flex-1 text-center bg-primary-700 hover:bg-primary-800 text-white font-medium py-3 rounded-lg transition-colors flex items-center justify-center gap-2"
                      data-testid={`view-details-btn-${index}`}
                    >
                      <Eye className="w-4 h-4" /> View Details
                    </Link>
                    <button
                      onClick={() => handleAddToCart(product)}
                      className="flex-1 text-center bg-emerald-600 hover:bg-emerald-700 text-white font-medium py-3 rounded-lg transition-colors flex items-center justify-center gap-2"
                      data-testid={`add-cart-btn-${index}`}
                    >
                      <ShoppingCart className="w-4 h-4" /> Add to Cart
                    </button>
                  </div>
                </CardContent>
              </Card>
            </motion.div>
          ))}
        </div>
      </div>
    </div>
  );
};

export default Products;