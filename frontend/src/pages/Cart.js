import React from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { ShoppingCart, Trash2, Plus, Minus, ArrowLeft, ArrowRight } from 'lucide-react';
import { useCart } from '../contexts/CartContext';
import { Button } from '../components/ui/button';

const Cart = () => {
  const { cartItems, removeFromCart, updateQuantity, getCartTotal, clearCart } = useCart();
  const navigate = useNavigate();

  const handleCheckout = () => {
    // Navigate to customize page with cart data
    navigate('/customize', { state: { cartItems } });
  };

  if (cartItems.length === 0) {
    return (
      <div className="min-h-screen bg-slate-50 py-16" data-testid="cart-page">
        <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 text-center">
          <ShoppingCart className="w-24 h-24 text-slate-300 mx-auto mb-6" />
          <h1 className="text-3xl font-bold text-slate-900 mb-4">Your Cart is Empty</h1>
          <p className="text-slate-600 mb-8">
            Looks like you haven't added anything to your cart yet.
          </p>
          <div className="flex flex-wrap justify-center gap-4">
            <Link to="/products">
              <Button className="bg-teal-600 hover:bg-teal-700 text-white px-8 py-3">
                Browse Products
              </Button>
            </Link>
            <Link to="/customize">
              <Button variant="outline" className="px-8 py-3">
                Customize Glass
              </Button>
            </Link>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-slate-50 py-8" data-testid="cart-page">
      <div className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8">
        {/* Header */}
        <div className="flex items-center justify-between mb-8">
          <div>
            <h1 className="text-3xl font-bold text-slate-900">Shopping Cart</h1>
            <p className="text-slate-600">{cartItems.length} item(s) in your cart</p>
          </div>
          <Link to="/products" className="text-teal-600 hover:text-teal-700 flex items-center gap-2">
            <ArrowLeft className="w-4 h-4" /> Continue Shopping
          </Link>
        </div>

        <div className="grid lg:grid-cols-3 gap-8">
          {/* Cart Items */}
          <div className="lg:col-span-2 space-y-4">
            {cartItems.map((item) => (
              <div 
                key={item.id}
                className="bg-white rounded-xl shadow-sm border border-slate-100 p-6 flex gap-6"
              >
                {/* Image */}
                <div className="w-32 h-32 bg-slate-100 rounded-lg overflow-hidden flex-shrink-0">
                  {item.image ? (
                    <img 
                      src={item.image} 
                      alt={item.name}
                      className="w-full h-full object-cover"
                    />
                  ) : (
                    <div className="w-full h-full flex items-center justify-center text-slate-400">
                      <ShoppingCart className="w-12 h-12" />
                    </div>
                  )}
                </div>

                {/* Details */}
                <div className="flex-1">
                  <div className="flex justify-between">
                    <div>
                      <h3 className="text-lg font-semibold text-slate-900">{item.name}</h3>
                      {item.glassType && (
                        <p className="text-sm text-slate-500">Type: {item.glassType}</p>
                      )}
                      {item.dimensions && (
                        <p className="text-sm text-slate-500">Size: {item.dimensions}</p>
                      )}
                      {item.thickness && (
                        <p className="text-sm text-slate-500">Thickness: {item.thickness}mm</p>
                      )}
                    </div>
                    <button
                      onClick={() => removeFromCart(item.id)}
                      className="p-2 text-slate-400 hover:text-red-500 transition-colors h-fit"
                    >
                      <Trash2 className="w-5 h-5" />
                    </button>
                  </div>

                  <div className="flex items-center justify-between mt-4">
                    {/* Quantity */}
                    <div className="flex items-center gap-3 bg-slate-100 rounded-lg p-1">
                      <button
                        onClick={() => updateQuantity(item.id, (item.quantity || 1) - 1)}
                        className="w-8 h-8 rounded-lg bg-white shadow-sm hover:bg-slate-50 flex items-center justify-center transition-colors"
                      >
                        <Minus className="w-4 h-4" />
                      </button>
                      <span className="w-12 text-center font-semibold">{item.quantity || 1}</span>
                      <button
                        onClick={() => updateQuantity(item.id, (item.quantity || 1) + 1)}
                        className="w-8 h-8 rounded-lg bg-white shadow-sm hover:bg-slate-50 flex items-center justify-center transition-colors"
                      >
                        <Plus className="w-4 h-4" />
                      </button>
                    </div>

                    {/* Price */}
                    <p className="text-xl font-bold text-teal-600">
                      ₹{((item.totalPrice || item.basePrice || 0) * (item.quantity || 1)).toLocaleString()}
                    </p>
                  </div>
                </div>
              </div>
            ))}

            {/* Clear Cart */}
            <div className="text-right">
              <Button 
                variant="outline"
                onClick={clearCart}
                className="text-red-500 border-red-200 hover:bg-red-50"
              >
                <Trash2 className="w-4 h-4 mr-2" /> Clear Cart
              </Button>
            </div>
          </div>

          {/* Order Summary */}
          <div className="lg:col-span-1">
            <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6 sticky top-24">
              <h2 className="text-lg font-bold text-slate-900 mb-4">Order Summary</h2>
              
              <div className="space-y-3 mb-6">
                <div className="flex justify-between text-slate-600">
                  <span>Subtotal ({cartItems.length} items)</span>
                  <span>₹{getCartTotal().toLocaleString()}</span>
                </div>
                <div className="flex justify-between text-slate-600">
                  <span>Shipping</span>
                  <span className="text-emerald-600">Calculated at checkout</span>
                </div>
                <div className="flex justify-between text-slate-600">
                  <span>GST (18%)</span>
                  <span>₹{(getCartTotal() * 0.18).toLocaleString()}</span>
                </div>
                <div className="border-t pt-3">
                  <div className="flex justify-between text-lg font-bold text-slate-900">
                    <span>Estimated Total</span>
                    <span>₹{(getCartTotal() * 1.18).toLocaleString()}</span>
                  </div>
                </div>
              </div>

              <Button 
                onClick={handleCheckout}
                className="w-full bg-teal-600 hover:bg-teal-700 text-white py-4 text-lg"
              >
                Proceed to Checkout <ArrowRight className="w-5 h-5 ml-2" />
              </Button>

              <p className="text-xs text-slate-500 text-center mt-4">
                Secure checkout powered by Razorpay
              </p>

              {/* Trust Badges */}
              <div className="mt-6 pt-6 border-t">
                <div className="grid grid-cols-2 gap-4 text-center">
                  <div>
                    <p className="text-2xl font-bold text-teal-600">🛡️</p>
                    <p className="text-xs text-slate-600">Secure Payment</p>
                  </div>
                  <div>
                    <p className="text-2xl font-bold text-teal-600">🚚</p>
                    <p className="text-xs text-slate-600">Fast Delivery</p>
                  </div>
                  <div>
                    <p className="text-2xl font-bold text-teal-600">✓</p>
                    <p className="text-xs text-slate-600">Quality Assured</p>
                  </div>
                  <div>
                    <p className="text-2xl font-bold text-teal-600">💬</p>
                    <p className="text-xs text-slate-600">24/7 Support</p>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Cart;
