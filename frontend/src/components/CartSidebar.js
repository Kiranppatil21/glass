import React from 'react';
import { Link } from 'react-router-dom';
import { ShoppingCart, X, Trash2, Plus, Minus, ArrowRight } from 'lucide-react';
import { useCart } from '../contexts/CartContext';
import { Button } from './ui/button';

const CartSidebar = () => {
  const { 
    cartItems, 
    isCartOpen, 
    setIsCartOpen, 
    removeFromCart, 
    updateQuantity,
    getCartTotal,
    clearCart 
  } = useCart();

  if (!isCartOpen) return null;

  return (
    <>
      {/* Overlay */}
      <div 
        className="fixed inset-0 bg-black/50 z-50"
        onClick={() => setIsCartOpen(false)}
      />
      
      {/* Sidebar */}
      <div className="fixed right-0 top-0 h-full w-full max-w-md bg-white shadow-2xl z-50 flex flex-col" data-testid="cart-sidebar">
        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b bg-slate-50">
          <div className="flex items-center gap-3">
            <ShoppingCart className="w-6 h-6 text-teal-600" />
            <h2 className="text-lg font-bold text-slate-900">Your Cart</h2>
            <span className="bg-teal-600 text-white text-xs px-2 py-0.5 rounded-full">
              {cartItems.length} items
            </span>
          </div>
          <button 
            onClick={() => setIsCartOpen(false)}
            className="p-2 hover:bg-slate-200 rounded-full transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Cart Items */}
        <div className="flex-1 overflow-y-auto p-4">
          {cartItems.length === 0 ? (
            <div className="text-center py-12">
              <ShoppingCart className="w-16 h-16 text-slate-300 mx-auto mb-4" />
              <p className="text-slate-500 mb-4">Your cart is empty</p>
              <Button 
                onClick={() => setIsCartOpen(false)}
                variant="outline"
                className="border-teal-600 text-teal-600"
              >
                Continue Shopping
              </Button>
            </div>
          ) : (
            <div className="space-y-4">
              {cartItems.map((item) => (
                <div 
                  key={item.id}
                  className="flex gap-4 p-3 bg-slate-50 rounded-xl border border-slate-100"
                >
                  {/* Image */}
                  <div className="w-20 h-20 bg-slate-200 rounded-lg overflow-hidden flex-shrink-0">
                    {item.image ? (
                      <img 
                        src={item.image} 
                        alt={item.name}
                        className="w-full h-full object-cover"
                      />
                    ) : (
                      <div className="w-full h-full flex items-center justify-center text-slate-400">
                        <ShoppingCart className="w-8 h-8" />
                      </div>
                    )}
                  </div>

                  {/* Details */}
                  <div className="flex-1 min-w-0">
                    <h3 className="font-medium text-slate-900 truncate">{item.name}</h3>
                    {item.glassType && (
                      <p className="text-xs text-slate-500">{item.glassType}</p>
                    )}
                    {item.dimensions && (
                      <p className="text-xs text-slate-500">{item.dimensions}</p>
                    )}
                    {item.thickness && (
                      <p className="text-xs text-slate-500">{item.thickness}mm thickness</p>
                    )}
                    
                    <div className="flex items-center justify-between mt-2">
                      <p className="font-bold text-teal-600">
                        ₹{(item.totalPrice || item.basePrice || 0).toLocaleString()}
                      </p>
                      
                      {/* Quantity Controls */}
                      <div className="flex items-center gap-2">
                        <button
                          onClick={() => updateQuantity(item.id, (item.quantity || 1) - 1)}
                          className="w-7 h-7 rounded-full bg-slate-200 hover:bg-slate-300 flex items-center justify-center transition-colors"
                        >
                          <Minus className="w-3 h-3" />
                        </button>
                        <span className="w-8 text-center font-medium">{item.quantity || 1}</span>
                        <button
                          onClick={() => updateQuantity(item.id, (item.quantity || 1) + 1)}
                          className="w-7 h-7 rounded-full bg-slate-200 hover:bg-slate-300 flex items-center justify-center transition-colors"
                        >
                          <Plus className="w-3 h-3" />
                        </button>
                      </div>
                    </div>
                  </div>

                  {/* Remove Button */}
                  <button
                    onClick={() => removeFromCart(item.id)}
                    className="p-2 text-slate-400 hover:text-red-500 transition-colors self-start"
                  >
                    <Trash2 className="w-4 h-4" />
                  </button>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Footer */}
        {cartItems.length > 0 && (
          <div className="border-t bg-white p-4 space-y-4">
            {/* Total */}
            <div className="flex items-center justify-between">
              <span className="text-slate-600">Subtotal</span>
              <span className="text-xl font-bold text-slate-900">
                ₹{getCartTotal().toLocaleString()}
              </span>
            </div>
            
            <p className="text-xs text-slate-500">
              Shipping & taxes calculated at checkout
            </p>

            {/* Actions */}
            <div className="space-y-2">
              <Link to="/checkout" onClick={() => setIsCartOpen(false)}>
                <Button className="w-full bg-teal-600 hover:bg-teal-700 text-white py-3">
                  Proceed to Checkout <ArrowRight className="w-4 h-4 ml-2" />
                </Button>
              </Link>
              <div className="flex gap-2">
                <Button 
                  variant="outline" 
                  onClick={() => setIsCartOpen(false)}
                  className="flex-1"
                >
                  Continue Shopping
                </Button>
                <Button 
                  variant="outline"
                  onClick={clearCart}
                  className="text-red-500 border-red-200 hover:bg-red-50"
                >
                  Clear
                </Button>
              </div>
            </div>
          </div>
        )}
      </div>
    </>
  );
};

export default CartSidebar;
