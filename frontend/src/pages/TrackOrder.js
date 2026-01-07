import React, { useState } from 'react';
import { Card, CardContent } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Package, Search, CheckCircle, Clock, Truck, Box, Shield, Home, MessageCircle } from 'lucide-react';
import { toast } from 'sonner';
import api from '../utils/api';

const TrackOrder = () => {
  const [orderId, setOrderId] = useState('');
  const [mobileNumber, setMobileNumber] = useState('');
  const [orderInfo, setOrderInfo] = useState(null);
  const [loading, setLoading] = useState(false);
  const [showChat, setShowChat] = useState(false);

  const statusStages = {
    'pending': 0,
    'confirmed': 1,
    'production': 2,
    'quality_check': 3,
    'dispatched': 4,
    'delivered': 5
  };

  const stages = [
    { key: 'confirmed', label: 'Order Confirmed', icon: CheckCircle },
    { key: 'production', label: 'In Production', icon: Box },
    { key: 'quality_check', label: 'Quality Check', icon: Shield },
    { key: 'dispatched', label: 'Dispatched', icon: Truck },
    { key: 'delivered', label: 'Delivered', icon: Home }
  ];

  const calculateETA = (status, createdAt) => {
    const created = new Date(createdAt);
    const today = new Date();
    const daysPassed = Math.floor((today - created) / (1000 * 60 * 60 * 24));

    switch (status) {
      case 'confirmed':
        return `7-14 days (Production starts in ${Math.max(1, 2 - daysPassed)} days)`;
      case 'production':
        return `5-10 days (Manufacturing in progress)`;
      case 'quality_check':
        return `2-3 days (Final inspection)`;
      case 'dispatched':
        return `1-3 days (In transit)`;
      case 'delivered':
        return 'Delivered';
      default:
        return 'Awaiting confirmation';
    }
  };

  const handleTrack = async () => {
    if (!orderId.trim() && !mobileNumber.trim()) {
      toast.error('Please enter Order ID or Mobile Number');
      return;
    }

    setLoading(true);
    try {
      const response = await api.orders.track(orderId);
      setOrderInfo(response.data);
    } catch (error) {
      console.error('Failed to track order:', error);
      toast.error('Order not found. Please check your Order ID.');
      setOrderInfo(null);
    } finally {
      setLoading(false);
    }
  };

  const currentStage = orderInfo ? statusStages[orderInfo.status] || 0 : 0;

  return (
    <div className="min-h-screen py-20 bg-slate-50" data-testid="track-order-page">
      <div className="max-w-4xl mx-auto px-4">
        <div className="text-center mb-12">
          <Package className="w-16 h-16 text-primary-700 mx-auto mb-4" />
          <h1 className="text-4xl font-bold text-slate-900 mb-4">Track Your Order</h1>
          <p className="text-lg text-slate-600">Enter your order details to check real-time status</p>
        </div>

        <Card className="mb-8">
          <CardContent className="p-8">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
              <div>
                <label className="block text-sm font-medium text-slate-700 mb-2">Order ID</label>
                <input
                  type="text"
                  value={orderId}
                  onChange={(e) => setOrderId(e.target.value)}
                  className="w-full h-12 rounded-lg border-slate-300 px-4"
                  placeholder="Enter Order ID"
                  data-testid="order-id-input"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-slate-700 mb-2">Mobile Number (Optional)</label>
                <input
                  type="tel"
                  value={mobileNumber}
                  onChange={(e) => setMobileNumber(e.target.value)}
                  className="w-full h-12 rounded-lg border-slate-300 px-4"
                  placeholder="+91 XXXXX XXXXX"
                  data-testid="mobile-input"
                />
              </div>
            </div>
            <Button
              onClick={handleTrack}
              className="w-full bg-primary-700 hover:bg-primary-800 h-12"
              disabled={loading}
              data-testid="track-btn"
            >
              <Search className="mr-2" /> {loading ? 'Tracking...' : 'Track Order'}
            </Button>
          </CardContent>
        </Card>

        {orderInfo && (
          <div className="space-y-6">
            <Card>
              <CardContent className="p-8">
                <div className="flex items-center justify-between mb-6">
                  <div>
                    <h3 className="text-2xl font-bold text-slate-900 mb-1">Order #{orderInfo.order_id.slice(0, 8).toUpperCase()}</h3>
                    <p className="text-slate-600">{orderInfo.product_name}</p>
                  </div>
                  <div className="text-right">
                    <p className="text-sm text-slate-600 mb-1">Estimated Delivery</p>
                    <div className="flex items-center gap-2 text-primary-700 font-semibold">
                      <Clock className="w-5 h-5" />
                      {calculateETA(orderInfo.status, orderInfo.created_at)}
                    </div>
                  </div>
                </div>

                <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6 p-4 bg-slate-50 rounded-lg">
                  <div>
                    <p className="text-sm text-slate-600">Quantity</p>
                    <p className="font-semibold">{orderInfo.quantity || '-'} {orderInfo.unit || 'pcs'}</p>
                  </div>
                  <div>
                    <p className="text-sm text-slate-600">Total Amount</p>
                    <p className="font-semibold">₹{(orderInfo.total_price || 0).toLocaleString()}</p>
                  </div>
                  <div>
                    <p className="text-sm text-slate-600">Payment</p>
                    <p className={`font-semibold ${orderInfo.payment_status === 'paid' || orderInfo.payment_status === 'completed' ? 'text-green-600' : 'text-yellow-600'}`}>
                      {orderInfo.payment_status === 'paid' || orderInfo.payment_status === 'completed' ? 'Paid' : 'Pending'}
                    </p>
                  </div>
                  <div>
                    <p className="text-sm text-slate-600">Order Date</p>
                    <p className="font-semibold">{orderInfo.created_at ? new Date(orderInfo.created_at).toLocaleDateString() : '-'}</p>
                  </div>
                </div>

                <div className="relative">
                  <div className="absolute top-8 left-0 right-0 h-1 bg-slate-200">
                    <div
                      className="h-full bg-primary-600 transition-all duration-500"
                      style={{ width: `${(currentStage / (stages.length - 1)) * 100}%` }}
                    ></div>
                  </div>

                  <div className="relative flex justify-between">
                    {stages.map((stage, index) => {
                      const StageIcon = stage.icon;
                      const isCompleted = statusStages[orderInfo.status] >= statusStages[stage.key];
                      const isCurrent = orderInfo.status === stage.key;

                      return (
                        <div key={stage.key} className="flex flex-col items-center" style={{ width: '80px' }}>
                          <div
                            className={`w-16 h-16 rounded-full flex items-center justify-center mb-3 border-4 transition-all ${
                              isCompleted
                                ? 'bg-primary-600 border-primary-600 text-white'
                                : 'bg-white border-slate-200 text-slate-400'
                            } ${isCurrent ? 'ring-4 ring-primary-200' : ''}`}
                          >
                            <StageIcon className="w-8 h-8" />
                          </div>
                          <p className={`text-xs text-center font-medium ${isCompleted ? 'text-slate-900' : 'text-slate-500'}`}>
                            {stage.label}
                          </p>
                          {isCurrent && (
                            <p className="text-xs text-primary-600 font-semibold mt-1">Current</p>
                          )}
                        </div>
                      );
                    })}
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardContent className="p-6">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <div className="w-12 h-12 bg-primary-100 rounded-full flex items-center justify-center">
                      <MessageCircle className="w-6 h-6 text-primary-700" />
                    </div>
                    <div>
                      <h4 className="font-semibold text-slate-900">Need Help?</h4>
                      <p className="text-sm text-slate-600">Our support team is here to assist you</p>
                    </div>
                  </div>
                  <div className="flex gap-3">
                    <Button
                      onClick={() => setShowChat(!showChat)}
                      variant="outline"
                      data-testid="support-chat-btn"
                    >
                      <MessageCircle className="mr-2 w-4 h-4" />
                      Chat Support
                    </Button>
                    <Button
                      asChild
                      className="bg-green-600 hover:bg-green-700"
                    >
                      <a href="https://wa.me/919284701985" target="_blank" rel="noopener noreferrer">
                        WhatsApp Us
                      </a>
                    </Button>
                  </div>
                </div>

                {showChat && (
                  <div className="mt-6 p-4 bg-slate-50 rounded-lg border border-slate-200">
                    <p className="text-sm text-slate-600 mb-3">
                      For immediate assistance with Order #{orderInfo.order_id.slice(0, 8).toUpperCase()}:
                    </p>
                    <div className="space-y-2 text-sm">
                      <p><strong>Phone:</strong> +91 92847 01985</p>
                      <p><strong>Email:</strong> support@lucumaaglass.com</p>
                      <p><strong>Hours:</strong> Mon-Sat, 9 AM - 6 PM IST</p>
                    </div>
                  </div>
                )}
              </CardContent>
            </Card>
          </div>
        )}
      </div>
    </div>
  );
};

export default TrackOrder;
