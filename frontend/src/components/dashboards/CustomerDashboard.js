import React, { useState, useEffect } from 'react';
import { useAuth } from '../../contexts/AuthContext';
import { useNavigate, Link } from 'react-router-dom';
import { Card, CardContent, CardHeader, CardTitle } from '../ui/card';
import { Button } from '../ui/button';
import { 
  Package, User, FileText, Download, RotateCw, Award, 
  ShoppingCart, Plus, Settings, Phone, Mail, MapPin, Edit2, Save, X,
  Building, Calendar, Eye, CreditCard, Gift, Users, Copy, Share2, Loader2
} from 'lucide-react';
import { toast } from 'sonner';
import api from '../../utils/api';
import { erpApi } from '../../utils/erpApi';

const API_URL = process.env.REACT_APP_BACKEND_URL;

const CustomerDashboard = () => {
  const { user, loading: authLoading } = useAuth();
  const navigate = useNavigate();
  const [orders, setOrders] = useState([]);
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState('orders');
  const [editingProfile, setEditingProfile] = useState(false);
  const [profileData, setProfileData] = useState({
    name: '',
    email: '',
    phone: '',
    company_name: '',
    gst_number: '',
    address: '',
    city: '',
    state: '',
    pincode: ''
  });
  const [savingProfile, setSavingProfile] = useState(false);
  
  // Rewards state
  const [rewardsData, setRewardsData] = useState(null);
  const [referralData, setReferralData] = useState(null);
  const [loadingRewards, setLoadingRewards] = useState(false);

  useEffect(() => {
    if (!authLoading) {
      if (!user) {
        navigate('/login');
      } else {
        fetchOrders();
        fetchRewardsData();
        // Initialize profile data from user
        setProfileData({
          name: user.name || '',
          email: user.email || '',
          phone: user.phone || '',
          company_name: user.company_name || '',
          gst_number: user.gst_number || '',
          address: user.address || '',
          city: user.city || '',
          state: user.state || '',
          pincode: user.pincode || ''
        });
      }
    }
  }, [user, authLoading, navigate]);

  const fetchOrders = async () => {
    try {
      const response = await api.orders.getMyOrders();
      setOrders(response.data);
    } catch (error) {
      console.error('Failed to fetch orders:', error);
    } finally {
      setLoading(false);
    }
  };

  const fetchRewardsData = async () => {
    setLoadingRewards(true);
    try {
      const [balanceRes, referralRes] = await Promise.all([
        erpApi.rewards.getMyBalance(),
        erpApi.rewards.getMyReferralCode()
      ]);
      setRewardsData(balanceRes.data);
      setReferralData(referralRes.data);
    } catch (error) {
      console.error('Failed to fetch rewards:', error);
    } finally {
      setLoadingRewards(false);
    }
  };

  const copyReferralCode = () => {
    if (referralData?.referral_code) {
      navigator.clipboard.writeText(referralData.referral_code);
      toast.success('Referral code copied!');
    }
  };

  const shareReferralLink = () => {
    if (referralData?.referral_link) {
      if (navigator.share) {
        navigator.share({
          title: 'Join Lucumaa Glass',
          text: `Use my referral code ${referralData.referral_code} to get 10% off your first order!`,
          url: referralData.referral_link
        });
      } else {
        navigator.clipboard.writeText(referralData.referral_link);
        toast.success('Referral link copied!');
      }
    }
  };

  const handleRepeatOrder = (order) => {
    // Store order details in sessionStorage to pre-fill customize form
    sessionStorage.setItem('repeatOrder', JSON.stringify({
      product_id: order.product_id,
      thickness: order.thickness,
      width: order.width,
      height: order.height,
      quantity: order.quantity
    }));
    toast.success('Redirecting with previous specifications...');
    navigate('/customize');
  };

  const downloadInvoice = async (orderId) => {
    try {
      const token = localStorage.getItem('token');
      window.open(`${API_URL}/api/pdf/invoice/${orderId}?token=${token}`, '_blank');
      toast.success('Invoice download started');
    } catch (error) {
      toast.error('Failed to download invoice');
    }
  };

  const payRemainingAmount = async (order) => {
    try {
      const token = localStorage.getItem('token');
      const response = await fetch(`${API_URL}/api/orders/${order.id}/pay-remaining`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`
        }
      });
      
      const data = await response.json();
      if (!response.ok) throw new Error(data.detail || 'Failed to initiate payment');
      
      const options = {
        key: process.env.REACT_APP_RAZORPAY_KEY_ID,
        amount: data.amount * 100,
        currency: 'INR',
        order_id: data.razorpay_order_id,
        name: 'Lucumaa Glass',
        description: `Remaining Payment - Order #${order.order_number}`,
        handler: async (razorpayResponse) => {
          try {
            await fetch(`${API_URL}/api/orders/${order.id}/verify-remaining`, {
              method: 'POST',
              headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${token}`
              },
              body: JSON.stringify({
                razorpay_payment_id: razorpayResponse.razorpay_payment_id,
                razorpay_signature: razorpayResponse.razorpay_signature
              })
            });
            toast.success('Payment successful!');
            fetchOrders();
          } catch (err) {
            toast.error('Payment verification failed');
          }
        },
        prefill: {
          name: user?.name,
          email: user?.email,
          contact: user?.phone
        },
        theme: { color: '#0d9488' }
      };
      
      const razorpay = new window.Razorpay(options);
      razorpay.open();
    } catch (error) {
      toast.error(error.message || 'Failed to initiate payment');
    }
  };

  const downloadWarranty = (orderId) => {
    toast.success(`Downloading warranty certificate for order #${orderId.slice(0, 8)}...`);
  };

  const handleSaveProfile = async () => {
    setSavingProfile(true);
    try {
      const token = localStorage.getItem('token');
      const response = await fetch(`${API_URL}/api/users/profile`, {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify(profileData)
      });
      
      if (response.ok) {
        toast.success('Profile updated successfully!');
        setEditingProfile(false);
      } else {
        throw new Error('Failed to update profile');
      }
    } catch (error) {
      toast.error('Failed to update profile');
    } finally {
      setSavingProfile(false);
    }
  };

  const viewOrderDetails = (orderId) => {
    navigate(`/track?order=${orderId}`);
  };

  if (authLoading || loading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-xl text-slate-600">Loading...</div>
      </div>
    );
  }

  return (
    <div className="min-h-screen py-20 bg-slate-50" data-testid="customer-dashboard">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        {/* Header with Create Order Button */}
        <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4 mb-8">
          <div>
            <h1 className="text-3xl font-bold text-slate-900 mb-2">Welcome, {user?.name}!</h1>
            <p className="text-slate-600">Manage your orders, invoices, and profile</p>
          </div>
          <Link to="/customize">
            <Button className="bg-teal-600 hover:bg-teal-700 gap-2" data-testid="create-new-order-btn">
              <Plus className="w-5 h-5" />
              Create New Order
            </Button>
          </Link>
        </div>

        {/* Stats Cards */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 md:gap-6 mb-8">
          <Card>
            <CardContent className="p-4 md:p-6">
              <div className="flex items-center gap-3 md:gap-4">
                <div className="w-10 h-10 md:w-12 md:h-12 bg-teal-100 rounded-lg flex items-center justify-center">
                  <Package className="w-5 h-5 md:w-6 md:h-6 text-teal-700" />
                </div>
                <div>
                  <p className="text-xs md:text-sm text-slate-600">Total Orders</p>
                  <p className="text-xl md:text-2xl font-bold text-slate-900">{orders.length}</p>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardContent className="p-4 md:p-6">
              <div className="flex items-center gap-3 md:gap-4">
                <div className="w-10 h-10 md:w-12 md:h-12 bg-blue-100 rounded-lg flex items-center justify-center">
                  <FileText className="w-5 h-5 md:w-6 md:h-6 text-blue-700" />
                </div>
                <div>
                  <p className="text-xs md:text-sm text-slate-600">Active</p>
                  <p className="text-xl md:text-2xl font-bold text-slate-900">
                    {orders.filter(o => !['delivered', 'cancelled'].includes(o.status)).length}
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardContent className="p-4 md:p-6">
              <div className="flex items-center gap-3 md:gap-4">
                <div className="w-10 h-10 md:w-12 md:h-12 bg-green-100 rounded-lg flex items-center justify-center">
                  <Award className="w-5 h-5 md:w-6 md:h-6 text-green-700" />
                </div>
                <div>
                  <p className="text-xs md:text-sm text-slate-600">Delivered</p>
                  <p className="text-xl md:text-2xl font-bold text-slate-900">
                    {orders.filter(o => o.status === 'delivered').length}
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardContent className="p-4 md:p-6">
              <div className="flex items-center gap-3 md:gap-4">
                <div className="w-10 h-10 md:w-12 md:h-12 bg-purple-100 rounded-lg flex items-center justify-center">
                  <User className="w-5 h-5 md:w-6 md:h-6 text-purple-700" />
                </div>
                <div>
                  <p className="text-xs md:text-sm text-slate-600">Account</p>
                  <p className="text-lg md:text-xl font-bold text-slate-900 capitalize">{user?.role}</p>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Quick Actions - Mobile Friendly */}
        <div className="md:hidden mb-6">
          <Card>
            <CardContent className="p-4">
              <div className="grid grid-cols-2 gap-3">
                <Link to="/customize">
                  <Button variant="outline" className="w-full gap-2 h-auto py-3">
                    <ShoppingCart className="w-4 h-4" />
                    <span className="text-xs">New Order</span>
                  </Button>
                </Link>
                <Link to="/track">
                  <Button variant="outline" className="w-full gap-2 h-auto py-3">
                    <Eye className="w-4 h-4" />
                    <span className="text-xs">Track Order</span>
                  </Button>
                </Link>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Main Content Card */}
        <Card>
          <CardContent className="p-0">
            {/* Tabs */}
            <div className="border-b border-slate-200 overflow-x-auto">
              <div className="flex gap-1 md:gap-4 px-4 md:px-6 min-w-max">
                <button
                  onClick={() => setActiveTab('orders')}
                  className={`py-3 md:py-4 px-3 md:px-4 border-b-2 font-medium transition-colors whitespace-nowrap text-sm md:text-base ${
                    activeTab === 'orders'
                      ? 'border-teal-600 text-teal-600'
                      : 'border-transparent text-slate-600 hover:text-slate-900'
                  }`}
                  data-testid="tab-orders"
                >
                  <Package className="w-4 h-4 inline mr-1 md:mr-2" />
                  Orders
                </button>
                <button
                  onClick={() => setActiveTab('invoices')}
                  className={`py-3 md:py-4 px-3 md:px-4 border-b-2 font-medium transition-colors whitespace-nowrap text-sm md:text-base ${
                    activeTab === 'invoices'
                      ? 'border-teal-600 text-teal-600'
                      : 'border-transparent text-slate-600 hover:text-slate-900'
                  }`}
                  data-testid="tab-invoices"
                >
                  <FileText className="w-4 h-4 inline mr-1 md:mr-2" />
                  Invoices
                </button>
                <button
                  onClick={() => setActiveTab('warranties')}
                  className={`py-3 md:py-4 px-3 md:px-4 border-b-2 font-medium transition-colors whitespace-nowrap text-sm md:text-base ${
                    activeTab === 'warranties'
                      ? 'border-teal-600 text-teal-600'
                      : 'border-transparent text-slate-600 hover:text-slate-900'
                  }`}
                  data-testid="tab-warranties"
                >
                  <Award className="w-4 h-4 inline mr-1 md:mr-2" />
                  Warranty
                </button>
                <button
                  onClick={() => setActiveTab('profile')}
                  className={`py-3 md:py-4 px-3 md:px-4 border-b-2 font-medium transition-colors whitespace-nowrap text-sm md:text-base ${
                    activeTab === 'profile'
                      ? 'border-teal-600 text-teal-600'
                      : 'border-transparent text-slate-600 hover:text-slate-900'
                  }`}
                  data-testid="tab-profile"
                >
                  <User className="w-4 h-4 inline mr-1 md:mr-2" />
                  Profile
                </button>
                <button
                  onClick={() => setActiveTab('rewards')}
                  className={`py-3 md:py-4 px-3 md:px-4 border-b-2 font-medium transition-colors whitespace-nowrap text-sm md:text-base ${
                    activeTab === 'rewards'
                      ? 'border-purple-600 text-purple-600'
                      : 'border-transparent text-slate-600 hover:text-slate-900'
                  }`}
                  data-testid="tab-rewards"
                >
                  <Gift className="w-4 h-4 inline mr-1 md:mr-2" />
                  Rewards
                  {rewardsData?.total_available > 0 && (
                    <span className="ml-1 px-1.5 py-0.5 bg-purple-100 text-purple-700 text-xs rounded-full">
                      ₹{rewardsData.total_available}
                    </span>
                  )}
                </button>
              </div>
            </div>

            <div className="p-4 md:p-6">
              {/* Orders Tab */}
              {activeTab === 'orders' && (
                <div>
                  <div className="flex items-center justify-between mb-6">
                    <h2 className="text-lg md:text-xl font-bold text-slate-900">Order History</h2>
                    <Link to="/customize" className="hidden md:block">
                      <Button size="sm" className="bg-teal-600 hover:bg-teal-700 gap-2">
                        <Plus className="w-4 h-4" />
                        New Order
                      </Button>
                    </Link>
                  </div>
                  
                  {orders.length === 0 ? (
                    <div className="text-center py-12">
                      <ShoppingCart className="w-16 h-16 text-slate-300 mx-auto mb-4" />
                      <p className="text-slate-600 mb-4">No orders yet. Start your first order!</p>
                      <Link to="/customize">
                        <Button className="bg-teal-600 hover:bg-teal-700">
                          Create Your First Order
                        </Button>
                      </Link>
                    </div>
                  ) : (
                    <>
                      {/* Desktop Table */}
                      <div className="hidden md:block overflow-x-auto">
                        <table className="w-full">
                          <thead>
                            <tr className="border-b border-slate-200">
                              <th className="text-left py-3 px-4 text-slate-700 font-medium">Order ID</th>
                              <th className="text-left py-3 px-4 text-slate-700 font-medium">Product</th>
                              <th className="text-left py-3 px-4 text-slate-700 font-medium">Size</th>
                              <th className="text-left py-3 px-4 text-slate-700 font-medium">Qty</th>
                              <th className="text-left py-3 px-4 text-slate-700 font-medium">Amount</th>
                              <th className="text-left py-3 px-4 text-slate-700 font-medium">Status</th>
                              <th className="text-left py-3 px-4 text-slate-700 font-medium">Date</th>
                              <th className="text-left py-3 px-4 text-slate-700 font-medium">Actions</th>
                            </tr>
                          </thead>
                          <tbody>
                            {orders.map((order) => (
                              <tr key={order.id} className="border-b border-slate-100 hover:bg-slate-50">
                                <td className="py-3 px-4 font-mono text-sm">{order.id.slice(0, 8)}</td>
                                <td className="py-3 px-4">{order.product_name}</td>
                                <td className="py-3 px-4 text-sm text-slate-600">
                                  {order.width && order.height ? `${order.width}x${order.height}mm` : '-'}
                                  {order.thickness && ` • ${order.thickness}mm`}
                                </td>
                                <td className="py-3 px-4">{order.quantity}</td>
                                <td className="py-3 px-4 font-medium">₹{order.total_price?.toLocaleString()}</td>
                                <td className="py-3 px-4">
                                  <span className={`px-3 py-1 rounded-full text-xs font-medium ${
                                    order.status === 'delivered' ? 'bg-green-100 text-green-700' :
                                    order.status === 'confirmed' ? 'bg-blue-100 text-blue-700' :
                                    order.status === 'processing' ? 'bg-purple-100 text-purple-700' :
                                    order.status === 'pending' ? 'bg-yellow-100 text-yellow-700' :
                                    order.status === 'dispatched' ? 'bg-indigo-100 text-indigo-700' :
                                    'bg-slate-100 text-slate-700'
                                  }`}>
                                    {order.status}
                                  </span>
                                  {order.remaining_amount > 0 && order.payment_status !== 'completed' && (
                                    <span className="ml-2 px-2 py-1 rounded text-xs bg-amber-100 text-amber-700">
                                      Due: ₹{order.remaining_amount?.toLocaleString()}
                                    </span>
                                  )}
                                </td>
                                <td className="py-3 px-4 text-sm text-slate-600">
                                  {new Date(order.created_at).toLocaleDateString()}
                                </td>
                                <td className="py-3 px-4">
                                  <div className="flex items-center gap-2">
                                    <Button
                                      onClick={() => viewOrderDetails(order.id)}
                                      variant="ghost"
                                      size="sm"
                                      className="text-slate-600 hover:text-slate-900"
                                    >
                                      <Eye className="w-4 h-4" />
                                    </Button>
                                    {order.remaining_amount > 0 && order.payment_status !== 'completed' && (
                                      <Button
                                        onClick={() => payRemainingAmount(order)}
                                        size="sm"
                                        className="bg-amber-500 hover:bg-amber-600 text-white"
                                        data-testid={`pay-remaining-${order.id}`}
                                      >
                                        <CreditCard className="w-4 h-4 mr-1" />
                                        Pay ₹{order.remaining_amount?.toLocaleString()}
                                      </Button>
                                    )}
                                    <Button
                                      onClick={() => handleRepeatOrder(order)}
                                      variant="ghost"
                                      size="sm"
                                      className="text-teal-600 hover:text-teal-700"
                                      data-testid={`repeat-order-${order.id}`}
                                    >
                                      <RotateCw className="w-4 h-4 mr-1" />
                                      Repeat
                                    </Button>
                                  </div>
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>

                      {/* Mobile Cards */}
                      <div className="md:hidden space-y-4">
                        {orders.map((order) => (
                          <Card key={order.id} className="border-slate-200">
                            <CardContent className="p-4">
                              <div className="flex justify-between items-start mb-3">
                                <div>
                                  <p className="font-mono text-sm text-slate-500">#{order.id.slice(0, 8)}</p>
                                  <h3 className="font-semibold text-slate-900">{order.product_name}</h3>
                                </div>
                                <span className={`px-2 py-1 rounded-full text-xs font-medium ${
                                  order.status === 'delivered' ? 'bg-green-100 text-green-700' :
                                  order.status === 'confirmed' ? 'bg-blue-100 text-blue-700' :
                                  order.status === 'processing' ? 'bg-purple-100 text-purple-700' :
                                  'bg-yellow-100 text-yellow-700'
                                }`}>
                                  {order.status}
                                </span>
                              </div>
                              <div className="grid grid-cols-2 gap-2 text-sm mb-3">
                                <div>
                                  <span className="text-slate-500">Size:</span>
                                  <span className="ml-1">{order.width}x{order.height}mm</span>
                                </div>
                                <div>
                                  <span className="text-slate-500">Qty:</span>
                                  <span className="ml-1">{order.quantity}</span>
                                </div>
                                <div>
                                  <span className="text-slate-500">Amount:</span>
                                  <span className="ml-1 font-medium">₹{order.total_price?.toLocaleString()}</span>
                                </div>
                                <div>
                                  <span className="text-slate-500">Date:</span>
                                  <span className="ml-1">{new Date(order.created_at).toLocaleDateString()}</span>
                                </div>
                              </div>
                              <div className="flex gap-2">
                                <Button
                                  onClick={() => viewOrderDetails(order.id)}
                                  variant="outline"
                                  size="sm"
                                  className="flex-1"
                                >
                                  <Eye className="w-4 h-4 mr-1" />
                                  View
                                </Button>
                                <Button
                                  onClick={() => handleRepeatOrder(order)}
                                  size="sm"
                                  className="flex-1 bg-teal-600 hover:bg-teal-700"
                                >
                                  <RotateCw className="w-4 h-4 mr-1" />
                                  Repeat
                                </Button>
                              </div>
                            </CardContent>
                          </Card>
                        ))}
                      </div>
                    </>
                  )}
                </div>
              )}

              {/* Invoices Tab */}
              {activeTab === 'invoices' && (
                <div>
                  <h2 className="text-lg md:text-xl font-bold text-slate-900 mb-6">Invoices & GST Documents</h2>
                  {orders.filter(o => o.payment_status === 'completed' || o.status === 'delivered').length === 0 ? (
                    <div className="text-center py-12">
                      <FileText className="w-16 h-16 text-slate-300 mx-auto mb-4" />
                      <p className="text-slate-600">No invoices available yet</p>
                    </div>
                  ) : (
                    <div className="space-y-4">
                      {orders.filter(o => o.payment_status === 'completed' || o.status === 'delivered').map((order) => (
                        <Card key={order.id} className="border-slate-200">
                          <CardContent className="p-4 md:p-6 flex flex-col md:flex-row md:items-center justify-between gap-4">
                            <div>
                              <h3 className="font-semibold text-slate-900 mb-1">
                                Invoice #{order.id.slice(0, 8).toUpperCase()}
                              </h3>
                              <p className="text-sm text-slate-600">
                                {order.product_name} - ₹{order.total_price?.toLocaleString()} (incl. 18% GST)
                              </p>
                              <p className="text-xs text-slate-500 mt-1">
                                Date: {new Date(order.created_at).toLocaleDateString()}
                              </p>
                            </div>
                            <Button
                              onClick={() => downloadInvoice(order.id)}
                              variant="outline"
                              className="gap-2 w-full md:w-auto"
                              data-testid={`download-invoice-${order.id}`}
                            >
                              <Download className="w-4 h-4" />
                              Download PDF
                            </Button>
                          </CardContent>
                        </Card>
                      ))}
                    </div>
                  )}
                </div>
              )}

              {/* Warranties Tab */}
              {activeTab === 'warranties' && (
                <div>
                  <h2 className="text-lg md:text-xl font-bold text-slate-900 mb-6">Warranty Certificates</h2>
                  {orders.filter(o => o.status === 'delivered').length === 0 ? (
                    <div className="text-center py-12">
                      <Award className="w-16 h-16 text-slate-300 mx-auto mb-4" />
                      <p className="text-slate-600">No warranty certificates available yet</p>
                      <p className="text-sm text-slate-500 mt-2">Warranties are issued after delivery</p>
                    </div>
                  ) : (
                    <div className="space-y-4">
                      {orders.filter(o => o.status === 'delivered').map((order) => (
                        <Card key={order.id} className="border-slate-200">
                          <CardContent className="p-4 md:p-6 flex flex-col md:flex-row md:items-center justify-between gap-4">
                            <div>
                              <h3 className="font-semibold text-slate-900 mb-1">
                                {order.product_name}
                              </h3>
                              <p className="text-sm text-slate-600">
                                Warranty Period: 5 Years (Manufacturing Defects)
                              </p>
                              <p className="text-xs text-slate-500 mt-1">
                                Valid until: {new Date(new Date(order.created_at).setFullYear(new Date(order.created_at).getFullYear() + 5)).toLocaleDateString()}
                              </p>
                            </div>
                            <Button
                              onClick={() => downloadWarranty(order.id)}
                              variant="outline"
                              className="gap-2 w-full md:w-auto"
                              data-testid={`download-warranty-${order.id}`}
                            >
                              <Download className="w-4 h-4" />
                              Download Certificate
                            </Button>
                          </CardContent>
                        </Card>
                      ))}
                    </div>
                  )}
                </div>
              )}

              {/* Profile Tab */}
              {activeTab === 'profile' && (
                <div>
                  <div className="flex items-center justify-between mb-6">
                    <h2 className="text-lg md:text-xl font-bold text-slate-900">My Profile</h2>
                    {!editingProfile ? (
                      <Button
                        onClick={() => setEditingProfile(true)}
                        variant="outline"
                        className="gap-2"
                        data-testid="edit-profile-btn"
                      >
                        <Edit2 className="w-4 h-4" />
                        Edit Profile
                      </Button>
                    ) : (
                      <div className="flex gap-2">
                        <Button
                          onClick={() => setEditingProfile(false)}
                          variant="outline"
                          size="sm"
                        >
                          <X className="w-4 h-4" />
                        </Button>
                        <Button
                          onClick={handleSaveProfile}
                          className="bg-teal-600 hover:bg-teal-700 gap-2"
                          size="sm"
                          disabled={savingProfile}
                          data-testid="save-profile-btn"
                        >
                          <Save className="w-4 h-4" />
                          Save
                        </Button>
                      </div>
                    )}
                  </div>

                  <div className="grid md:grid-cols-2 gap-6">
                    {/* Personal Info */}
                    <Card>
                      <CardHeader>
                        <CardTitle className="text-base flex items-center gap-2">
                          <User className="w-4 h-4 text-teal-600" />
                          Personal Information
                        </CardTitle>
                      </CardHeader>
                      <CardContent className="space-y-4">
                        <div>
                          <label className="block text-sm font-medium text-slate-700 mb-1">Full Name</label>
                          {editingProfile ? (
                            <input
                              type="text"
                              value={profileData.name}
                              onChange={(e) => setProfileData({ ...profileData, name: e.target.value })}
                              className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-teal-500 focus:border-teal-500"
                            />
                          ) : (
                            <p className="text-slate-900">{profileData.name || '-'}</p>
                          )}
                        </div>
                        <div>
                          <label className="block text-sm font-medium text-slate-700 mb-1">Email</label>
                          <p className="text-slate-900 flex items-center gap-2">
                            <Mail className="w-4 h-4 text-slate-400" />
                            {profileData.email || '-'}
                          </p>
                        </div>
                        <div>
                          <label className="block text-sm font-medium text-slate-700 mb-1">Phone</label>
                          {editingProfile ? (
                            <input
                              type="tel"
                              value={profileData.phone}
                              onChange={(e) => setProfileData({ ...profileData, phone: e.target.value })}
                              className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-teal-500 focus:border-teal-500"
                            />
                          ) : (
                            <p className="text-slate-900 flex items-center gap-2">
                              <Phone className="w-4 h-4 text-slate-400" />
                              {profileData.phone || '-'}
                            </p>
                          )}
                        </div>
                      </CardContent>
                    </Card>

                    {/* Business Info */}
                    <Card>
                      <CardHeader>
                        <CardTitle className="text-base flex items-center gap-2">
                          <Building className="w-4 h-4 text-teal-600" />
                          Business Information
                        </CardTitle>
                      </CardHeader>
                      <CardContent className="space-y-4">
                        <div>
                          <label className="block text-sm font-medium text-slate-700 mb-1">Company Name</label>
                          {editingProfile ? (
                            <input
                              type="text"
                              value={profileData.company_name}
                              onChange={(e) => setProfileData({ ...profileData, company_name: e.target.value })}
                              className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-teal-500 focus:border-teal-500"
                            />
                          ) : (
                            <p className="text-slate-900">{profileData.company_name || '-'}</p>
                          )}
                        </div>
                        <div>
                          <label className="block text-sm font-medium text-slate-700 mb-1">GST Number</label>
                          {editingProfile ? (
                            <input
                              type="text"
                              value={profileData.gst_number}
                              onChange={(e) => setProfileData({ ...profileData, gst_number: e.target.value })}
                              className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-teal-500 focus:border-teal-500"
                              placeholder="e.g., 27AAPFU0939F1ZV"
                            />
                          ) : (
                            <p className="text-slate-900">{profileData.gst_number || '-'}</p>
                          )}
                        </div>
                      </CardContent>
                    </Card>

                    {/* Address */}
                    <Card className="md:col-span-2">
                      <CardHeader>
                        <CardTitle className="text-base flex items-center gap-2">
                          <MapPin className="w-4 h-4 text-teal-600" />
                          Address
                        </CardTitle>
                      </CardHeader>
                      <CardContent>
                        {editingProfile ? (
                          <div className="grid md:grid-cols-2 gap-4">
                            <div className="md:col-span-2">
                              <label className="block text-sm font-medium text-slate-700 mb-1">Street Address</label>
                              <input
                                type="text"
                                value={profileData.address}
                                onChange={(e) => setProfileData({ ...profileData, address: e.target.value })}
                                className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-teal-500 focus:border-teal-500"
                              />
                            </div>
                            <div>
                              <label className="block text-sm font-medium text-slate-700 mb-1">City</label>
                              <input
                                type="text"
                                value={profileData.city}
                                onChange={(e) => setProfileData({ ...profileData, city: e.target.value })}
                                className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-teal-500 focus:border-teal-500"
                              />
                            </div>
                            <div>
                              <label className="block text-sm font-medium text-slate-700 mb-1">State</label>
                              <input
                                type="text"
                                value={profileData.state}
                                onChange={(e) => setProfileData({ ...profileData, state: e.target.value })}
                                className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-teal-500 focus:border-teal-500"
                              />
                            </div>
                            <div>
                              <label className="block text-sm font-medium text-slate-700 mb-1">Pincode</label>
                              <input
                                type="text"
                                value={profileData.pincode}
                                onChange={(e) => setProfileData({ ...profileData, pincode: e.target.value })}
                                className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-teal-500 focus:border-teal-500"
                              />
                            </div>
                          </div>
                        ) : (
                          <p className="text-slate-900">
                            {profileData.address ? (
                              <>
                                {profileData.address}
                                {profileData.city && `, ${profileData.city}`}
                                {profileData.state && `, ${profileData.state}`}
                                {profileData.pincode && ` - ${profileData.pincode}`}
                              </>
                            ) : (
                              '-'
                            )}
                          </p>
                        )}
                      </CardContent>
                    </Card>

                    {/* Account Info */}
                    <Card className="md:col-span-2">
                      <CardHeader>
                        <CardTitle className="text-base flex items-center gap-2">
                          <Settings className="w-4 h-4 text-teal-600" />
                          Account Information
                        </CardTitle>
                      </CardHeader>
                      <CardContent>
                        <div className="grid md:grid-cols-3 gap-4">
                          <div>
                            <label className="block text-sm font-medium text-slate-700 mb-1">Account Type</label>
                            <p className="text-slate-900 capitalize">{user?.role}</p>
                          </div>
                          <div>
                            <label className="block text-sm font-medium text-slate-700 mb-1">Member Since</label>
                            <p className="text-slate-900 flex items-center gap-2">
                              <Calendar className="w-4 h-4 text-slate-400" />
                              {user?.created_at ? new Date(user.created_at).toLocaleDateString() : 'N/A'}
                            </p>
                          </div>
                          <div>
                            <label className="block text-sm font-medium text-slate-700 mb-1">Total Orders</label>
                            <p className="text-slate-900">{orders.length}</p>
                          </div>
                        </div>
                      </CardContent>
                    </Card>
                  </div>
                </div>
              )}

              {/* Rewards Tab */}
              {activeTab === 'rewards' && (
                <div>
                  <h2 className="text-lg md:text-xl font-bold text-slate-900 mb-6">Rewards & Referrals</h2>
                  
                  {loadingRewards ? (
                    <div className="flex items-center justify-center py-12">
                      <Loader2 className="w-8 h-8 animate-spin text-purple-600" />
                    </div>
                  ) : (
                    <div className="grid md:grid-cols-2 gap-6">
                      {/* Rewards Balance */}
                      <Card>
                        <CardHeader>
                          <CardTitle className="text-base flex items-center gap-2">
                            <Gift className="w-4 h-4 text-purple-600" />
                            Rewards Balance
                          </CardTitle>
                        </CardHeader>
                        <CardContent>
                          <div className="text-center py-6">
                            <div className="text-3xl font-bold text-purple-600 mb-2">
                              ₹{rewardsData?.total_available || 0}
                            </div>
                            <p className="text-slate-600 mb-4">Available to redeem</p>
                            {rewardsData?.total_available > 0 && (
                              <Button className="bg-purple-600 hover:bg-purple-700">
                                Redeem Rewards
                              </Button>
                            )}
                          </div>
                          
                          {rewardsData?.transactions && rewardsData.transactions.length > 0 && (
                            <div className="mt-6">
                              <h4 className="font-medium text-slate-900 mb-3">Recent Transactions</h4>
                              <div className="space-y-2">
                                {rewardsData.transactions.slice(0, 3).map((transaction, index) => (
                                  <div key={index} className="flex justify-between items-center py-2 border-b border-slate-100">
                                    <div>
                                      <p className="text-sm font-medium text-slate-900">{transaction.description}</p>
                                      <p className="text-xs text-slate-500">{new Date(transaction.created_at).toLocaleDateString()}</p>
                                    </div>
                                    <span className={`text-sm font-medium ${
                                      transaction.type === 'earned' ? 'text-green-600' : 'text-red-600'
                                    }`}>
                                      {transaction.type === 'earned' ? '+' : '-'}₹{transaction.amount}
                                    </span>
                                  </div>
                                ))}
                              </div>
                            </div>
                          )}
                        </CardContent>
                      </Card>

                      {/* Referral Program */}
                      <Card>
                        <CardHeader>
                          <CardTitle className="text-base flex items-center gap-2">
                            <Users className="w-4 h-4 text-purple-600" />
                            Referral Program
                          </CardTitle>
                        </CardHeader>
                        <CardContent>
                          <div className="space-y-4">
                            <div className="bg-purple-50 p-4 rounded-lg">
                              <h4 className="font-medium text-purple-900 mb-2">Earn ₹500 for each referral!</h4>
                              <p className="text-sm text-purple-700">
                                Share your referral code and earn rewards when friends place their first order.
                              </p>
                            </div>
                            
                            {referralData && (
                              <div className="space-y-3">
                                <div>
                                  <label className="block text-sm font-medium text-slate-700 mb-1">Your Referral Code</label>
                                  <div className="flex gap-2">
                                    <input
                                      type="text"
                                      value={referralData.referral_code}
                                      readOnly
                                      className="flex-1 px-3 py-2 border rounded-lg bg-slate-50 font-mono text-sm"
                                    />
                                    <Button
                                      onClick={copyReferralCode}
                                      variant="outline"
                                      size="sm"
                                      className="gap-1"
                                    >
                                      <Copy className="w-4 h-4" />
                                      Copy
                                    </Button>
                                  </div>
                                </div>
                                
                                <div>
                                  <label className="block text-sm font-medium text-slate-700 mb-1">Share Link</label>
                                  <Button
                                    onClick={shareReferralLink}
                                    variant="outline"
                                    className="w-full gap-2"
                                  >
                                    <Share2 className="w-4 h-4" />
                                    Share Referral Link
                                  </Button>
                                </div>
                                
                                <div className="grid grid-cols-2 gap-4 pt-4">
                                  <div className="text-center">
                                    <div className="text-2xl font-bold text-slate-900">{referralData.total_referrals || 0}</div>
                                    <p className="text-sm text-slate-600">Total Referrals</p>
                                  </div>
                                  <div className="text-center">
                                    <div className="text-2xl font-bold text-purple-600">₹{referralData.total_earned || 0}</div>
                                    <p className="text-sm text-slate-600">Total Earned</p>
                                  </div>
                                </div>
                              </div>
                            )}
                          </div>
                        </CardContent>
                      </Card>
                    </div>
                  )}
                </div>
              )}
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
};

export default CustomerDashboard;
