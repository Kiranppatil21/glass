import React, { useState, useEffect } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { 
  Package, Wallet, Gift, User, Clock, CheckCircle, Truck, Factory,
  Copy, RefreshCw, AlertCircle, LogOut, CreditCard, Banknote, 
  Eye, Download, Plus, Hammer, ShieldCheck, ArrowRight, FileText, Share2, Building,
  Edit, X, Save, Loader2, MapPin, Phone, Mail
} from 'lucide-react';
import { toast } from 'sonner';
import { useAuth } from '../contexts/AuthContext';
import axios from 'axios';
import ShareModal from '../components/ShareModal';
import { erpApi } from '../utils/erpApi';

const API_BASE = process.env.REACT_APP_BACKEND_URL;
const COMPANY_LOGO = "https://customer-assets.emergentagent.com/job_0aec802e-e67b-4582-8fac-1517907b7262/artifacts/752tez4i_Logo%20Cucumaa%20Glass.png";

const CustomerPortalEnhanced = () => {
  const { user, logout } = useAuth();
  const navigate = useNavigate();
  const [activeTab, setActiveTab] = useState('orders');
  const [orders, setOrders] = useState([]);
  const [jobWorkOrders, setJobWorkOrders] = useState([]);
  const [allOrders, setAllOrders] = useState([]);
  const [profile, setProfile] = useState(null);
  const [customerMasterProfile, setCustomerMasterProfile] = useState(null);
  const [ledgerSummary, setLedgerSummary] = useState(null);
  const [loading, setLoading] = useState(true);
  const [selectedOrder, setSelectedOrder] = useState(null);
  const [paymentModalOrder, setPaymentModalOrder] = useState(null);
  const [paymentMethod, setPaymentMethod] = useState(null); // null = not selected yet
  const [processingPayment, setProcessingPayment] = useState(false);
  const [shareModalOrder, setShareModalOrder] = useState(null);
  
  // Profile Edit State
  const [showEditProfile, setShowEditProfile] = useState(false);
  const [editingProfile, setEditingProfile] = useState(null);
  const [savingProfile, setSavingProfile] = useState(false);
  const [states, setStates] = useState([]);

  useEffect(() => {
    const token = localStorage.getItem('token');
    if (!token) {
      navigate('/login');
      return;
    }
    fetchAllData();
    fetchStates();
  }, [navigate]);

  const getAuthHeaders = () => {
    const token = localStorage.getItem('token');
    return { Authorization: `Bearer ${token}` };
  };

  const fetchStates = async () => {
    try {
      const response = await erpApi.customerMaster.getStates();
      setStates(response.data || []);
    } catch (error) {
      console.error('Failed to fetch states:', error);
    }
  };

  const fetchAllData = async () => {
    try {
      const [ordersRes, jobWorkRes, profileRes] = await Promise.all([
        axios.get(`${API_BASE}/api/orders/my-orders`, { headers: getAuthHeaders() }),
        axios.get(`${API_BASE}/api/erp/job-work/my-orders`, { headers: getAuthHeaders() }).catch(() => ({ data: [] })),
        axios.get(`${API_BASE}/api/users/profile`, { headers: getAuthHeaders() }).catch(() => ({ data: null }))
      ]);
      
      const regularOrders = (ordersRes.data || []).map(o => ({ ...o, order_type: 'regular' }));
      const jwOrders = (jobWorkRes.data || []).map(o => ({ ...o, order_type: 'job_work' }));
      
      const merged = [...regularOrders, ...jwOrders].sort((a, b) => 
        new Date(b.created_at) - new Date(a.created_at)
      );
      
      setOrders(regularOrders);
      setJobWorkOrders(jwOrders);
      setAllOrders(merged);
      setProfile(profileRes.data);

      // Try to fetch Customer Master profile by mobile number
      if (profileRes.data?.phone) {
        try {
          const customerMasterRes = await axios.get(
            `${API_BASE}/api/erp/customer-master/search/for-invoice?q=${profileRes.data.phone}`,
            { headers: getAuthHeaders() }
          );
          if (customerMasterRes.data?.length > 0) {
            const customerProfile = customerMasterRes.data[0];
            setCustomerMasterProfile(customerProfile);
            
            // Fetch ledger/outstanding if we have customer profile
            if (customerProfile.id) {
              const ledgerRes = await axios.get(
                `${API_BASE}/api/erp/customer-master/${customerProfile.id}`,
                { headers: getAuthHeaders() }
              ).catch(() => ({ data: null }));
              
              if (ledgerRes.data?.linked_data) {
                setLedgerSummary(ledgerRes.data.linked_data);
              }
            }
          }
        } catch (err) {
          console.log('Customer Master profile not found');
        }
      }
    } catch (error) {
      console.error('Failed to fetch data:', error);
      if (error.response?.status === 401) {
        logout();
        navigate('/login');
      }
    } finally {
      setLoading(false);
    }
  };

  // Regular Order Status Flow
  const regularStatusFlow = [
    { key: 'confirmed', label: 'Order Confirmed', icon: CheckCircle },
    { key: 'processing', label: 'Production', icon: Factory },
    { key: 'quality_check', label: 'Quality Check', icon: ShieldCheck },
    { key: 'ready_for_dispatch', label: 'Ready', icon: Package },
    { key: 'dispatched', label: 'Dispatched', icon: Truck },
  ];

  // Job Work Status Flow
  const jobWorkStatusFlow = [
    { key: 'material_received', label: 'Material Received', icon: Package },
    { key: 'in_process', label: 'Production Started', icon: Factory },
    { key: 'completed', label: 'Production Completed', icon: CheckCircle },
    { key: 'ready_for_delivery', label: 'Ready', icon: Package },
    { key: 'delivered', label: 'Dispatched', icon: Truck },
  ];

  // Get status index
  const getStatusIndex = (order) => {
    const flow = order.order_type === 'job_work' ? jobWorkStatusFlow : regularStatusFlow;
    const idx = flow.findIndex(s => s.key === order.status);
    return idx >= 0 ? idx : -1;
  };

  // Payment handling for regular orders
  const handlePayRemaining = async (order) => {
    setProcessingPayment(true);
    if (paymentMethod === 'cash') {
      try {
        await axios.patch(
          `${API_BASE}/api/orders/${order.id}`,
          { remaining_payment_preference: 'cash' },
          { headers: { ...getAuthHeaders(), 'Content-Type': 'application/json' } }
        );
        toast.success('Cash payment preference saved. Admin will collect payment.');
        setPaymentModalOrder(null);
        setPaymentMethod(null);
        fetchAllData();
      } catch (error) {
        toast.error('Failed to update');
      } finally {
        setProcessingPayment(false);
      }
    } else {
      try {
        const res = await axios.post(`${API_BASE}/api/orders/${order.id}/pay-remaining`, {}, { headers: getAuthHeaders() });
        
        const options = {
          key: process.env.REACT_APP_RAZORPAY_KEY_ID,
          amount: res.data.amount,
          currency: 'INR',
          order_id: res.data.razorpay_order_id,
          name: 'Lucumaa Glass',
          description: `Payment - Order #${order.order_number}`,
          handler: async (response) => {
            try {
              await axios.post(`${API_BASE}/api/orders/${order.id}/verify-remaining`, {
                razorpay_payment_id: response.razorpay_payment_id,
                razorpay_signature: response.razorpay_signature
              }, { headers: { ...getAuthHeaders(), 'Content-Type': 'application/json' } });
              toast.success('Payment successful!');
              setPaymentModalOrder(null);
              setPaymentMethod(null);
              fetchAllData();
            } catch (err) {
              toast.error('Payment verification failed');
            }
          },
          modal: {
            ondismiss: () => setProcessingPayment(false)
          },
          prefill: { name: user?.name, email: user?.email },
          theme: { color: '#0d9488' }
        };
        
        new window.Razorpay(options).open();
      } catch (error) {
        toast.error('Failed to initiate payment');
      } finally {
        setProcessingPayment(false);
      }
    }
  };

  // Payment handling for job work orders
  const handleJobWorkPayment = async (order) => {
    setProcessingPayment(true);
    if (paymentMethod === 'cash') {
      try {
        await axios.post(`${API_BASE}/api/erp/job-work/orders/${order.id}/set-cash-preference`, {}, { headers: getAuthHeaders() });
        toast.success('Cash payment preference saved. Please pay at our office.');
        setPaymentModalOrder(null);
        setPaymentMethod(null);
        fetchAllData();
      } catch (error) {
        toast.error('Failed to set preference');
      } finally {
        setProcessingPayment(false);
      }
    } else {
      try {
        const res = await axios.post(`${API_BASE}/api/erp/job-work/orders/${order.id}/initiate-payment`, {}, { headers: getAuthHeaders() });
        
        const options = {
          key: process.env.REACT_APP_RAZORPAY_KEY_ID,
          amount: res.data.amount,
          currency: 'INR',
          order_id: res.data.razorpay_order_id,
          name: 'Lucumaa Glass',
          description: `Job Work - ${res.data.job_work_number}`,
          handler: async (response) => {
            try {
              await axios.post(`${API_BASE}/api/erp/job-work/orders/${order.id}/verify-payment?razorpay_payment_id=${response.razorpay_payment_id}&razorpay_signature=${response.razorpay_signature}`, {}, { headers: getAuthHeaders() });
              toast.success('Payment successful!');
              setPaymentModalOrder(null);
              setPaymentMethod(null);
              fetchAllData();
            } catch (err) {
              toast.error('Payment verification failed');
            }
          },
          modal: {
            ondismiss: () => setProcessingPayment(false)
          },
          prefill: { name: order.customer_name, email: order.email },
          theme: { color: '#ea580c' }
        };
        
        new window.Razorpay(options).open();
      } catch (error) {
        toast.error('Failed to initiate payment');
      } finally {
        setProcessingPayment(false);
      }
    }
  };

  const downloadInvoice = (orderId, isJobWork = false) => {
    const token = localStorage.getItem('token');
    if (isJobWork) {
      window.open(`${API_BASE}/api/erp/pdf/job-work-invoice/${orderId}?token=${token}`, '_blank');
    } else {
      window.open(`${API_BASE}/api/erp/pdf/invoice/${orderId}?token=${token}`, '_blank');
    }
  };

  const downloadReceipt = (orderId) => {
    const token = localStorage.getItem('token');
    window.open(`${API_BASE}/api/erp/pdf/payment-receipt/${orderId}?token=${token}`, '_blank');
  };

  // Check if profile is incomplete
  const isProfileIncomplete = () => {
    if (!profile) return true;
    return !profile.name || !profile.phone || !profile.company_name || !profile.address;
  };

  // Get payment badge
  const getPaymentBadge = (order) => {
    if (order.order_type === 'job_work') {
      if (order.payment_status === 'completed') return { color: 'bg-green-100 text-green-700', label: 'Paid' };
      if (order.payment_preference === 'cash') return { color: 'bg-orange-100 text-orange-700', label: 'Cash Pending' };
      if (order.payment_status === 'partially_paid') return { color: 'bg-amber-100 text-amber-700', label: `${order.advance_percent || 50}% Paid` };
      if (order.advance_paid > 0) return { color: 'bg-amber-100 text-amber-700', label: `${order.advance_percent || 50}% Paid` };
      return { color: 'bg-red-100 text-red-700', label: 'Payment Pending' };
    } else {
      const advancePaid = order.advance_payment_status === 'paid';
      const remainingPaid = order.remaining_payment_status === 'paid' || order.remaining_payment_status === 'cash_received';
      if (order.advance_percent === 100 && advancePaid) return { color: 'bg-green-100 text-green-700', label: 'Paid' };
      if (advancePaid && remainingPaid) return { color: 'bg-green-100 text-green-700', label: 'Paid' };
      if (advancePaid && order.remaining_payment_preference === 'cash') return { color: 'bg-orange-100 text-orange-700', label: 'Cash Pending' };
      if (advancePaid) return { color: 'bg-amber-100 text-amber-700', label: `${order.advance_percent}% Paid` };
      return { color: 'bg-red-100 text-red-700', label: 'Payment Pending' };
    }
  };

  // Check if needs payment
  const needsPayment = (order) => {
    if (order.order_type === 'job_work') {
      // Job work: needs payment if not completed and not cash preference
      return order.payment_status !== 'completed' && order.payment_preference !== 'cash';
    }
    // Regular order: needs remaining payment
    return order.advance_payment_status === 'paid' && 
      order.remaining_payment_status !== 'paid' && 
      order.remaining_payment_status !== 'cash_received' &&
      order.remaining_payment_preference !== 'cash' &&
      order.remaining_amount > 0;
  };

  // Get remaining amount
  const getRemainingAmount = (order) => {
    if (order.order_type === 'job_work') {
      if (order.payment_status === 'completed') return 0;
      return (order.summary?.grand_total || 0) - (order.advance_paid || 0);
    }
    return order.remaining_amount || 0;
  };

  // ============ PROFILE EDIT FUNCTIONS ============
  
  const handleEditProfile = () => {
    // Initialize edit form with existing data
    setEditingProfile({
      customer_type: customerMasterProfile?.customer_type || 'individual',
      company_name: customerMasterProfile?.company_name || profile?.company_name || '',
      individual_name: customerMasterProfile?.individual_name || profile?.name || '',
      contact_person: customerMasterProfile?.contact_person || profile?.name || '',
      mobile: customerMasterProfile?.mobile || profile?.phone || '',
      email: customerMasterProfile?.email || profile?.email || '',
      needs_gst_invoice: customerMasterProfile?.needs_gst_invoice || !!customerMasterProfile?.gstin || false,
      gstin: customerMasterProfile?.gstin || '',
      pan: customerMasterProfile?.pan || '',
      gst_type: customerMasterProfile?.gst_type || 'unregistered',
      place_of_supply: customerMasterProfile?.place_of_supply || '',
      billing_address: customerMasterProfile?.billing_address || {
        address_line1: '',
        address_line2: '',
        city: '',
        state: '',
        state_code: '',
        pin_code: '',
        country: 'India'
      },
      shipping_addresses: customerMasterProfile?.shipping_addresses || []
    });
    setShowEditProfile(true);
  };

  const handleSaveProfile = async () => {
    if (!editingProfile) return;
    
    setSavingProfile(true);
    try {
      // Validate required fields
      if (!editingProfile.mobile || !/^[6-9]\d{9}$/.test(editingProfile.mobile)) {
        toast.error('Valid 10-digit mobile number required');
        setSavingProfile(false);
        return;
      }
      
      // Validate GST fields if needs_gst_invoice is checked
      if (editingProfile.needs_gst_invoice) {
        if (!editingProfile.gstin) {
          toast.error('GSTIN is required when GST Invoice is needed');
          setSavingProfile(false);
          return;
        }
        if (!editingProfile.company_name) {
          toast.error('Company/Firm name is required for GST Invoice');
          setSavingProfile(false);
          return;
        }
        if (!editingProfile.pan) {
          toast.error('PAN is required for GST Invoice');
          setSavingProfile(false);
          return;
        }
      }
      
      if (editingProfile.gstin && !/^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z]{1}[1-9A-Z]{1}Z[0-9A-Z]{1}$/i.test(editingProfile.gstin)) {
        toast.error('Invalid GSTIN format');
        setSavingProfile(false);
        return;
      }
      
      if (editingProfile.pan && !/^[A-Z]{5}[0-9]{4}[A-Z]{1}$/i.test(editingProfile.pan)) {
        toast.error('Invalid PAN format');
        setSavingProfile(false);
        return;
      }
      
      if (!editingProfile.billing_address?.address_line1) {
        toast.error('Billing address is required');
        setSavingProfile(false);
        return;
      }

      if (customerMasterProfile?.id) {
        // Update existing profile
        await erpApi.customerMaster.update(customerMasterProfile.id, editingProfile);
        toast.success('Profile updated successfully!');
      } else {
        // Create new profile
        await erpApi.customerMaster.create(editingProfile);
        toast.success('Profile created successfully!');
      }
      
      setShowEditProfile(false);
      fetchAllData(); // Refresh data
    } catch (error) {
      toast.error(error.response?.data?.detail || 'Failed to save profile');
    } finally {
      setSavingProfile(false);
    }
  };

  const handleAddShippingAddress = () => {
    setEditingProfile(prev => ({
      ...prev,
      shipping_addresses: [
        ...prev.shipping_addresses,
        {
          id: Date.now().toString(),
          site_name: '',
          address_line1: '',
          address_line2: '',
          city: '',
          state: '',
          pin_code: '',
          contact_person: '',
          contact_phone: '',
          is_default: false
        }
      ]
    }));
  };

  const handleUpdateShippingAddress = (index, field, value) => {
    setEditingProfile(prev => {
      const updated = [...prev.shipping_addresses];
      updated[index] = { ...updated[index], [field]: value };
      return { ...prev, shipping_addresses: updated };
    });
  };

  const handleRemoveShippingAddress = (index) => {
    setEditingProfile(prev => ({
      ...prev,
      shipping_addresses: prev.shipping_addresses.filter((_, i) => i !== index)
    }));
  };

  const tabs = [
    { id: 'orders', label: 'All Orders', icon: Package, count: allOrders.length },
    { id: 'wallet', label: 'Wallet', icon: Wallet },
    { id: 'profile', label: 'Profile', icon: User },
  ];

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-slate-50">
        <RefreshCw className="w-8 h-8 animate-spin text-teal-600" />
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-slate-50" data-testid="customer-portal-enhanced">
      {/* Header */}
      <header className="bg-white border-b border-slate-200 sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-16">
            <Link to="/" className="flex items-center gap-2">
              <img src={COMPANY_LOGO} alt="Lucumaa Glass" className="h-10 w-auto object-contain" />
              <span className="font-bold text-xl text-slate-900">Lucumaa Glass</span>
            </Link>
            
            <div className="flex items-center gap-3">
              <Link to="/customize">
                <Button size="sm" className="bg-teal-600 hover:bg-teal-700 gap-2">
                  <Plus className="w-4 h-4" /> New Order
                </Button>
              </Link>
              <Link to="/job-work">
                <Button size="sm" variant="outline" className="gap-2 border-orange-300 text-orange-600 hover:bg-orange-50">
                  <Hammer className="w-4 h-4" /> Job Work
                </Button>
              </Link>
              <Button variant="outline" size="sm" onClick={() => { logout(); navigate('/'); }}>
                <LogOut className="w-4 h-4 mr-2" /> Logout
              </Button>
            </div>
          </div>
        </div>
      </header>

      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
        {/* Tabs */}
        <div className="flex gap-2 mb-6 bg-white rounded-xl p-2 shadow-sm">
          {tabs.map((tab) => {
            const Icon = tab.icon;
            return (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`flex items-center gap-2 px-4 py-2 rounded-lg font-medium transition-all ${
                  activeTab === tab.id ? 'bg-teal-600 text-white shadow-md' : 'text-slate-600 hover:bg-slate-100'
                }`}
              >
                <Icon className="w-4 h-4" />
                {tab.label}
                {tab.count !== undefined && (
                  <span className={`px-1.5 py-0.5 rounded-full text-xs ${activeTab === tab.id ? 'bg-teal-500' : 'bg-slate-200'}`}>
                    {tab.count}
                  </span>
                )}
              </button>
            );
          })}
        </div>

        {/* All Orders Tab */}
        {activeTab === 'orders' && (
          <div className="space-y-3">
            {/* Profile Incomplete Warning */}
            {isProfileIncomplete() && (
              <Card className="bg-amber-50 border-amber-200 mb-4">
                <CardContent className="p-4 flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <AlertCircle className="w-5 h-5 text-amber-600" />
                    <div>
                      <p className="font-medium text-amber-800">Complete Your Profile</p>
                      <p className="text-sm text-amber-700">Update business details, GSTIN & address for proper invoicing</p>
                    </div>
                  </div>
                  <Button 
                    size="sm" 
                    className="bg-amber-500 hover:bg-amber-600"
                    onClick={() => setActiveTab('profile')}
                  >
                    Update Profile
                  </Button>
                </CardContent>
              </Card>
            )}
            
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-xl font-bold text-slate-900">All Orders ({allOrders.length})</h2>
              <div className="flex gap-2 text-xs">
                <span className="px-2 py-1 bg-teal-100 text-teal-700 rounded">Regular: {orders.length}</span>
                <span className="px-2 py-1 bg-orange-100 text-orange-700 rounded">Job Work: {jobWorkOrders.length}</span>
              </div>
            </div>

            {allOrders.length === 0 ? (
              <Card>
                <CardContent className="py-12 text-center">
                  <Package className="w-16 h-16 text-slate-300 mx-auto mb-4" />
                  <p className="text-slate-600 mb-4">No orders yet</p>
                  <div className="flex gap-3 justify-center">
                    <Link to="/customize"><Button className="bg-teal-600 hover:bg-teal-700">Create Order</Button></Link>
                    <Link to="/job-work"><Button variant="outline" className="border-orange-300 text-orange-600">Job Work</Button></Link>
                  </div>
                </CardContent>
              </Card>
            ) : (
              allOrders.map((order, index) => {
                const isJobWork = order.order_type === 'job_work';
                const statusFlow = isJobWork ? jobWorkStatusFlow : regularStatusFlow;
                const currentStatusIdx = getStatusIndex(order);
                const paymentBadge = getPaymentBadge(order);
                const showPayButton = needsPayment(order);
                const remaining = getRemainingAmount(order);
                
                const orderNumber = isJobWork ? order.job_work_number : `#${order.order_number || order.id.slice(0, 6)}`;
                const qty = isJobWork ? order.summary?.total_pieces : order.quantity;
                const total = isJobWork ? order.summary?.grand_total : order.total_price;
                const advancePaid = isJobWork ? (order.advance_paid || 0) : (order.advance_amount || 0);
                const cashPaid = (!isJobWork && order.remaining_payment_status === 'cash_received') ? order.remaining_amount : 0;

                return (
                  <Card key={order.id} className={`hover:shadow-md transition-shadow ${isJobWork ? 'border-l-4 border-l-orange-500' : 'border-l-4 border-l-teal-500'}`}>
                    <CardContent className="p-4">
                      {/* Line 1: Order Info */}
                      <div className="flex flex-wrap items-center gap-2 text-sm mb-3">
                        <span className="font-bold text-slate-400 w-6">{index + 1}.</span>
                        <span className={`font-mono font-bold px-2 py-0.5 rounded ${isJobWork ? 'text-orange-700 bg-orange-50' : 'text-teal-700 bg-teal-50'}`}>
                          {orderNumber}
                        </span>
                        <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${isJobWork ? 'bg-orange-100 text-orange-700' : 'bg-teal-100 text-teal-700'}`}>
                          {isJobWork ? 'Job Work' : 'Regular'}
                        </span>
                        <span className="text-slate-600">QTY-{qty}</span>
                        <span className="text-slate-500">{new Date(order.created_at).toLocaleDateString('en-IN')}</span>
                        <span className="font-bold text-slate-900">₹{total?.toLocaleString()}</span>
                        {advancePaid > 0 && <span className="text-green-600 text-xs">Adv: ₹{advancePaid?.toLocaleString()} ✓</span>}
                        {cashPaid > 0 && <span className="text-blue-600 text-xs">Cash: ₹{cashPaid?.toLocaleString()} ✓</span>}
                        <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${paymentBadge.color}`}>{paymentBadge.label}</span>
                        {remaining > 0 && <span className="text-amber-600 font-medium text-xs">Remaining: ₹{remaining?.toLocaleString()}</span>}
                        
                        {/* Payment Selection & Pay Button - Step 1: Select Method, Step 2: Pay */}
                        {showPayButton && (
                          <div className="flex items-center gap-1 ml-auto">
                            {/* Download Icons - Always visible */}
                            <Button 
                              variant="ghost" 
                              size="sm" 
                              className="h-7 w-7 p-0 text-slate-500 hover:text-slate-700" 
                              onClick={() => setSelectedOrder(order)} 
                              title="View Details"
                              data-testid={`view-${order.id}`}
                            >
                              <Eye className="w-4 h-4" />
                            </Button>
                            <Button 
                              variant="ghost" 
                              size="sm" 
                              className="h-7 w-7 p-0 text-green-600 hover:text-green-700" 
                              onClick={() => downloadReceipt(order.id)} 
                              title="Download Receipt"
                              data-testid={`receipt-${order.id}`}
                            >
                              <FileText className="w-4 h-4" />
                            </Button>
                            <Button 
                              variant="ghost" 
                              size="sm" 
                              className="h-7 w-7 p-0 text-red-600 hover:text-red-700" 
                              onClick={() => downloadInvoice(order.id, isJobWork)} 
                              title="Download Invoice"
                              data-testid={`invoice-${order.id}`}
                            >
                              <Download className="w-4 h-4" />
                            </Button>
                            <Button 
                              variant="ghost" 
                              size="sm" 
                              className="h-7 w-7 p-0 text-purple-600 hover:text-purple-700" 
                              onClick={() => setShareModalOrder({ order, isJobWork })} 
                              title="Share Invoice/Receipt"
                              data-testid={`share-${order.id}`}
                            >
                              <Share2 className="w-4 h-4" />
                            </Button>
                            
                            {/* Divider */}
                            <div className="w-px h-5 bg-slate-200 mx-1"></div>
                            
                            {/* Step 1: Select Payment Method */}
                            <button
                              onClick={() => { 
                                setPaymentModalOrder(order); 
                                setPaymentMethod(paymentModalOrder?.id === order.id && paymentMethod === 'online' ? null : 'online'); 
                              }}
                              className={`h-7 text-xs px-2 rounded border flex items-center gap-1 ${
                                paymentModalOrder?.id === order.id && paymentMethod === 'online' 
                                  ? 'border-blue-500 bg-blue-50 text-blue-700' 
                                  : 'border-slate-200 hover:border-slate-300'
                              }`}
                              data-testid={`payment-online-${order.id}`}
                            >
                              <CreditCard className="w-3 h-3" /> Online
                            </button>
                            <button
                              onClick={() => { 
                                setPaymentModalOrder(order); 
                                setPaymentMethod(paymentModalOrder?.id === order.id && paymentMethod === 'cash' ? null : 'cash'); 
                              }}
                              className={`h-7 text-xs px-2 rounded border flex items-center gap-1 ${
                                paymentModalOrder?.id === order.id && paymentMethod === 'cash' 
                                  ? 'border-green-500 bg-green-50 text-green-700' 
                                  : 'border-slate-200 hover:border-slate-300'
                              }`}
                              data-testid={`payment-cash-${order.id}`}
                            >
                              <Banknote className="w-3 h-3" /> Cash
                            </button>
                            
                            {/* Step 2: Pay Button - Only shows after method selected */}
                            {paymentModalOrder?.id === order.id && paymentMethod && (
                              <Button 
                                size="sm"
                                className={`h-7 text-xs px-3 ${
                                  paymentMethod === 'cash' 
                                    ? 'bg-green-500 hover:bg-green-600' 
                                    : 'bg-blue-500 hover:bg-blue-600'
                                } text-white`}
                                disabled={processingPayment}
                                onClick={() => {
                                  if (isJobWork) {
                                    handleJobWorkPayment(order);
                                  } else {
                                    handlePayRemaining(order);
                                  }
                                }}
                                data-testid={`pay-now-${order.id}`}
                              >
                                {processingPayment ? '...' : `Pay ₹${remaining?.toLocaleString()}`}
                              </Button>
                            )}
                          </div>
                        )}
                        
                        {/* For orders without payment needed - show all download options */}
                        {!showPayButton && (
                          <div className="flex items-center gap-1 ml-auto">
                            <Button 
                              variant="ghost" 
                              size="sm" 
                              className="h-7 w-7 p-0 text-slate-500 hover:text-slate-700" 
                              onClick={() => setSelectedOrder(order)} 
                              title="View Details"
                              data-testid={`view-${order.id}`}
                            >
                              <Eye className="w-4 h-4" />
                            </Button>
                            <Button 
                              variant="ghost" 
                              size="sm" 
                              className="h-7 w-7 p-0 text-green-600 hover:text-green-700" 
                              onClick={() => downloadReceipt(order.id)} 
                              title="Download Receipt"
                              data-testid={`receipt-${order.id}`}
                            >
                              <FileText className="w-4 h-4" />
                            </Button>
                            <Button 
                              variant="ghost" 
                              size="sm" 
                              className="h-7 w-7 p-0 text-red-600 hover:text-red-700" 
                              onClick={() => downloadInvoice(order.id, isJobWork)} 
                              title="Download Invoice"
                              data-testid={`invoice-${order.id}`}
                            >
                              <Download className="w-4 h-4" />
                            </Button>
                            <Button 
                              variant="ghost" 
                              size="sm" 
                              className="h-7 w-7 p-0 text-purple-600 hover:text-purple-700" 
                              onClick={() => setShareModalOrder({ order, isJobWork })} 
                              title="Share Invoice/Receipt"
                              data-testid={`share-${order.id}`}
                            >
                              <Share2 className="w-4 h-4" />
                            </Button>
                          </div>
                        )}
                      </div>
                      
                      {/* Line 2: Status Timeline */}
                      <div className="flex items-center gap-1 overflow-x-auto pb-1 pl-6">
                        {statusFlow.map((status, idx) => {
                          const StatusIcon = status.icon;
                          const isActive = idx === currentStatusIdx;
                          const isPassed = idx < currentStatusIdx;
                          const isPending = idx > currentStatusIdx;
                          
                          return (
                            <React.Fragment key={status.key}>
                              <div className={`flex items-center gap-1 px-2 py-1 rounded text-xs whitespace-nowrap ${
                                isActive ? (isJobWork ? 'bg-orange-500 text-white' : 'bg-teal-500 text-white') :
                                isPassed ? 'bg-green-100 text-green-700' :
                                'bg-slate-100 text-slate-400'
                              }`}>
                                <StatusIcon className="w-3 h-3" />
                                {status.label}
                              </div>
                              {idx < statusFlow.length - 1 && (
                                <ArrowRight className={`w-3 h-3 flex-shrink-0 ${isPassed ? 'text-green-500' : 'text-slate-300'}`} />
                              )}
                            </React.Fragment>
                          );
                        })}
                      </div>
                      
                      {/* Line 3: Product Info */}
                      <div className="mt-2 text-xs text-slate-500 pl-6">
                        {isJobWork 
                          ? `${order.summary?.total_sqft?.toFixed(1)} sq.ft • ${order.item_details?.map(i => `${i.thickness_mm}mm`).join(', ')}`
                          : `${order.product_name} • ${order.thickness}mm • ${order.width}" × ${order.height}"`
                        }
                      </div>
                    </CardContent>
                  </Card>
                );
              })
            )}
          </div>
        )}

        {/* Wallet Tab */}
        {activeTab === 'wallet' && (
          <div className="space-y-6">
            <Card className="bg-gradient-to-br from-teal-500 to-teal-600 text-white">
              <CardContent className="p-8">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-teal-100">Available Balance</p>
                    <p className="text-5xl font-bold mt-2">₹{profile?.wallet?.balance || 0}</p>
                  </div>
                  <Wallet className="w-20 h-20 opacity-50" />
                </div>
              </CardContent>
            </Card>

            <div className="grid md:grid-cols-2 gap-6">
              <Card>
                <CardHeader>
                  <CardTitle className="text-base flex items-center gap-2">
                    <Gift className="w-5 h-5 text-purple-600" /> Referral Program
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <p className="text-sm text-slate-600 mb-4">Share your code & earn ₹100!</p>
                  <div className="bg-slate-100 px-4 py-3 rounded-lg text-center mb-4">
                    <span className="text-xl font-bold text-purple-600 tracking-widest">
                      {profile?.wallet?.referral_code || 'N/A'}
                    </span>
                  </div>
                  <Button variant="outline" className="w-full gap-2" onClick={() => {
                    navigator.clipboard.writeText(profile?.wallet?.referral_code || '');
                    toast.success('Copied!');
                  }}>
                    <Copy className="w-4 h-4" /> Copy Code
                  </Button>
                </CardContent>
              </Card>

              <Card>
                <CardHeader><CardTitle className="text-base">Wallet Benefits</CardTitle></CardHeader>
                <CardContent>
                  <ul className="space-y-2 text-sm text-slate-600">
                    <li className="flex items-center gap-2"><CheckCircle className="w-4 h-4 text-teal-600" /> Use up to 25% of order value</li>
                    <li className="flex items-center gap-2"><CheckCircle className="w-4 h-4 text-teal-600" /> Minimum order ₹500</li>
                    <li className="flex items-center gap-2"><CheckCircle className="w-4 h-4 text-teal-600" /> Earn 2% cashback</li>
                  </ul>
                </CardContent>
              </Card>
            </div>
          </div>
        )}

        {/* Profile Tab */}
        {activeTab === 'profile' && profile && (
          <div className="space-y-6">
            {/* Edit Profile Button */}
            <div className="flex justify-end">
              <Button
                onClick={handleEditProfile}
                className="flex items-center gap-2"
                data-testid="edit-profile-btn"
              >
                <Edit className="w-4 h-4" />
                Edit Profile
              </Button>
            </div>

            {/* Customer Master Profile Card */}
            {customerMasterProfile && (
              <Card className="border-blue-200 bg-blue-50/50">
                <CardHeader className="pb-2">
                  <CardTitle className="text-base flex items-center justify-between">
                    <span className="flex items-center gap-2">
                      {customerMasterProfile.invoice_type === 'B2B' ? (
                        <Building className="w-5 h-5 text-purple-600" />
                      ) : (
                        <User className="w-5 h-5 text-teal-600" />
                      )}
                      Customer Profile
                    </span>
                    <span className={`px-2 py-1 text-xs rounded-full ${
                      customerMasterProfile.invoice_type === 'B2B' 
                        ? 'bg-purple-100 text-purple-700' 
                        : 'bg-green-100 text-green-700'
                    }`}>
                      {customerMasterProfile.invoice_type}
                    </span>
                  </CardTitle>
                </CardHeader>
                <CardContent className="grid md:grid-cols-2 gap-4">
                  <div>
                    <label className="text-xs text-slate-500">Customer Code</label>
                    <p className="font-mono text-blue-600">{customerMasterProfile.customer_code}</p>
                  </div>
                  <div>
                    <label className="text-xs text-slate-500">Name</label>
                    <p className="font-medium">{customerMasterProfile.display_name}</p>
                  </div>
                  {customerMasterProfile.gstin && (
                    <div>
                      <label className="text-xs text-slate-500">GSTIN</label>
                      <p className="font-mono">{customerMasterProfile.gstin}</p>
                    </div>
                  )}
                  {customerMasterProfile.credit_type === 'credit_allowed' && (
                    <>
                      <div>
                        <label className="text-xs text-slate-500">Credit Limit</label>
                        <p className="font-medium text-green-600">₹{customerMasterProfile.credit_limit?.toLocaleString()}</p>
                      </div>
                      <div>
                        <label className="text-xs text-slate-500">Credit Days</label>
                        <p className="font-medium">{customerMasterProfile.credit_days} days</p>
                      </div>
                    </>
                  )}
                </CardContent>
              </Card>
            )}

            {/* Outstanding & Ledger Summary */}
            {ledgerSummary && (
              <Card className="border-orange-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-base flex items-center gap-2">
                    <CreditCard className="w-5 h-5 text-orange-600" />
                    Account Summary
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                    <div className="bg-blue-50 rounded-lg p-3 text-center">
                      <p className="text-xs text-blue-600">Total Orders</p>
                      <p className="text-xl font-bold text-blue-800">{ledgerSummary.total_orders || 0}</p>
                    </div>
                    <div className="bg-green-50 rounded-lg p-3 text-center">
                      <p className="text-xs text-green-600">Total Spent</p>
                      <p className="text-xl font-bold text-green-800">₹{(ledgerSummary.total_spent || 0).toLocaleString()}</p>
                    </div>
                    <div className="bg-red-50 rounded-lg p-3 text-center">
                      <p className="text-xs text-red-600">Outstanding</p>
                      <p className="text-xl font-bold text-red-800">₹{(ledgerSummary.outstanding_balance || 0).toLocaleString()}</p>
                    </div>
                    {ledgerSummary.ageing && (
                      <div className="bg-amber-50 rounded-lg p-3 text-center">
                        <p className="text-xs text-amber-600">Overdue (90+ days)</p>
                        <p className="text-xl font-bold text-amber-800">₹{(ledgerSummary.ageing.over_90 || 0).toLocaleString()}</p>
                      </div>
                    )}
                  </div>

                  {/* Ageing Breakdown */}
                  {ledgerSummary.ageing && (ledgerSummary.outstanding_balance || 0) > 0 && (
                    <div className="mt-4 pt-4 border-t">
                      <p className="text-sm font-medium text-slate-700 mb-2">Payment Ageing</p>
                      <div className="grid grid-cols-4 gap-2 text-center text-xs">
                        <div className="bg-green-100 rounded p-2">
                          <p className="text-green-600">0-30 Days</p>
                          <p className="font-semibold text-green-800">₹{(ledgerSummary.ageing['0_30'] || 0).toLocaleString()}</p>
                        </div>
                        <div className="bg-yellow-100 rounded p-2">
                          <p className="text-yellow-600">31-60 Days</p>
                          <p className="font-semibold text-yellow-800">₹{(ledgerSummary.ageing['31_60'] || 0).toLocaleString()}</p>
                        </div>
                        <div className="bg-orange-100 rounded p-2">
                          <p className="text-orange-600">61-90 Days</p>
                          <p className="font-semibold text-orange-800">₹{(ledgerSummary.ageing['61_90'] || 0).toLocaleString()}</p>
                        </div>
                        <div className="bg-red-100 rounded p-2">
                          <p className="text-red-600">90+ Days</p>
                          <p className="font-semibold text-red-800">₹{(ledgerSummary.ageing['over_90'] || 0).toLocaleString()}</p>
                        </div>
                      </div>
                    </div>
                  )}
                </CardContent>
              </Card>
            )}

            <div className="grid md:grid-cols-2 gap-6">
              <Card>
                <CardHeader>
                  <CardTitle className="text-base flex items-center gap-2">
                    <User className="w-5 h-5 text-teal-600" /> Personal Info
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div><label className="text-xs text-slate-500">Name</label><p className="font-medium">{profile.name || user?.name || '-'}</p></div>
                  <div><label className="text-xs text-slate-500">Email</label><p className="font-medium">{profile.email || user?.email || '-'}</p></div>
                  <div><label className="text-xs text-slate-500">Phone</label><p className="font-medium">{profile.phone || '-'}</p></div>
                  <div><label className="text-xs text-slate-500">Company</label><p className="font-medium">{profile.company_name || customerMasterProfile?.company_name || '-'}</p></div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader><CardTitle className="text-base">Account Stats</CardTitle></CardHeader>
                <CardContent className="space-y-4">
                  <div className="flex justify-between"><span className="text-slate-600">Regular Orders</span><span className="font-bold text-teal-600">{orders.length}</span></div>
                  <div className="flex justify-between"><span className="text-slate-600">Job Work Orders</span><span className="font-bold text-orange-600">{jobWorkOrders.length}</span></div>
                  <div className="flex justify-between"><span className="text-slate-600">Wallet Balance</span><span className="font-bold text-teal-600">₹{profile?.wallet?.balance || 0}</span></div>
                </CardContent>
              </Card>
            </div>

            {/* Billing Address from Customer Master */}
            {customerMasterProfile?.billing_address && (
              <Card>
                <CardHeader>
                  <CardTitle className="text-base flex items-center gap-2">
                    <Package className="w-5 h-5 text-slate-600" /> Billing Address
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <p className="text-slate-700">{customerMasterProfile.billing_address.address_line1}</p>
                  {customerMasterProfile.billing_address.address_line2 && (
                    <p className="text-slate-600">{customerMasterProfile.billing_address.address_line2}</p>
                  )}
                  <p className="text-slate-600">
                    {customerMasterProfile.billing_address.city}, {customerMasterProfile.billing_address.state} - {customerMasterProfile.billing_address.pin_code}
                  </p>
                </CardContent>
              </Card>
            )}
          </div>
        )}
      </div>

      {/* Order Details Modal */}
      {selectedOrder && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
          <Card className="max-w-2xl w-full max-h-[90vh] overflow-y-auto">
            <CardHeader className="flex flex-row items-center justify-between">
              <CardTitle className="flex items-center gap-2">
                {selectedOrder.order_type === 'job_work' ? (
                  <><Hammer className="w-5 h-5 text-orange-600" /> {selectedOrder.job_work_number}</>
                ) : (
                  <><Package className="w-5 h-5 text-teal-600" /> Order #{selectedOrder.order_number}</>
                )}
              </CardTitle>
              <button onClick={() => setSelectedOrder(null)} className="text-slate-400 hover:text-slate-600 text-xl">×</button>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div><label className="text-xs text-slate-500">Type</label><p className={`font-medium ${selectedOrder.order_type === 'job_work' ? 'text-orange-600' : 'text-teal-600'}`}>{selectedOrder.order_type === 'job_work' ? 'Job Work' : 'Regular'}</p></div>
                <div><label className="text-xs text-slate-500">Date</label><p>{new Date(selectedOrder.created_at).toLocaleDateString('en-IN')}</p></div>
                <div><label className="text-xs text-slate-500">Quantity</label><p>{selectedOrder.order_type === 'job_work' ? selectedOrder.summary?.total_pieces : selectedOrder.quantity}</p></div>
                <div><label className="text-xs text-slate-500">Total</label><p className="font-bold text-lg">₹{(selectedOrder.order_type === 'job_work' ? selectedOrder.summary?.grand_total : selectedOrder.total_price)?.toLocaleString()}</p></div>
              </div>

              <div className="bg-slate-50 rounded-lg p-4">
                <h4 className="font-medium mb-3">Payment Details</h4>
                <div className="space-y-2 text-sm">
                  <div className="flex justify-between"><span>Total</span><span>₹{(selectedOrder.order_type === 'job_work' ? selectedOrder.summary?.grand_total : selectedOrder.total_price)?.toLocaleString()}</span></div>
                  {(selectedOrder.advance_amount > 0 || selectedOrder.advance_paid > 0) && (
                    <div className="flex justify-between text-green-600"><span>Advance Paid</span><span>₹{(selectedOrder.advance_amount || selectedOrder.advance_paid)?.toLocaleString()}</span></div>
                  )}
                  {getRemainingAmount(selectedOrder) > 0 && (
                    <div className="flex justify-between text-amber-600 font-medium"><span>Remaining</span><span>₹{getRemainingAmount(selectedOrder)?.toLocaleString()}</span></div>
                  )}
                </div>
              </div>

              <Button className="w-full" onClick={() => setSelectedOrder(null)}>Close</Button>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Share Modal */}
      <ShareModal 
        isOpen={!!shareModalOrder}
        onClose={() => setShareModalOrder(null)}
        order={shareModalOrder?.order}
        isJobWork={shareModalOrder?.isJobWork}
      />

      {/* Edit Profile Modal */}
      {showEditProfile && editingProfile && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4" data-testid="edit-profile-modal">
          <div className="bg-white rounded-xl w-full max-w-3xl max-h-[90vh] overflow-hidden flex flex-col">
            {/* Modal Header */}
            <div className="px-6 py-4 border-b bg-gradient-to-r from-teal-600 to-blue-600 flex items-center justify-between">
              <h2 className="text-xl font-semibold text-white">Edit Your Profile</h2>
              <button onClick={() => setShowEditProfile(false)} className="text-white/80 hover:text-white">
                <X className="w-5 h-5" />
              </button>
            </div>

            {/* Modal Body */}
            <div className="flex-1 overflow-y-auto p-6 space-y-6">
              {/* 1. Basic Identity */}
              <div className="space-y-4">
                <h3 className="font-semibold text-gray-800 border-b pb-2 flex items-center gap-2">
                  <User className="w-5 h-5 text-teal-600" />
                  1. Basic Identity
                </h3>
                <div className="grid md:grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Customer Type</label>
                    <select
                      value={editingProfile.customer_type}
                      onChange={(e) => setEditingProfile({ ...editingProfile, customer_type: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-teal-500"
                    >
                      <option value="individual">Individual / Retail</option>
                      <option value="proprietor">Proprietor</option>
                      <option value="partnership">Partnership</option>
                      <option value="pvt_ltd">Pvt Ltd</option>
                      <option value="ltd">Ltd</option>
                      <option value="builder">Builder</option>
                      <option value="dealer">Dealer</option>
                      <option value="architect">Architect</option>
                    </select>
                  </div>
                  
                  {/* Company Name - Show when GST needed OR company type */}
                  {(editingProfile.needs_gst_invoice || ['pvt_ltd', 'ltd'].includes(editingProfile.customer_type)) && (
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">Company / Firm Name *</label>
                      <input
                        type="text"
                        value={editingProfile.company_name}
                        onChange={(e) => setEditingProfile({ ...editingProfile, company_name: e.target.value })}
                        className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-teal-500"
                        placeholder="Company Name"
                      />
                    </div>
                  )}
                  {/* Individual Name - Show for retail without GST requirement */}
                  {!editingProfile.needs_gst_invoice && !['pvt_ltd', 'ltd'].includes(editingProfile.customer_type) && (
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">Your Name *</label>
                      <input
                        type="text"
                        value={editingProfile.individual_name}
                        onChange={(e) => setEditingProfile({ ...editingProfile, individual_name: e.target.value })}
                        className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-teal-500"
                        placeholder="Your Name"
                      />
                    </div>
                  )}
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Contact Person</label>
                    <input
                      type="text"
                      value={editingProfile.contact_person}
                      onChange={(e) => setEditingProfile({ ...editingProfile, contact_person: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-teal-500"
                      placeholder="Contact Person"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      <Phone className="w-3 h-3 inline mr-1" />
                      Mobile Number *
                    </label>
                    <input
                      type="tel"
                      value={editingProfile.mobile}
                      onChange={(e) => setEditingProfile({ ...editingProfile, mobile: e.target.value.replace(/\D/g, '').slice(0, 10) })}
                      className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-teal-500"
                      placeholder="10-digit mobile"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      <Mail className="w-3 h-3 inline mr-1" />
                      Email ID
                    </label>
                    <input
                      type="email"
                      value={editingProfile.email}
                      onChange={(e) => setEditingProfile({ ...editingProfile, email: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-teal-500"
                      placeholder="email@example.com"
                    />
                  </div>
                </div>
              </div>

              {/* 2. GST & Tax Details */}
              <div className="space-y-4">
                <h3 className="font-semibold text-gray-800 border-b pb-2 flex items-center gap-2">
                  <FileText className="w-5 h-5 text-blue-600" />
                  2. GST & Tax Details
                </h3>
                
                {/* GST Checkbox - Show for non-company types */}
                {!['pvt_ltd', 'ltd'].includes(editingProfile.customer_type) && (
                  <div className="p-4 bg-blue-50 rounded-lg border border-blue-200">
                    <label className="flex items-center gap-3 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={editingProfile.needs_gst_invoice || false}
                        onChange={(e) => {
                          const checked = e.target.checked;
                          setEditingProfile(prev => ({
                            ...prev,
                            needs_gst_invoice: checked,
                            gst_type: checked ? 'regular' : 'unregistered'
                          }));
                        }}
                        className="w-5 h-5 rounded border-blue-400 text-blue-600 focus:ring-blue-500"
                      />
                      <div>
                        <span className="font-medium text-blue-800">Do you have GSTIN / Do you need GST Invoice?</span>
                        <p className="text-xs text-blue-600 mt-0.5">
                          {editingProfile.needs_gst_invoice 
                            ? '✓ B2B Invoice will be generated with GST details' 
                            : 'B2C Invoice will be generated in your name'}
                        </p>
                      </div>
                    </label>
                  </div>
                )}
                
                {/* Company type note */}
                {['pvt_ltd', 'ltd'].includes(editingProfile.customer_type) && (
                  <div className="p-3 bg-purple-50 rounded-lg text-sm text-purple-700">
                    <strong>Company Account:</strong> GST Invoice (B2B) is mandatory for Pvt Ltd / Ltd companies
                  </div>
                )}

                {/* GST Fields - Show only when needed */}
                {(editingProfile.needs_gst_invoice || ['pvt_ltd', 'ltd'].includes(editingProfile.customer_type)) && (
                  <div className="grid md:grid-cols-2 gap-4 p-4 bg-gray-50 rounded-lg border">
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">GSTIN Number *</label>
                      <input
                        type="text"
                        value={editingProfile.gstin}
                        onChange={(e) => setEditingProfile({ ...editingProfile, gstin: e.target.value.toUpperCase() })}
                        className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-teal-500 font-mono"
                        placeholder="22AAAAA0000A1Z5"
                        maxLength={15}
                      />
                      <p className="text-xs text-gray-500 mt-1">Format: State Code + PAN + Entity + Z + Checksum</p>
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">PAN Number *</label>
                      <input
                        type="text"
                        value={editingProfile.pan}
                        onChange={(e) => setEditingProfile({ ...editingProfile, pan: e.target.value.toUpperCase() })}
                        className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-teal-500 font-mono"
                        placeholder="AAAAA0000A"
                        maxLength={10}
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">GST Type</label>
                      <select
                        value={editingProfile.gst_type}
                        onChange={(e) => setEditingProfile({ ...editingProfile, gst_type: e.target.value })}
                        className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-teal-500"
                      >
                        <option value="regular">Regular</option>
                        <option value="composition">Composition</option>
                      </select>
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">Place of Supply</label>
                      <select
                        value={editingProfile.place_of_supply}
                        onChange={(e) => setEditingProfile({ ...editingProfile, place_of_supply: e.target.value })}
                        className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-teal-500"
                      >
                        <option value="">Select State</option>
                        {states.map(s => (
                          <option key={s.code} value={s.name}>{s.name}</option>
                        ))}
                      </select>
                    </div>
                  </div>
                )}
                
                {/* B2C Note when GST not needed */}
                {!editingProfile.needs_gst_invoice && !['pvt_ltd', 'ltd'].includes(editingProfile.customer_type) && (
                  <div className="p-3 bg-green-50 rounded-lg text-sm text-green-700">
                    <strong>Retail Customer (B2C):</strong> Invoice will be generated in your individual name. GST will be included in the price.
                  </div>
                )}
              </div>

              {/* 3. Billing Address */}
              <div className="space-y-4">
                <h3 className="font-semibold text-gray-800 border-b pb-2 flex items-center gap-2">
                  <MapPin className="w-5 h-5 text-orange-600" />
                  3. Billing Address (Mandatory)
                </h3>
                <div className="grid md:grid-cols-2 gap-4">
                  <div className="md:col-span-2">
                    <label className="block text-sm font-medium text-gray-700 mb-1">Address Line 1 *</label>
                    <input
                      type="text"
                      value={editingProfile.billing_address?.address_line1 || ''}
                      onChange={(e) => setEditingProfile({
                        ...editingProfile,
                        billing_address: { ...editingProfile.billing_address, address_line1: e.target.value }
                      })}
                      className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-teal-500"
                      placeholder="Street Address"
                    />
                  </div>
                  <div className="md:col-span-2">
                    <label className="block text-sm font-medium text-gray-700 mb-1">Address Line 2</label>
                    <input
                      type="text"
                      value={editingProfile.billing_address?.address_line2 || ''}
                      onChange={(e) => setEditingProfile({
                        ...editingProfile,
                        billing_address: { ...editingProfile.billing_address, address_line2: e.target.value }
                      })}
                      className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-teal-500"
                      placeholder="Area, Landmark"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">City *</label>
                    <input
                      type="text"
                      value={editingProfile.billing_address?.city || ''}
                      onChange={(e) => setEditingProfile({
                        ...editingProfile,
                        billing_address: { ...editingProfile.billing_address, city: e.target.value }
                      })}
                      className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-teal-500"
                      placeholder="City"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">State *</label>
                    <select
                      value={editingProfile.billing_address?.state || ''}
                      onChange={(e) => {
                        const selected = states.find(s => s.name === e.target.value);
                        setEditingProfile({
                          ...editingProfile,
                          billing_address: {
                            ...editingProfile.billing_address,
                            state: e.target.value,
                            state_code: selected?.code || ''
                          }
                        });
                      }}
                      className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-teal-500"
                    >
                      <option value="">Select State</option>
                      {states.map(s => (
                        <option key={s.code} value={s.name}>{s.name}</option>
                      ))}
                    </select>
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">PIN Code *</label>
                    <input
                      type="text"
                      value={editingProfile.billing_address?.pin_code || ''}
                      onChange={(e) => setEditingProfile({
                        ...editingProfile,
                        billing_address: { ...editingProfile.billing_address, pin_code: e.target.value.replace(/\D/g, '').slice(0, 6) }
                      })}
                      className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-teal-500"
                      placeholder="6-digit PIN"
                      maxLength={6}
                    />
                  </div>
                </div>
              </div>

              {/* 4. Shipping Addresses */}
              <div className="space-y-4">
                <h3 className="font-semibold text-gray-800 border-b pb-2 flex items-center justify-between">
                  <span className="flex items-center gap-2">
                    <Truck className="w-5 h-5 text-green-600" />
                    4. Shipping / Site Addresses (Optional)
                  </span>
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    onClick={handleAddShippingAddress}
                    className="text-xs"
                  >
                    <Plus className="w-3 h-3 mr-1" /> Add Address
                  </Button>
                </h3>
                <p className="text-sm text-gray-500">Transport charges will be calculated based on shipping location</p>
                
                {editingProfile.shipping_addresses?.map((addr, idx) => (
                  <div key={addr.id || idx} className="p-4 border rounded-lg bg-gray-50 space-y-3">
                    <div className="flex justify-between items-center">
                      <span className="text-sm font-medium text-gray-700">Shipping Address #{idx + 1}</span>
                      <button
                        onClick={() => handleRemoveShippingAddress(idx)}
                        className="text-red-500 hover:text-red-700 text-sm"
                      >
                        Remove
                      </button>
                    </div>
                    <div className="grid md:grid-cols-2 gap-3">
                      <div>
                        <label className="block text-xs text-gray-600 mb-1">Project / Site Name</label>
                        <input
                          type="text"
                          value={addr.site_name || ''}
                          onChange={(e) => handleUpdateShippingAddress(idx, 'site_name', e.target.value)}
                          className="w-full px-2 py-1.5 text-sm border rounded focus:ring-2 focus:ring-teal-500"
                          placeholder="Site Name"
                        />
                      </div>
                      <div>
                        <label className="block text-xs text-gray-600 mb-1">Contact (Supervisor/Manager)</label>
                        <input
                          type="text"
                          value={addr.contact_person || ''}
                          onChange={(e) => handleUpdateShippingAddress(idx, 'contact_person', e.target.value)}
                          className="w-full px-2 py-1.5 text-sm border rounded focus:ring-2 focus:ring-teal-500"
                          placeholder="Contact Person"
                        />
                      </div>
                      <div className="md:col-span-2">
                        <label className="block text-xs text-gray-600 mb-1">Address</label>
                        <input
                          type="text"
                          value={addr.address_line1 || ''}
                          onChange={(e) => handleUpdateShippingAddress(idx, 'address_line1', e.target.value)}
                          className="w-full px-2 py-1.5 text-sm border rounded focus:ring-2 focus:ring-teal-500"
                          placeholder="Full Address"
                        />
                      </div>
                      <div>
                        <label className="block text-xs text-gray-600 mb-1">City</label>
                        <input
                          type="text"
                          value={addr.city || ''}
                          onChange={(e) => handleUpdateShippingAddress(idx, 'city', e.target.value)}
                          className="w-full px-2 py-1.5 text-sm border rounded focus:ring-2 focus:ring-teal-500"
                          placeholder="City"
                        />
                      </div>
                      <div>
                        <label className="block text-xs text-gray-600 mb-1">State</label>
                        <select
                          value={addr.state || ''}
                          onChange={(e) => handleUpdateShippingAddress(idx, 'state', e.target.value)}
                          className="w-full px-2 py-1.5 text-sm border rounded focus:ring-2 focus:ring-teal-500"
                        >
                          <option value="">Select</option>
                          {states.map(s => (
                            <option key={s.code} value={s.name}>{s.name}</option>
                          ))}
                        </select>
                      </div>
                      <div>
                        <label className="block text-xs text-gray-600 mb-1">PIN Code</label>
                        <input
                          type="text"
                          value={addr.pin_code || ''}
                          onChange={(e) => handleUpdateShippingAddress(idx, 'pin_code', e.target.value.replace(/\D/g, '').slice(0, 6))}
                          className="w-full px-2 py-1.5 text-sm border rounded focus:ring-2 focus:ring-teal-500"
                          placeholder="PIN"
                          maxLength={6}
                        />
                      </div>
                      <div>
                        <label className="block text-xs text-gray-600 mb-1">Contact Phone</label>
                        <input
                          type="tel"
                          value={addr.contact_phone || ''}
                          onChange={(e) => handleUpdateShippingAddress(idx, 'contact_phone', e.target.value.replace(/\D/g, '').slice(0, 10))}
                          className="w-full px-2 py-1.5 text-sm border rounded focus:ring-2 focus:ring-teal-500"
                          placeholder="Phone"
                        />
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* Modal Footer */}
            <div className="px-6 py-4 border-t bg-gray-50 flex justify-between">
              <button
                onClick={() => setShowEditProfile(false)}
                className="px-4 py-2 text-gray-600 hover:text-gray-800"
              >
                Cancel
              </button>
              <Button
                onClick={handleSaveProfile}
                disabled={savingProfile}
                className="flex items-center gap-2"
                data-testid="save-profile-btn"
              >
                {savingProfile ? (
                  <>
                    <Loader2 className="w-4 h-4 animate-spin" />
                    Saving...
                  </>
                ) : (
                  <>
                    <Save className="w-4 h-4" />
                    Save Profile
                  </>
                )}
              </Button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default CustomerPortalEnhanced;
