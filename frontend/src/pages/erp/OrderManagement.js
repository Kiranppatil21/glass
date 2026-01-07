import React, { useState, useEffect } from 'react';
import { Card, CardContent } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { 
  Package, IndianRupee, Truck, FileText, CheckCircle, Clock, AlertCircle,
  Filter, Download, Search, Eye, Printer, CreditCard, MapPin, User, Building,
  Car, Phone, X, Send, Loader2, Share2
} from 'lucide-react';
import { toast } from 'sonner';
import erpApi from '../../utils/erpApi';
import ShareModal from '../../components/ShareModal';

const OrderManagement = () => {
  const [orders, setOrders] = useState([]);
  const [loading, setLoading] = useState(true);
  const [statusFilter, setStatusFilter] = useState('all');
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedOrder, setSelectedOrder] = useState(null);
  const [showOrderModal, setShowOrderModal] = useState(false);
  const [showTransportModal, setShowTransportModal] = useState(false);
  const [showDispatchModal, setShowDispatchModal] = useState(false);
  const [shareModalOrder, setShareModalOrder] = useState(null);
  const [transportData, setTransportData] = useState({
    transport_charge: '',
    transport_note: 'Transport charge extra applicable',
    vehicle_type: ''
  });
  const [transportReport, setTransportReport] = useState(null);
  const [vehicleReport, setVehicleReport] = useState(null);
  const [cashData, setCashData] = useState(null);
  const [pnlReport, setPnlReport] = useState(null);
  const [activeTab, setActiveTab] = useState('orders');
  
  // Dispatch modal state
  const [vehicles, setVehicles] = useState([]);
  const [drivers, setDrivers] = useState([]);
  const [dispatchForm, setDispatchForm] = useState({
    vehicle_id: '',
    driver_id: '',
    estimated_delivery_time: '',
    notes: ''
  });
  const [dispatchLoading, setDispatchLoading] = useState(false);

  useEffect(() => {
    fetchOrders();
  }, [statusFilter]);

  const fetchOrders = async () => {
    try {
      setLoading(true);
      // Use direct axios call since admin/orders is not under /erp prefix
      const token = localStorage.getItem('token');
      const response = await fetch(`${process.env.REACT_APP_BACKEND_URL}/api/admin/orders`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      const data = await response.json();
      let filteredOrders = Array.isArray(data) ? data : [];
      
      if (statusFilter !== 'all') {
        filteredOrders = filteredOrders.filter(o => o.status === statusFilter);
      }
      
      setOrders(filteredOrders);
    } catch (error) {
      console.error('Failed to fetch orders:', error);
      toast.error('Failed to load orders');
    } finally {
      setLoading(false);
    }
  };

  const fetchTransportReport = async () => {
    try {
      const token = localStorage.getItem('token');
      const response = await fetch(`${process.env.REACT_APP_BACKEND_URL}/api/reports/transport-charges`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      const data = await response.json();
      setTransportReport(data);
    } catch (error) {
      console.error('Failed to fetch transport report:', error);
      toast.error('Failed to load transport report');
    }
  };

  const fetchVehicleReport = async () => {
    try {
      const token = localStorage.getItem('token');
      const response = await fetch(`${process.env.REACT_APP_BACKEND_URL}/api/reports/vehicle-expenses`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      const data = await response.json();
      setVehicleReport(data);
    } catch (error) {
      console.error('Failed to fetch vehicle report:', error);
      toast.error('Failed to load vehicle report');
    }
  };

  const fetchCashBalance = async () => {
    try {
      const token = localStorage.getItem('token');
      const response = await fetch(`${process.env.REACT_APP_BACKEND_URL}/api/erp/cash/balance`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      const data = await response.json();
      setCashData(data);
    } catch (error) {
      console.error('Failed to fetch cash balance:', error);
      toast.error('Failed to load cash data');
    }
  };

  const fetchPnlReport = async () => {
    try {
      const token = localStorage.getItem('token');
      const response = await fetch(`${process.env.REACT_APP_BACKEND_URL}/api/erp/cash/pnl`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      const data = await response.json();
      setPnlReport(data);
    } catch (error) {
      console.error('Failed to fetch P&L report:', error);
      toast.error('Failed to load P&L report');
    }
  };

  const handleMarkCashReceived = async (orderId) => {
    if (!window.confirm('Are you sure you want to mark cash as received for this order?')) return;
    
    try {
      const token = localStorage.getItem('token');
      const response = await fetch(`${process.env.REACT_APP_BACKEND_URL}/api/orders/${orderId}/mark-cash-received`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}` }
      });
      const data = await response.json();
      if (!response.ok) throw new Error(data.detail || 'Failed');
      toast.success(`Cash received! Amount: ₹${data.amount_received?.toLocaleString()}`);
      fetchOrders();
      setShowOrderModal(false);
    } catch (error) {
      toast.error(error.message || 'Failed to mark cash received');
    }
  };

  const handleAddTransportCharge = async () => {
    if (!selectedOrder || !transportData.transport_charge) {
      toast.error('Please enter transport charge');
      return;
    }
    
    try {
      const token = localStorage.getItem('token');
      const response = await fetch(`${process.env.REACT_APP_BACKEND_URL}/api/orders/${selectedOrder.id}/add-transport-charge`, {
        method: 'POST',
        headers: { 
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          transport_charge: parseFloat(transportData.transport_charge),
          transport_note: transportData.transport_note,
          vehicle_type: transportData.vehicle_type
        })
      });
      const data = await response.json();
      if (!response.ok) throw new Error(data.detail || 'Failed');
      toast.success('Transport charge added successfully');
      fetchOrders();
      setShowTransportModal(false);
      setTransportData({ transport_charge: '', transport_note: 'Transport charge extra applicable', vehicle_type: '' });
    } catch (error) {
      toast.error(error.message || 'Failed to add transport charge');
    }
  };

  const handleCreateDispatchSlip = async (orderId) => {
    try {
      const token = localStorage.getItem('token');
      const response = await fetch(`${process.env.REACT_APP_BACKEND_URL}/api/orders/${orderId}/create-dispatch-slip`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}` }
      });
      const data = await response.json();
      if (!response.ok) throw new Error(data.detail || 'Failed');
      toast.success(`Dispatch Slip Created: ${data.dispatch_slip_number}`);
      fetchOrders();
      setShowOrderModal(false);
    } catch (error) {
      toast.error(error.message || 'Failed to create dispatch slip');
    }
  };

  const handleMarkDispatched = async (orderId) => {
    try {
      const token = localStorage.getItem('token');
      const response = await fetch(`${process.env.REACT_APP_BACKEND_URL}/api/orders/${orderId}/mark-dispatched`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}` }
      });
      const data = await response.json();
      if (!response.ok) throw new Error(data.detail || 'Failed');
      toast.success('Order marked as dispatched');
      fetchOrders();
      setShowOrderModal(false);
    } catch (error) {
      toast.error(error.message || 'Failed to mark as dispatched');
    }
  };

  // Open dispatch modal and fetch vehicles/drivers
  const openDispatchModal = async (order) => {
    setSelectedOrder(order);
    setDispatchForm({ vehicle_id: '', driver_id: '', estimated_delivery_time: '', notes: '' });
    setShowDispatchModal(true);
    
    try {
      const [vehiclesRes, driversRes] = await Promise.all([
        erpApi.transport.getVehicles('available'),
        erpApi.transport.getDrivers('available')
      ]);
      setVehicles(vehiclesRes.data.vehicles || []);
      setDrivers(driversRes.data.drivers || []);
    } catch (error) {
      console.error('Failed to fetch vehicles/drivers:', error);
      toast.error('Failed to load vehicles and drivers');
    }
  };

  // Submit dispatch
  const handleSubmitDispatch = async () => {
    if (!dispatchForm.vehicle_id || !dispatchForm.driver_id) {
      toast.error('Please select both vehicle and driver');
      return;
    }
    
    setDispatchLoading(true);
    try {
      await erpApi.transport.createDispatch({
        order_id: selectedOrder.id,
        vehicle_id: dispatchForm.vehicle_id,
        driver_id: dispatchForm.driver_id,
        estimated_delivery_time: dispatchForm.estimated_delivery_time,
        notes: dispatchForm.notes
      });
      
      toast.success('Order dispatched! Customer notified via WhatsApp & Email');
      setShowDispatchModal(false);
      setShowOrderModal(false);
      fetchOrders();
    } catch (error) {
      console.error('Dispatch failed:', error);
      toast.error(error.response?.data?.detail || 'Failed to dispatch order');
    } finally {
      setDispatchLoading(false);
    }
  };

  // Helper to check if payment is fully settled
  const isPaymentSettled = (order) => {
    if (!order) return false;
    // Full payment completed
    if (order.payment_status === 'completed') return true;
    // 100% advance paid
    if (order.advance_percent === 100 && order.advance_payment_status === 'paid') return true;
    // Advance paid + Remaining paid (online or cash)
    if (order.advance_payment_status === 'paid' && 
        (order.remaining_payment_status === 'paid' || order.remaining_payment_status === 'cash_received')) {
      return true;
    }
    return false;
  };

  const handleDownloadDispatchSlip = async (orderId) => {
    try {
      const token = localStorage.getItem('token');
      const response = await fetch(`${process.env.REACT_APP_BACKEND_URL}/api/erp/pdf/dispatch-slip/${orderId}`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      if (!response.ok) throw new Error('Failed to generate PDF');
      
      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `dispatch_slip_${orderId}.pdf`;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      a.remove();
      toast.success('Dispatch slip downloaded!');
    } catch (error) {
      toast.error(error.message || 'Failed to download dispatch slip');
    }
  };

  const handleDownloadInvoice = async (orderId) => {
    try {
      const token = localStorage.getItem('token');
      const response = await fetch(`${process.env.REACT_APP_BACKEND_URL}/api/erp/pdf/invoice/${orderId}`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      if (!response.ok) throw new Error('Failed to generate invoice');
      
      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `invoice_${orderId}.pdf`;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      a.remove();
      toast.success('Invoice downloaded!');
    } catch (error) {
      toast.error(error.message || 'Failed to download invoice');
    }
  };

  const handleDownloadDayBook = async () => {
    try {
      const token = localStorage.getItem('token');
      const today = new Date().toISOString().split('T')[0];
      const response = await fetch(`${process.env.REACT_APP_BACKEND_URL}/api/erp/pdf/cash-daybook?date=${today}`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      if (!response.ok) throw new Error('Failed to generate Day Book');
      
      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `cash_daybook_${today}.pdf`;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      a.remove();
      toast.success('Cash Day Book downloaded!');
    } catch (error) {
      toast.error(error.message || 'Failed to download day book');
    }
  };

  const getStatusBadge = (status) => {
    const statusConfig = {
      pending: { color: 'bg-yellow-100 text-yellow-700', icon: Clock, label: 'Pending' },
      confirmed: { color: 'bg-blue-100 text-blue-700', icon: CheckCircle, label: 'Confirmed' },
      processing: { color: 'bg-purple-100 text-purple-700', icon: Package, label: 'Processing' },
      ready_for_dispatch: { color: 'bg-teal-100 text-teal-700', icon: Truck, label: 'Ready for Dispatch' },
      dispatched: { color: 'bg-indigo-100 text-indigo-700', icon: Truck, label: 'Dispatched' },
      delivered: { color: 'bg-green-100 text-green-700', icon: CheckCircle, label: 'Delivered' },
      cancelled: { color: 'bg-red-100 text-red-700', icon: AlertCircle, label: 'Cancelled' }
    };
    const config = statusConfig[status] || statusConfig.pending;
    const Icon = config.icon;
    return (
      <span className={`px-2 py-1 rounded-full text-xs font-medium flex items-center gap-1 ${config.color}`}>
        <Icon className="w-3 h-3" /> {config.label}
      </span>
    );
  };

  const getPaymentBadge = (order) => {
    if (order.payment_status === 'completed') {
      return <span className="px-2 py-1 rounded-full text-xs bg-green-100 text-green-700">✅ Fully Paid</span>;
    }
    if (order.advance_payment_status === 'paid' && order.remaining_payment_status !== 'paid' && order.remaining_payment_status !== 'cash_received') {
      return <span className="px-2 py-1 rounded-full text-xs bg-amber-100 text-amber-700">💳 {order.advance_percent}% Advance Paid</span>;
    }
    if (order.remaining_payment_status === 'cash_received') {
      return <span className="px-2 py-1 rounded-full text-xs bg-green-100 text-green-700">💵 Cash Received</span>;
    }
    return <span className="px-2 py-1 rounded-full text-xs bg-red-100 text-red-700">⏳ Payment Pending</span>;
  };

  const filteredOrders = orders.filter(order => {
    if (!searchTerm) return true;
    const search = searchTerm.toLowerCase();
    return (
      order.order_number?.toLowerCase().includes(search) ||
      order.customer_name?.toLowerCase().includes(search) ||
      order.company_name?.toLowerCase().includes(search) ||
      order.product_name?.toLowerCase().includes(search)
    );
  });

  const vehicleTypes = ['Truck', 'Tempo', 'Pickup', 'Auto', 'Bike', 'Other'];

  return (
    <div className="p-6 space-y-6" data-testid="order-management">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Order Management</h1>
          <p className="text-slate-600">Manage orders, payments, transport & dispatch</p>
        </div>
      </div>

      {/* Tabs */}
      <div className="flex gap-2 border-b border-slate-200 overflow-x-auto">
        {['orders', 'cash', 'transport', 'vehicle-expenses', 'pnl'].map(tab => (
          <button
            key={tab}
            onClick={() => {
              setActiveTab(tab);
              if (tab === 'transport') fetchTransportReport();
              if (tab === 'vehicle-expenses') fetchVehicleReport();
              if (tab === 'cash') fetchCashBalance();
              if (tab === 'pnl') fetchPnlReport();
            }}
            className={`px-4 py-2 font-medium border-b-2 transition-colors whitespace-nowrap ${
              activeTab === tab
                ? 'border-teal-600 text-teal-600'
                : 'border-transparent text-slate-500 hover:text-slate-700'
            }`}
          >
            {tab === 'orders' && '📦 Orders'}
            {tab === 'cash' && '💵 Cash Management'}
            {tab === 'transport' && '🚚 Transport Report'}
            {tab === 'vehicle-expenses' && '🚗 Vehicle Expenses'}
            {tab === 'pnl' && '📊 P&L Report'}
          </button>
        ))}
      </div>

      {/* Orders Tab */}
      {activeTab === 'orders' && (
        <>
          {/* Filters */}
          <Card>
            <CardContent className="p-4">
              <div className="flex flex-wrap gap-4 items-center">
                <div className="flex-1 min-w-[200px]">
                  <div className="relative">
                    <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
                    <input
                      type="text"
                      placeholder="Search by order #, customer, company..."
                      value={searchTerm}
                      onChange={(e) => setSearchTerm(e.target.value)}
                      className="w-full pl-10 pr-4 py-2 border border-slate-300 rounded-lg"
                    />
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  <Filter className="w-4 h-4 text-slate-400" />
                  <select
                    value={statusFilter}
                    onChange={(e) => setStatusFilter(e.target.value)}
                    className="border border-slate-300 rounded-lg px-3 py-2"
                  >
                    <option value="all">All Status</option>
                    <option value="pending">Pending</option>
                    <option value="confirmed">Confirmed</option>
                    <option value="processing">Processing</option>
                    <option value="ready_for_dispatch">Ready for Dispatch</option>
                    <option value="dispatched">Dispatched</option>
                    <option value="delivered">Delivered</option>
                  </select>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Stats */}
          <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
            <Card className="bg-slate-50">
              <CardContent className="p-4 text-center">
                <p className="text-2xl font-bold text-slate-700">{orders.length}</p>
                <p className="text-xs text-slate-500">Total Orders</p>
              </CardContent>
            </Card>
            <Card className="bg-yellow-50">
              <CardContent className="p-4 text-center">
                <p className="text-2xl font-bold text-yellow-700">
                  {orders.filter(o => o.advance_payment_status === 'paid' && o.remaining_payment_status === 'pending').length}
                </p>
                <p className="text-xs text-yellow-600">Awaiting Payment</p>
              </CardContent>
            </Card>
            <Card className="bg-teal-50">
              <CardContent className="p-4 text-center">
                <p className="text-2xl font-bold text-teal-700">
                  {orders.filter(o => o.status === 'ready_for_dispatch').length}
                </p>
                <p className="text-xs text-teal-600">Ready to Dispatch</p>
              </CardContent>
            </Card>
            <Card className="bg-green-50">
              <CardContent className="p-4 text-center">
                <p className="text-2xl font-bold text-green-700">
                  ₹{orders.filter(o => o.payment_status === 'completed').reduce((a, b) => a + (b.total_price || 0), 0).toLocaleString()}
                </p>
                <p className="text-xs text-green-600">Collected</p>
              </CardContent>
            </Card>
            <Card className="bg-blue-50">
              <CardContent className="p-4 text-center">
                <p className="text-2xl font-bold text-blue-700">
                  ₹{orders.reduce((a, b) => a + (b.transport_charge || 0), 0).toLocaleString()}
                </p>
                <p className="text-xs text-blue-600">Transport Charges</p>
              </CardContent>
            </Card>
          </div>

          {/* Orders Table */}
          <Card>
            <CardContent className="p-0">
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead className="bg-slate-50 border-b">
                    <tr>
                      <th className="text-left p-4 font-medium text-slate-600">Order #</th>
                      <th className="text-left p-4 font-medium text-slate-600">Customer</th>
                      <th className="text-left p-4 font-medium text-slate-600">Product</th>
                      <th className="text-right p-4 font-medium text-slate-600">Amount</th>
                      <th className="text-center p-4 font-medium text-slate-600">Payment</th>
                      <th className="text-center p-4 font-medium text-slate-600">Status</th>
                      <th className="text-center p-4 font-medium text-slate-600">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filteredOrders.map((order) => (
                      <tr key={order.id} className="border-b hover:bg-slate-50">
                        <td className="p-4">
                          <span className="font-mono font-bold text-teal-600">#{order.order_number || order.id?.slice(0, 8)}</span>
                          {order.dispatch_slip_number && (
                            <p className="text-xs text-slate-500 mt-1">📄 {order.dispatch_slip_number}</p>
                          )}
                        </td>
                        <td className="p-4">
                          <p className="font-medium text-slate-900">{order.customer_name || 'N/A'}</p>
                          {order.company_name && (
                            <p className="text-xs text-slate-500">{order.company_name}</p>
                          )}
                        </td>
                        <td className="p-4">
                          <p className="text-slate-900">{order.product_name}</p>
                          <p className="text-xs text-slate-500">
                            {order.width}" × {order.height}" × {order.thickness}mm • Qty: {order.quantity}
                          </p>
                        </td>
                        <td className="p-4 text-right">
                          <p className="font-bold text-slate-900">₹{order.total_price?.toLocaleString()}</p>
                          {order.transport_charge > 0 && (
                            <p className="text-xs text-blue-600">+₹{order.transport_charge} transport</p>
                          )}
                          {order.advance_percent < 100 && (
                            <p className="text-xs text-slate-500">
                              Adv: ₹{order.advance_amount?.toLocaleString()} | Rem: ₹{order.remaining_amount?.toLocaleString()}
                            </p>
                          )}
                        </td>
                        <td className="p-4 text-center">{getPaymentBadge(order)}</td>
                        <td className="p-4 text-center">{getStatusBadge(order.status)}</td>
                        <td className="p-4 text-center">
                          <Button
                            size="sm"
                            variant="outline"
                            onClick={() => { setSelectedOrder(order); setShowOrderModal(true); }}
                          >
                            <Eye className="w-4 h-4 mr-1" /> View
                          </Button>
                          <Button
                            size="sm"
                            variant="outline"
                            className="ml-2 text-purple-600 border-purple-200 hover:bg-purple-50"
                            onClick={() => setShareModalOrder({ order, isJobWork: false })}
                          >
                            <Share2 className="w-4 h-4 mr-1" /> Share
                          </Button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </CardContent>
          </Card>
        </>
      )}

      {/* Transport Report Tab */}
      {activeTab === 'transport' && transportReport && (
        <div className="space-y-6">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <Card className="bg-blue-50">
              <CardContent className="p-6">
                <Truck className="w-8 h-8 text-blue-600 mb-2" />
                <p className="text-3xl font-bold text-blue-700">{transportReport.summary?.total_orders_with_transport}</p>
                <p className="text-blue-600">Orders with Transport</p>
              </CardContent>
            </Card>
            <Card className="bg-green-50">
              <CardContent className="p-6">
                <IndianRupee className="w-8 h-8 text-green-600 mb-2" />
                <p className="text-3xl font-bold text-green-700">₹{transportReport.summary?.total_transport_collected?.toLocaleString()}</p>
                <p className="text-green-600">Total Collected</p>
              </CardContent>
            </Card>
          </div>

          <Card>
            <CardContent className="p-6">
              <h3 className="font-bold text-slate-900 mb-4">Vehicle-wise Breakdown</h3>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                {Object.entries(transportReport.vehicle_wise || {}).map(([vehicle, data]) => (
                  <div key={vehicle} className="p-4 bg-slate-50 rounded-lg">
                    <p className="font-medium text-slate-900">{vehicle}</p>
                    <p className="text-2xl font-bold text-teal-600">₹{data.total?.toLocaleString()}</p>
                    <p className="text-xs text-slate-500">{data.count} orders</p>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardContent className="p-6">
              <h3 className="font-bold text-slate-900 mb-4">Transport Charges Details</h3>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="bg-slate-50">
                    <tr>
                      <th className="text-left p-3">Order #</th>
                      <th className="text-left p-3">Customer</th>
                      <th className="text-left p-3">Vehicle</th>
                      <th className="text-right p-3">Charge</th>
                      <th className="text-left p-3">Added By</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(transportReport.orders || []).map((order, idx) => (
                      <tr key={idx} className="border-b">
                        <td className="p-3 font-mono">#{order.order_number}</td>
                        <td className="p-3">{order.customer_name}</td>
                        <td className="p-3">{order.vehicle_type || '-'}</td>
                        <td className="p-3 text-right font-bold">₹{order.transport_charge?.toLocaleString()}</td>
                        <td className="p-3">{order.added_by || '-'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Vehicle Expenses Tab */}
      {activeTab === 'vehicle-expenses' && vehicleReport && (
        <div className="space-y-6">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <Card className="bg-red-50">
              <CardContent className="p-6">
                <p className="text-sm text-red-600 mb-1">Total Expenses</p>
                <p className="text-3xl font-bold text-red-700">₹{vehicleReport.summary?.total_vehicle_expenses?.toLocaleString()}</p>
              </CardContent>
            </Card>
            <Card className="bg-green-50">
              <CardContent className="p-6">
                <p className="text-sm text-green-600 mb-1">Transport Collections</p>
                <p className="text-3xl font-bold text-green-700">₹{vehicleReport.summary?.total_transport_collections?.toLocaleString()}</p>
              </CardContent>
            </Card>
            <Card className={vehicleReport.summary?.net_profit_loss >= 0 ? 'bg-green-50' : 'bg-red-50'}>
              <CardContent className="p-6">
                <p className={`text-sm mb-1 ${vehicleReport.summary?.net_profit_loss >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                  Net {vehicleReport.summary?.net_profit_loss >= 0 ? 'Profit' : 'Loss'}
                </p>
                <p className={`text-3xl font-bold ${vehicleReport.summary?.net_profit_loss >= 0 ? 'text-green-700' : 'text-red-700'}`}>
                  ₹{Math.abs(vehicleReport.summary?.net_profit_loss || 0).toLocaleString()}
                </p>
              </CardContent>
            </Card>
          </div>

          <Card>
            <CardContent className="p-6">
              <h3 className="font-bold text-slate-900 mb-4">Vehicle-wise P&L</h3>
              <div className="space-y-4">
                {Object.entries(vehicleReport.vehicle_wise || {}).map(([vehicle, data]) => (
                  <div key={vehicle} className="p-4 bg-slate-50 rounded-lg">
                    <div className="flex items-center justify-between mb-3">
                      <h4 className="font-bold text-slate-900">🚗 {vehicle}</h4>
                      <span className={`px-3 py-1 rounded-full text-sm font-bold ${
                        data.net >= 0 ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'
                      }`}>
                        Net: ₹{data.net?.toLocaleString()}
                      </span>
                    </div>
                    <div className="grid grid-cols-3 gap-4 text-sm">
                      <div>
                        <p className="text-slate-500">Expenses</p>
                        <p className="font-bold text-red-600">₹{data.expenses?.toLocaleString()}</p>
                      </div>
                      <div>
                        <p className="text-slate-500">Collections</p>
                        <p className="font-bold text-green-600">₹{data.collections?.toLocaleString()}</p>
                      </div>
                      <div>
                        <p className="text-slate-500">Deliveries</p>
                        <p className="font-bold text-slate-700">{data.collections_count}</p>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Cash Management Tab */}
      {activeTab === 'cash' && (
        <div className="space-y-6">
          {/* Header with Print Button */}
          <div className="flex items-center justify-between">
            <h2 className="text-xl font-bold text-slate-900">💵 Cash Management</h2>
            <Button 
              variant="outline"
              onClick={handleDownloadDayBook}
              className="border-purple-600 text-purple-600 hover:bg-purple-50"
            >
              <Printer className="w-4 h-4 mr-2" /> Print Day Book
            </Button>
          </div>

          {/* Cash Balance Cards */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <Card className="bg-green-50 border-green-200">
              <CardContent className="p-6">
                <IndianRupee className="w-8 h-8 text-green-600 mb-2" />
                <p className="text-3xl font-bold text-green-700">₹{cashData?.current_balance?.toLocaleString() || 0}</p>
                <p className="text-green-600">Cash in Hand</p>
              </CardContent>
            </Card>
            <Card className="bg-blue-50 border-blue-200">
              <CardContent className="p-6">
                <p className="text-sm text-blue-600 mb-1">Today's Cash In</p>
                <p className="text-2xl font-bold text-blue-700">₹{cashData?.today?.cash_in?.toLocaleString() || 0}</p>
              </CardContent>
            </Card>
            <Card className="bg-red-50 border-red-200">
              <CardContent className="p-6">
                <p className="text-sm text-red-600 mb-1">Today's Cash Out</p>
                <p className="text-2xl font-bold text-red-700">₹{cashData?.today?.cash_out?.toLocaleString() || 0}</p>
              </CardContent>
            </Card>
            <Card className="bg-purple-50 border-purple-200">
              <CardContent className="p-6">
                <p className="text-sm text-purple-600 mb-1">Today's Net</p>
                <p className={`text-2xl font-bold ${(cashData?.today?.net || 0) >= 0 ? 'text-green-700' : 'text-red-700'}`}>
                  ₹{cashData?.today?.net?.toLocaleString() || 0}
                </p>
              </CardContent>
            </Card>
          </div>

          {/* Recent Transactions */}
          <Card>
            <CardContent className="p-6">
              <div className="flex items-center justify-between mb-4">
                <h3 className="font-bold text-slate-900">Recent Transactions</h3>
                <span className="text-sm text-slate-500">{cashData?.today?.transactions_count || 0} today</span>
              </div>
              <div className="space-y-3">
                {(cashData?.recent_transactions || []).map((txn, idx) => (
                  <div key={idx} className={`p-4 rounded-lg border-l-4 ${
                    txn.direction === 'in' ? 'bg-green-50 border-l-green-500' : 'bg-red-50 border-l-red-500'
                  }`}>
                    <div className="flex items-center justify-between">
                      <div>
                        <p className="font-medium text-slate-900">{txn.description}</p>
                        <p className="text-sm text-slate-500">
                          {txn.party_name && <span>{txn.party_name} • </span>}
                          <span>{txn.transaction_type?.replace('_', ' ')}</span>
                        </p>
                        <p className="text-xs text-slate-400 mt-1">
                          By {txn.recorded_by_name} • {new Date(txn.timestamp).toLocaleString()}
                        </p>
                      </div>
                      <p className={`text-xl font-bold ${txn.direction === 'in' ? 'text-green-600' : 'text-red-600'}`}>
                        {txn.direction === 'in' ? '+' : '-'}₹{txn.amount?.toLocaleString()}
                      </p>
                    </div>
                  </div>
                ))}
                {(!cashData?.recent_transactions || cashData.recent_transactions.length === 0) && (
                  <p className="text-center text-slate-500 py-8">No transactions today</p>
                )}
              </div>
            </CardContent>
          </Card>
        </div>
      )}

      {/* P&L Report Tab */}
      {activeTab === 'pnl' && pnlReport && (
        <div className="space-y-6">
          {/* P&L Summary */}
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <Card className="bg-blue-50 border-blue-200">
              <CardContent className="p-6">
                <p className="text-sm text-blue-600 mb-1">Total Revenue</p>
                <p className="text-2xl font-bold text-blue-700">₹{pnlReport.revenue?.total_revenue?.toLocaleString() || 0}</p>
                <p className="text-xs text-blue-500 mt-1">
                  Products: ₹{pnlReport.revenue?.product_sales?.toLocaleString() || 0}
                </p>
              </CardContent>
            </Card>
            <Card className="bg-green-50 border-green-200">
              <CardContent className="p-6">
                <p className="text-sm text-green-600 mb-1">Collections</p>
                <p className="text-2xl font-bold text-green-700">₹{pnlReport.collections?.total?.toLocaleString() || 0}</p>
                <p className="text-xs text-green-500 mt-1">
                  Cash: ₹{pnlReport.collections?.cash?.toLocaleString() || 0} | Online: ₹{pnlReport.collections?.online?.toLocaleString() || 0}
                </p>
              </CardContent>
            </Card>
            <Card className="bg-red-50 border-red-200">
              <CardContent className="p-6">
                <p className="text-sm text-red-600 mb-1">Total Expenses</p>
                <p className="text-2xl font-bold text-red-700">₹{pnlReport.expenses?.total?.toLocaleString() || 0}</p>
              </CardContent>
            </Card>
            <Card className={pnlReport.profit_loss?.gross_profit >= 0 ? 'bg-emerald-50 border-emerald-200' : 'bg-red-50 border-red-200'}>
              <CardContent className="p-6">
                <p className={`text-sm mb-1 ${pnlReport.profit_loss?.gross_profit >= 0 ? 'text-emerald-600' : 'text-red-600'}`}>
                  {pnlReport.profit_loss?.gross_profit >= 0 ? '📈 Gross Profit' : '📉 Loss'}
                </p>
                <p className={`text-2xl font-bold ${pnlReport.profit_loss?.gross_profit >= 0 ? 'text-emerald-700' : 'text-red-700'}`}>
                  ₹{Math.abs(pnlReport.profit_loss?.gross_profit || 0).toLocaleString()}
                </p>
                <p className="text-xs text-slate-500 mt-1">
                  Margin: {pnlReport.profit_loss?.profit_margin || 0}%
                </p>
              </CardContent>
            </Card>
          </div>

          {/* Expense Breakdown */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <Card>
              <CardContent className="p-6">
                <h3 className="font-bold text-slate-900 mb-4">💰 Revenue Breakdown</h3>
                <div className="space-y-3">
                  <div className="flex justify-between p-3 bg-blue-50 rounded-lg">
                    <span className="text-slate-700">Product Sales</span>
                    <span className="font-bold text-blue-700">₹{pnlReport.revenue?.product_sales?.toLocaleString() || 0}</span>
                  </div>
                  <div className="flex justify-between p-3 bg-teal-50 rounded-lg">
                    <span className="text-slate-700">Transport Charges</span>
                    <span className="font-bold text-teal-700">₹{pnlReport.revenue?.transport_charges?.toLocaleString() || 0}</span>
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardContent className="p-6">
                <h3 className="font-bold text-slate-900 mb-4">📉 Expenses by Category</h3>
                <div className="space-y-2">
                  {Object.entries(pnlReport.expenses?.by_category || {}).map(([category, amount]) => (
                    <div key={category} className="flex justify-between p-2 border-b border-slate-100">
                      <span className="text-slate-600 capitalize">{category.replace('_', ' ')}</span>
                      <span className="font-medium text-red-600">₹{amount?.toLocaleString() || 0}</span>
                    </div>
                  ))}
                  {Object.keys(pnlReport.expenses?.by_category || {}).length === 0 && (
                    <p className="text-slate-500 text-center py-4">No expenses recorded</p>
                  )}
                </div>
              </CardContent>
            </Card>
          </div>

          {/* Period Info */}
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center justify-between">
                <span className="text-slate-500">
                  Period: {pnlReport.period?.start} to {pnlReport.period?.end}
                </span>
                <span className="text-slate-500">
                  Total Orders: {pnlReport.orders_count || 0}
                </span>
              </div>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Order Detail Modal */}
      {showOrderModal && selectedOrder && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
          <Card className="w-full max-w-2xl max-h-[90vh] overflow-y-auto">
            <CardContent className="p-6">
              <div className="flex items-center justify-between mb-6">
                <h2 className="text-xl font-bold text-slate-900">
                  Order #{selectedOrder.order_number || selectedOrder.id?.slice(0, 8)}
                </h2>
                <Button variant="ghost" onClick={() => setShowOrderModal(false)}>✕</Button>
              </div>

              {/* Order Info */}
              <div className="grid grid-cols-2 gap-4 mb-6">
                <div>
                  <p className="text-sm text-slate-500">Customer</p>
                  <p className="font-medium">{selectedOrder.customer_name}</p>
                  {selectedOrder.company_name && (
                    <p className="text-sm text-slate-500">{selectedOrder.company_name}</p>
                  )}
                </div>
                <div>
                  <p className="text-sm text-slate-500">Product</p>
                  <p className="font-medium">{selectedOrder.product_name}</p>
                  <p className="text-sm text-slate-500">
                    {selectedOrder.width}" × {selectedOrder.height}" × {selectedOrder.thickness}mm • Qty: {selectedOrder.quantity}
                  </p>
                </div>
              </div>

              {/* Payment Info */}
              <div className="bg-slate-50 p-4 rounded-lg mb-6">
                <h3 className="font-bold text-slate-900 mb-3">💳 Payment Details</h3>
                <div className="grid grid-cols-2 gap-4 text-sm">
                  <div>
                    <p className="text-slate-500">Total Amount</p>
                    <p className="text-xl font-bold">₹{selectedOrder.total_price?.toLocaleString()}</p>
                  </div>
                  <div>
                    <p className="text-slate-500">Advance ({selectedOrder.advance_percent}%)</p>
                    <p className="font-bold text-green-600">
                      ₹{selectedOrder.advance_amount?.toLocaleString()} 
                      {selectedOrder.advance_payment_status === 'paid' && ' ✅'}
                    </p>
                  </div>
                  {selectedOrder.advance_percent < 100 && (
                    <div>
                      <p className="text-slate-500">Remaining</p>
                      <p className={`font-bold ${selectedOrder.remaining_payment_status === 'paid' || selectedOrder.remaining_payment_status === 'cash_received' ? 'text-green-600' : 'text-amber-600'}`}>
                        ₹{selectedOrder.remaining_amount?.toLocaleString()}
                        {selectedOrder.remaining_payment_status === 'cash_received' && ' 💵 Cash'}
                        {selectedOrder.remaining_payment_status === 'paid' && ' ✅ Online'}
                      </p>
                    </div>
                  )}
                  {selectedOrder.transport_charge > 0 && (
                    <div>
                      <p className="text-slate-500">Transport Charge</p>
                      <p className="font-bold text-blue-600">₹{selectedOrder.transport_charge?.toLocaleString()}</p>
                      <p className="text-xs text-slate-400">{selectedOrder.transport_vehicle_type}</p>
                    </div>
                  )}
                </div>
                
                {selectedOrder.cash_received_by_name && (
                  <div className="mt-3 pt-3 border-t border-slate-200">
                    <p className="text-xs text-slate-500">
                      Cash received by: <span className="font-medium">{selectedOrder.cash_received_by_name}</span> at {new Date(selectedOrder.cash_received_at).toLocaleString()}
                    </p>
                  </div>
                )}
              </div>

              {/* Transport Charge Note */}
              {selectedOrder.transport_charge_note && (
                <div className="bg-blue-50 p-3 rounded-lg mb-6 text-sm">
                  <p className="font-medium text-blue-800">📝 {selectedOrder.transport_charge_note}</p>
                </div>
              )}

              {/* Dispatch Info */}
              {selectedOrder.dispatch_slip_number && (
                <div className="bg-teal-50 p-4 rounded-lg mb-6">
                  <h3 className="font-bold text-teal-900 mb-2">📄 Dispatch Slip</h3>
                  <p className="font-mono text-lg">{selectedOrder.dispatch_slip_number}</p>
                  <p className="text-sm text-teal-600">Created by: {selectedOrder.dispatch_created_by}</p>
                </div>
              )}

              {/* Status */}
              <div className="mb-6">
                <p className="text-sm text-slate-500 mb-2">Status</p>
                {getStatusBadge(selectedOrder.status)}
              </div>

              {/* Actions */}
              <div className="flex flex-wrap gap-3 pt-4 border-t">
                {/* Mark Cash Received - Only if advance paid and remaining pending */}
                {selectedOrder.advance_payment_status === 'paid' && 
                 selectedOrder.remaining_payment_status === 'pending' && (
                  <Button 
                    className="bg-green-600 hover:bg-green-700"
                    onClick={() => handleMarkCashReceived(selectedOrder.id)}
                  >
                    💵 Mark Cash Received (₹{selectedOrder.remaining_amount?.toLocaleString()})
                  </Button>
                )}

                {/* Add Transport Charge */}
                {!selectedOrder.transport_charge && (
                  <Button 
                    variant="outline"
                    onClick={() => { setShowTransportModal(true); }}
                  >
                    🚚 Add Transport Charge
                  </Button>
                )}

                {/* Create Dispatch Slip - Only if payment FULLY settled */}
                {isPaymentSettled(selectedOrder) && !selectedOrder.dispatch_slip_number && (
                  <Button 
                    className="bg-teal-600 hover:bg-teal-700"
                    onClick={() => handleCreateDispatchSlip(selectedOrder.id)}
                  >
                    📄 Create Dispatch Slip
                  </Button>
                )}

                {/* Payment NOT settled - show warning */}
                {!isPaymentSettled(selectedOrder) && !selectedOrder.dispatch_slip_number && (
                  <div className="bg-amber-50 border border-amber-200 p-3 rounded-lg text-sm">
                    <p className="font-medium text-amber-800">⚠️ Payment Pending</p>
                    <p className="text-amber-600">Dispatch slip can only be created after full payment is settled.</p>
                  </div>
                )}

                {/* Mark Dispatched - Opens dispatch modal with vehicle/driver selection - Only if payment settled */}
                {selectedOrder.dispatch_slip_number && 
                 isPaymentSettled(selectedOrder) &&
                 (selectedOrder.status === 'ready_for_dispatch' || selectedOrder.status === 'processing') && 
                 !selectedOrder.vehicle_number && (
                  <Button 
                    className="bg-indigo-600 hover:bg-indigo-700"
                    onClick={() => openDispatchModal(selectedOrder)}
                  >
                    <Truck className="w-4 h-4 mr-2" /> Dispatch Order
                  </Button>
                )}

                {/* Already dispatched - show info */}
                {selectedOrder.vehicle_number && (
                  <div className="bg-indigo-50 p-3 rounded-lg text-sm">
                    <p className="font-medium text-indigo-800 flex items-center gap-1">
                      <Truck className="w-4 h-4" /> Dispatched
                    </p>
                    <p className="text-indigo-600">{selectedOrder.vehicle_number} • {selectedOrder.driver_name}</p>
                    {selectedOrder.driver_phone && (
                      <p className="text-indigo-500 flex items-center gap-1">
                        <Phone className="w-3 h-3" /> {selectedOrder.driver_phone}
                      </p>
                    )}
                  </div>
                )}

                {/* Download Dispatch Slip PDF - Only if payment settled */}
                {selectedOrder.dispatch_slip_number && isPaymentSettled(selectedOrder) && (
                  <Button 
                    variant="outline"
                    onClick={() => handleDownloadDispatchSlip(selectedOrder.id)}
                  >
                    <Printer className="w-4 h-4 mr-2" /> Download PDF
                  </Button>
                )}

                {/* Download Invoice PDF - Always available */}
                <Button 
                  variant="outline"
                  onClick={() => handleDownloadInvoice(selectedOrder.id)}
                >
                  🧾 Invoice PDF
                </Button>
              </div>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Transport Charge Modal */}
      {showTransportModal && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
          <Card className="w-full max-w-md">
            <CardContent className="p-6">
              <h2 className="text-xl font-bold text-slate-900 mb-4">Add Transport Charge</h2>
              
              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-1">Transport Charge (₹) *</label>
                  <input
                    type="number"
                    value={transportData.transport_charge}
                    onChange={(e) => setTransportData({ ...transportData, transport_charge: e.target.value })}
                    className="w-full px-4 py-2 border border-slate-300 rounded-lg"
                    placeholder="Enter amount"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-1">Vehicle Type</label>
                  <select
                    value={transportData.vehicle_type}
                    onChange={(e) => setTransportData({ ...transportData, vehicle_type: e.target.value })}
                    className="w-full px-4 py-2 border border-slate-300 rounded-lg"
                  >
                    <option value="">Select Vehicle</option>
                    {vehicleTypes.map(v => (
                      <option key={v} value={v}>{v}</option>
                    ))}
                  </select>
                </div>

                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-1">Note</label>
                  <input
                    type="text"
                    value={transportData.transport_note}
                    onChange={(e) => setTransportData({ ...transportData, transport_note: e.target.value })}
                    className="w-full px-4 py-2 border border-slate-300 rounded-lg"
                    placeholder="Transport charge extra applicable"
                  />
                </div>

                <div className="flex gap-3 pt-4">
                  <Button variant="outline" className="flex-1" onClick={() => setShowTransportModal(false)}>
                    Cancel
                  </Button>
                  <Button className="flex-1 bg-teal-600 hover:bg-teal-700" onClick={handleAddTransportCharge}>
                    Add Charge
                  </Button>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Dispatch Order Modal */}
      {showDispatchModal && selectedOrder && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
          <Card className="w-full max-w-lg">
            <CardContent className="p-6">
              <div className="flex items-center justify-between mb-6">
                <h2 className="text-xl font-bold text-slate-900 flex items-center gap-2">
                  <Truck className="w-6 h-6 text-indigo-600" />
                  Dispatch Order
                </h2>
                <Button variant="ghost" size="sm" onClick={() => setShowDispatchModal(false)}>
                  <X className="w-5 h-5" />
                </Button>
              </div>

              {/* Order Summary */}
              <div className="bg-slate-50 p-4 rounded-lg mb-6">
                <p className="font-bold text-slate-900">Order #{selectedOrder.order_number}</p>
                <p className="text-sm text-slate-600">{selectedOrder.customer_name}</p>
                <p className="text-sm text-slate-500 flex items-center gap-1 mt-1">
                  <MapPin className="w-3 h-3" />
                  {selectedOrder.delivery_address || 'No address provided'}
                </p>
              </div>

              <div className="space-y-4">
                {/* Vehicle Selection */}
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-1">
                    <Car className="w-4 h-4 inline mr-1" />
                    Select Vehicle *
                  </label>
                  <select
                    value={dispatchForm.vehicle_id}
                    onChange={(e) => setDispatchForm({ ...dispatchForm, vehicle_id: e.target.value })}
                    className="w-full h-11 px-3 border border-slate-300 rounded-lg"
                    data-testid="dispatch-vehicle-select"
                  >
                    <option value="">-- Select Vehicle --</option>
                    {vehicles.map((v) => (
                      <option key={v.id} value={v.id}>
                        {v.vehicle_number} ({v.vehicle_type}) - {v.capacity_sqft} sq.ft
                      </option>
                    ))}
                  </select>
                  {vehicles.length === 0 && (
                    <p className="text-xs text-amber-600 mt-1">No available vehicles. Add vehicles in Transport Management.</p>
                  )}
                </div>

                {/* Driver Selection */}
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-1">
                    <User className="w-4 h-4 inline mr-1" />
                    Select Driver *
                  </label>
                  <select
                    value={dispatchForm.driver_id}
                    onChange={(e) => setDispatchForm({ ...dispatchForm, driver_id: e.target.value })}
                    className="w-full h-11 px-3 border border-slate-300 rounded-lg"
                    data-testid="dispatch-driver-select"
                  >
                    <option value="">-- Select Driver --</option>
                    {drivers.map((d) => (
                      <option key={d.id} value={d.id}>
                        {d.name} - {d.phone}
                      </option>
                    ))}
                  </select>
                  {drivers.length === 0 && (
                    <p className="text-xs text-amber-600 mt-1">No available drivers. Add drivers in Transport Management.</p>
                  )}
                </div>

                {/* Estimated Delivery Time */}
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-1">
                    Estimated Delivery Time (Optional)
                  </label>
                  <input
                    type="datetime-local"
                    value={dispatchForm.estimated_delivery_time}
                    onChange={(e) => setDispatchForm({ ...dispatchForm, estimated_delivery_time: e.target.value })}
                    className="w-full h-11 px-3 border border-slate-300 rounded-lg"
                  />
                </div>

                {/* Notes */}
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-1">
                    Dispatch Notes (Optional)
                  </label>
                  <textarea
                    value={dispatchForm.notes}
                    onChange={(e) => setDispatchForm({ ...dispatchForm, notes: e.target.value })}
                    className="w-full px-3 py-2 border border-slate-300 rounded-lg h-20"
                    placeholder="Any special instructions for the driver..."
                  />
                </div>

                {/* Info about notification */}
                <div className="bg-blue-50 p-3 rounded-lg text-sm text-blue-700">
                  <p className="font-medium">📱 Auto Notification</p>
                  <p>Customer will receive WhatsApp & Email with driver details once dispatched.</p>
                </div>

                {/* Actions */}
                <div className="flex gap-3 pt-4 border-t">
                  <Button 
                    variant="outline" 
                    className="flex-1" 
                    onClick={() => setShowDispatchModal(false)}
                  >
                    Cancel
                  </Button>
                  <Button 
                    className="flex-1 bg-indigo-600 hover:bg-indigo-700 gap-2" 
                    onClick={handleSubmitDispatch}
                    disabled={dispatchLoading || !dispatchForm.vehicle_id || !dispatchForm.driver_id}
                    data-testid="submit-dispatch-btn"
                  >
                    {dispatchLoading ? (
                      <>
                        <Loader2 className="w-4 h-4 animate-spin" />
                        Dispatching...
                      </>
                    ) : (
                      <>
                        <Send className="w-4 h-4" />
                        Dispatch & Notify
                      </>
                    )}
                  </Button>
                </div>
              </div>
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
    </div>
  );
};

export default OrderManagement;
