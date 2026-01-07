import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { 
  Users, Plus, Search, Filter, Building2, Phone, Mail, CreditCard,
  FileText, Eye, Edit, Trash2, ChevronDown, CheckCircle, Clock,
  AlertCircle, IndianRupee, Loader2, X, Download, Share2, RefreshCw,
  BookOpen, Layers, Calendar, TrendingUp, TrendingDown, Send, Banknote,
  ArrowUpRight, CheckCircle2, XCircle, Timer
} from 'lucide-react';
import { toast } from 'sonner';
import erpApi from '../../utils/erpApi';

const API_BASE = process.env.REACT_APP_BACKEND_URL;

const VendorManagement = () => {
  const [vendors, setVendors] = useState([]);
  const [loading, setLoading] = useState(true);
  const [selectedVendor, setSelectedVendor] = useState(null);
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [showPOModal, setShowPOModal] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [categoryFilter, setCategoryFilter] = useState('all');
  const [activeTab, setActiveTab] = useState('vendors');
  const [pos, setPOs] = useState([]);
  const [selectedPO, setSelectedPO] = useState(null);
  const [showPaymentModal, setShowPaymentModal] = useState(false);
  const [showBalanceSheet, setShowBalanceSheet] = useState(false);
  const [balanceSheetData, setBalanceSheetData] = useState(null);
  const [showBulkPayment, setShowBulkPayment] = useState(false);
  const [selectedPOsForBulk, setSelectedPOsForBulk] = useState([]);
  const [bulkPaymentVendor, setBulkPaymentVendor] = useState(null);

  const [newVendor, setNewVendor] = useState({
    name: '',
    company_name: '',
    email: '',
    phone: '',
    gst_number: '',
    pan_number: '',
    bank_name: '',
    bank_account: '',
    ifsc_code: '',
    upi_id: '',
    address: '',
    category: 'raw_material',
    credit_days: 30,
    credit_limit: 100000,
    notes: ''
  });

  const [newPO, setNewPO] = useState({
    vendor_id: '',
    items: [{ name: '', description: '', quantity: 1, unit: 'pcs', unit_price: 0, gst_rate: 18 }],
    delivery_date: '',
    delivery_address: '',
    payment_terms: '30_days',
    notes: ''
  });

  const [paymentData, setPaymentData] = useState({
    payment_type: 'advance',
    percentage: null,
    amount: 0,
    payment_mode: 'razorpay',
    utr_reference: '',
    notes: ''
  });

  const [bulkPaymentData, setBulkPaymentData] = useState({
    payment_mode: 'bank_transfer',
    utr_reference: '',
    notes: ''
  });

  useEffect(() => {
    fetchData();
  }, [categoryFilter]);

  const getAuthHeaders = () => {
    const token = localStorage.getItem('token');
    return { Authorization: `Bearer ${token}` };
  };

  const fetchData = async () => {
    setLoading(true);
    try {
      const [vendorsRes, posRes] = await Promise.all([
        fetch(`${API_BASE}/api/erp/vendors/${categoryFilter !== 'all' ? `?category=${categoryFilter}` : ''}`, { headers: getAuthHeaders() }),
        fetch(`${API_BASE}/api/erp/vendors/po/list`, { headers: getAuthHeaders() })
      ]);
      
      const vendorsData = await vendorsRes.json();
      const posData = await posRes.json();
      
      setVendors(vendorsData.vendors || []);
      setPOs(posData.purchase_orders || []);
    } catch (error) {
      console.error('Failed to fetch data:', error);
      toast.error('Failed to load data');
    } finally {
      setLoading(false);
    }
  };

  const createVendor = async () => {
    if (!newVendor.name || !newVendor.phone) {
      toast.error('Name and phone are required');
      return;
    }

    try {
      const res = await fetch(`${API_BASE}/api/erp/vendors/`, {
        method: 'POST',
        headers: { ...getAuthHeaders(), 'Content-Type': 'application/json' },
        body: JSON.stringify(newVendor)
      });
      
      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || 'Failed to create vendor');
      }
      
      toast.success('Vendor created successfully');
      setShowCreateModal(false);
      setNewVendor({
        name: '', company_name: '', email: '', phone: '', gst_number: '', pan_number: '',
        bank_name: '', bank_account: '', ifsc_code: '', upi_id: '', address: '',
        category: 'raw_material', credit_days: 30, credit_limit: 100000, notes: ''
      });
      fetchData();
    } catch (error) {
      toast.error(error.message);
    }
  };

  const createPO = async () => {
    if (!newPO.vendor_id || newPO.items.length === 0) {
      toast.error('Select vendor and add items');
      return;
    }

    try {
      const res = await fetch(`${API_BASE}/api/erp/vendors/po`, {
        method: 'POST',
        headers: { ...getAuthHeaders(), 'Content-Type': 'application/json' },
        body: JSON.stringify(newPO)
      });
      
      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || 'Failed to create PO');
      }
      
      const data = await res.json();
      toast.success(`PO ${data.po.po_number} created`);
      setShowPOModal(false);
      setNewPO({
        vendor_id: '', items: [{ name: '', description: '', quantity: 1, unit: 'pcs', unit_price: 0, gst_rate: 18 }],
        delivery_date: '', delivery_address: '', payment_terms: '30_days', notes: ''
      });
      fetchData();
    } catch (error) {
      toast.error(error.message);
    }
  };

  const submitPO = async (poId) => {
    try {
      const res = await fetch(`${API_BASE}/api/erp/vendors/po/${poId}/submit`, {
        method: 'POST',
        headers: getAuthHeaders()
      });
      
      if (!res.ok) throw new Error('Failed to submit PO');
      
      toast.success('PO submitted for approval');
      fetchData();
    } catch (error) {
      toast.error(error.message);
    }
  };

  const approvePO = async (poId, status) => {
    try {
      const res = await fetch(`${API_BASE}/api/erp/vendors/po/${poId}/approve`, {
        method: 'POST',
        headers: { ...getAuthHeaders(), 'Content-Type': 'application/json' },
        body: JSON.stringify({ status, notes: '' })
      });
      
      if (!res.ok) throw new Error(`Failed to ${status} PO`);
      
      toast.success(`PO ${status}`);
      fetchData();
    } catch (error) {
      toast.error(error.message);
    }
  };

  const [payoutProcessing, setPayoutProcessing] = useState(false);
  const [payoutStatus, setPayoutStatus] = useState(null);

  const initiatePayment = async () => {
    if (!selectedPO || paymentData.amount <= 0) {
      toast.error('Invalid payment amount');
      return;
    }

    try {
      setPayoutProcessing(true);
      
      const res = await fetch(`${API_BASE}/api/erp/vendors/po/${selectedPO.id}/initiate-payment`, {
        method: 'POST',
        headers: { ...getAuthHeaders(), 'Content-Type': 'application/json' },
        body: JSON.stringify({
          po_id: selectedPO.id,
          ...paymentData
        })
      });

      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || 'Failed to initiate payment');
      }

      const data = await res.json();
      
      // For Razorpay Payout mode (outgoing transfer to vendor)
      if (paymentData.payment_mode === 'razorpay' || paymentData.payment_mode === 'razorpay_payout') {
        // Payout initiated - poll for status
        toast.info(`Payout initiated to ${data.vendor_bank || 'vendor bank'}. Processing...`);
        setPayoutStatus({
          payment_id: data.payment_id,
          status: 'processing',
          payout_id: data.payout_id,
          amount: data.amount,
          mock_mode: data.mock_mode
        });
        
        // Poll for completion
        await pollPayoutStatus(data.payment_id);
      } else {
        // For manual payments (UPI, bank transfer, etc.), need UTR/reference
        if (!paymentData.utr_reference && paymentData.payment_mode !== 'cash') {
          toast.error('Please enter UTR/Transaction Reference');
          setPayoutProcessing(false);
          return;
        }
        
        const transRef = paymentData.utr_reference || `CASH_${Date.now()}`;
        const verifyRes = await fetch(`${API_BASE}/api/erp/vendors/payment/${data.payment_id}/record-manual?transaction_ref=${transRef}`, {
          method: 'POST',
          headers: getAuthHeaders()
        });

        if (!verifyRes.ok) throw new Error('Payment verification failed');

        const verifyData = await verifyRes.json();
        toast.success(`Payment recorded! Receipt: ${verifyData.receipt_number}`);
        closePaymentModal();
        fetchData();
      }
    } catch (error) {
      toast.error(error.message);
    } finally {
      setPayoutProcessing(false);
    }
  };

  const pollPayoutStatus = async (paymentId, maxAttempts = 10) => {
    let attempts = 0;
    
    const checkStatus = async () => {
      try {
        const res = await fetch(`${API_BASE}/api/erp/vendors/payment/${paymentId}/status`, {
          headers: getAuthHeaders()
        });
        
        if (!res.ok) throw new Error('Failed to get status');
        
        const statusData = await res.json();
        setPayoutStatus(statusData);
        
        if (statusData.status === 'completed') {
          toast.success(`Payout successful! UTR: ${statusData.utr || 'N/A'}, Receipt: ${statusData.receipt_number}`);
          closePaymentModal();
          fetchData();
          return true;
        } else if (statusData.status === 'failed' || statusData.status === 'reversed') {
          toast.error(`Payout ${statusData.status}: ${statusData.failure_reason || 'Unknown error'}`);
          setPayoutProcessing(false);
          return true;
        }
        
        return false;
      } catch (error) {
        console.error('Status check error:', error);
        return false;
      }
    };
    
    // Initial check
    if (await checkStatus()) return;
    
    // Poll every 2 seconds
    const pollInterval = setInterval(async () => {
      attempts++;
      const completed = await checkStatus();
      
      if (completed || attempts >= maxAttempts) {
        clearInterval(pollInterval);
        if (attempts >= maxAttempts && !completed) {
          toast.info('Payout is still processing. UTR will be updated when bank confirms the transfer.');
          closePaymentModal();
          fetchData();
        }
      }
    }, 2000);
  };

  const closePaymentModal = () => {
    setShowPaymentModal(false);
    setSelectedPO(null);
    setPayoutStatus(null);
    setPayoutProcessing(false);
    setPaymentData({ payment_type: 'advance', percentage: null, amount: 0, payment_mode: 'razorpay', utr_reference: '', notes: '' });
  };

  // REMOVED: openRazorpayCheckout - Not needed for vendor payouts
  // Vendor payments are OUTGOING transfers (payouts), not incoming (checkout)

  const downloadPDF = (type, id) => {
    const token = localStorage.getItem('token');
    let url = '';
    
    switch(type) {
      case 'po':
        url = `${API_BASE}/api/erp/pdf/purchase-order/${id}?token=${token}`;
        break;
      case 'receipt':
        url = `${API_BASE}/api/erp/pdf/vendor-payment-receipt/${id}?token=${token}`;
        break;
      default:
        return;
    }
    
    window.open(url, '_blank');
  };

  // Fetch vendor balance sheet
  const fetchBalanceSheet = async (vendorId) => {
    try {
      const res = await fetch(`${API_BASE}/api/erp/vendors/${vendorId}/balance-sheet`, {
        headers: getAuthHeaders()
      });
      if (!res.ok) throw new Error('Failed to fetch balance sheet');
      const data = await res.json();
      setBalanceSheetData(data);
      setShowBalanceSheet(true);
    } catch (error) {
      toast.error(error.message);
    }
  };

  // Open bulk payment for a vendor
  const openBulkPayment = (vendor) => {
    const vendorPOs = pos.filter(po => 
      po.vendor_id === vendor.id && 
      po.status === 'approved' && 
      po.payment_status !== 'fully_paid'
    );
    
    if (vendorPOs.length === 0) {
      toast.error('No pending POs for this vendor');
      return;
    }
    
    setBulkPaymentVendor(vendor);
    setSelectedPOsForBulk(vendorPOs.map(po => po.id));
    setBulkPaymentData({ payment_mode: 'bank_transfer', utr_reference: '', notes: '' });
    setShowBulkPayment(true);
  };

  // Calculate total for selected POs
  const calculateBulkTotal = () => {
    return pos
      .filter(po => selectedPOsForBulk.includes(po.id))
      .reduce((sum, po) => sum + (po.outstanding_balance || po.grand_total), 0);
  };

  // Process bulk payment
  const processBulkPayment = async () => {
    if (selectedPOsForBulk.length === 0) {
      toast.error('Please select at least one PO');
      return;
    }

    if (bulkPaymentData.payment_mode !== 'razorpay' && bulkPaymentData.payment_mode !== 'cash' && !bulkPaymentData.utr_reference) {
      toast.error('Please enter UTR/Transaction Reference');
      return;
    }

    try {
      setPayoutProcessing(true);
      
      // Initiate bulk payment
      const initRes = await fetch(`${API_BASE}/api/erp/vendors/bulk-payment`, {
        method: 'POST',
        headers: { ...getAuthHeaders(), 'Content-Type': 'application/json' },
        body: JSON.stringify({
          vendor_id: bulkPaymentVendor.id,
          po_ids: selectedPOsForBulk,
          payment_mode: bulkPaymentData.payment_mode,
          transaction_ref: bulkPaymentData.utr_reference,
          notes: bulkPaymentData.notes
        })
      });

      if (!initRes.ok) {
        const err = await initRes.json();
        throw new Error(err.detail || 'Failed to initiate bulk payment');
      }

      const initData = await initRes.json();

      if (bulkPaymentData.payment_mode === 'razorpay') {
        // For Razorpay payout mode - auto-process in mock mode
        toast.info(`Bulk payout initiated for ${initData.po_count} POs. Processing...`);
        
        // Complete the bulk payment (mock mode will auto-complete)
        const completeRes = await fetch(
          `${API_BASE}/api/erp/vendors/bulk-payment/${initData.bulk_payment_id}/complete`,
          { method: 'POST', headers: getAuthHeaders() }
        );

        if (!completeRes.ok) throw new Error('Failed to complete bulk payout');

        const completeData = await completeRes.json();
        toast.success(`Bulk payout completed! Receipt: ${completeData.bulk_receipt_number}. ${initData.mock_mode ? '(MOCK MODE)' : ''}`);
        closeBulkPaymentModal();
        fetchData();
      } else {
        // Complete manual bulk payment
        const completeRes = await fetch(
          `${API_BASE}/api/erp/vendors/bulk-payment/${initData.bulk_payment_id}/complete?transaction_ref=${bulkPaymentData.utr_reference || 'CASH_' + Date.now()}`,
          { method: 'POST', headers: getAuthHeaders() }
        );

        if (!completeRes.ok) throw new Error('Failed to complete bulk payment');

        const completeData = await completeRes.json();
        toast.success(`Bulk payment completed! Receipt: ${completeData.bulk_receipt_number}`);
        closeBulkPaymentModal();
        fetchData();
      }
    } catch (error) {
      toast.error(error.message);
    } finally {
      setPayoutProcessing(false);
    }
  };

  const closeBulkPaymentModal = () => {
    setShowBulkPayment(false);
    setBulkPaymentVendor(null);
    setSelectedPOsForBulk([]);
    setBulkPaymentData({ payment_mode: 'bank_transfer', utr_reference: '', notes: '' });
  };

  const filteredVendors = vendors.filter(v => 
    v.name?.toLowerCase().includes(searchQuery.toLowerCase()) ||
    v.company_name?.toLowerCase().includes(searchQuery.toLowerCase()) ||
    v.vendor_code?.toLowerCase().includes(searchQuery.toLowerCase())
  );

  const categoryOptions = [
    { value: 'raw_material', label: 'Raw Material', color: 'bg-blue-100 text-blue-700' },
    { value: 'glass_processing', label: 'Glass Processing', color: 'bg-purple-100 text-purple-700' },
    { value: 'logistics', label: 'Logistics', color: 'bg-amber-100 text-amber-700' },
    { value: 'job_work', label: 'Job Work', color: 'bg-orange-100 text-orange-700' },
    { value: 'other', label: 'Other', color: 'bg-slate-100 text-slate-700' }
  ];

  const paymentStatusConfig = {
    unpaid: { color: 'bg-red-100 text-red-700', label: 'Unpaid' },
    partially_paid: { color: 'bg-amber-100 text-amber-700', label: 'Partial' },
    fully_paid: { color: 'bg-green-100 text-green-700', label: 'Paid' }
  };

  const poStatusConfig = {
    draft: { color: 'bg-slate-100 text-slate-700', label: 'Draft' },
    pending_approval: { color: 'bg-amber-100 text-amber-700', label: 'Pending' },
    approved: { color: 'bg-green-100 text-green-700', label: 'Approved' },
    rejected: { color: 'bg-red-100 text-red-700', label: 'Rejected' },
    completed: { color: 'bg-blue-100 text-blue-700', label: 'Completed' }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-96">
        <Loader2 className="w-8 h-8 animate-spin text-violet-600" />
      </div>
    );
  }

  return (
    <div className="p-6 max-w-7xl mx-auto">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Vendor Management</h1>
          <p className="text-slate-600">Manage vendors, POs, and payments</p>
        </div>
        <div className="flex gap-2">
          <Button variant="outline" onClick={fetchData}>
            <RefreshCw className="w-4 h-4 mr-2" /> Refresh
          </Button>
          <Button onClick={() => setShowCreateModal(true)} className="bg-violet-600 hover:bg-violet-700">
            <Plus className="w-4 h-4 mr-2" /> Add Vendor
          </Button>
          <Button onClick={() => setShowPOModal(true)} className="bg-teal-600 hover:bg-teal-700">
            <FileText className="w-4 h-4 mr-2" /> Create PO
          </Button>
        </div>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
        <Card className="border-l-4 border-l-violet-500">
          <CardContent className="p-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-slate-600">Total Vendors</p>
                <p className="text-2xl font-bold text-violet-600">{vendors.length}</p>
              </div>
              <Users className="w-8 h-8 text-violet-200" />
            </div>
          </CardContent>
        </Card>
        
        <Card className="border-l-4 border-l-teal-500">
          <CardContent className="p-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-slate-600">Active POs</p>
                <p className="text-2xl font-bold text-teal-600">{pos.filter(p => p.status === 'approved').length}</p>
              </div>
              <FileText className="w-8 h-8 text-teal-200" />
            </div>
          </CardContent>
        </Card>
        
        <Card className="border-l-4 border-l-amber-500">
          <CardContent className="p-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-slate-600">Pending Approval</p>
                <p className="text-2xl font-bold text-amber-600">{pos.filter(p => p.status === 'pending_approval').length}</p>
              </div>
              <Clock className="w-8 h-8 text-amber-200" />
            </div>
          </CardContent>
        </Card>
        
        <Card className="border-l-4 border-l-red-500">
          <CardContent className="p-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-slate-600">Outstanding</p>
                <p className="text-2xl font-bold text-red-600">
                  ₹{pos.reduce((sum, p) => sum + (p.outstanding_balance || 0), 0).toLocaleString()}
                </p>
              </div>
              <IndianRupee className="w-8 h-8 text-red-200" />
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Tabs */}
      <div className="flex gap-2 mb-4">
        <Button
          variant={activeTab === 'vendors' ? 'default' : 'outline'}
          onClick={() => setActiveTab('vendors')}
          className={activeTab === 'vendors' ? 'bg-violet-600' : ''}
        >
          <Users className="w-4 h-4 mr-2" /> Vendors ({vendors.length})
        </Button>
        <Button
          variant={activeTab === 'pos' ? 'default' : 'outline'}
          onClick={() => setActiveTab('pos')}
          className={activeTab === 'pos' ? 'bg-teal-600' : ''}
        >
          <FileText className="w-4 h-4 mr-2" /> Purchase Orders ({pos.length})
        </Button>
      </div>

      {/* Vendors Tab */}
      {activeTab === 'vendors' && (
        <Card>
          <CardHeader>
            <div className="flex items-center justify-between">
              <CardTitle>Vendors</CardTitle>
              <div className="flex gap-2">
                <div className="relative">
                  <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
                  <input
                    type="text"
                    placeholder="Search vendors..."
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    className="pl-10 pr-4 h-9 rounded-lg border border-slate-300 w-64"
                  />
                </div>
                <select
                  value={categoryFilter}
                  onChange={(e) => setCategoryFilter(e.target.value)}
                  className="h-9 rounded-lg border border-slate-300 px-3"
                >
                  <option value="all">All Categories</option>
                  {categoryOptions.map(opt => (
                    <option key={opt.value} value={opt.value}>{opt.label}</option>
                  ))}
                </select>
              </div>
            </div>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b">
                    <th className="text-left py-3 px-2 text-sm font-medium text-slate-600">Code</th>
                    <th className="text-left py-3 px-2 text-sm font-medium text-slate-600">Vendor</th>
                    <th className="text-left py-3 px-2 text-sm font-medium text-slate-600">Category</th>
                    <th className="text-left py-3 px-2 text-sm font-medium text-slate-600">Contact</th>
                    <th className="text-right py-3 px-2 text-sm font-medium text-slate-600">Balance</th>
                    <th className="text-center py-3 px-2 text-sm font-medium text-slate-600">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredVendors.map((vendor) => {
                    const category = categoryOptions.find(c => c.value === vendor.category) || categoryOptions[4];
                    return (
                      <tr key={vendor.id} className="border-b hover:bg-slate-50">
                        <td className="py-3 px-2">
                          <span className="font-mono text-sm text-violet-600">{vendor.vendor_code}</span>
                        </td>
                        <td className="py-3 px-2">
                          <p className="font-medium text-slate-900">{vendor.name}</p>
                          <p className="text-xs text-slate-500">{vendor.company_name}</p>
                        </td>
                        <td className="py-3 px-2">
                          <span className={`px-2 py-1 rounded text-xs font-medium ${category.color}`}>
                            {category.label}
                          </span>
                        </td>
                        <td className="py-3 px-2">
                          <p className="text-sm text-slate-600">{vendor.phone}</p>
                          <p className="text-xs text-slate-400">{vendor.email}</p>
                        </td>
                        <td className="py-3 px-2 text-right">
                          <p className={`font-bold ${vendor.current_balance > 0 ? 'text-red-600' : 'text-green-600'}`}>
                            ₹{Math.abs(vendor.current_balance || 0).toLocaleString()}
                          </p>
                          <p className="text-xs text-slate-500">
                            {vendor.current_balance > 0 ? 'Payable' : 'Credit'}
                          </p>
                        </td>
                        <td className="py-3 px-2 text-center">
                          <div className="flex items-center gap-1 justify-center">
                            <Button
                              size="sm"
                              variant="outline"
                              onClick={() => setSelectedVendor(vendor)}
                              title="View Details"
                            >
                              <Eye className="w-4 h-4" />
                            </Button>
                            <Button
                              size="sm"
                              variant="outline"
                              className="text-indigo-600 border-indigo-200 hover:bg-indigo-50"
                              onClick={() => fetchBalanceSheet(vendor.id)}
                              title="Balance Sheet"
                            >
                              <BookOpen className="w-4 h-4" />
                            </Button>
                            {vendor.current_balance > 0 && (
                              <Button
                                size="sm"
                                className="bg-violet-600 hover:bg-violet-700 text-white"
                                onClick={() => openBulkPayment(vendor)}
                                title="Bulk Pay"
                              >
                                <Layers className="w-4 h-4" />
                              </Button>
                            )}
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      )}

      {/* POs Tab */}
      {activeTab === 'pos' && (
        <Card>
          <CardHeader>
            <CardTitle>Purchase Orders</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b">
                    <th className="text-left py-3 px-2 text-sm font-medium text-slate-600">PO #</th>
                    <th className="text-left py-3 px-2 text-sm font-medium text-slate-600">Vendor</th>
                    <th className="text-left py-3 px-2 text-sm font-medium text-slate-600">Date</th>
                    <th className="text-left py-3 px-2 text-sm font-medium text-slate-600">Status</th>
                    <th className="text-right py-3 px-2 text-sm font-medium text-slate-600">Amount</th>
                    <th className="text-left py-3 px-2 text-sm font-medium text-slate-600">Payment</th>
                    <th className="text-center py-3 px-2 text-sm font-medium text-slate-600">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {pos.map((po) => {
                    const statusInfo = poStatusConfig[po.status] || poStatusConfig.draft;
                    const paymentInfo = paymentStatusConfig[po.payment_status] || paymentStatusConfig.unpaid;
                    return (
                      <tr key={po.id} className="border-b hover:bg-slate-50">
                        <td className="py-3 px-2">
                          <span className="font-mono text-sm text-teal-600">{po.po_number}</span>
                        </td>
                        <td className="py-3 px-2">
                          <p className="font-medium text-slate-900">{po.vendor_name}</p>
                          <p className="text-xs text-slate-500">{po.vendor_code}</p>
                        </td>
                        <td className="py-3 px-2 text-sm text-slate-600">
                          {po.created_at?.slice(0, 10)}
                        </td>
                        <td className="py-3 px-2">
                          <span className={`px-2 py-1 rounded text-xs font-medium ${statusInfo.color}`}>
                            {statusInfo.label}
                          </span>
                        </td>
                        <td className="py-3 px-2 text-right">
                          <p className="font-bold text-slate-900">₹{po.grand_total?.toLocaleString()}</p>
                          {po.outstanding_balance > 0 && (
                            <p className="text-xs text-red-600">Due: ₹{po.outstanding_balance?.toLocaleString()}</p>
                          )}
                        </td>
                        <td className="py-3 px-2">
                          <span className={`px-2 py-1 rounded text-xs font-medium ${paymentInfo.color}`}>
                            {paymentInfo.label}
                          </span>
                        </td>
                        <td className="py-3 px-2">
                          <div className="flex items-center justify-center gap-1">
                            <Button size="sm" variant="ghost" onClick={() => downloadPDF('po', po.id)} title="Download PO">
                              <Download className="w-4 h-4" />
                            </Button>
                            
                            {po.status === 'draft' && (
                              <Button size="sm" variant="outline" onClick={() => submitPO(po.id)}>
                                Submit
                              </Button>
                            )}
                            
                            {po.status === 'pending_approval' && (
                              <>
                                <Button size="sm" className="bg-green-600 hover:bg-green-700" onClick={() => approvePO(po.id, 'approved')}>
                                  <CheckCircle className="w-4 h-4" />
                                </Button>
                                <Button size="sm" variant="destructive" onClick={() => approvePO(po.id, 'rejected')}>
                                  <X className="w-4 h-4" />
                                </Button>
                              </>
                            )}
                            
                            {po.status === 'approved' && po.payment_status !== 'fully_paid' && (
                              <Button 
                                size="sm" 
                                className="bg-violet-600 hover:bg-violet-700"
                                onClick={() => { setSelectedPO(po); setShowPaymentModal(true); setPaymentData({...paymentData, amount: po.outstanding_balance || 0}); }}
                              >
                                <CreditCard className="w-4 h-4 mr-1" /> Pay
                              </Button>
                            )}
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Create Vendor Modal */}
      {showCreateModal && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
          <Card className="w-full max-w-2xl max-h-[90vh] overflow-y-auto">
            <CardHeader>
              <div className="flex items-center justify-between">
                <CardTitle>Add New Vendor</CardTitle>
                <Button variant="ghost" size="sm" onClick={() => setShowCreateModal(false)}>
                  <X className="w-5 h-5" />
                </Button>
              </div>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="text-sm font-medium text-slate-700">Name *</label>
                  <input
                    type="text"
                    value={newVendor.name}
                    onChange={(e) => setNewVendor({...newVendor, name: e.target.value})}
                    className="w-full h-10 rounded-lg border border-slate-300 px-3 mt-1"
                    placeholder="Contact Name"
                  />
                </div>
                <div>
                  <label className="text-sm font-medium text-slate-700">Company Name</label>
                  <input
                    type="text"
                    value={newVendor.company_name}
                    onChange={(e) => setNewVendor({...newVendor, company_name: e.target.value})}
                    className="w-full h-10 rounded-lg border border-slate-300 px-3 mt-1"
                    placeholder="Company Name"
                  />
                </div>
                <div>
                  <label className="text-sm font-medium text-slate-700">Phone *</label>
                  <input
                    type="text"
                    value={newVendor.phone}
                    onChange={(e) => setNewVendor({...newVendor, phone: e.target.value})}
                    className="w-full h-10 rounded-lg border border-slate-300 px-3 mt-1"
                    placeholder="Phone Number"
                  />
                </div>
                <div>
                  <label className="text-sm font-medium text-slate-700">Email</label>
                  <input
                    type="email"
                    value={newVendor.email}
                    onChange={(e) => setNewVendor({...newVendor, email: e.target.value})}
                    className="w-full h-10 rounded-lg border border-slate-300 px-3 mt-1"
                    placeholder="Email"
                  />
                </div>
                <div>
                  <label className="text-sm font-medium text-slate-700">Category</label>
                  <select
                    value={newVendor.category}
                    onChange={(e) => setNewVendor({...newVendor, category: e.target.value})}
                    className="w-full h-10 rounded-lg border border-slate-300 px-3 mt-1"
                  >
                    {categoryOptions.map(opt => (
                      <option key={opt.value} value={opt.value}>{opt.label}</option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="text-sm font-medium text-slate-700">GST Number</label>
                  <input
                    type="text"
                    value={newVendor.gst_number}
                    onChange={(e) => setNewVendor({...newVendor, gst_number: e.target.value})}
                    className="w-full h-10 rounded-lg border border-slate-300 px-3 mt-1"
                    placeholder="GSTIN"
                  />
                </div>
                <div>
                  <label className="text-sm font-medium text-slate-700">Bank Name</label>
                  <input
                    type="text"
                    value={newVendor.bank_name}
                    onChange={(e) => setNewVendor({...newVendor, bank_name: e.target.value})}
                    className="w-full h-10 rounded-lg border border-slate-300 px-3 mt-1"
                    placeholder="Bank Name"
                  />
                </div>
                <div>
                  <label className="text-sm font-medium text-slate-700">Bank Account</label>
                  <input
                    type="text"
                    value={newVendor.bank_account}
                    onChange={(e) => setNewVendor({...newVendor, bank_account: e.target.value})}
                    className="w-full h-10 rounded-lg border border-slate-300 px-3 mt-1"
                    placeholder="Account Number"
                  />
                </div>
                <div>
                  <label className="text-sm font-medium text-slate-700">IFSC Code</label>
                  <input
                    type="text"
                    value={newVendor.ifsc_code}
                    onChange={(e) => setNewVendor({...newVendor, ifsc_code: e.target.value})}
                    className="w-full h-10 rounded-lg border border-slate-300 px-3 mt-1"
                    placeholder="IFSC"
                  />
                </div>
                <div>
                  <label className="text-sm font-medium text-slate-700">UPI ID</label>
                  <input
                    type="text"
                    value={newVendor.upi_id}
                    onChange={(e) => setNewVendor({...newVendor, upi_id: e.target.value})}
                    className="w-full h-10 rounded-lg border border-slate-300 px-3 mt-1"
                    placeholder="vendor@upi"
                  />
                </div>
              </div>
              <div>
                <label className="text-sm font-medium text-slate-700">Address</label>
                <textarea
                  value={newVendor.address}
                  onChange={(e) => setNewVendor({...newVendor, address: e.target.value})}
                  className="w-full rounded-lg border border-slate-300 px-3 py-2 mt-1 h-20"
                  placeholder="Full Address"
                />
              </div>
              <div className="flex gap-3 pt-4">
                <Button variant="outline" className="flex-1" onClick={() => setShowCreateModal(false)}>
                  Cancel
                </Button>
                <Button className="flex-1 bg-violet-600 hover:bg-violet-700" onClick={createVendor}>
                  <Plus className="w-4 h-4 mr-2" /> Create Vendor
                </Button>
              </div>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Create PO Modal */}
      {showPOModal && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
          <Card className="w-full max-w-3xl max-h-[90vh] overflow-y-auto">
            <CardHeader>
              <div className="flex items-center justify-between">
                <CardTitle>Create Purchase Order</CardTitle>
                <Button variant="ghost" size="sm" onClick={() => setShowPOModal(false)}>
                  <X className="w-5 h-5" />
                </Button>
              </div>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="text-sm font-medium text-slate-700">Select Vendor *</label>
                  <select
                    value={newPO.vendor_id}
                    onChange={(e) => setNewPO({...newPO, vendor_id: e.target.value})}
                    className="w-full h-10 rounded-lg border border-slate-300 px-3 mt-1"
                  >
                    <option value="">-- Select Vendor --</option>
                    {vendors.map(v => (
                      <option key={v.id} value={v.id}>{v.vendor_code} - {v.name}</option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="text-sm font-medium text-slate-700">Payment Terms</label>
                  <select
                    value={newPO.payment_terms}
                    onChange={(e) => setNewPO({...newPO, payment_terms: e.target.value})}
                    className="w-full h-10 rounded-lg border border-slate-300 px-3 mt-1"
                  >
                    <option value="immediate">Immediate</option>
                    <option value="15_days">15 Days</option>
                    <option value="30_days">30 Days</option>
                    <option value="45_days">45 Days</option>
                    <option value="60_days">60 Days</option>
                  </select>
                </div>
              </div>

              {/* Items */}
              <div>
                <div className="flex items-center justify-between mb-2">
                  <label className="text-sm font-medium text-slate-700">Items</label>
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() => setNewPO({
                      ...newPO,
                      items: [...newPO.items, { name: '', description: '', quantity: 1, unit: 'pcs', unit_price: 0, gst_rate: 18 }]
                    })}
                  >
                    <Plus className="w-4 h-4 mr-1" /> Add Item
                  </Button>
                </div>
                <div className="space-y-2">
                  {newPO.items.map((item, idx) => (
                    <div key={idx} className="grid grid-cols-6 gap-2 bg-slate-50 p-2 rounded-lg">
                      <input
                        type="text"
                        placeholder="Item Name"
                        value={item.name}
                        onChange={(e) => {
                          const items = [...newPO.items];
                          items[idx].name = e.target.value;
                          setNewPO({...newPO, items});
                        }}
                        className="col-span-2 h-9 rounded border border-slate-300 px-2 text-sm"
                      />
                      <input
                        type="number"
                        placeholder="Qty"
                        value={item.quantity}
                        onChange={(e) => {
                          const items = [...newPO.items];
                          items[idx].quantity = parseFloat(e.target.value) || 0;
                          setNewPO({...newPO, items});
                        }}
                        className="h-9 rounded border border-slate-300 px-2 text-sm"
                      />
                      <input
                        type="number"
                        placeholder="Price"
                        value={item.unit_price}
                        onChange={(e) => {
                          const items = [...newPO.items];
                          items[idx].unit_price = parseFloat(e.target.value) || 0;
                          setNewPO({...newPO, items});
                        }}
                        className="h-9 rounded border border-slate-300 px-2 text-sm"
                      />
                      <div className="text-right font-medium text-sm py-2">
                        ₹{(item.quantity * item.unit_price).toLocaleString()}
                      </div>
                      <Button
                        size="sm"
                        variant="ghost"
                        className="text-red-600"
                        onClick={() => {
                          const items = newPO.items.filter((_, i) => i !== idx);
                          setNewPO({...newPO, items: items.length ? items : [{ name: '', description: '', quantity: 1, unit: 'pcs', unit_price: 0, gst_rate: 18 }]});
                        }}
                      >
                        <Trash2 className="w-4 h-4" />
                      </Button>
                    </div>
                  ))}
                </div>
                <div className="text-right mt-2 font-bold text-lg">
                  Total: ₹{newPO.items.reduce((sum, item) => sum + (item.quantity * item.unit_price * 1.18), 0).toLocaleString()} (incl. GST)
                </div>
              </div>

              <div>
                <label className="text-sm font-medium text-slate-700">Notes</label>
                <textarea
                  value={newPO.notes}
                  onChange={(e) => setNewPO({...newPO, notes: e.target.value})}
                  className="w-full rounded-lg border border-slate-300 px-3 py-2 mt-1 h-16"
                  placeholder="Special instructions..."
                />
              </div>

              <div className="flex gap-3 pt-4">
                <Button variant="outline" className="flex-1" onClick={() => setShowPOModal(false)}>
                  Cancel
                </Button>
                <Button className="flex-1 bg-teal-600 hover:bg-teal-700" onClick={createPO}>
                  <FileText className="w-4 h-4 mr-2" /> Create PO
                </Button>
              </div>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Payment Modal */}
      {showPaymentModal && selectedPO && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
          <Card className="w-full max-w-md">
            <CardHeader className="bg-gradient-to-r from-violet-600 to-purple-600 text-white rounded-t-lg">
              <div className="flex items-center justify-between">
                <div>
                  <CardTitle className="text-white flex items-center gap-2">
                    <Send className="w-5 h-5" /> Pay Vendor
                  </CardTitle>
                  <p className="text-violet-100 text-sm mt-1">Outgoing Bank Transfer</p>
                </div>
                <Button variant="ghost" size="sm" className="text-white hover:bg-white/20" onClick={closePaymentModal}>
                  <X className="w-5 h-5" />
                </Button>
              </div>
            </CardHeader>
            <CardContent className="space-y-4 pt-6">
              {/* Payout Status Display */}
              {payoutStatus && (
                <div className={`rounded-lg p-4 ${
                  payoutStatus.status === 'completed' ? 'bg-green-50 border border-green-200' :
                  payoutStatus.status === 'failed' ? 'bg-red-50 border border-red-200' :
                  'bg-amber-50 border border-amber-200'
                }`}>
                  <div className="flex items-center gap-2 mb-2">
                    {payoutStatus.status === 'completed' ? (
                      <CheckCircle2 className="w-5 h-5 text-green-600" />
                    ) : payoutStatus.status === 'failed' ? (
                      <XCircle className="w-5 h-5 text-red-600" />
                    ) : (
                      <Timer className="w-5 h-5 text-amber-600 animate-pulse" />
                    )}
                    <span className={`font-semibold ${
                      payoutStatus.status === 'completed' ? 'text-green-700' :
                      payoutStatus.status === 'failed' ? 'text-red-700' :
                      'text-amber-700'
                    }`}>
                      {payoutStatus.status === 'completed' ? 'Payout Successful' :
                       payoutStatus.status === 'failed' ? 'Payout Failed' :
                       'Processing Payout...'}
                    </span>
                    {payoutStatus.mock_mode && (
                      <span className="text-xs bg-slate-200 text-slate-600 px-2 py-0.5 rounded">MOCK</span>
                    )}
                  </div>
                  {payoutStatus.utr && (
                    <p className="text-sm">
                      <span className="text-slate-600">UTR:</span>{' '}
                      <span className="font-mono font-bold">{payoutStatus.utr}</span>
                    </p>
                  )}
                  {payoutStatus.receipt_number && (
                    <p className="text-sm">
                      <span className="text-slate-600">Receipt:</span>{' '}
                      <span className="font-mono">{payoutStatus.receipt_number}</span>
                    </p>
                  )}
                </div>
              )}

              <div className="bg-violet-50 rounded-lg p-4 text-center">
                <p className="text-sm text-violet-600 mb-1">PO: {selectedPO.po_number}</p>
                <p className="text-xl font-bold text-violet-700">{selectedPO.vendor_name}</p>
                <p className="text-2xl font-bold text-slate-900 mt-2">
                  Outstanding: ₹{selectedPO.outstanding_balance?.toLocaleString()}
                </p>
              </div>

              {/* Vendor Bank Info */}
              {selectedPO.vendor && (
                <div className="bg-slate-50 rounded-lg p-3 text-sm">
                  <p className="text-slate-600 mb-1">Transfer to:</p>
                  <p className="font-medium">{selectedPO.vendor?.bank_name || 'Bank'} - XXXX{selectedPO.vendor?.bank_account?.slice(-4) || '****'}</p>
                  <p className="text-slate-500">IFSC: {selectedPO.vendor?.ifsc_code || 'N/A'}</p>
                </div>
              )}

              {/* Quick Percentage Buttons */}
              <div>
                <label className="text-sm font-medium text-slate-700 block mb-2">Quick Select</label>
                <div className="flex gap-2">
                  {[25, 50, 75, 100].map(pct => (
                    <Button
                      key={pct}
                      variant={paymentData.percentage === pct ? 'default' : 'outline'}
                      className={paymentData.percentage === pct ? 'bg-violet-600' : ''}
                      onClick={() => setPaymentData({
                        ...paymentData,
                        percentage: pct,
                        amount: Math.round(selectedPO.grand_total * pct / 100)
                      })}
                      disabled={payoutProcessing}
                    >
                      {pct}%
                    </Button>
                  ))}
                </div>
              </div>

              <div>
                <label className="text-sm font-medium text-slate-700">Payment Amount *</label>
                <div className="relative mt-1">
                  <span className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-500">₹</span>
                  <input
                    type="number"
                    value={paymentData.amount}
                    onChange={(e) => setPaymentData({...paymentData, amount: parseFloat(e.target.value) || 0, percentage: null})}
                    className="w-full h-12 rounded-lg border border-slate-300 pl-8 pr-3 text-lg font-bold"
                    disabled={payoutProcessing}
                  />
                </div>
              </div>

              <div>
                <label className="text-sm font-medium text-slate-700">Payment Mode</label>
                <select
                  value={paymentData.payment_mode}
                  onChange={(e) => setPaymentData({...paymentData, payment_mode: e.target.value})}
                  className="w-full h-10 rounded-lg border border-slate-300 px-3 mt-1"
                  disabled={payoutProcessing}
                >
                  <option value="razorpay">RazorpayX Payout (Auto UTR)</option>
                  <option value="bank_transfer">Bank Transfer / NEFT / RTGS (Manual UTR)</option>
                  <option value="upi">UPI Transfer (Manual UTR)</option>
                  <option value="net_banking">Net Banking (Manual UTR)</option>
                  <option value="cash">Cash</option>
                </select>
                {paymentData.payment_mode === 'razorpay' && (
                  <p className="text-xs text-green-600 mt-1 flex items-center gap-1">
                    <ArrowUpRight className="w-3 h-3" />
                    Outgoing transfer to vendor&apos;s bank. UTR auto-captured.
                  </p>
                )}
              </div>

              {/* UTR/Reference Input - Show for manual payment modes */}
              {paymentData.payment_mode !== 'razorpay' && paymentData.payment_mode !== 'cash' && (
                <div>
                  <label className="text-sm font-medium text-slate-700">UTR / Transaction Reference *</label>
                  <input
                    type="text"
                    value={paymentData.utr_reference}
                    onChange={(e) => setPaymentData({...paymentData, utr_reference: e.target.value.toUpperCase()})}
                    placeholder="Enter UTR number or transaction reference"
                    className="w-full h-10 rounded-lg border border-slate-300 px-3 mt-1"
                    disabled={payoutProcessing}
                  />
                  <p className="text-xs text-slate-500 mt-1">Enter the UTR number from your bank statement</p>
                </div>
              )}

              <div className="flex gap-3 pt-4">
                <Button variant="outline" className="flex-1" onClick={closePaymentModal} disabled={payoutProcessing}>
                  Cancel
                </Button>
                <Button 
                  className="flex-1 bg-violet-600 hover:bg-violet-700" 
                  onClick={initiatePayment}
                  disabled={
                    payoutProcessing ||
                    paymentData.amount <= 0 || 
                    paymentData.amount > selectedPO.outstanding_balance ||
                    (paymentData.payment_mode !== 'razorpay' && paymentData.payment_mode !== 'cash' && !paymentData.utr_reference)
                  }
                >
                  {payoutProcessing ? (
                    <>
                      <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                      Processing...
                    </>
                  ) : (
                    <>
                      {paymentData.payment_mode === 'razorpay' ? (
                        <><Send className="w-4 h-4 mr-2" /> Send Payout</>
                      ) : (
                        <><Banknote className="w-4 h-4 mr-2" /> Record Payment</>
                      )}{' '}
                      ₹{paymentData.amount?.toLocaleString()}
                    </>
                  )}
                </Button>
              </div>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Balance Sheet Modal */}
      {showBalanceSheet && balanceSheetData && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center p-4 z-50">
          <Card className="w-full max-w-4xl max-h-[90vh] overflow-y-auto">
            <CardHeader className="flex flex-row items-center justify-between bg-gradient-to-r from-indigo-600 to-purple-600 text-white rounded-t-lg">
              <div>
                <CardTitle className="text-white">Vendor Balance Sheet</CardTitle>
                <p className="text-indigo-100 text-sm">{balanceSheetData.vendor?.name} ({balanceSheetData.vendor?.vendor_code})</p>
              </div>
              <Button variant="ghost" size="sm" className="text-white hover:bg-white/20" onClick={() => setShowBalanceSheet(false)}>
                <X className="w-5 h-5" />
              </Button>
            </CardHeader>
            <CardContent className="p-6 space-y-6">
              {/* Summary Cards */}
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <div className="bg-slate-50 rounded-lg p-4 text-center">
                  <p className="text-sm text-slate-600">Opening Balance</p>
                  <p className="text-xl font-bold text-slate-900">₹{balanceSheetData.summary?.opening_balance?.toLocaleString()}</p>
                </div>
                <div className="bg-red-50 rounded-lg p-4 text-center">
                  <TrendingUp className="w-5 h-5 mx-auto text-red-500 mb-1" />
                  <p className="text-sm text-red-600">Total PO Value</p>
                  <p className="text-xl font-bold text-red-700">₹{balanceSheetData.summary?.total_po_value?.toLocaleString()}</p>
                  <p className="text-xs text-red-500">{balanceSheetData.summary?.po_count} POs</p>
                </div>
                <div className="bg-green-50 rounded-lg p-4 text-center">
                  <TrendingDown className="w-5 h-5 mx-auto text-green-500 mb-1" />
                  <p className="text-sm text-green-600">Total Payments</p>
                  <p className="text-xl font-bold text-green-700">₹{balanceSheetData.summary?.total_payments?.toLocaleString()}</p>
                  <p className="text-xs text-green-500">{balanceSheetData.summary?.payment_count} payments</p>
                </div>
                <div className={`rounded-lg p-4 text-center ${balanceSheetData.summary?.closing_balance > 0 ? 'bg-amber-50' : 'bg-teal-50'}`}>
                  <p className="text-sm text-slate-600">Closing Balance</p>
                  <p className={`text-xl font-bold ${balanceSheetData.summary?.closing_balance > 0 ? 'text-amber-700' : 'text-teal-700'}`}>
                    ₹{Math.abs(balanceSheetData.summary?.closing_balance || 0).toLocaleString()}
                  </p>
                  <p className="text-xs text-slate-500">{balanceSheetData.summary?.closing_balance > 0 ? 'Payable' : 'Credit'}</p>
                </div>
              </div>

              {/* Financial Year & Period */}
              <div className="flex items-center gap-4 text-sm text-slate-600">
                <span className="flex items-center gap-1"><Calendar className="w-4 h-4" /> FY: {balanceSheetData.financial_year}</span>
                <span>Period: {balanceSheetData.period?.start} to {balanceSheetData.period?.end}</span>
              </div>

              {/* Monthly Breakdown Table */}
              <div>
                <h3 className="text-lg font-semibold mb-3">Monthly Breakdown</h3>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="bg-slate-100">
                        <th className="text-left py-2 px-3">Month</th>
                        <th className="text-center py-2 px-3">POs</th>
                        <th className="text-right py-2 px-3">PO Value</th>
                        <th className="text-center py-2 px-3">Payments</th>
                        <th className="text-right py-2 px-3">Payment Value</th>
                        <th className="text-right py-2 px-3">Net</th>
                        <th className="text-right py-2 px-3">Closing</th>
                      </tr>
                    </thead>
                    <tbody>
                      {balanceSheetData.monthly_breakdown?.map((month, idx) => (
                        <tr key={idx} className="border-b hover:bg-slate-50">
                          <td className="py-2 px-3 font-medium">{month.month}</td>
                          <td className="py-2 px-3 text-center">{month.po_count}</td>
                          <td className="py-2 px-3 text-right text-red-600">₹{month.po_value?.toLocaleString()}</td>
                          <td className="py-2 px-3 text-center">{month.payment_count}</td>
                          <td className="py-2 px-3 text-right text-green-600">₹{month.payment_value?.toLocaleString()}</td>
                          <td className={`py-2 px-3 text-right font-medium ${month.net > 0 ? 'text-red-600' : 'text-green-600'}`}>
                            {month.net > 0 ? '+' : ''}₹{month.net?.toLocaleString()}
                          </td>
                          <td className="py-2 px-3 text-right font-bold">₹{month.closing_balance?.toLocaleString()}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              {/* Top Purchases */}
              {balanceSheetData.top_purchases?.length > 0 && (
                <div>
                  <h3 className="text-lg font-semibold mb-3">Top Purchases</h3>
                  <div className="space-y-2">
                    {balanceSheetData.top_purchases.map((po, idx) => (
                      <div key={idx} className="flex items-center justify-between p-3 bg-slate-50 rounded-lg">
                        <div>
                          <span className="font-mono text-teal-600">{po.po_number}</span>
                          <span className="text-slate-500 text-sm ml-2">{po.date}</span>
                        </div>
                        <div className="text-right">
                          <span className="font-bold">₹{po.amount?.toLocaleString()}</span>
                          <span className={`ml-2 text-xs px-2 py-0.5 rounded ${
                            po.status === 'fully_paid' ? 'bg-green-100 text-green-700' : 'bg-amber-100 text-amber-700'
                          }`}>{po.status}</span>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      )}

      {/* Bulk Payment Modal */}
      {showBulkPayment && bulkPaymentVendor && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center p-4 z-50">
          <Card className="w-full max-w-2xl max-h-[90vh] overflow-y-auto">
            <CardHeader className="flex flex-row items-center justify-between bg-gradient-to-r from-violet-600 to-purple-600 text-white rounded-t-lg">
              <div>
                <CardTitle className="text-white flex items-center gap-2">
                  <Layers className="w-5 h-5" /> Bulk Payment
                </CardTitle>
                <p className="text-violet-100 text-sm">{bulkPaymentVendor.name} ({bulkPaymentVendor.vendor_code})</p>
              </div>
              <Button variant="ghost" size="sm" className="text-white hover:bg-white/20" onClick={() => setShowBulkPayment(false)}>
                <X className="w-5 h-5" />
              </Button>
            </CardHeader>
            <CardContent className="p-6 space-y-4">
              {/* PO Selection */}
              <div>
                <label className="text-sm font-medium text-slate-700 mb-2 block">Select POs to Pay</label>
                <div className="border rounded-lg max-h-48 overflow-y-auto">
                  {pos.filter(po => po.vendor_id === bulkPaymentVendor.id && po.status === 'approved' && po.payment_status !== 'fully_paid').map(po => (
                    <label key={po.id} className="flex items-center justify-between p-3 hover:bg-slate-50 border-b cursor-pointer">
                      <div className="flex items-center gap-3">
                        <input
                          type="checkbox"
                          checked={selectedPOsForBulk.includes(po.id)}
                          onChange={(e) => {
                            if (e.target.checked) {
                              setSelectedPOsForBulk([...selectedPOsForBulk, po.id]);
                            } else {
                              setSelectedPOsForBulk(selectedPOsForBulk.filter(id => id !== po.id));
                            }
                          }}
                          className="w-4 h-4 rounded border-slate-300 text-violet-600"
                        />
                        <div>
                          <span className="font-mono text-sm text-teal-600">{po.po_number}</span>
                          <span className="text-slate-500 text-xs ml-2">{po.created_at?.slice(0, 10)}</span>
                        </div>
                      </div>
                      <span className="font-bold text-red-600">₹{(po.outstanding_balance || po.grand_total)?.toLocaleString()}</span>
                    </label>
                  ))}
                </div>
                <div className="flex justify-between mt-2 text-sm">
                  <span className="text-slate-600">{selectedPOsForBulk.length} POs selected</span>
                  <button 
                    className="text-violet-600 hover:underline"
                    onClick={() => {
                      const allIds = pos.filter(po => po.vendor_id === bulkPaymentVendor.id && po.status === 'approved' && po.payment_status !== 'fully_paid').map(po => po.id);
                      setSelectedPOsForBulk(selectedPOsForBulk.length === allIds.length ? [] : allIds);
                    }}
                  >
                    {selectedPOsForBulk.length === pos.filter(po => po.vendor_id === bulkPaymentVendor.id).length ? 'Deselect All' : 'Select All'}
                  </button>
                </div>
              </div>

              {/* Total Amount */}
              <div className="bg-violet-50 rounded-lg p-4 text-center">
                <p className="text-sm text-violet-600">Total Payment Amount</p>
                <p className="text-3xl font-bold text-violet-700">₹{calculateBulkTotal().toLocaleString()}</p>
              </div>

              {/* Payment Mode */}
              <div>
                <label className="text-sm font-medium text-slate-700">Payment Mode</label>
                <select
                  value={bulkPaymentData.payment_mode}
                  onChange={(e) => setBulkPaymentData({...bulkPaymentData, payment_mode: e.target.value})}
                  className="w-full h-10 rounded-lg border border-slate-300 px-3 mt-1"
                  disabled={payoutProcessing}
                >
                  <option value="razorpay">RazorpayX Bulk Payout (Auto UTR)</option>
                  <option value="bank_transfer">Bank Transfer / NEFT / RTGS (Manual UTR)</option>
                  <option value="upi">UPI Transfer (Manual UTR)</option>
                  <option value="net_banking">Net Banking (Manual UTR)</option>
                  <option value="cash">Cash</option>
                </select>
                {bulkPaymentData.payment_mode === 'razorpay' && (
                  <p className="text-xs text-green-600 mt-1 flex items-center gap-1">
                    <ArrowUpRight className="w-3 h-3" />
                    Bulk outgoing transfer to vendor&apos;s bank.
                  </p>
                )}
              </div>

              {/* UTR Input for manual payments */}
              {bulkPaymentData.payment_mode !== 'razorpay' && bulkPaymentData.payment_mode !== 'cash' && (
                <div>
                  <label className="text-sm font-medium text-slate-700">UTR / Transaction Reference *</label>
                  <input
                    type="text"
                    value={bulkPaymentData.utr_reference}
                    onChange={(e) => setBulkPaymentData({...bulkPaymentData, utr_reference: e.target.value.toUpperCase()})}
                    placeholder="Enter UTR number"
                    className="w-full h-10 rounded-lg border border-slate-300 px-3 mt-1"
                    disabled={payoutProcessing}
                  />
                </div>
              )}

              {/* Notes */}
              <div>
                <label className="text-sm font-medium text-slate-700">Notes (Optional)</label>
                <textarea
                  value={bulkPaymentData.notes}
                  onChange={(e) => setBulkPaymentData({...bulkPaymentData, notes: e.target.value})}
                  placeholder="Payment notes..."
                  className="w-full h-20 rounded-lg border border-slate-300 px-3 py-2 mt-1"
                  disabled={payoutProcessing}
                />
              </div>

              {/* Actions */}
              <div className="flex gap-3 pt-4">
                <Button variant="outline" className="flex-1" onClick={closeBulkPaymentModal} disabled={payoutProcessing}>
                  Cancel
                </Button>
                <Button 
                  className="flex-1 bg-violet-600 hover:bg-violet-700"
                  onClick={processBulkPayment}
                  disabled={payoutProcessing || selectedPOsForBulk.length === 0 || (bulkPaymentData.payment_mode !== 'razorpay' && bulkPaymentData.payment_mode !== 'cash' && !bulkPaymentData.utr_reference)}
                >
                  {payoutProcessing ? (
                    <>
                      <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                      Processing...
                    </>
                  ) : (
                    <>
                      {bulkPaymentData.payment_mode === 'razorpay' ? (
                        <><Send className="w-4 h-4 mr-2" /> Send Bulk Payout</>
                      ) : (
                        <><Layers className="w-4 h-4 mr-2" /> Record Bulk Payment</>
                      )}
                    </>
                  )}
                </Button>
              </div>
            </CardContent>
          </Card>
        </div>
      )}
    </div>
  );
};

export default VendorManagement;
