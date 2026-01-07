import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { 
  BookOpen, Users, Building2, FileText, Download, Mail, Share2,
  Search, Filter, Calendar, RefreshCw, Loader2, ChevronDown, ChevronRight,
  TrendingUp, TrendingDown, AlertCircle, CheckCircle, Clock, Lock, Unlock,
  Settings, Eye, Plus, FileSpreadsheet, BarChart3, PieChart, ArrowUpRight,
  ArrowDownRight, Scale, Receipt, CreditCard, Wallet, IndianRupee
} from 'lucide-react';
import { toast } from 'sonner';
import { useAuth } from '../../contexts/AuthContext';

const API_BASE = process.env.REACT_APP_BACKEND_URL;

const LedgerManagement = () => {
  const { user } = useAuth();
  const [activeTab, setActiveTab] = useState('customer');
  const [loading, setLoading] = useState(false);
  const [customers, setCustomers] = useState([]);
  const [vendors, setVendors] = useState([]);
  const [glAccounts, setGLAccounts] = useState([]);
  const [selectedParty, setSelectedParty] = useState(null);
  const [ledgerData, setLedgerData] = useState(null);
  const [dateRange, setDateRange] = useState({
    start: new Date(new Date().getFullYear(), new Date().getMonth(), 1).toISOString().split('T')[0],
    end: new Date().toISOString().split('T')[0]
  });
  const [outstandingReport, setOutstandingReport] = useState(null);
  const [trialBalance, setTrialBalance] = useState(null);
  const [periodLocks, setPeriodLocks] = useState([]);
  const [openingBalances, setOpeningBalances] = useState([]);
  const [showOpeningBalanceModal, setShowOpeningBalanceModal] = useState(false);
  const [showPeriodLockModal, setShowPeriodLockModal] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');

  const getAuthHeaders = () => ({
    'Authorization': `Bearer ${localStorage.getItem('token')}`
  });

  const isAdmin = ['super_admin', 'admin'].includes(user?.role);
  const isCA = user?.role === 'ca' || user?.role === 'auditor';

  useEffect(() => {
    fetchCustomers();
    fetchVendors();
    fetchGLAccounts();
    fetchPeriodLocks();
    fetchOpeningBalances();
  }, []);

  const fetchCustomers = async () => {
    try {
      const res = await fetch(`${API_BASE}/api/erp/crm/customers`, {
        headers: getAuthHeaders()
      });
      if (res.ok) {
        const data = await res.json();
        setCustomers(data.customers || []);
      }
    } catch (error) {
      console.error('Error fetching customers:', error);
    }
  };

  const fetchVendors = async () => {
    try {
      const res = await fetch(`${API_BASE}/api/erp/vendors/`, {
        headers: getAuthHeaders()
      });
      if (res.ok) {
        const data = await res.json();
        setVendors(data.vendors || []);
      }
    } catch (error) {
      console.error('Error fetching vendors:', error);
    }
  };

  const fetchGLAccounts = async () => {
    try {
      const res = await fetch(`${API_BASE}/api/erp/ledger/gl/accounts`, {
        headers: getAuthHeaders()
      });
      if (res.ok) {
        const data = await res.json();
        setGLAccounts(data.accounts || []);
      }
    } catch (error) {
      // GL accounts may not be initialized yet
      console.log('GL accounts not initialized');
    }
  };

  const fetchPeriodLocks = async () => {
    try {
      const res = await fetch(`${API_BASE}/api/erp/ledger/period-locks`, {
        headers: getAuthHeaders()
      });
      if (res.ok) {
        const data = await res.json();
        setPeriodLocks(data.locks || []);
      }
    } catch (error) {
      console.error('Error fetching period locks:', error);
    }
  };

  const fetchOpeningBalances = async () => {
    try {
      const res = await fetch(`${API_BASE}/api/erp/ledger/opening-balances`, {
        headers: getAuthHeaders()
      });
      if (res.ok) {
        const data = await res.json();
        setOpeningBalances(data.opening_balances || []);
      }
    } catch (error) {
      console.error('Error fetching opening balances:', error);
    }
  };

  const fetchCustomerLedger = async (customerId) => {
    setLoading(true);
    try {
      const res = await fetch(
        `${API_BASE}/api/erp/ledger/customer/${customerId}?start_date=${dateRange.start}&end_date=${dateRange.end}`,
        { headers: getAuthHeaders() }
      );
      if (res.ok) {
        const data = await res.json();
        setLedgerData(data);
      } else {
        toast.error('Failed to fetch customer ledger');
      }
    } catch (error) {
      toast.error('Error fetching ledger');
    } finally {
      setLoading(false);
    }
  };

  const fetchVendorLedger = async (vendorId) => {
    setLoading(true);
    try {
      const res = await fetch(
        `${API_BASE}/api/erp/ledger/vendor/${vendorId}?start_date=${dateRange.start}&end_date=${dateRange.end}`,
        { headers: getAuthHeaders() }
      );
      if (res.ok) {
        const data = await res.json();
        setLedgerData(data);
      } else {
        toast.error('Failed to fetch vendor ledger');
      }
    } catch (error) {
      toast.error('Error fetching ledger');
    } finally {
      setLoading(false);
    }
  };

  const fetchCustomerOutstanding = async () => {
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE}/api/erp/ledger/customers/outstanding?include_ageing=true`, {
        headers: getAuthHeaders()
      });
      if (res.ok) {
        const data = await res.json();
        setOutstandingReport(data);
      }
    } catch (error) {
      toast.error('Error fetching outstanding report');
    } finally {
      setLoading(false);
    }
  };

  const fetchVendorOutstanding = async () => {
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE}/api/erp/ledger/vendors/outstanding?include_ageing=true`, {
        headers: getAuthHeaders()
      });
      if (res.ok) {
        const data = await res.json();
        setOutstandingReport(data);
      }
    } catch (error) {
      toast.error('Error fetching outstanding report');
    } finally {
      setLoading(false);
    }
  };

  const fetchTrialBalance = async () => {
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE}/api/erp/ledger/gl/trial-balance?as_of_date=${dateRange.end}`, {
        headers: getAuthHeaders()
      });
      if (res.ok) {
        const data = await res.json();
        setTrialBalance(data);
      }
    } catch (error) {
      toast.error('Error fetching trial balance');
    } finally {
      setLoading(false);
    }
  };

  const initializeGLAccounts = async () => {
    try {
      const res = await fetch(`${API_BASE}/api/erp/ledger/init-gl-accounts`, {
        method: 'POST',
        headers: getAuthHeaders()
      });
      if (res.ok) {
        toast.success('GL accounts initialized successfully');
        fetchGLAccounts();
      } else {
        const err = await res.json();
        toast.error(err.detail || 'Failed to initialize GL accounts');
      }
    } catch (error) {
      toast.error('Error initializing GL accounts');
    }
  };

  const handlePartySelect = (party, type) => {
    setSelectedParty({ ...party, type });
    if (type === 'customer') {
      fetchCustomerLedger(party.id);
    } else {
      fetchVendorLedger(party.id);
    }
  };

  const downloadLedgerPDF = () => {
    if (!ledgerData) return;
    // Generate PDF download logic
    toast.info('PDF download feature - Coming soon!');
  };

  const downloadLedgerExcel = () => {
    if (!ledgerData) return;
    // Generate Excel download logic
    toast.info('Excel download feature - Coming soon!');
  };

  const shareLedgerWhatsApp = () => {
    if (!ledgerData) return;
    const message = `Ledger Statement for ${selectedParty?.name || selectedParty?.company_name}\nPeriod: ${dateRange.start} to ${dateRange.end}\nClosing Balance: ₹${ledgerData.summary?.closing_balance?.toLocaleString()} ${ledgerData.summary?.balance_type}`;
    window.open(`https://wa.me/?text=${encodeURIComponent(message)}`, '_blank');
  };

  const tabs = [
    { id: 'customer', label: 'Customer Ledger', icon: Users, color: 'text-blue-400' },
    { id: 'vendor', label: 'Vendor Ledger', icon: Building2, color: 'text-violet-400' },
    { id: 'general', label: 'General Ledger', icon: Scale, color: 'text-emerald-400' },
    { id: 'outstanding', label: 'Outstanding Report', icon: AlertCircle, color: 'text-amber-400' },
    { id: 'settings', label: 'Settings', icon: Settings, color: 'text-slate-400' }
  ];

  // Filter for settings tab - only show for admins
  const visibleTabs = tabs.filter(tab => {
    if (tab.id === 'settings' && !isAdmin) return false;
    return true;
  });

  const filteredCustomers = customers.filter(c => 
    c.name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
    c.company_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
    c.phone?.includes(searchTerm)
  );

  const filteredVendors = vendors.filter(v =>
    v.name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
    v.company_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
    v.vendor_code?.toLowerCase().includes(searchTerm.toLowerCase())
  );

  return (
    <div className="space-y-6" data-testid="ledger-management">
      {/* Header */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold text-slate-900 flex items-center gap-2">
            <BookOpen className="w-7 h-7 text-emerald-600" />
            Ledger Management
          </h1>
          <p className="text-slate-600 text-sm mt-1">Customer, Vendor & General Ledger with CA-ready reports</p>
        </div>
        {isCA && (
          <div className="flex items-center gap-2 px-4 py-2 bg-amber-50 border border-amber-200 rounded-lg">
            <Eye className="w-4 h-4 text-amber-600" />
            <span className="text-sm text-amber-700 font-medium">Read-Only Access (CA/Auditor)</span>
          </div>
        )}
      </div>

      {/* Tabs */}
      <div className="flex flex-wrap gap-2 border-b border-slate-200 pb-2">
        {visibleTabs.map(tab => (
          <button
            key={tab.id}
            onClick={() => {
              setActiveTab(tab.id);
              setSelectedParty(null);
              setLedgerData(null);
              setOutstandingReport(null);
            }}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg transition-all ${
              activeTab === tab.id 
                ? 'bg-slate-900 text-white' 
                : 'bg-slate-100 text-slate-700 hover:bg-slate-200'
            }`}
            data-testid={`tab-${tab.id}`}
          >
            <tab.icon className={`w-4 h-4 ${activeTab === tab.id ? 'text-white' : tab.color}`} />
            {tab.label}
          </button>
        ))}
      </div>

      {/* Date Range Filter */}
      {['customer', 'vendor', 'general'].includes(activeTab) && (
        <Card className="bg-slate-50">
          <CardContent className="py-4">
            <div className="flex flex-wrap items-center gap-4">
              <div className="flex items-center gap-2">
                <Calendar className="w-4 h-4 text-slate-500" />
                <span className="text-sm font-medium text-slate-700">Period:</span>
              </div>
              <input
                type="date"
                value={dateRange.start}
                onChange={(e) => setDateRange({...dateRange, start: e.target.value})}
                className="h-9 px-3 rounded-lg border border-slate-300 text-sm"
              />
              <span className="text-slate-500">to</span>
              <input
                type="date"
                value={dateRange.end}
                onChange={(e) => setDateRange({...dateRange, end: e.target.value})}
                className="h-9 px-3 rounded-lg border border-slate-300 text-sm"
              />
              <Button 
                variant="outline" 
                size="sm"
                onClick={() => {
                  if (selectedParty) {
                    if (selectedParty.type === 'customer') fetchCustomerLedger(selectedParty.id);
                    else fetchVendorLedger(selectedParty.id);
                  }
                }}
              >
                <RefreshCw className="w-4 h-4 mr-1" /> Refresh
              </Button>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Customer Ledger Tab */}
      {activeTab === 'customer' && (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Customer List */}
          <Card className="lg:col-span-1">
            <CardHeader className="pb-3">
              <CardTitle className="text-base flex items-center gap-2">
                <Users className="w-5 h-5 text-blue-500" />
                Select Customer
              </CardTitle>
              <div className="relative mt-2">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
                <input
                  type="text"
                  placeholder="Search customers..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="w-full h-9 pl-9 pr-3 rounded-lg border border-slate-300 text-sm"
                />
              </div>
            </CardHeader>
            <CardContent className="max-h-[500px] overflow-y-auto">
              <div className="space-y-2">
                {filteredCustomers.map(customer => (
                  <div
                    key={customer.id}
                    onClick={() => handlePartySelect(customer, 'customer')}
                    className={`p-3 rounded-lg cursor-pointer transition-all ${
                      selectedParty?.id === customer.id 
                        ? 'bg-blue-100 border-2 border-blue-500' 
                        : 'bg-slate-50 hover:bg-slate-100 border border-transparent'
                    }`}
                  >
                    <p className="font-medium text-slate-900">{customer.name}</p>
                    {customer.company_name && (
                      <p className="text-xs text-slate-500">{customer.company_name}</p>
                    )}
                    <p className="text-xs text-slate-400">{customer.phone}</p>
                  </div>
                ))}
                {filteredCustomers.length === 0 && (
                  <p className="text-center text-slate-500 py-4">No customers found</p>
                )}
              </div>
            </CardContent>
          </Card>

          {/* Ledger Display */}
          <Card className="lg:col-span-2">
            <CardHeader className="border-b">
              <div className="flex items-center justify-between">
                <div>
                  <CardTitle className="text-lg">
                    {selectedParty ? `${selectedParty.name} - Ledger Statement` : 'Select a customer to view ledger'}
                  </CardTitle>
                  {ledgerData && (
                    <p className="text-xs text-slate-500 mt-1">
                      Period: {dateRange.start} to {dateRange.end}
                    </p>
                  )}
                </div>
                {ledgerData && (
                  <div className="flex items-center gap-2">
                    <Button variant="outline" size="sm" onClick={downloadLedgerPDF}>
                      <Download className="w-4 h-4 mr-1" /> PDF
                    </Button>
                    <Button variant="outline" size="sm" onClick={downloadLedgerExcel}>
                      <FileSpreadsheet className="w-4 h-4 mr-1" /> Excel
                    </Button>
                    <Button variant="outline" size="sm" onClick={shareLedgerWhatsApp}>
                      <Share2 className="w-4 h-4 mr-1" /> Share
                    </Button>
                  </div>
                )}
              </div>
            </CardHeader>
            <CardContent className="p-0">
              {loading ? (
                <div className="flex items-center justify-center py-12">
                  <Loader2 className="w-8 h-8 animate-spin text-blue-500" />
                </div>
              ) : ledgerData ? (
                <div>
                  {/* Summary */}
                  <div className="grid grid-cols-4 gap-4 p-4 bg-slate-50 border-b">
                    <div className="text-center">
                      <p className="text-xs text-slate-500">Opening Balance</p>
                      <p className="text-lg font-bold text-slate-900">
                        ₹{ledgerData.opening_balance?.amount?.toLocaleString() || 0}
                        <span className="text-xs ml-1">{ledgerData.opening_balance?.type === 'debit' ? 'Dr' : 'Cr'}</span>
                      </p>
                    </div>
                    <div className="text-center">
                      <p className="text-xs text-slate-500">Total Debit</p>
                      <p className="text-lg font-bold text-green-600">₹{ledgerData.summary?.total_debit?.toLocaleString()}</p>
                    </div>
                    <div className="text-center">
                      <p className="text-xs text-slate-500">Total Credit</p>
                      <p className="text-lg font-bold text-red-600">₹{ledgerData.summary?.total_credit?.toLocaleString()}</p>
                    </div>
                    <div className="text-center">
                      <p className="text-xs text-slate-500">Closing Balance</p>
                      <p className="text-lg font-bold text-blue-600">
                        ₹{Math.abs(ledgerData.summary?.closing_balance)?.toLocaleString()}
                        <span className="text-xs ml-1">{ledgerData.summary?.balance_type}</span>
                      </p>
                    </div>
                  </div>

                  {/* Ledger Entries Table */}
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead className="bg-slate-100">
                        <tr>
                          <th className="px-4 py-3 text-left font-medium text-slate-700">Date</th>
                          <th className="px-4 py-3 text-left font-medium text-slate-700">Particulars</th>
                          <th className="px-4 py-3 text-left font-medium text-slate-700">Ref No.</th>
                          <th className="px-4 py-3 text-right font-medium text-slate-700">Debit</th>
                          <th className="px-4 py-3 text-right font-medium text-slate-700">Credit</th>
                          <th className="px-4 py-3 text-right font-medium text-slate-700">Balance</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-100">
                        {/* Opening Balance Row */}
                        <tr className="bg-blue-50">
                          <td className="px-4 py-3 font-medium">{ledgerData.opening_balance?.as_of_date || '-'}</td>
                          <td className="px-4 py-3 font-medium text-blue-700">Opening Balance</td>
                          <td className="px-4 py-3">-</td>
                          <td className="px-4 py-3 text-right">
                            {ledgerData.opening_balance?.type === 'debit' ? `₹${ledgerData.opening_balance?.amount?.toLocaleString()}` : '-'}
                          </td>
                          <td className="px-4 py-3 text-right">
                            {ledgerData.opening_balance?.type === 'credit' ? `₹${ledgerData.opening_balance?.amount?.toLocaleString()}` : '-'}
                          </td>
                          <td className="px-4 py-3 text-right font-medium">
                            ₹{ledgerData.opening_balance?.amount?.toLocaleString() || 0}
                          </td>
                        </tr>
                        {ledgerData.entries?.map((entry, idx) => (
                          <tr key={idx} className="hover:bg-slate-50">
                            <td className="px-4 py-3">{entry.transaction_date}</td>
                            <td className="px-4 py-3">
                              <span className={`px-2 py-0.5 rounded text-xs font-medium ${
                                entry.entry_type === 'sales_invoice' ? 'bg-blue-100 text-blue-700' :
                                entry.entry_type === 'payment_received' ? 'bg-green-100 text-green-700' :
                                entry.entry_type === 'credit_note' ? 'bg-red-100 text-red-700' :
                                'bg-slate-100 text-slate-700'
                              }`}>
                                {entry.entry_type?.replace(/_/g, ' ').toUpperCase()}
                              </span>
                              <p className="text-xs text-slate-500 mt-1">{entry.description}</p>
                            </td>
                            <td className="px-4 py-3 font-mono text-xs">{entry.reference_number}</td>
                            <td className="px-4 py-3 text-right text-green-600">
                              {entry.debit > 0 ? `₹${entry.debit?.toLocaleString()}` : '-'}
                            </td>
                            <td className="px-4 py-3 text-right text-red-600">
                              {entry.credit > 0 ? `₹${entry.credit?.toLocaleString()}` : '-'}
                            </td>
                            <td className="px-4 py-3 text-right font-medium">
                              ₹{Math.abs(entry.running_balance)?.toLocaleString()}
                              <span className="text-xs ml-1">{entry.running_balance >= 0 ? 'Dr' : 'Cr'}</span>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                      <tfoot className="bg-slate-100 font-semibold">
                        <tr>
                          <td colSpan="3" className="px-4 py-3">Closing Balance</td>
                          <td className="px-4 py-3 text-right">₹{ledgerData.summary?.total_debit?.toLocaleString()}</td>
                          <td className="px-4 py-3 text-right">₹{ledgerData.summary?.total_credit?.toLocaleString()}</td>
                          <td className="px-4 py-3 text-right text-blue-600">
                            ₹{Math.abs(ledgerData.summary?.closing_balance)?.toLocaleString()}
                            <span className="text-xs ml-1">{ledgerData.summary?.balance_type}</span>
                          </td>
                        </tr>
                      </tfoot>
                    </table>
                  </div>

                  {/* Disclaimer */}
                  <div className="p-4 bg-amber-50 border-t border-amber-200">
                    <p className="text-xs text-amber-700 text-center font-medium">
                      {ledgerData.disclaimer}
                    </p>
                  </div>
                </div>
              ) : (
                <div className="flex flex-col items-center justify-center py-12 text-slate-500">
                  <BookOpen className="w-12 h-12 mb-3 text-slate-300" />
                  <p>Select a customer from the list to view their ledger</p>
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      )}

      {/* Vendor Ledger Tab */}
      {activeTab === 'vendor' && (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Vendor List */}
          <Card className="lg:col-span-1">
            <CardHeader className="pb-3">
              <CardTitle className="text-base flex items-center gap-2">
                <Building2 className="w-5 h-5 text-violet-500" />
                Select Vendor
              </CardTitle>
              <div className="relative mt-2">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
                <input
                  type="text"
                  placeholder="Search vendors..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="w-full h-9 pl-9 pr-3 rounded-lg border border-slate-300 text-sm"
                />
              </div>
            </CardHeader>
            <CardContent className="max-h-[500px] overflow-y-auto">
              <div className="space-y-2">
                {filteredVendors.map(vendor => (
                  <div
                    key={vendor.id}
                    onClick={() => handlePartySelect(vendor, 'vendor')}
                    className={`p-3 rounded-lg cursor-pointer transition-all ${
                      selectedParty?.id === vendor.id 
                        ? 'bg-violet-100 border-2 border-violet-500' 
                        : 'bg-slate-50 hover:bg-slate-100 border border-transparent'
                    }`}
                  >
                    <p className="font-medium text-slate-900">{vendor.name || vendor.company_name}</p>
                    <p className="text-xs text-slate-500">{vendor.vendor_code}</p>
                    <p className="text-xs text-slate-400">{vendor.phone}</p>
                  </div>
                ))}
                {filteredVendors.length === 0 && (
                  <p className="text-center text-slate-500 py-4">No vendors found</p>
                )}
              </div>
            </CardContent>
          </Card>

          {/* Vendor Ledger Display - Similar structure to Customer */}
          <Card className="lg:col-span-2">
            <CardHeader className="border-b">
              <div className="flex items-center justify-between">
                <div>
                  <CardTitle className="text-lg">
                    {selectedParty ? `${selectedParty.name || selectedParty.company_name} - Ledger Statement` : 'Select a vendor to view ledger'}
                  </CardTitle>
                  {ledgerData && (
                    <p className="text-xs text-slate-500 mt-1">
                      Period: {dateRange.start} to {dateRange.end}
                    </p>
                  )}
                </div>
                {ledgerData && (
                  <div className="flex items-center gap-2">
                    <Button variant="outline" size="sm" onClick={downloadLedgerPDF}>
                      <Download className="w-4 h-4 mr-1" /> PDF
                    </Button>
                    <Button variant="outline" size="sm" onClick={downloadLedgerExcel}>
                      <FileSpreadsheet className="w-4 h-4 mr-1" /> Excel
                    </Button>
                    <Button variant="outline" size="sm" onClick={shareLedgerWhatsApp}>
                      <Share2 className="w-4 h-4 mr-1" /> Share
                    </Button>
                  </div>
                )}
              </div>
            </CardHeader>
            <CardContent className="p-0">
              {loading ? (
                <div className="flex items-center justify-center py-12">
                  <Loader2 className="w-8 h-8 animate-spin text-violet-500" />
                </div>
              ) : ledgerData ? (
                <div>
                  {/* Summary */}
                  <div className="grid grid-cols-4 gap-4 p-4 bg-slate-50 border-b">
                    <div className="text-center">
                      <p className="text-xs text-slate-500">Opening Balance</p>
                      <p className="text-lg font-bold text-slate-900">
                        ₹{ledgerData.opening_balance?.amount?.toLocaleString() || 0}
                        <span className="text-xs ml-1">{ledgerData.opening_balance?.type === 'credit' ? 'Cr' : 'Dr'}</span>
                      </p>
                    </div>
                    <div className="text-center">
                      <p className="text-xs text-slate-500">Total Purchases</p>
                      <p className="text-lg font-bold text-red-600">₹{ledgerData.summary?.total_credit?.toLocaleString()}</p>
                    </div>
                    <div className="text-center">
                      <p className="text-xs text-slate-500">Total Payments</p>
                      <p className="text-lg font-bold text-green-600">₹{ledgerData.summary?.total_debit?.toLocaleString()}</p>
                    </div>
                    <div className="text-center">
                      <p className="text-xs text-slate-500">Closing Balance</p>
                      <p className="text-lg font-bold text-violet-600">
                        ₹{Math.abs(ledgerData.summary?.closing_balance)?.toLocaleString()}
                        <span className="text-xs ml-1">{ledgerData.summary?.balance_type}</span>
                      </p>
                    </div>
                  </div>

                  {/* Ledger Entries Table - Same structure */}
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead className="bg-slate-100">
                        <tr>
                          <th className="px-4 py-3 text-left font-medium text-slate-700">Date</th>
                          <th className="px-4 py-3 text-left font-medium text-slate-700">Particulars</th>
                          <th className="px-4 py-3 text-left font-medium text-slate-700">Ref No.</th>
                          <th className="px-4 py-3 text-right font-medium text-slate-700">Debit</th>
                          <th className="px-4 py-3 text-right font-medium text-slate-700">Credit</th>
                          <th className="px-4 py-3 text-right font-medium text-slate-700">Balance</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-100">
                        {ledgerData.entries?.map((entry, idx) => (
                          <tr key={idx} className="hover:bg-slate-50">
                            <td className="px-4 py-3">{entry.transaction_date}</td>
                            <td className="px-4 py-3">
                              <span className={`px-2 py-0.5 rounded text-xs font-medium ${
                                entry.entry_type === 'purchase_bill' ? 'bg-red-100 text-red-700' :
                                entry.entry_type === 'payment_made' ? 'bg-green-100 text-green-700' :
                                entry.entry_type === 'debit_note' ? 'bg-blue-100 text-blue-700' :
                                'bg-slate-100 text-slate-700'
                              }`}>
                                {entry.entry_type?.replace(/_/g, ' ').toUpperCase()}
                              </span>
                              <p className="text-xs text-slate-500 mt-1">{entry.description}</p>
                            </td>
                            <td className="px-4 py-3 font-mono text-xs">{entry.reference_number}</td>
                            <td className="px-4 py-3 text-right text-green-600">
                              {entry.debit > 0 ? `₹${entry.debit?.toLocaleString()}` : '-'}
                            </td>
                            <td className="px-4 py-3 text-right text-red-600">
                              {entry.credit > 0 ? `₹${entry.credit?.toLocaleString()}` : '-'}
                            </td>
                            <td className="px-4 py-3 text-right font-medium">
                              ₹{Math.abs(entry.running_balance)?.toLocaleString()}
                              <span className="text-xs ml-1">{entry.running_balance >= 0 ? 'Cr' : 'Dr'}</span>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>

                  {/* Disclaimer */}
                  <div className="p-4 bg-amber-50 border-t border-amber-200">
                    <p className="text-xs text-amber-700 text-center font-medium">
                      {ledgerData.disclaimer}
                    </p>
                  </div>
                </div>
              ) : (
                <div className="flex flex-col items-center justify-center py-12 text-slate-500">
                  <BookOpen className="w-12 h-12 mb-3 text-slate-300" />
                  <p>Select a vendor from the list to view their ledger</p>
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      )}

      {/* General Ledger Tab */}
      {activeTab === 'general' && (
        <div className="space-y-6">
          {/* GL Accounts */}
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <CardTitle className="flex items-center gap-2">
                  <Scale className="w-5 h-5 text-emerald-500" />
                  Chart of Accounts
                </CardTitle>
                {glAccounts.length === 0 && isAdmin && (
                  <Button onClick={initializeGLAccounts}>
                    <Plus className="w-4 h-4 mr-2" /> Initialize GL Accounts
                  </Button>
                )}
                <Button variant="outline" onClick={fetchTrialBalance}>
                  <BarChart3 className="w-4 h-4 mr-2" /> View Trial Balance
                </Button>
              </div>
            </CardHeader>
            <CardContent>
              {glAccounts.length > 0 ? (
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                  {['asset', 'liability', 'income', 'expense', 'equity'].map(type => (
                    <div key={type} className="border rounded-lg p-4">
                      <h3 className="font-semibold text-slate-900 capitalize mb-3 flex items-center gap-2">
                        {type === 'asset' && <TrendingUp className="w-4 h-4 text-green-500" />}
                        {type === 'liability' && <TrendingDown className="w-4 h-4 text-red-500" />}
                        {type === 'income' && <ArrowUpRight className="w-4 h-4 text-blue-500" />}
                        {type === 'expense' && <ArrowDownRight className="w-4 h-4 text-orange-500" />}
                        {type === 'equity' && <Scale className="w-4 h-4 text-violet-500" />}
                        {type.charAt(0).toUpperCase() + type.slice(1)}s
                      </h3>
                      <div className="space-y-2">
                        {glAccounts.filter(a => a.type === type).map(account => (
                          <div key={account.code} className="flex items-center justify-between text-sm">
                            <span className="text-slate-600">{account.code} - {account.name}</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="text-center py-8 text-slate-500">
                  <Scale className="w-12 h-12 mx-auto mb-3 text-slate-300" />
                  <p>GL accounts not initialized. Click "Initialize GL Accounts" to set up the chart of accounts.</p>
                </div>
              )}
            </CardContent>
          </Card>

          {/* Trial Balance */}
          {trialBalance && (
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <BarChart3 className="w-5 h-5 text-blue-500" />
                  Trial Balance as of {trialBalance.as_of_date}
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead className="bg-slate-100">
                      <tr>
                        <th className="px-4 py-3 text-left">Account Code</th>
                        <th className="px-4 py-3 text-left">Account Name</th>
                        <th className="px-4 py-3 text-left">Type</th>
                        <th className="px-4 py-3 text-right">Debit</th>
                        <th className="px-4 py-3 text-right">Credit</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-100">
                      {trialBalance.trial_balance?.map((item, idx) => (
                        <tr key={idx} className="hover:bg-slate-50">
                          <td className="px-4 py-3 font-mono">{item.account_code}</td>
                          <td className="px-4 py-3">{item.account_name}</td>
                          <td className="px-4 py-3 capitalize">{item.account_type}</td>
                          <td className="px-4 py-3 text-right text-green-600">
                            {item.debit > 0 ? `₹${item.debit.toLocaleString()}` : '-'}
                          </td>
                          <td className="px-4 py-3 text-right text-red-600">
                            {item.credit > 0 ? `₹${item.credit.toLocaleString()}` : '-'}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                    <tfoot className="bg-slate-100 font-semibold">
                      <tr>
                        <td colSpan="3" className="px-4 py-3">Total</td>
                        <td className="px-4 py-3 text-right text-green-700">₹{trialBalance.totals?.debit?.toLocaleString()}</td>
                        <td className="px-4 py-3 text-right text-red-700">₹{trialBalance.totals?.credit?.toLocaleString()}</td>
                      </tr>
                    </tfoot>
                  </table>
                </div>
                <div className={`mt-4 p-3 rounded-lg text-center ${trialBalance.is_balanced ? 'bg-green-50 text-green-700' : 'bg-red-50 text-red-700'}`}>
                  {trialBalance.is_balanced ? (
                    <span className="flex items-center justify-center gap-2">
                      <CheckCircle className="w-5 h-5" /> Trial Balance is Balanced
                    </span>
                  ) : (
                    <span className="flex items-center justify-center gap-2">
                      <AlertCircle className="w-5 h-5" /> 
                      Trial Balance has difference of ₹{Math.abs(trialBalance.totals?.difference)?.toLocaleString()}
                    </span>
                  )}
                </div>
              </CardContent>
            </Card>
          )}
        </div>
      )}

      {/* Outstanding Report Tab */}
      {activeTab === 'outstanding' && (
        <div className="space-y-6">
          <div className="flex gap-4">
            <Button onClick={fetchCustomerOutstanding} variant={outstandingReport?.customers ? 'default' : 'outline'}>
              <Users className="w-4 h-4 mr-2" /> Customer Outstanding
            </Button>
            <Button onClick={fetchVendorOutstanding} variant={outstandingReport?.vendors ? 'default' : 'outline'}>
              <Building2 className="w-4 h-4 mr-2" /> Vendor Outstanding
            </Button>
          </div>

          {loading ? (
            <div className="flex items-center justify-center py-12">
              <Loader2 className="w-8 h-8 animate-spin text-amber-500" />
            </div>
          ) : outstandingReport ? (
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <AlertCircle className="w-5 h-5 text-amber-500" />
                  {outstandingReport.customers ? 'Customer' : 'Vendor'} Outstanding Report (Udhaari)
                </CardTitle>
              </CardHeader>
              <CardContent>
                {/* Summary Cards */}
                <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mb-6">
                  <div className="bg-slate-100 rounded-lg p-4 text-center">
                    <p className="text-xs text-slate-500">Total Parties</p>
                    <p className="text-2xl font-bold text-slate-900">
                      {outstandingReport.summary?.total_customers || outstandingReport.summary?.total_vendors}
                    </p>
                  </div>
                  <div className="bg-amber-100 rounded-lg p-4 text-center">
                    <p className="text-xs text-amber-700">Total Outstanding</p>
                    <p className="text-2xl font-bold text-amber-700">
                      ₹{outstandingReport.summary?.total_outstanding?.toLocaleString()}
                    </p>
                  </div>
                  {outstandingReport.summary?.ageing && Object.entries(outstandingReport.summary.ageing).map(([bucket, amount]) => (
                    <div key={bucket} className={`rounded-lg p-4 text-center ${
                      bucket === '90+' ? 'bg-red-100' : bucket === '61-90' ? 'bg-orange-100' : bucket === '31-60' ? 'bg-yellow-100' : 'bg-green-100'
                    }`}>
                      <p className={`text-xs ${bucket === '90+' ? 'text-red-700' : bucket === '61-90' ? 'text-orange-700' : bucket === '31-60' ? 'text-yellow-700' : 'text-green-700'}`}>
                        {bucket} Days
                      </p>
                      <p className={`text-lg font-bold ${bucket === '90+' ? 'text-red-700' : bucket === '61-90' ? 'text-orange-700' : bucket === '31-60' ? 'text-yellow-700' : 'text-green-700'}`}>
                        ₹{amount?.toLocaleString()}
                      </p>
                    </div>
                  ))}
                </div>

                {/* Outstanding List */}
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead className="bg-slate-100">
                      <tr>
                        <th className="px-4 py-3 text-left">Party Name</th>
                        <th className="px-4 py-3 text-right">Outstanding</th>
                        <th className="px-4 py-3 text-right">0-30 Days</th>
                        <th className="px-4 py-3 text-right">31-60 Days</th>
                        <th className="px-4 py-3 text-right">61-90 Days</th>
                        <th className="px-4 py-3 text-right">90+ Days</th>
                        <th className="px-4 py-3 text-left">Last Txn</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-100">
                      {(outstandingReport.customers || outstandingReport.vendors)?.map((party, idx) => (
                        <tr key={idx} className="hover:bg-slate-50">
                          <td className="px-4 py-3 font-medium">{party.party_name}</td>
                          <td className="px-4 py-3 text-right font-bold text-amber-600">₹{party.outstanding?.toLocaleString()}</td>
                          <td className="px-4 py-3 text-right text-green-600">₹{party.ageing?.['0-30']?.toLocaleString() || 0}</td>
                          <td className="px-4 py-3 text-right text-yellow-600">₹{party.ageing?.['31-60']?.toLocaleString() || 0}</td>
                          <td className="px-4 py-3 text-right text-orange-600">₹{party.ageing?.['61-90']?.toLocaleString() || 0}</td>
                          <td className="px-4 py-3 text-right text-red-600">₹{party.ageing?.['90+']?.toLocaleString() || 0}</td>
                          <td className="px-4 py-3 text-slate-500">{party.last_transaction}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </CardContent>
            </Card>
          ) : (
            <div className="text-center py-12 text-slate-500">
              <AlertCircle className="w-12 h-12 mx-auto mb-3 text-slate-300" />
              <p>Click on "Customer Outstanding" or "Vendor Outstanding" to view the report</p>
            </div>
          )}
        </div>
      )}

      {/* Settings Tab */}
      {activeTab === 'settings' && isAdmin && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Period Locks */}
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <CardTitle className="flex items-center gap-2">
                  <Lock className="w-5 h-5 text-red-500" />
                  Period Locks
                </CardTitle>
                <Button size="sm" onClick={() => setShowPeriodLockModal(true)}>
                  <Plus className="w-4 h-4 mr-1" /> Add Lock
                </Button>
              </div>
            </CardHeader>
            <CardContent>
              <div className="space-y-3">
                {periodLocks.map(lock => (
                  <div key={lock.id} className={`p-4 rounded-lg border ${lock.is_active ? 'bg-red-50 border-red-200' : 'bg-slate-50 border-slate-200'}`}>
                    <div className="flex items-center justify-between">
                      <div>
                        <p className="font-medium text-slate-900">
                          {lock.period_type === 'quarterly' ? 'Quarterly' : lock.period_type === 'half_yearly' ? 'Half Yearly' : 'Yearly'} Lock
                        </p>
                        <p className="text-sm text-slate-500">{lock.period_start} to {lock.period_end}</p>
                        <p className="text-xs text-slate-400 mt-1">
                          Locked for: {lock.locked_by_roles?.join(', ')}
                        </p>
                      </div>
                      <div className="flex items-center gap-2">
                        {lock.is_active ? (
                          <span className="flex items-center gap-1 text-xs text-red-600 font-medium">
                            <Lock className="w-3 h-3" /> Active
                          </span>
                        ) : (
                          <span className="flex items-center gap-1 text-xs text-slate-500 font-medium">
                            <Unlock className="w-3 h-3" /> Inactive
                          </span>
                        )}
                      </div>
                    </div>
                  </div>
                ))}
                {periodLocks.length === 0 && (
                  <p className="text-center text-slate-500 py-4">No period locks configured</p>
                )}
              </div>
            </CardContent>
          </Card>

          {/* Opening Balances */}
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <CardTitle className="flex items-center gap-2">
                  <Wallet className="w-5 h-5 text-green-500" />
                  Opening Balances
                </CardTitle>
                <Button size="sm" onClick={() => setShowOpeningBalanceModal(true)}>
                  <Plus className="w-4 h-4 mr-1" /> Set Balance
                </Button>
              </div>
            </CardHeader>
            <CardContent>
              <div className="space-y-2">
                {openingBalances.slice(0, 10).map(ob => (
                  <div key={ob.id} className="flex items-center justify-between p-3 bg-slate-50 rounded-lg">
                    <div>
                      <p className="font-medium text-slate-900">{ob.party_name}</p>
                      <p className="text-xs text-slate-500 capitalize">{ob.party_type}</p>
                    </div>
                    <div className="text-right">
                      <p className="font-bold text-slate-900">₹{ob.amount?.toLocaleString()}</p>
                      <p className={`text-xs ${ob.balance_type === 'debit' ? 'text-green-600' : 'text-red-600'}`}>
                        {ob.balance_type === 'debit' ? 'Dr' : 'Cr'}
                      </p>
                    </div>
                  </div>
                ))}
                {openingBalances.length === 0 && (
                  <p className="text-center text-slate-500 py-4">No opening balances set</p>
                )}
                {openingBalances.length > 10 && (
                  <p className="text-center text-slate-500 text-sm">
                    + {openingBalances.length - 10} more balances
                  </p>
                )}
              </div>
            </CardContent>
          </Card>
        </div>
      )}
    </div>
  );
};

export default LedgerManagement;
