import React, { useState, useEffect } from 'react';
import { Card, CardContent } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { 
  Calculator, Plus, FileText, IndianRupee, TrendingUp, TrendingDown, AlertCircle,
  CheckCircle, Clock, Filter, Download, XCircle, CreditCard, FileSpreadsheet,
  PieChart, BarChart3, Calendar, ArrowUpRight, ArrowDownRight, Wallet, Printer, Share2
} from 'lucide-react';
import { toast } from 'sonner';
import erpApi from '../../utils/erpApi';
import ShareModal from '../../components/ShareModal';

const AccountsDashboard = () => {
  const [activeSection, setActiveSection] = useState('overview');
  const [dashboard, setDashboard] = useState({});
  const [invoices, setInvoices] = useState([]);
  const [profitLoss, setProfitLoss] = useState(null);
  const [gstReport, setGstReport] = useState(null);
  const [loading, setLoading] = useState(true);
  const [statusFilter, setStatusFilter] = useState('all');
  const [showCreateInvoice, setShowCreateInvoice] = useState(false);
  const [showPayment, setShowPayment] = useState(false);
  const [selectedInvoice, setSelectedInvoice] = useState(null);
  const [exporting, setExporting] = useState(false);
  const [shareModalOrder, setShareModalOrder] = useState(null);
  
  // Date range for reports
  const today = new Date();
  const firstDayOfMonth = new Date(today.getFullYear(), today.getMonth(), 1);
  const [dateRange, setDateRange] = useState({
    start: firstDayOfMonth.toISOString().split('T')[0],
    end: today.toISOString().split('T')[0]
  });
  
  const [newInvoice, setNewInvoice] = useState({
    customer_name: '',
    customer_gst: '',
    customer_address: '',
    items: [],
    due_date: '',
    notes: '',
    is_interstate: false
  });

  const [newItem, setNewItem] = useState({
    description: '',
    quantity: 0,
    unit_price: 0,
    hsn_code: ''
  });

  const [payment, setPayment] = useState({
    amount: 0,
    payment_method: 'cash',
    reference: '',
    notes: ''
  });

  useEffect(() => {
    fetchDashboard();
    fetchInvoices();
  }, []);

  useEffect(() => {
    if (activeSection === 'reports' || activeSection === 'profit-loss') {
      fetchProfitLoss();
      fetchGSTReport();
    }
  }, [activeSection, dateRange]);

  const fetchDashboard = async () => {
    try {
      const response = await erpApi.accounts.getDashboard();
      setDashboard(response.data || {});
    } catch (error) {
      console.error('Failed to fetch dashboard:', error);
    } finally {
      setLoading(false);
    }
  };

  const fetchInvoices = async () => {
    try {
      const response = await erpApi.accounts.getInvoices();
      setInvoices(response.data || []);
    } catch (error) {
      console.error('Failed to fetch invoices:', error);
    }
  };

  const fetchProfitLoss = async () => {
    try {
      const response = await erpApi.accounts.getProfitLoss(dateRange.start, dateRange.end);
      setProfitLoss(response.data);
    } catch (error) {
      console.error('Failed to fetch P&L:', error);
    }
  };

  const fetchGSTReport = async () => {
    try {
      const currentMonth = dateRange.start.slice(0, 7);
      const response = await erpApi.accounts.getGSTReport(currentMonth);
      setGstReport(response.data);
    } catch (error) {
      console.error('Failed to fetch GST report:', error);
    }
  };

  const handleCreateInvoice = async (e) => {
    e.preventDefault();
    if (newInvoice.items.length === 0) {
      toast.error('Add at least one item');
      return;
    }
    try {
      const response = await erpApi.accounts.createInvoice(newInvoice);
      toast.success(`Invoice ${response.data.invoice_number} created!`);
      setShowCreateInvoice(false);
      resetInvoiceForm();
      fetchInvoices();
      fetchDashboard();
    } catch (error) {
      console.error('Failed to create invoice:', error);
      toast.error('Failed to create invoice');
    }
  };

  const handleRecordPayment = async (e) => {
    e.preventDefault();
    if (!selectedInvoice) return;
    try {
      await erpApi.accounts.recordPayment(selectedInvoice.id, payment);
      toast.success('Payment recorded!');
      setShowPayment(false);
      setPayment({ amount: 0, payment_method: 'cash', reference: '', notes: '' });
      setSelectedInvoice(null);
      fetchInvoices();
      fetchDashboard();
    } catch (error) {
      console.error('Failed to record payment:', error);
      toast.error('Failed to record payment');
    }
  };

  const handleExport = async (reportType, format) => {
    setExporting(true);
    try {
      const API_URL = process.env.REACT_APP_BACKEND_URL;
      const token = localStorage.getItem('token');
      let url;
      
      if (reportType === 'bulk' || reportType === 'bulk-vendors') {
        // Bulk export with all data
        const includeVendors = reportType === 'bulk-vendors';
        url = `${API_URL}/api/erp/reports/bulk-export?start_date=${dateRange.start}&end_date=${dateRange.end}&include_vendors=${includeVendors}&token=${token}`;
      } else {
        url = `${API_URL}/api/erp/reports/${reportType}/export?start_date=${dateRange.start}&end_date=${dateRange.end}&format=${format}`;
      }
      
      const response = await fetch(url, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      
      if (!response.ok) throw new Error('Export failed');
      
      const blob = await response.blob();
      
      const link = document.createElement('a');
      link.href = window.URL.createObjectURL(blob);
      
      if (reportType === 'bulk' || reportType === 'bulk-vendors') {
        link.download = `bulk_export_${dateRange.start}_to_${dateRange.end}.xlsx`;
      } else {
        link.download = `${reportType}_${dateRange.start}_to_${dateRange.end}.${format === 'excel' ? 'xlsx' : 'pdf'}`;
      }
      link.click();
      
      toast.success(`${reportType === 'bulk' || reportType === 'bulk-vendors' ? 'Bulk data' : reportType} exported successfully!`);
    } catch (error) {
      console.error('Export failed:', error);
      toast.error('Export failed');
    } finally {
      setExporting(false);
    }
  };

  const resetInvoiceForm = () => {
    setNewInvoice({
      customer_name: '',
      customer_gst: '',
      customer_address: '',
      items: [],
      due_date: '',
      notes: '',
      is_interstate: false
    });
  };

  const addItemToInvoice = () => {
    if (!newItem.description || newItem.quantity <= 0 || newItem.unit_price <= 0) {
      toast.error('Fill in item details');
      return;
    }
    setNewInvoice({
      ...newInvoice,
      items: [...newInvoice.items, { ...newItem }]
    });
    setNewItem({ description: '', quantity: 0, unit_price: 0, hsn_code: '' });
  };

  const removeItemFromInvoice = (index) => {
    setNewInvoice({
      ...newInvoice,
      items: newInvoice.items.filter((_, i) => i !== index)
    });
  };

  const calculateInvoiceTotal = () => {
    const subtotal = newInvoice.items.reduce((sum, item) => sum + (item.quantity * item.unit_price), 0);
    const gst = subtotal * 0.18;
    return { subtotal, gst, total: subtotal + gst };
  };

  const openPaymentModal = (invoice) => {
    setSelectedInvoice(invoice);
    setPayment({
      amount: invoice.total - (invoice.amount_paid || 0),
      payment_method: 'cash',
      reference: '',
      notes: ''
    });
    setShowPayment(true);
  };

  const filteredInvoices = statusFilter === 'all' 
    ? invoices 
    : invoices.filter(inv => inv.payment_status === statusFilter);

  const handlePrintInvoiceList = () => {
    const printWindow = window.open('', '_blank');
    printWindow.document.write(`
      <!DOCTYPE html>
      <html>
      <head>
        <title>Invoice Report - Lucumaa Glass</title>
        <style>
          body { font-family: Arial, sans-serif; padding: 30px; color: #1e293b; }
          .header { text-align: center; border-bottom: 3px solid #0d9488; padding-bottom: 20px; margin-bottom: 30px; }
          .company { font-size: 28px; font-weight: bold; color: #0d9488; }
          .date { color: #64748b; margin-top: 5px; }
          table { width: 100%; border-collapse: collapse; margin-top: 20px; }
          th, td { border: 1px solid #e2e8f0; padding: 10px; text-align: left; }
          th { background: #0d9488; color: white; }
          tr:nth-child(even) { background: #f9fafb; }
          .status-paid { color: #22c55e; font-weight: bold; }
          .status-pending { color: #eab308; font-weight: bold; }
          .status-partial { color: #3b82f6; font-weight: bold; }
          .summary { margin-top: 30px; background: #f1f5f9; padding: 20px; border-radius: 8px; }
          .summary-row { display: flex; justify-content: space-between; margin-bottom: 10px; }
          .footer { margin-top: 40px; text-align: center; color: #64748b; font-size: 12px; }
          @media print { th { background: #333 !important; -webkit-print-color-adjust: exact; } }
        </style>
      </head>
      <body>
        <div class="header">
          <div class="company">LUCUMAA GLASS - INVOICE REPORT</div>
          <div class="date">Generated: ${new Date().toLocaleString()} | Filter: ${statusFilter.toUpperCase()}</div>
        </div>

        <table>
          <thead>
            <tr>
              <th>Invoice #</th>
              <th>Customer</th>
              <th>Date</th>
              <th>Total</th>
              <th>Paid</th>
              <th>Balance</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            ${filteredInvoices.map(inv => `
              <tr>
                <td><strong>${inv.invoice_number}</strong></td>
                <td>${inv.customer_name}</td>
                <td>${new Date(inv.created_at).toLocaleDateString()}</td>
                <td>₹${inv.total?.toLocaleString()}</td>
                <td>₹${(inv.amount_paid || 0).toLocaleString()}</td>
                <td>₹${(inv.total - (inv.amount_paid || 0)).toLocaleString()}</td>
                <td class="status-${inv.payment_status}">${inv.payment_status?.toUpperCase()}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>

        <div class="summary">
          <div class="summary-row">
            <strong>Total Invoices:</strong>
            <span>${filteredInvoices.length}</span>
          </div>
          <div class="summary-row">
            <strong>Total Amount:</strong>
            <span>₹${filteredInvoices.reduce((sum, inv) => sum + (inv.total || 0), 0).toLocaleString()}</span>
          </div>
          <div class="summary-row">
            <strong>Total Received:</strong>
            <span>₹${filteredInvoices.reduce((sum, inv) => sum + (inv.amount_paid || 0), 0).toLocaleString()}</span>
          </div>
          <div class="summary-row">
            <strong>Total Outstanding:</strong>
            <span>₹${filteredInvoices.reduce((sum, inv) => sum + ((inv.total || 0) - (inv.amount_paid || 0)), 0).toLocaleString()}</span>
          </div>
        </div>

        <div class="footer">Lucumaa Glass ERP System</div>
        <script>window.onload = function() { window.print(); }</script>
      </body>
      </html>
    `);
    printWindow.document.close();
  };

  const handlePrintPL = () => {
    if (!profitLoss) return;
    const printWindow = window.open('', '_blank');
    printWindow.document.write(`
      <!DOCTYPE html>
      <html>
      <head>
        <title>Profit & Loss Statement - Lucumaa Glass</title>
        <style>
          body { font-family: Arial, sans-serif; padding: 30px; color: #1e293b; max-width: 800px; margin: 0 auto; }
          .header { text-align: center; border-bottom: 3px solid #0d9488; padding-bottom: 20px; margin-bottom: 30px; }
          .company { font-size: 28px; font-weight: bold; color: #0d9488; }
          .period { color: #64748b; margin-top: 5px; }
          .section { margin-bottom: 25px; border: 1px solid #e2e8f0; border-radius: 8px; overflow: hidden; }
          .section-header { background: #f1f5f9; padding: 12px 15px; font-weight: bold; font-size: 14px; }
          .section-revenue { background: #dcfce7; color: #166534; }
          .section-cogs { background: #fee2e2; color: #991b1b; }
          .section-expense { background: #fef3c7; color: #92400e; }
          .row { display: flex; justify-content: space-between; padding: 10px 15px; border-bottom: 1px solid #f1f5f9; }
          .row:last-child { border-bottom: none; }
          .row-total { background: #f8fafc; font-weight: bold; }
          .net-section { background: ${profitLoss.net_profit >= 0 ? '#0d9488' : '#dc2626'}; color: white; padding: 20px; border-radius: 8px; text-align: center; margin-top: 20px; }
          .net-label { font-size: 14px; opacity: 0.9; }
          .net-value { font-size: 32px; font-weight: bold; margin-top: 5px; }
          .net-margin { font-size: 14px; opacity: 0.8; margin-top: 5px; }
          .footer { margin-top: 40px; text-align: center; color: #64748b; font-size: 12px; }
        </style>
      </head>
      <body>
        <div class="header">
          <div class="company">PROFIT & LOSS STATEMENT</div>
          <div class="period">Period: ${dateRange.start} to ${dateRange.end}</div>
        </div>

        <div class="section">
          <div class="section-header section-revenue">REVENUE</div>
          <div class="row">
            <span>Total Sales (${profitLoss.revenue.invoice_count} invoices)</span>
            <span>₹${profitLoss.revenue.total_sales.toLocaleString()}</span>
          </div>
        </div>

        <div class="section">
          <div class="section-header section-cogs">COST OF GOODS SOLD</div>
          <div class="row">
            <span>Total Purchases (${profitLoss.cost_of_goods.po_count} POs)</span>
            <span>₹${profitLoss.cost_of_goods.total_purchases.toLocaleString()}</span>
          </div>
        </div>

        <div class="section">
          <div class="row row-total">
            <span>GROSS PROFIT</span>
            <span style="color: ${profitLoss.gross_profit >= 0 ? '#16a34a' : '#dc2626'}">₹${profitLoss.gross_profit.toLocaleString()}</span>
          </div>
        </div>

        <div class="section">
          <div class="section-header section-expense">OPERATING EXPENSES</div>
          <div class="row">
            <span>Breakage/Wastage Loss</span>
            <span>₹${profitLoss.operating_expenses.breakage_loss.toLocaleString()}</span>
          </div>
          <div class="row">
            <span>Salaries & Wages</span>
            <span>₹${profitLoss.operating_expenses.salaries.toLocaleString()}</span>
          </div>
          <div class="row row-total">
            <span>Total Operating Expenses</span>
            <span>₹${profitLoss.operating_expenses.total.toLocaleString()}</span>
          </div>
        </div>

        <div class="net-section">
          <div class="net-label">NET ${profitLoss.net_profit >= 0 ? 'PROFIT' : 'LOSS'}</div>
          <div class="net-value">₹${Math.abs(profitLoss.net_profit).toLocaleString()}</div>
          <div class="net-margin">Profit Margin: ${profitLoss.profit_margin}%</div>
        </div>

        <div class="footer">
          Generated on ${new Date().toLocaleString()} | Lucumaa Glass ERP System
        </div>
        <script>window.onload = function() { window.print(); }</script>
      </body>
      </html>
    `);
    printWindow.document.close();
  };

  const statusConfig = {
    pending: { color: 'bg-yellow-100 text-yellow-700', icon: Clock },
    partial: { color: 'bg-blue-100 text-blue-700', icon: CreditCard },
    paid: { color: 'bg-green-100 text-green-700', icon: CheckCircle }
  };

  const sections = [
    { id: 'overview', label: 'Overview', icon: PieChart },
    { id: 'invoices', label: 'Invoices', icon: FileText },
    { id: 'profit-loss', label: 'Profit & Loss', icon: TrendingUp },
    { id: 'reports', label: 'Reports & Export', icon: FileSpreadsheet },
  ];

  return (
    <div className="min-h-screen py-8 bg-slate-50" data-testid="accounts-dashboard">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        {/* Header */}
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-3xl font-bold text-slate-900">Accounts & Finance</h1>
            <p className="text-slate-600">Invoicing, Payments, Reports & Analytics</p>
          </div>
          <Button 
            onClick={() => setShowCreateInvoice(true)}
            className="bg-teal-600 hover:bg-teal-700"
            data-testid="create-invoice-btn"
          >
            <Plus className="w-4 h-4 mr-2" />
            Create Invoice
          </Button>
        </div>

        {/* Section Tabs */}
        <div className="flex gap-2 mb-6 bg-white rounded-xl p-2 shadow-sm">
          {sections.map((section) => {
            const Icon = section.icon;
            return (
              <button
                key={section.id}
                onClick={() => setActiveSection(section.id)}
                className={`flex items-center gap-2 px-4 py-2 rounded-lg font-medium transition-all ${
                  activeSection === section.id
                    ? 'bg-teal-600 text-white shadow-md'
                    : 'text-slate-600 hover:bg-slate-100'
                }`}
              >
                <Icon className="w-4 h-4" />
                {section.label}
              </button>
            );
          })}
        </div>

        {/* Overview Section */}
        {activeSection === 'overview' && (
          <>
            {/* Stats Cards */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
              <Card className="bg-gradient-to-br from-teal-500 to-teal-600 text-white">
                <CardContent className="p-5">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-teal-100 text-sm">Monthly Sales</p>
                      <p className="text-3xl font-bold">₹{(dashboard.monthly_sales || 0).toLocaleString()}</p>
                    </div>
                    <div className="w-12 h-12 bg-white/20 rounded-xl flex items-center justify-center">
                      <TrendingUp className="w-6 h-6" />
                    </div>
                  </div>
                </CardContent>
              </Card>
              
              <Card className="bg-gradient-to-br from-blue-500 to-blue-600 text-white">
                <CardContent className="p-5">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-blue-100 text-sm">Receivables</p>
                      <p className="text-3xl font-bold">₹{(dashboard.total_receivables || 0).toLocaleString()}</p>
                      <p className="text-blue-200 text-xs mt-1">{dashboard.pending_invoices || 0} pending invoices</p>
                    </div>
                    <div className="w-12 h-12 bg-white/20 rounded-xl flex items-center justify-center">
                      <Wallet className="w-6 h-6" />
                    </div>
                  </div>
                </CardContent>
              </Card>
              
              <Card className={`${dashboard.overdue_count > 0 ? 'bg-gradient-to-br from-red-500 to-red-600 text-white' : 'bg-white'}`}>
                <CardContent className="p-5">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className={`text-sm ${dashboard.overdue_count > 0 ? 'text-red-100' : 'text-slate-600'}`}>Overdue</p>
                      <p className={`text-3xl font-bold ${dashboard.overdue_count > 0 ? '' : 'text-slate-900'}`}>
                        ₹{(dashboard.overdue_amount || 0).toLocaleString()}
                      </p>
                      <p className={`text-xs mt-1 ${dashboard.overdue_count > 0 ? 'text-red-200' : 'text-slate-500'}`}>
                        {dashboard.overdue_count || 0} overdue invoices
                      </p>
                    </div>
                    <div className={`w-12 h-12 rounded-xl flex items-center justify-center ${dashboard.overdue_count > 0 ? 'bg-white/20' : 'bg-red-100'}`}>
                      <AlertCircle className={`w-6 h-6 ${dashboard.overdue_count > 0 ? '' : 'text-red-500'}`} />
                    </div>
                  </div>
                </CardContent>
              </Card>
              
              <Card className="bg-gradient-to-br from-purple-500 to-purple-600 text-white">
                <CardContent className="p-5">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-purple-100 text-sm">GST Collected</p>
                      <p className="text-3xl font-bold">₹{(dashboard.monthly_gst_collected || 0).toLocaleString()}</p>
                      <p className="text-purple-200 text-xs mt-1">This month</p>
                    </div>
                    <div className="w-12 h-12 bg-white/20 rounded-xl flex items-center justify-center">
                      <Calculator className="w-6 h-6" />
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>

            {/* Quick Actions & Recent */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
              {/* Recent Invoices */}
              <div className="lg:col-span-2">
                <Card>
                  <CardContent className="p-6">
                    <div className="flex items-center justify-between mb-4">
                      <h3 className="font-bold text-slate-900">Recent Invoices</h3>
                      <button
                        onClick={() => setActiveSection('invoices')}
                        className="text-teal-600 text-sm font-medium hover:underline"
                      >
                        View All →
                      </button>
                    </div>
                    <div className="space-y-3">
                      {invoices.slice(0, 5).map((inv) => {
                        const statusInfo = statusConfig[inv.payment_status] || statusConfig.pending;
                        return (
                          <div key={inv.id} className="flex items-center justify-between p-3 bg-slate-50 rounded-lg">
                            <div className="flex items-center gap-3">
                              <div className={`w-2 h-2 rounded-full ${
                                inv.payment_status === 'paid' ? 'bg-green-500' :
                                inv.payment_status === 'partial' ? 'bg-blue-500' : 'bg-yellow-500'
                              }`} />
                              <div>
                                <p className="font-medium text-slate-900">{inv.invoice_number}</p>
                                <p className="text-xs text-slate-500">{inv.customer_name}</p>
                              </div>
                            </div>
                            <div className="flex items-center gap-2">
                              <Button 
                                size="sm" 
                                variant="ghost"
                                className="h-7 w-7 p-0 text-purple-600"
                                onClick={() => setShareModalOrder({ order: { ...inv, id: inv.order_id || inv.id }, isJobWork: false })}
                              >
                                <Share2 className="w-4 h-4" />
                              </Button>
                              <div className="text-right">
                                <p className="font-bold text-slate-900">₹{inv.total?.toLocaleString()}</p>
                                <p className={`text-xs font-medium ${
                                  inv.payment_status === 'paid' ? 'text-green-600' :
                                  inv.payment_status === 'partial' ? 'text-blue-600' : 'text-yellow-600'
                                }`}>{inv.payment_status?.toUpperCase()}</p>
                              </div>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </CardContent>
                </Card>
              </div>

              {/* Quick Actions */}
              <div>
                <Card>
                  <CardContent className="p-6">
                    <h3 className="font-bold text-slate-900 mb-4">Quick Actions</h3>
                    <div className="space-y-3">
                      <button
                        onClick={() => setShowCreateInvoice(true)}
                        className="w-full flex items-center gap-3 p-3 bg-teal-50 text-teal-700 rounded-lg hover:bg-teal-100 transition-colors"
                      >
                        <Plus className="w-5 h-5" />
                        <span className="font-medium">Create Invoice</span>
                      </button>
                      <button
                        onClick={() => setActiveSection('reports')}
                        className="w-full flex items-center gap-3 p-3 bg-purple-50 text-purple-700 rounded-lg hover:bg-purple-100 transition-colors"
                      >
                        <Download className="w-5 h-5" />
                        <span className="font-medium">Export Reports</span>
                      </button>
                      <button
                        onClick={() => setActiveSection('profit-loss')}
                        className="w-full flex items-center gap-3 p-3 bg-blue-50 text-blue-700 rounded-lg hover:bg-blue-100 transition-colors"
                      >
                        <BarChart3 className="w-5 h-5" />
                        <span className="font-medium">View P&L Statement</span>
                      </button>
                    </div>
                  </CardContent>
                </Card>
              </div>
            </div>
          </>
        )}

        {/* Invoices Section */}
        {activeSection === 'invoices' && (
          <>
            {/* Status Filter */}
            <div className="flex items-center justify-between mb-6">
              <div className="flex items-center gap-3">
                <Filter className="w-5 h-5 text-slate-500" />
                {['all', 'pending', 'partial', 'paid'].map(status => (
                  <button
                    key={status}
                    onClick={() => setStatusFilter(status)}
                    className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                      statusFilter === status
                        ? 'bg-teal-600 text-white'
                        : 'bg-white text-slate-700 hover:bg-slate-100'
                    }`}
                  >
                    {status.charAt(0).toUpperCase() + status.slice(1)}
                  </button>
                ))}
              </div>
              <Button
                variant="outline"
                onClick={handlePrintInvoiceList}
                data-testid="print-invoices-btn"
              >
                <Printer className="w-4 h-4 mr-2" />
                Print List
              </Button>
            </div>

            {/* Invoice List */}
            <div className="space-y-4">
              {filteredInvoices.map((invoice) => {
                const statusInfo = statusConfig[invoice.payment_status] || statusConfig.pending;
                const StatusIcon = statusInfo.icon;
                const balance = invoice.total - (invoice.amount_paid || 0);

                return (
                  <Card key={invoice.id} className="hover:shadow-lg transition-shadow">
                    <CardContent className="p-6">
                      <div className="flex items-start justify-between">
                        <div className="flex-1">
                          <div className="flex items-center gap-3 mb-2">
                            <h3 className="text-xl font-bold text-slate-900">{invoice.invoice_number}</h3>
                            <span className={`px-3 py-1 rounded-full text-sm font-medium flex items-center gap-1 ${statusInfo.color}`}>
                              <StatusIcon className="w-4 h-4" />
                              {invoice.payment_status?.toUpperCase()}
                            </span>
                          </div>
                          <p className="text-slate-600 mb-2">{invoice.customer_name}</p>
                          {invoice.customer_gst && (
                            <p className="text-sm text-slate-500">GST: {invoice.customer_gst}</p>
                          )}
                          <p className="text-sm text-slate-500 mt-2">
                            {invoice.items?.length || 0} items • Created: {new Date(invoice.created_at).toLocaleDateString()}
                            {invoice.due_date && ` • Due: ${invoice.due_date}`}
                          </p>
                        </div>

                        <div className="text-right">
                          <div className="mb-4">
                            <p className="text-sm text-slate-500">Total</p>
                            <p className="text-2xl font-bold text-slate-900">₹{invoice.total?.toLocaleString()}</p>
                            {invoice.amount_paid > 0 && (
                              <p className="text-sm text-green-600">Paid: ₹{invoice.amount_paid?.toLocaleString()}</p>
                            )}
                            {balance > 0 && (
                              <p className="text-sm font-medium text-red-600">Balance: ₹{balance.toLocaleString()}</p>
                            )}
                          </div>

                          <div className="flex gap-2">
                            {invoice.payment_status !== 'paid' && (
                              <Button
                                size="sm"
                                className="bg-green-600 hover:bg-green-700"
                                onClick={() => openPaymentModal(invoice)}
                              >
                                <CreditCard className="w-4 h-4 mr-1" />
                                Receive
                              </Button>
                            )}
                          </div>
                        </div>
                      </div>

                      {/* Tax breakdown */}
                      <div className="mt-4 pt-4 border-t border-slate-100 grid grid-cols-4 gap-4 text-sm">
                        <div>
                          <span className="text-slate-500">Subtotal:</span>
                          <span className="ml-2 font-medium">₹{invoice.subtotal?.toLocaleString()}</span>
                        </div>
                        <div>
                          <span className="text-slate-500">CGST:</span>
                          <span className="ml-2 font-medium">₹{invoice.cgst?.toLocaleString()}</span>
                        </div>
                        <div>
                          <span className="text-slate-500">SGST:</span>
                          <span className="ml-2 font-medium">₹{invoice.sgst?.toLocaleString()}</span>
                        </div>
                        {invoice.igst > 0 && (
                          <div>
                            <span className="text-slate-500">IGST:</span>
                            <span className="ml-2 font-medium">₹{invoice.igst?.toLocaleString()}</span>
                          </div>
                        )}
                      </div>
                    </CardContent>
                  </Card>
                );
              })}

              {filteredInvoices.length === 0 && !loading && (
                <div className="text-center py-12 bg-white rounded-xl">
                  <FileText className="w-16 h-16 text-slate-300 mx-auto mb-4" />
                  <p className="text-slate-600">No invoices found.</p>
                </div>
              )}
            </div>
          </>
        )}

        {/* Profit & Loss Section */}
        {activeSection === 'profit-loss' && (
          <>
            {/* Date Range Selector */}
            <Card className="mb-6">
              <CardContent className="p-4">
                <div className="flex flex-wrap items-center gap-4">
                  <Calendar className="w-5 h-5 text-slate-600" />
                  <span className="font-medium text-slate-700">Period:</span>
                  
                  {/* Quick Presets */}
                  <div className="flex gap-1">
                    <Button
                      variant={dateRange.start === new Date().toISOString().split('T')[0] ? "default" : "outline"}
                      size="sm"
                      onClick={() => {
                        const today = new Date();
                        setDateRange({
                          start: today.toISOString().split('T')[0],
                          end: today.toISOString().split('T')[0]
                        });
                      }}
                    >
                      Today
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => {
                        const today = new Date();
                        const weekAgo = new Date(today);
                        weekAgo.setDate(today.getDate() - 7);
                        setDateRange({
                          start: weekAgo.toISOString().split('T')[0],
                          end: today.toISOString().split('T')[0]
                        });
                      }}
                    >
                      Last 7 Days
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => {
                        const today = new Date();
                        const firstDay = new Date(today.getFullYear(), today.getMonth(), 1);
                        setDateRange({
                          start: firstDay.toISOString().split('T')[0],
                          end: today.toISOString().split('T')[0]
                        });
                      }}
                    >
                      This Month
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => {
                        const today = new Date();
                        const firstDayLastMonth = new Date(today.getFullYear(), today.getMonth() - 1, 1);
                        const lastDayLastMonth = new Date(today.getFullYear(), today.getMonth(), 0);
                        setDateRange({
                          start: firstDayLastMonth.toISOString().split('T')[0],
                          end: lastDayLastMonth.toISOString().split('T')[0]
                        });
                      }}
                    >
                      Last Month
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => {
                        const today = new Date();
                        const threeMonthsAgo = new Date(today.getFullYear(), today.getMonth() - 3, 1);
                        setDateRange({
                          start: threeMonthsAgo.toISOString().split('T')[0],
                          end: today.toISOString().split('T')[0]
                        });
                      }}
                    >
                      Last 3 Months
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => {
                        const today = new Date();
                        // Financial year starts April 1
                        const fyStart = today.getMonth() >= 3 
                          ? new Date(today.getFullYear(), 3, 1)
                          : new Date(today.getFullYear() - 1, 3, 1);
                        setDateRange({
                          start: fyStart.toISOString().split('T')[0],
                          end: today.toISOString().split('T')[0]
                        });
                      }}
                    >
                      This FY
                    </Button>
                  </div>
                  
                  {/* Custom Date Inputs */}
                  <div className="flex items-center gap-2 ml-2 pl-2 border-l border-slate-200">
                    <input
                      type="date"
                      value={dateRange.start}
                      onChange={(e) => setDateRange({...dateRange, start: e.target.value})}
                      className="h-9 rounded-lg border border-slate-300 px-3 text-sm"
                    />
                    <span className="text-slate-500">to</span>
                    <input
                      type="date"
                      value={dateRange.end}
                      onChange={(e) => setDateRange({...dateRange, end: e.target.value})}
                      className="h-9 rounded-lg border border-slate-300 px-3 text-sm"
                    />
                  </div>
                  
                  <div className="ml-auto flex gap-2">
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={handlePrintPL}
                      disabled={!profitLoss}
                      data-testid="print-pl-btn"
                    >
                      <Printer className="w-4 h-4 mr-1" />
                      Print
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => handleExport('profit-loss', 'excel')}
                      disabled={exporting}
                    >
                      <FileSpreadsheet className="w-4 h-4 mr-1" />
                      Excel
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => handleExport('profit-loss', 'pdf')}
                      disabled={exporting}
                    >
                      <FileText className="w-4 h-4 mr-1" />
                      PDF
                    </Button>
                  </div>
                </div>
              </CardContent>
            </Card>

            {profitLoss && (
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* P&L Statement */}
                <Card className="lg:col-span-2">
                  <CardContent className="p-6">
                    <h3 className="text-xl font-bold text-slate-900 mb-6">Profit & Loss Statement</h3>
                    
                    <div className="space-y-6">
                      {/* Revenue */}
                      <div className="bg-green-50 rounded-xl p-4">
                        <div className="flex items-center justify-between mb-3">
                          <h4 className="font-bold text-green-800 flex items-center gap-2">
                            <ArrowUpRight className="w-5 h-5" />
                            REVENUE
                          </h4>
                        </div>
                        <div className="flex justify-between items-center">
                          <span className="text-green-700">Total Sales ({profitLoss.revenue.invoice_count} invoices)</span>
                          <span className="text-2xl font-bold text-green-700">₹{profitLoss.revenue.total_sales.toLocaleString()}</span>
                        </div>
                      </div>

                      {/* Cost of Goods */}
                      <div className="bg-red-50 rounded-xl p-4">
                        <div className="flex items-center justify-between mb-3">
                          <h4 className="font-bold text-red-800 flex items-center gap-2">
                            <ArrowDownRight className="w-5 h-5" />
                            COST OF GOODS SOLD
                          </h4>
                        </div>
                        <div className="flex justify-between items-center">
                          <span className="text-red-700">Total Purchases ({profitLoss.cost_of_goods.po_count} POs)</span>
                          <span className="text-2xl font-bold text-red-700">₹{profitLoss.cost_of_goods.total_purchases.toLocaleString()}</span>
                        </div>
                      </div>

                      {/* Gross Profit */}
                      <div className="border-2 border-slate-200 rounded-xl p-4">
                        <div className="flex justify-between items-center">
                          <span className="font-bold text-slate-800">GROSS PROFIT</span>
                          <span className={`text-2xl font-bold ${profitLoss.gross_profit >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                            ₹{profitLoss.gross_profit.toLocaleString()}
                          </span>
                        </div>
                      </div>

                      {/* Operating Expenses */}
                      <div className="bg-orange-50 rounded-xl p-4">
                        <h4 className="font-bold text-orange-800 mb-3">OPERATING EXPENSES</h4>
                        <div className="space-y-2">
                          <div className="flex justify-between">
                            <span className="text-orange-700">Breakage/Wastage Loss</span>
                            <span className="font-medium text-orange-700">₹{profitLoss.operating_expenses.breakage_loss.toLocaleString()}</span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-orange-700">Salaries & Wages</span>
                            <span className="font-medium text-orange-700">₹{profitLoss.operating_expenses.salaries.toLocaleString()}</span>
                          </div>
                          <div className="flex justify-between pt-2 border-t border-orange-200">
                            <span className="font-bold text-orange-800">Total Expenses</span>
                            <span className="font-bold text-orange-800">₹{profitLoss.operating_expenses.total.toLocaleString()}</span>
                          </div>
                        </div>
                      </div>

                      {/* Net Profit */}
                      <div className={`rounded-xl p-6 ${profitLoss.net_profit >= 0 ? 'bg-gradient-to-r from-green-500 to-emerald-600' : 'bg-gradient-to-r from-red-500 to-red-600'} text-white`}>
                        <div className="flex justify-between items-center">
                          <div>
                            <h4 className="font-bold text-lg">NET {profitLoss.net_profit >= 0 ? 'PROFIT' : 'LOSS'}</h4>
                            <p className="text-sm opacity-80">Profit Margin: {profitLoss.profit_margin}%</p>
                          </div>
                          <span className="text-4xl font-bold">₹{Math.abs(profitLoss.net_profit).toLocaleString()}</span>
                        </div>
                      </div>

                      {/* GST Summary */}
                      <div className="bg-purple-50 rounded-xl p-4">
                        <h4 className="font-bold text-purple-800 mb-3">GST SUMMARY</h4>
                        <div className="grid grid-cols-3 gap-4">
                          <div className="text-center p-3 bg-white rounded-lg">
                            <p className="text-sm text-purple-600">Output GST</p>
                            <p className="text-xl font-bold text-purple-700">₹{profitLoss.gst_summary.collected.toLocaleString()}</p>
                          </div>
                          <div className="text-center p-3 bg-white rounded-lg">
                            <p className="text-sm text-purple-600">Input Credit</p>
                            <p className="text-xl font-bold text-purple-700">₹{profitLoss.gst_summary.paid.toLocaleString()}</p>
                          </div>
                          <div className="text-center p-3 bg-purple-100 rounded-lg">
                            <p className="text-sm text-purple-600">Net Liability</p>
                            <p className="text-xl font-bold text-purple-800">₹{profitLoss.gst_summary.net_liability.toLocaleString()}</p>
                          </div>
                        </div>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              </div>
            )}
          </>
        )}

        {/* Reports & Export Section */}
        {activeSection === 'reports' && (
          <>
            {/* Date Range Selector */}
            <Card className="mb-6">
              <CardContent className="p-4">
                <div className="flex items-center gap-4">
                  <Calendar className="w-5 h-5 text-slate-600" />
                  <span className="font-medium text-slate-700">Report Period:</span>
                  <input
                    type="date"
                    value={dateRange.start}
                    onChange={(e) => setDateRange({...dateRange, start: e.target.value})}
                    className="h-10 rounded-lg border border-slate-300 px-3"
                  />
                  <span className="text-slate-500">to</span>
                  <input
                    type="date"
                    value={dateRange.end}
                    onChange={(e) => setDateRange({...dateRange, end: e.target.value})}
                    className="h-10 rounded-lg border border-slate-300 px-3"
                  />
                </div>
              </CardContent>
            </Card>

            {/* Report Cards */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              {/* Invoices Report */}
              <Card className="hover:shadow-lg transition-shadow">
                <CardContent className="p-6">
                  <div className="flex items-center gap-4 mb-4">
                    <div className="w-12 h-12 bg-teal-100 rounded-xl flex items-center justify-center">
                      <FileText className="w-6 h-6 text-teal-600" />
                    </div>
                    <div>
                      <h3 className="font-bold text-slate-900">Invoice Report</h3>
                      <p className="text-sm text-slate-500">All invoices with GST details</p>
                    </div>
                  </div>
                  <div className="flex gap-2">
                    <Button
                      className="flex-1 bg-green-600 hover:bg-green-700"
                      onClick={() => handleExport('invoices', 'excel')}
                      disabled={exporting}
                    >
                      <FileSpreadsheet className="w-4 h-4 mr-2" />
                      Download Excel
                    </Button>
                    <Button
                      variant="outline"
                      className="flex-1"
                      onClick={() => handleExport('invoices', 'pdf')}
                      disabled={exporting}
                    >
                      <FileText className="w-4 h-4 mr-2" />
                      Download PDF
                    </Button>
                  </div>
                </CardContent>
              </Card>

              {/* Profit & Loss Report */}
              <Card className="hover:shadow-lg transition-shadow">
                <CardContent className="p-6">
                  <div className="flex items-center gap-4 mb-4">
                    <div className="w-12 h-12 bg-blue-100 rounded-xl flex items-center justify-center">
                      <TrendingUp className="w-6 h-6 text-blue-600" />
                    </div>
                    <div>
                      <h3 className="font-bold text-slate-900">Profit & Loss</h3>
                      <p className="text-sm text-slate-500">Complete P&L statement</p>
                    </div>
                  </div>
                  <div className="flex gap-2">
                    <Button
                      className="flex-1 bg-green-600 hover:bg-green-700"
                      onClick={() => handleExport('profit-loss', 'excel')}
                      disabled={exporting}
                    >
                      <FileSpreadsheet className="w-4 h-4 mr-2" />
                      Download Excel
                    </Button>
                    <Button
                      variant="outline"
                      className="flex-1"
                      onClick={() => handleExport('profit-loss', 'pdf')}
                      disabled={exporting}
                    >
                      <FileText className="w-4 h-4 mr-2" />
                      Download PDF
                    </Button>
                  </div>
                </CardContent>
              </Card>

              {/* Ledger Report */}
              <Card className="hover:shadow-lg transition-shadow">
                <CardContent className="p-6">
                  <div className="flex items-center gap-4 mb-4">
                    <div className="w-12 h-12 bg-purple-100 rounded-xl flex items-center justify-center">
                      <Calculator className="w-6 h-6 text-purple-600" />
                    </div>
                    <div>
                      <h3 className="font-bold text-slate-900">Ledger Report</h3>
                      <p className="text-sm text-slate-500">All debit/credit entries</p>
                    </div>
                  </div>
                  <div className="flex gap-2">
                    <Button
                      className="flex-1 bg-green-600 hover:bg-green-700"
                      onClick={() => handleExport('ledger', 'excel')}
                      disabled={exporting}
                    >
                      <FileSpreadsheet className="w-4 h-4 mr-2" />
                      Download Excel
                    </Button>
                  </div>
                </CardContent>
              </Card>

              {/* Payments Report */}
              <Card className="hover:shadow-lg transition-shadow">
                <CardContent className="p-6">
                  <div className="flex items-center gap-4 mb-4">
                    <div className="w-12 h-12 bg-green-100 rounded-xl flex items-center justify-center">
                      <CreditCard className="w-6 h-6 text-green-600" />
                    </div>
                    <div>
                      <h3 className="font-bold text-slate-900">Payments Report</h3>
                      <p className="text-sm text-slate-500">All received payments</p>
                    </div>
                  </div>
                  <div className="flex gap-2">
                    <Button
                      className="flex-1 bg-green-600 hover:bg-green-700"
                      onClick={() => handleExport('payments', 'excel')}
                      disabled={exporting}
                    >
                      <FileSpreadsheet className="w-4 h-4 mr-2" />
                      Download Excel
                    </Button>
                  </div>
                </CardContent>
              </Card>

              {/* Bulk Export - All Data */}
              <Card className="hover:shadow-lg transition-shadow md:col-span-2 bg-gradient-to-br from-violet-50 to-purple-50 border-violet-200">
                <CardContent className="p-6">
                  <div className="flex items-center gap-4 mb-4">
                    <div className="w-12 h-12 bg-violet-100 rounded-xl flex items-center justify-center">
                      <Download className="w-6 h-6 text-violet-600" />
                    </div>
                    <div>
                      <h3 className="font-bold text-slate-900">Bulk Export - All Data</h3>
                      <p className="text-sm text-slate-500">Download everything in one Excel file (Invoices, Orders, Job Work, Payments)</p>
                    </div>
                  </div>
                  <div className="flex gap-2">
                    <Button
                      className="flex-1 bg-violet-600 hover:bg-violet-700"
                      onClick={() => handleExport('bulk', 'excel')}
                      disabled={exporting}
                    >
                      <FileSpreadsheet className="w-4 h-4 mr-2" />
                      {exporting ? 'Generating...' : 'Download All Data (Excel)'}
                    </Button>
                    <Button
                      variant="outline"
                      className="border-violet-300 text-violet-700 hover:bg-violet-100"
                      onClick={() => handleExport('bulk-vendors', 'excel')}
                      disabled={exporting}
                    >
                      <Download className="w-4 h-4 mr-2" />
                      Include Vendor Payments
                    </Button>
                  </div>
                </CardContent>
              </Card>
            </div>
          </>
        )}

        {/* Create Invoice Modal */}
        {showCreateInvoice && (
          <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
            <Card className="max-w-3xl w-full max-h-[90vh] overflow-y-auto">
              <CardContent className="p-8">
                <h2 className="text-2xl font-bold text-slate-900 mb-6">Create Sales Invoice</h2>
                <form onSubmit={handleCreateInvoice} className="space-y-6">
                  {/* Customer Info */}
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Customer Name *</label>
                      <input
                        type="text"
                        value={newInvoice.customer_name}
                        onChange={(e) => setNewInvoice({...newInvoice, customer_name: e.target.value})}
                        className="w-full h-12 rounded-lg border border-slate-300 px-4"
                        required
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Customer GST</label>
                      <input
                        type="text"
                        value={newInvoice.customer_gst}
                        onChange={(e) => setNewInvoice({...newInvoice, customer_gst: e.target.value})}
                        className="w-full h-12 rounded-lg border border-slate-300 px-4"
                        placeholder="e.g., 27AABCU9603R1ZX"
                      />
                    </div>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-2">Address</label>
                    <input
                      type="text"
                      value={newInvoice.customer_address}
                      onChange={(e) => setNewInvoice({...newInvoice, customer_address: e.target.value})}
                      className="w-full h-12 rounded-lg border border-slate-300 px-4"
                    />
                  </div>

                  <div className="flex items-center gap-2">
                    <input
                      type="checkbox"
                      id="interstate"
                      checked={newInvoice.is_interstate}
                      onChange={(e) => setNewInvoice({...newInvoice, is_interstate: e.target.checked})}
                      className="w-4 h-4"
                    />
                    <label htmlFor="interstate" className="text-sm text-slate-700">
                      Interstate Sale (IGST applicable)
                    </label>
                  </div>

                  {/* Items */}
                  <div className="border rounded-lg p-4">
                    <h3 className="font-medium text-slate-900 mb-4">Invoice Items</h3>
                    <div className="grid grid-cols-5 gap-3 mb-3">
                      <input
                        type="text"
                        placeholder="Description"
                        value={newItem.description}
                        onChange={(e) => setNewItem({...newItem, description: e.target.value})}
                        className="col-span-2 h-10 rounded border border-slate-300 px-3 text-sm"
                      />
                      <input
                        type="number"
                        placeholder="Qty"
                        value={newItem.quantity || ''}
                        onChange={(e) => setNewItem({...newItem, quantity: parseFloat(e.target.value) || 0})}
                        className="h-10 rounded border border-slate-300 px-3 text-sm"
                        min="0.01"
                        step="0.01"
                      />
                      <input
                        type="number"
                        placeholder="Price"
                        value={newItem.unit_price || ''}
                        onChange={(e) => setNewItem({...newItem, unit_price: parseFloat(e.target.value) || 0})}
                        className="h-10 rounded border border-slate-300 px-3 text-sm"
                        min="0"
                      />
                      <Button type="button" size="sm" onClick={addItemToInvoice}>
                        <Plus className="w-4 h-4" />
                      </Button>
                    </div>

                    {newInvoice.items.length > 0 && (
                      <div className="bg-slate-50 rounded-lg p-3 space-y-2">
                        {newInvoice.items.map((item, idx) => (
                          <div key={idx} className="flex justify-between items-center text-sm">
                            <span>{item.description}</span>
                            <div className="flex items-center gap-3">
                              <span>{item.quantity} × ₹{item.unit_price} = ₹{(item.quantity * item.unit_price).toLocaleString()}</span>
                              <button
                                type="button"
                                onClick={() => removeItemFromInvoice(idx)}
                                className="text-red-500 hover:text-red-700"
                              >
                                <XCircle className="w-4 h-4" />
                              </button>
                            </div>
                          </div>
                        ))}
                        <div className="border-t pt-2 mt-2 space-y-1">
                          <div className="flex justify-between text-sm">
                            <span>Subtotal:</span>
                            <span>₹{calculateInvoiceTotal().subtotal.toLocaleString()}</span>
                          </div>
                          <div className="flex justify-between text-sm">
                            <span>GST (18%):</span>
                            <span>₹{calculateInvoiceTotal().gst.toLocaleString()}</span>
                          </div>
                          <div className="flex justify-between font-bold text-lg border-t pt-1">
                            <span>Total:</span>
                            <span className="text-green-600">₹{calculateInvoiceTotal().total.toLocaleString()}</span>
                          </div>
                        </div>
                      </div>
                    )}
                  </div>

                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Due Date</label>
                      <input
                        type="date"
                        value={newInvoice.due_date}
                        onChange={(e) => setNewInvoice({...newInvoice, due_date: e.target.value})}
                        className="w-full h-12 rounded-lg border border-slate-300 px-4"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Notes</label>
                      <input
                        type="text"
                        value={newInvoice.notes}
                        onChange={(e) => setNewInvoice({...newInvoice, notes: e.target.value})}
                        className="w-full h-12 rounded-lg border border-slate-300 px-4"
                      />
                    </div>
                  </div>

                  <div className="flex gap-3 pt-4">
                    <Button type="submit" className="flex-1 bg-teal-600 hover:bg-teal-700">
                      Create Invoice
                    </Button>
                    <Button 
                      type="button"
                      variant="outline"
                      onClick={() => setShowCreateInvoice(false)}
                      className="flex-1"
                    >
                      Cancel
                    </Button>
                  </div>
                </form>
              </CardContent>
            </Card>
          </div>
        )}

        {/* Record Payment Modal */}
        {showPayment && selectedInvoice && (
          <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
            <Card className="max-w-md w-full">
              <CardContent className="p-8">
                <h2 className="text-2xl font-bold text-slate-900 mb-2">Record Payment</h2>
                <p className="text-slate-600 mb-6">{selectedInvoice.invoice_number}</p>

                <div className="bg-slate-100 rounded-lg p-4 mb-6">
                  <div className="flex justify-between mb-2">
                    <span className="text-slate-600">Invoice Total</span>
                    <span className="font-bold">₹{selectedInvoice.total?.toLocaleString()}</span>
                  </div>
                  <div className="flex justify-between mb-2">
                    <span className="text-slate-600">Already Paid</span>
                    <span className="text-green-600">₹{(selectedInvoice.amount_paid || 0).toLocaleString()}</span>
                  </div>
                  <div className="flex justify-between border-t pt-2">
                    <span className="font-medium">Balance Due</span>
                    <span className="font-bold text-red-600">
                      ₹{(selectedInvoice.total - (selectedInvoice.amount_paid || 0)).toLocaleString()}
                    </span>
                  </div>
                </div>

                <form onSubmit={handleRecordPayment} className="space-y-4">
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-2">Amount Received *</label>
                    <input
                      type="number"
                      value={payment.amount}
                      onChange={(e) => setPayment({...payment, amount: parseFloat(e.target.value) || 0})}
                      className="w-full h-12 rounded-lg border border-slate-300 px-4"
                      min="0.01"
                      max={selectedInvoice.total - (selectedInvoice.amount_paid || 0)}
                      step="0.01"
                      required
                    />
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-2">Payment Method</label>
                    <select
                      value={payment.payment_method}
                      onChange={(e) => setPayment({...payment, payment_method: e.target.value})}
                      className="w-full h-12 rounded-lg border border-slate-300 px-4"
                    >
                      <option value="cash">Cash</option>
                      <option value="bank_transfer">Bank Transfer</option>
                      <option value="cheque">Cheque</option>
                      <option value="upi">UPI</option>
                      <option value="card">Card</option>
                    </select>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-2">Reference / Transaction ID</label>
                    <input
                      type="text"
                      value={payment.reference}
                      onChange={(e) => setPayment({...payment, reference: e.target.value})}
                      className="w-full h-12 rounded-lg border border-slate-300 px-4"
                      placeholder="Optional"
                    />
                  </div>

                  <div className="flex gap-3 pt-4">
                    <Button type="submit" className="flex-1 bg-green-600 hover:bg-green-700">
                      <CheckCircle className="w-4 h-4 mr-2" />
                      Record Payment
                    </Button>
                    <Button 
                      type="button"
                      variant="outline"
                      onClick={() => {
                        setShowPayment(false);
                        setSelectedInvoice(null);
                      }}
                      className="flex-1"
                    >
                      Cancel
                    </Button>
                  </div>
                </form>
              </CardContent>
            </Card>
          </div>
        )}
      </div>

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

export default AccountsDashboard;
