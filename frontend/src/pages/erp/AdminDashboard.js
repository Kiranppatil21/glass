import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { Card, CardContent } from '../../components/ui/card';
import { 
  ShoppingCart, Package, AlertTriangle, DollarSign, 
  Users, TrendingUp, Factory, Clock, CheckCircle,
  XCircle, Loader, Box, Truck, BarChart3, Printer, Hammer, Bell, AlertCircle
} from 'lucide-react';
import { toast } from 'sonner';
import { erpApi } from '../../utils/erpApi';
import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  PieChart, Pie, Cell, BarChart, Bar, Legend
} from 'recharts';

const AdminDashboard = () => {
  const navigate = useNavigate();
  const [dashboard, setDashboard] = useState(null);
  const [loading, setLoading] = useState(true);
  const [revenueData, setRevenueData] = useState([]);
  const [productionData, setProductionData] = useState([]);
  const [expenseData, setExpenseData] = useState([]);
  const [topCustomers, setTopCustomers] = useState([]);
  const [jobWorkStats, setJobWorkStats] = useState(null);
  const [paymentAlerts, setPaymentAlerts] = useState(null);

  useEffect(() => {
    fetchAllData();
    const interval = setInterval(fetchAllData, 60000); // Refresh every minute
    return () => clearInterval(interval);
  }, []);

  const fetchAllData = async () => {
    try {
      const API_BASE = process.env.REACT_APP_BACKEND_URL;
      const token = localStorage.getItem('token');
      const headers = { Authorization: `Bearer ${token}` };
      
      const [dashRes, revRes, prodRes, expRes, custRes, jwRes, alertsRes] = await Promise.all([
        erpApi.admin.getDashboard(),
        erpApi.admin.getRevenueChart(6),
        erpApi.admin.getProductionChart(),
        erpApi.admin.getExpenseChart(),
        erpApi.admin.getTopCustomers(),
        erpApi.jobWork.getDashboard().catch(() => ({ data: null })),
        fetch(`${API_BASE}/api/erp/alerts/payment-dues/preview`, { headers }).then(r => r.json()).catch(() => null)
      ]);
      
      setDashboard(dashRes.data);
      setRevenueData(revRes.data);
      setProductionData(prodRes.data);
      setExpenseData(expRes.data);
      setTopCustomers(custRes.data);
      setJobWorkStats(jwRes.data);
      setPaymentAlerts(alertsRes);
    } catch (error) {
      console.error('Failed to fetch dashboard:', error);
      toast.error('Failed to load dashboard data');
    } finally {
      setLoading(false);
    }
  };

  const formatCurrency = (value) => {
    return new Intl.NumberFormat('en-IN', {
      style: 'currency',
      currency: 'INR',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(value);
  };

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-slate-50">
        <Loader className="w-8 h-8 animate-spin text-teal-600" />
      </div>
    );
  }

  const productionStages = [
    { key: 'pending', label: 'Pending', icon: Clock, color: 'text-yellow-600 bg-yellow-100' },
    { key: 'cutting', label: 'Cutting', icon: Box, color: 'text-blue-600 bg-blue-100' },
    { key: 'polishing', label: 'Polishing', icon: Factory, color: 'text-purple-600 bg-purple-100' },
    { key: 'grinding', label: 'Grinding', icon: Factory, color: 'text-indigo-600 bg-indigo-100' },
    { key: 'toughening', label: 'Toughening', icon: Factory, color: 'text-orange-600 bg-orange-100' },
    { key: 'quality_check', label: 'QC', icon: CheckCircle, color: 'text-green-600 bg-green-100' },
    { key: 'packing', label: 'Packing', icon: Package, color: 'text-teal-600 bg-teal-100' },
    { key: 'dispatched', label: 'Dispatched', icon: Truck, color: 'text-cyan-600 bg-cyan-100' },
  ];

  const COLORS = ['#3b82f6', '#10b981', '#ef4444', '#f59e0b', '#8b5cf6'];

  const handlePrintDashboard = () => {
    const printWindow = window.open('', '_blank');
    printWindow.document.write(`
      <!DOCTYPE html>
      <html>
      <head>
        <title>Admin Dashboard Report - Lucumaa Glass</title>
        <style>
          body { font-family: Arial, sans-serif; padding: 30px; color: #1e293b; }
          .header { text-align: center; border-bottom: 3px solid #0d9488; padding-bottom: 20px; margin-bottom: 30px; }
          .company { font-size: 28px; font-weight: bold; color: #0d9488; }
          .date { color: #64748b; margin-top: 5px; }
          .metrics { display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin-bottom: 30px; }
          .metric-card { border: 1px solid #e2e8f0; border-radius: 8px; padding: 15px; text-align: center; }
          .metric-value { font-size: 24px; font-weight: bold; color: #0d9488; }
          .metric-label { color: #64748b; font-size: 14px; margin-top: 5px; }
          .section { margin-bottom: 30px; }
          .section-title { font-size: 18px; font-weight: bold; color: #1e293b; border-bottom: 2px solid #e2e8f0; padding-bottom: 10px; margin-bottom: 15px; }
          .pipeline { display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; }
          .stage { background: #f1f5f9; border-radius: 8px; padding: 15px; text-align: center; }
          .stage-count { font-size: 24px; font-weight: bold; color: #0d9488; }
          .stage-name { color: #64748b; font-size: 12px; margin-top: 5px; }
          .customers-table { width: 100%; border-collapse: collapse; }
          .customers-table th, .customers-table td { border: 1px solid #e2e8f0; padding: 10px; text-align: left; }
          .customers-table th { background: #0d9488; color: white; }
          .footer { margin-top: 40px; text-align: center; color: #64748b; font-size: 12px; border-top: 1px solid #e2e8f0; padding-top: 20px; }
          @media print { .metric-card { border: 1px solid #ccc; } }
        </style>
      </head>
      <body>
        <div class="header">
          <div class="company">LUCUMAA GLASS - ADMIN DASHBOARD</div>
          <div class="date">Generated: ${new Date().toLocaleString()}</div>
        </div>

        <div class="metrics">
          <div class="metric-card">
            <div class="metric-value">${dashboard?.orders_today || 0}</div>
            <div class="metric-label">Today's Orders</div>
          </div>
          <div class="metric-card">
            <div class="metric-value" style="color: #dc2626;">₹${(dashboard?.breakage_today || 0).toLocaleString()}</div>
            <div class="metric-label">Breakage Today</div>
          </div>
          <div class="metric-card">
            <div class="metric-value" style="color: #f59e0b;">${dashboard?.low_stock_items || 0}</div>
            <div class="metric-label">Low Stock Items</div>
          </div>
          <div class="metric-card">
            <div class="metric-value" style="color: #22c55e;">${dashboard?.present_employees || 0}</div>
            <div class="metric-label">Present Today</div>
          </div>
        </div>

        <div class="section">
          <div class="section-title">Production Pipeline</div>
          <div class="pipeline">
            ${productionStages.map(stage => `
              <div class="stage">
                <div class="stage-count">${dashboard?.production_stats?.[stage.key] || 0}</div>
                <div class="stage-name">${stage.label}</div>
              </div>
            `).join('')}
          </div>
        </div>

        <div class="section">
          <div class="section-title">Top Customers</div>
          <table class="customers-table">
            <thead>
              <tr>
                <th>Rank</th>
                <th>Customer</th>
                <th>Orders</th>
                <th>Revenue</th>
              </tr>
            </thead>
            <tbody>
              ${topCustomers.length > 0 ? topCustomers.map((c, i) => `
                <tr>
                  <td>#${i + 1}</td>
                  <td>${c.name}</td>
                  <td>${c.orders}</td>
                  <td>₹${c.revenue?.toLocaleString()}</td>
                </tr>
              `).join('') : '<tr><td colspan="4" style="text-align: center;">No customer data</td></tr>'}
            </tbody>
          </table>
        </div>

        <div class="footer">
          Lucumaa Glass ERP System | Factory Control Center Report
        </div>
        <script>window.onload = function() { window.print(); }</script>
      </body>
      </html>
    `);
    printWindow.document.close();
  };

  return (
    <div className="min-h-screen py-8 bg-slate-50" data-testid="erp-admin-dashboard">
      <div className="max-w-[1600px] mx-auto px-4 sm:px-6 lg:px-8">
        {/* Header */}
        <div className="mb-8 flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold text-slate-900 mb-2">Factory Control Center</h1>
            <p className="text-slate-600">Real-time overview • Last updated: {new Date().toLocaleTimeString()}</p>
          </div>
          <button
            onClick={handlePrintDashboard}
            className="flex items-center gap-2 px-4 py-2 bg-white border border-slate-200 rounded-lg hover:bg-slate-50 transition-colors text-slate-700"
            data-testid="print-admin-dashboard-btn"
          >
            <Printer className="w-4 h-4" />
            Print Report
          </button>
        </div>

        {/* Top Metrics */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
          <Card className="border-l-4 border-l-blue-500 hover:shadow-lg transition-shadow">
            <CardContent className="p-5">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-slate-600 mb-1">Today&apos;s Orders</p>
                  <p className="text-3xl font-bold text-slate-900">{dashboard?.orders_today || 0}</p>
                </div>
                <div className="w-12 h-12 bg-blue-100 rounded-full flex items-center justify-center">
                  <ShoppingCart className="w-6 h-6 text-blue-600" />
                </div>
              </div>
            </CardContent>
          </Card>

          <Card className="border-l-4 border-l-red-500 hover:shadow-lg transition-shadow">
            <CardContent className="p-5">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-slate-600 mb-1">Breakage Today</p>
                  <p className="text-3xl font-bold text-red-600">{formatCurrency(dashboard?.breakage_today || 0)}</p>
                </div>
                <div className="w-12 h-12 bg-red-100 rounded-full flex items-center justify-center">
                  <AlertTriangle className="w-6 h-6 text-red-600" />
                </div>
              </div>
            </CardContent>
          </Card>

          <Card className="border-l-4 border-l-orange-500 hover:shadow-lg transition-shadow">
            <CardContent className="p-5">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-slate-600 mb-1">Low Stock Items</p>
                  <p className="text-3xl font-bold text-orange-600">{dashboard?.low_stock_items || 0}</p>
                </div>
                <div className="w-12 h-12 bg-orange-100 rounded-full flex items-center justify-center">
                  <Package className="w-6 h-6 text-orange-600" />
                </div>
              </div>
            </CardContent>
          </Card>

          <Card className="border-l-4 border-l-green-500 hover:shadow-lg transition-shadow">
            <CardContent className="p-5">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-slate-600 mb-1">Present Today</p>
                  <p className="text-3xl font-bold text-green-600">{dashboard?.present_employees || 0}</p>
                </div>
                <div className="w-12 h-12 bg-green-100 rounded-full flex items-center justify-center">
                  <Users className="w-6 h-6 text-green-600" />
                </div>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Job Work Stats Row */}
        {jobWorkStats && (
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
            <Card className="border-l-4 border-l-orange-500 hover:shadow-lg transition-shadow cursor-pointer" onClick={() => navigate('/erp/job-work')}>
              <CardContent className="p-5">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm text-slate-600 mb-1">Job Work Orders</p>
                    <p className="text-3xl font-bold text-orange-600">{jobWorkStats?.total_orders || 0}</p>
                  </div>
                  <div className="w-12 h-12 bg-orange-100 rounded-full flex items-center justify-center">
                    <Hammer className="w-6 h-6 text-orange-600" />
                  </div>
                </div>
                <p className="text-xs text-slate-500 mt-2">Pending: {jobWorkStats?.pending_orders || 0}</p>
              </CardContent>
            </Card>

            <Card className="border-l-4 border-l-purple-500 hover:shadow-lg transition-shadow">
              <CardContent className="p-5">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm text-slate-600 mb-1">JW Revenue</p>
                    <p className="text-3xl font-bold text-purple-600">{formatCurrency(jobWorkStats?.total_revenue || 0)}</p>
                  </div>
                  <div className="w-12 h-12 bg-purple-100 rounded-full flex items-center justify-center">
                    <TrendingUp className="w-6 h-6 text-purple-600" />
                  </div>
                </div>
                <p className="text-xs text-slate-500 mt-2">This month</p>
              </CardContent>
            </Card>

            <Card className="border-l-4 border-l-cyan-500 hover:shadow-lg transition-shadow">
              <CardContent className="p-5">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm text-slate-600 mb-1">JW Collected</p>
                    <p className="text-3xl font-bold text-cyan-600">{formatCurrency(jobWorkStats?.total_collected || 0)}</p>
                  </div>
                  <div className="w-12 h-12 bg-cyan-100 rounded-full flex items-center justify-center">
                    <DollarSign className="w-6 h-6 text-cyan-600" />
                  </div>
                </div>
                <p className="text-xs text-slate-500 mt-2">Total received</p>
              </CardContent>
            </Card>

            <Card className="border-l-4 border-l-amber-500 hover:shadow-lg transition-shadow">
              <CardContent className="p-5">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm text-slate-600 mb-1">In Process</p>
                    <p className="text-3xl font-bold text-amber-600">{jobWorkStats?.in_process || 0}</p>
                  </div>
                  <div className="w-12 h-12 bg-amber-100 rounded-full flex items-center justify-center">
                    <Factory className="w-6 h-6 text-amber-600" />
                  </div>
                </div>
                <p className="text-xs text-slate-500 mt-2">Currently processing</p>
              </CardContent>
            </Card>
          </div>
        )}

        {/* Payment Alerts Section */}
        {paymentAlerts && (paymentAlerts.summary?.total_customer_alerts > 0 || paymentAlerts.summary?.total_vendor_alerts > 0) && (
          <div className="mb-6">
            <Card className="border-l-4 border-l-red-500 bg-gradient-to-r from-red-50 to-amber-50">
              <CardContent className="p-5">
                <div className="flex items-center justify-between mb-4">
                  <div className="flex items-center gap-3">
                    <div className="w-10 h-10 bg-red-100 rounded-full flex items-center justify-center">
                      <Bell className="w-5 h-5 text-red-600" />
                    </div>
                    <div>
                      <h3 className="font-bold text-slate-900">Payment Alerts</h3>
                      <p className="text-sm text-slate-500">Dues requiring attention</p>
                    </div>
                  </div>
                  <button 
                    className="px-4 py-2 bg-red-600 text-white rounded-lg text-sm hover:bg-red-700 transition-colors"
                    onClick={() => navigate('/erp/accounts')}
                  >
                    View All
                  </button>
                </div>
                
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                  <div className="bg-white rounded-lg p-3 text-center">
                    <p className="text-2xl font-bold text-red-600">{paymentAlerts.summary?.customer_overdue || 0}</p>
                    <p className="text-xs text-slate-500">Customer Overdue</p>
                  </div>
                  <div className="bg-white rounded-lg p-3 text-center">
                    <p className="text-2xl font-bold text-amber-600">{paymentAlerts.summary?.total_customer_alerts - paymentAlerts.summary?.customer_overdue || 0}</p>
                    <p className="text-xs text-slate-500">Customer Due Soon</p>
                  </div>
                  <div className="bg-white rounded-lg p-3 text-center">
                    <p className="text-2xl font-bold text-red-600">{paymentAlerts.summary?.vendor_overdue || 0}</p>
                    <p className="text-xs text-slate-500">Vendor Overdue</p>
                  </div>
                  <div className="bg-white rounded-lg p-3 text-center">
                    <p className="text-2xl font-bold text-amber-600">{paymentAlerts.summary?.total_vendor_alerts - paymentAlerts.summary?.vendor_overdue || 0}</p>
                    <p className="text-xs text-slate-500">Vendor Due Soon</p>
                  </div>
                </div>
                
                {/* Quick list of overdue */}
                {paymentAlerts.customer_dues?.filter(d => d.status === 'overdue').slice(0, 3).length > 0 && (
                  <div className="mt-4 p-3 bg-red-100 rounded-lg">
                    <p className="text-xs font-semibold text-red-700 mb-2">OVERDUE CUSTOMER PAYMENTS:</p>
                    {paymentAlerts.customer_dues?.filter(d => d.status === 'overdue').slice(0, 3).map((due, idx) => (
                      <div key={idx} className="flex justify-between text-sm text-red-800">
                        <span>{due.customer_name} ({due.order_number})</span>
                        <span className="font-bold">₹{due.amount_due?.toLocaleString()}</span>
                      </div>
                    ))}
                  </div>
                )}
              </CardContent>
            </Card>
          </div>
        )}

        {/* Charts Row */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
          {/* Revenue Trend Chart */}
          <Card>
            <CardContent className="p-6">
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-lg font-bold text-slate-900">Revenue & Collections</h3>
                <TrendingUp className="w-5 h-5 text-teal-600" />
              </div>
              <div className="h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={revenueData}>
                    <defs>
                      <linearGradient id="colorRevenue" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="#0d9488" stopOpacity={0.3}/>
                        <stop offset="95%" stopColor="#0d9488" stopOpacity={0}/>
                      </linearGradient>
                      <linearGradient id="colorCollections" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.3}/>
                        <stop offset="95%" stopColor="#3b82f6" stopOpacity={0}/>
                      </linearGradient>
                    </defs>
                    <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                    <XAxis dataKey="month" tick={{ fontSize: 12 }} stroke="#64748b" />
                    <YAxis tick={{ fontSize: 12 }} stroke="#64748b" tickFormatter={(v) => `₹${(v/1000).toFixed(0)}k`} />
                    <Tooltip 
                      formatter={(value) => formatCurrency(value)}
                      contentStyle={{ borderRadius: '8px', border: '1px solid #e2e8f0' }}
                    />
                    <Area type="monotone" dataKey="revenue" stroke="#0d9488" strokeWidth={2} fillOpacity={1} fill="url(#colorRevenue)" name="Revenue" />
                    <Area type="monotone" dataKey="collections" stroke="#3b82f6" strokeWidth={2} fillOpacity={1} fill="url(#colorCollections)" name="Collections" />
                  </AreaChart>
                </ResponsiveContainer>
              </div>
              <div className="flex justify-center gap-6 mt-4">
                <div className="flex items-center gap-2">
                  <div className="w-3 h-3 rounded-full bg-teal-600"></div>
                  <span className="text-sm text-slate-600">Revenue</span>
                </div>
                <div className="flex items-center gap-2">
                  <div className="w-3 h-3 rounded-full bg-blue-500"></div>
                  <span className="text-sm text-slate-600">Collections</span>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Expense Breakdown Pie Chart */}
          <Card>
            <CardContent className="p-6">
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-lg font-bold text-slate-900">Monthly Expense Breakdown</h3>
                <DollarSign className="w-5 h-5 text-teal-600" />
              </div>
              <div className="h-64 flex items-center">
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie
                      data={expenseData}
                      cx="50%"
                      cy="50%"
                      innerRadius={60}
                      outerRadius={90}
                      paddingAngle={5}
                      dataKey="value"
                    >
                      {expenseData.map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={entry.color || COLORS[index % COLORS.length]} />
                      ))}
                    </Pie>
                    <Tooltip formatter={(value) => formatCurrency(value)} />
                  </PieChart>
                </ResponsiveContainer>
              </div>
              <div className="flex flex-wrap justify-center gap-4 mt-2">
                {expenseData.map((item, index) => (
                  <div key={index} className="flex items-center gap-2">
                    <div className="w-3 h-3 rounded-full" style={{ backgroundColor: item.color || COLORS[index] }}></div>
                    <span className="text-sm text-slate-600">{item.name}: {formatCurrency(item.value)}</span>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Production Pipeline & Top Customers */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-6">
          {/* Production Pipeline - 2 cols */}
          <Card className="lg:col-span-2">
            <CardContent className="p-6">
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-lg font-bold text-slate-900">Production Pipeline</h3>
                <div className="flex items-center gap-2 text-sm text-slate-500">
                  <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse"></div>
                  Live
                </div>
              </div>
              <div className="grid grid-cols-4 md:grid-cols-8 gap-3">
                {productionStages.map((stage) => {
                  const count = dashboard?.production_stats?.[stage.key] || 0;
                  const StageIcon = stage.icon;
                  
                  return (
                    <div
                      key={stage.key}
                      className="cursor-pointer group"
                      onClick={() => navigate(`/erp/production?stage=${stage.key}`)}
                    >
                      <div className="text-center p-3 rounded-lg border-2 border-transparent hover:border-teal-500 hover:shadow-md transition-all bg-white">
                        <div className={`w-10 h-10 ${stage.color} rounded-full flex items-center justify-center mx-auto mb-2`}>
                          <StageIcon className="w-5 h-5" />
                        </div>
                        <p className="text-xl font-bold text-slate-900">{count}</p>
                        <p className="text-xs text-slate-500 truncate">{stage.label}</p>
                      </div>
                    </div>
                  );
                })}
              </div>
            </CardContent>
          </Card>

          {/* Top Customers - 1 col */}
          <Card>
            <CardContent className="p-6">
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-lg font-bold text-slate-900">Top Customers</h3>
                <BarChart3 className="w-5 h-5 text-teal-600" />
              </div>
              <div className="space-y-3">
                {topCustomers.length > 0 ? topCustomers.map((customer, index) => (
                  <div key={index} className="flex items-center justify-between p-3 bg-slate-50 rounded-lg">
                    <div className="flex items-center gap-3">
                      <div className={`w-8 h-8 rounded-full flex items-center justify-center text-white text-sm font-bold ${
                        index === 0 ? 'bg-yellow-500' : index === 1 ? 'bg-slate-400' : index === 2 ? 'bg-amber-600' : 'bg-slate-300'
                      }`}>
                        {index + 1}
                      </div>
                      <div>
                        <p className="font-medium text-slate-900 text-sm">{customer.name}</p>
                        <p className="text-xs text-slate-500">{customer.orders} orders</p>
                      </div>
                    </div>
                    <p className="font-bold text-teal-600 text-sm">{formatCurrency(customer.revenue)}</p>
                  </div>
                )) : (
                  <div className="text-center py-8 text-slate-400">
                    <Users className="w-12 h-12 mx-auto mb-2 opacity-50" />
                    <p className="text-sm">No customer data yet</p>
                  </div>
                )}
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Quick Actions & Alerts */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <Card>
            <CardContent className="p-6">
              <h3 className="text-lg font-bold text-slate-900 mb-4">Quick Actions</h3>
              <div className="grid grid-cols-2 gap-3">
                <button
                  onClick={() => navigate('/erp/crm')}
                  className="flex items-center gap-3 p-4 bg-blue-50 hover:bg-blue-100 rounded-lg transition-colors text-left"
                >
                  <Users className="w-5 h-5 text-blue-600" />
                  <div>
                    <p className="font-medium text-slate-900 text-sm">Manage Leads</p>
                    <p className="text-xs text-slate-500">Customer pipeline</p>
                  </div>
                </button>
                
                <button
                  onClick={() => navigate('/erp/production')}
                  className="flex items-center gap-3 p-4 bg-green-50 hover:bg-green-100 rounded-lg transition-colors text-left"
                >
                  <Factory className="w-5 h-5 text-green-600" />
                  <div>
                    <p className="font-medium text-slate-900 text-sm">Production</p>
                    <p className="text-xs text-slate-500">Job cards</p>
                  </div>
                </button>
                
                <button
                  onClick={() => navigate('/erp/inventory')}
                  className="flex items-center gap-3 p-4 bg-orange-50 hover:bg-orange-100 rounded-lg transition-colors text-left"
                >
                  <Package className="w-5 h-5 text-orange-600" />
                  <div>
                    <p className="font-medium text-slate-900 text-sm">Inventory</p>
                    <p className="text-xs text-slate-500">Stock management</p>
                  </div>
                </button>
                
                <button
                  onClick={() => navigate('/erp/accounts')}
                  className="flex items-center gap-3 p-4 bg-purple-50 hover:bg-purple-100 rounded-lg transition-colors text-left"
                >
                  <DollarSign className="w-5 h-5 text-purple-600" />
                  <div>
                    <p className="font-medium text-slate-900 text-sm">Accounts</p>
                    <p className="text-xs text-slate-500">Invoices & P&L</p>
                  </div>
                </button>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardContent className="p-6">
              <h3 className="text-lg font-bold text-slate-900 mb-4">System Alerts</h3>
              <div className="space-y-3">
                {dashboard?.low_stock_items > 0 && (
                  <div className="flex items-start gap-3 p-3 bg-orange-50 border-l-4 border-orange-500 rounded-r-lg">
                    <AlertTriangle className="w-5 h-5 text-orange-600 mt-0.5 flex-shrink-0" />
                    <div>
                      <p className="font-medium text-slate-900 text-sm">{dashboard.low_stock_items} Low Stock Items</p>
                      <p className="text-xs text-slate-500">Raw materials need reordering</p>
                    </div>
                  </div>
                )}
                
                {dashboard?.pending_pos > 0 && (
                  <div className="flex items-start gap-3 p-3 bg-yellow-50 border-l-4 border-yellow-500 rounded-r-lg">
                    <Clock className="w-5 h-5 text-yellow-600 mt-0.5 flex-shrink-0" />
                    <div>
                      <p className="font-medium text-slate-900 text-sm">{dashboard.pending_pos} Pending POs</p>
                      <p className="text-xs text-slate-500">Approval required</p>
                    </div>
                  </div>
                )}
                
                {dashboard?.breakage_today > 5000 && (
                  <div className="flex items-start gap-3 p-3 bg-red-50 border-l-4 border-red-500 rounded-r-lg">
                    <XCircle className="w-5 h-5 text-red-600 mt-0.5 flex-shrink-0" />
                    <div>
                      <p className="font-medium text-slate-900 text-sm">High Breakage Alert</p>
                      <p className="text-xs text-slate-500">Today&apos;s loss exceeds threshold</p>
                    </div>
                  </div>
                )}
                
                {!dashboard?.low_stock_items && !dashboard?.pending_pos && dashboard?.breakage_today <= 5000 && (
                  <div className="flex items-center justify-center h-24 text-slate-400">
                    <div className="text-center">
                      <CheckCircle className="w-10 h-10 mx-auto mb-2 text-green-500" />
                      <p className="text-sm">All systems operational</p>
                    </div>
                  </div>
                )}
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
};

export default AdminDashboard;
