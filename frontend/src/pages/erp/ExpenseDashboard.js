import React, { useState, useEffect } from 'react';
import { Card, CardContent } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { 
  Receipt, Plus, Clock, CheckCircle, XCircle, AlertCircle,
  Filter, TrendingUp, TrendingDown, Upload, Wallet, Building,
  Settings, BarChart3, RefreshCw, Calendar, CreditCard, FileText
} from 'lucide-react';
import { toast } from 'sonner';
import erpApi from '../../utils/erpApi';

const ExpenseDashboard = () => {
  const [activeTab, setActiveTab] = useState('dashboard');
  const [dashboard, setDashboard] = useState(null);
  const [entries, setEntries] = useState([]);
  const [categories, setCategories] = useState([]);
  const [settings, setSettings] = useState(null);
  const [loading, setLoading] = useState(true);
  const [showAddModal, setShowAddModal] = useState(false);
  const [statusFilter, setStatusFilter] = useState('all');
  const [newExpense, setNewExpense] = useState({
    category_id: '',
    category_name: '',
    amount: '',
    description: '',
    payment_mode: 'cash',
    department: 'Admin',
    vendor_name: '',
    date: new Date().toISOString().split('T')[0]
  });

  useEffect(() => {
    fetchData();
  }, []);

  const fetchData = async () => {
    try {
      const [dashRes, catRes, entriesRes, settingsRes] = await Promise.all([
        erpApi.expenses.getDashboard(),
        erpApi.expenses.getCategories(),
        erpApi.expenses.getEntries({ limit: 50 }),
        erpApi.expenses.getSettings()
      ]);
      setDashboard(dashRes.data);
      setCategories(catRes.data);
      setEntries(entriesRes.data);
      setSettings(settingsRes.data);
    } catch (error) {
      console.error('Failed to fetch expense data:', error);
      toast.error('Failed to load expense data');
    } finally {
      setLoading(false);
    }
  };

  const handleCreateExpense = async (e) => {
    e.preventDefault();
    try {
      await erpApi.expenses.create(newExpense);
      toast.success('Expense entry created');
      setShowAddModal(false);
      setNewExpense({
        category_id: '',
        category_name: '',
        amount: '',
        description: '',
        payment_mode: 'cash',
        department: 'Admin',
        vendor_name: '',
        date: new Date().toISOString().split('T')[0]
      });
      fetchData();
    } catch (error) {
      toast.error(error.response?.data?.detail || 'Failed to create expense');
    }
  };

  const handleApprove = async (entryId, action) => {
    try {
      await erpApi.expenses.approve(entryId, { action });
      toast.success(`Expense ${action}d`);
      fetchData();
    } catch (error) {
      toast.error('Failed to process approval');
    }
  };

  const filteredEntries = statusFilter === 'all' 
    ? entries 
    : entries.filter(e => e.status === statusFilter);

  const tabs = [
    { id: 'dashboard', label: 'Dashboard', icon: BarChart3 },
    { id: 'entries', label: 'Entries', icon: Receipt },
    { id: 'approvals', label: 'Approvals', icon: CheckCircle },
    { id: 'settings', label: 'Settings', icon: Settings },
  ];

  const statusConfig = {
    pending: { color: 'bg-yellow-100 text-yellow-700', icon: Clock },
    supervisor_approved: { color: 'bg-blue-100 text-blue-700', icon: AlertCircle },
    approved: { color: 'bg-green-100 text-green-700', icon: CheckCircle },
    rejected: { color: 'bg-red-100 text-red-700', icon: XCircle },
  };

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-slate-50">
        <RefreshCw className="w-8 h-8 animate-spin text-teal-600" />
      </div>
    );
  }

  return (
    <div className="min-h-screen py-8 bg-slate-50" data-testid="expense-dashboard">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        {/* Header */}
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-3xl font-bold text-slate-900">Daily Expenses</h1>
            <p className="text-slate-600">Track and manage operational expenses</p>
          </div>
          <Button
            onClick={() => setShowAddModal(true)}
            className="bg-teal-600 hover:bg-teal-700"
            data-testid="add-expense-btn"
          >
            <Plus className="w-4 h-4 mr-2" />
            Add Expense
          </Button>
        </div>

        {/* Tabs */}
        <div className="flex gap-2 mb-6 bg-white rounded-xl p-2 shadow-sm">
          {tabs.map((tab) => {
            const Icon = tab.icon;
            return (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`flex items-center gap-2 px-4 py-2 rounded-lg font-medium transition-all ${
                  activeTab === tab.id
                    ? 'bg-teal-600 text-white shadow-md'
                    : 'text-slate-600 hover:bg-slate-100'
                }`}
              >
                <Icon className="w-4 h-4" />
                {tab.label}
                {tab.id === 'approvals' && dashboard?.pending_approvals > 0 && (
                  <span className="ml-1 bg-red-500 text-white text-xs px-2 py-0.5 rounded-full">
                    {dashboard.pending_approvals}
                  </span>
                )}
              </button>
            );
          })}
        </div>

        {/* Dashboard Tab */}
        {activeTab === 'dashboard' && dashboard && (
          <>
            {/* Stats Cards */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
              <Card className="bg-gradient-to-br from-teal-500 to-teal-600 text-white">
                <CardContent className="p-5">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-teal-100 text-sm">Today's Expenses</p>
                      <p className="text-3xl font-bold">₹{dashboard.today.total.toLocaleString()}</p>
                      <p className="text-teal-100 text-xs mt-1">{dashboard.today.count} entries</p>
                    </div>
                    <Receipt className="w-10 h-10 opacity-80" />
                  </div>
                  <div className="mt-3 bg-white/20 rounded-full h-2">
                    <div 
                      className="bg-white rounded-full h-2" 
                      style={{ width: `${Math.min((dashboard.today.total / dashboard.today.limit) * 100, 100)}%` }}
                    />
                  </div>
                  <p className="text-teal-100 text-xs mt-1">Limit: ₹{dashboard.today.limit.toLocaleString()}</p>
                </CardContent>
              </Card>

              <Card className="bg-gradient-to-br from-purple-500 to-purple-600 text-white">
                <CardContent className="p-5">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-purple-100 text-sm">Monthly Total</p>
                      <p className="text-3xl font-bold">₹{dashboard.month.total.toLocaleString()}</p>
                      <p className="text-purple-100 text-xs mt-1">{dashboard.month.count} entries</p>
                    </div>
                    <Calendar className="w-10 h-10 opacity-80" />
                  </div>
                </CardContent>
              </Card>

              <Card className="bg-gradient-to-br from-orange-500 to-orange-600 text-white">
                <CardContent className="p-5">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-orange-100 text-sm">Pending Approvals</p>
                      <p className="text-3xl font-bold">{dashboard.pending_approvals}</p>
                      <p className="text-orange-100 text-xs mt-1">Awaiting action</p>
                    </div>
                    <Clock className="w-10 h-10 opacity-80" />
                  </div>
                </CardContent>
              </Card>

              <Card className="bg-gradient-to-br from-blue-500 to-blue-600 text-white">
                <CardContent className="p-5">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-blue-100 text-sm">Top Category</p>
                      <p className="text-xl font-bold truncate">
                        {dashboard.by_category[0]?.category || 'N/A'}
                      </p>
                      <p className="text-blue-100 text-sm">
                        ₹{dashboard.by_category[0]?.total?.toLocaleString() || 0}
                      </p>
                    </div>
                    <TrendingUp className="w-10 h-10 opacity-80" />
                  </div>
                </CardContent>
              </Card>
            </div>

            {/* Category & Department Breakdown */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
              <Card>
                <CardContent className="p-6">
                  <h3 className="font-bold text-slate-900 mb-4">By Category (This Month)</h3>
                  <div className="space-y-3">
                    {dashboard.by_category.slice(0, 6).map((cat, idx) => (
                      <div key={idx} className="flex items-center justify-between">
                        <span className="text-slate-700">{cat.category}</span>
                        <div className="flex items-center gap-3">
                          <span className="text-slate-500 text-sm">{cat.count} entries</span>
                          <span className="font-bold text-slate-900">₹{cat.total.toLocaleString()}</span>
                        </div>
                      </div>
                    ))}
                    {dashboard.by_category.length === 0 && (
                      <p className="text-slate-400 text-center py-4">No expenses this month</p>
                    )}
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardContent className="p-6">
                  <h3 className="font-bold text-slate-900 mb-4">By Department</h3>
                  <div className="space-y-3">
                    {dashboard.by_department.slice(0, 6).map((dept, idx) => (
                      <div key={idx} className="flex items-center justify-between">
                        <span className="text-slate-700">{dept.department || 'Unassigned'}</span>
                        <span className="font-bold text-slate-900">₹{dept.total.toLocaleString()}</span>
                      </div>
                    ))}
                    {dashboard.by_department.length === 0 && (
                      <p className="text-slate-400 text-center py-4">No department data</p>
                    )}
                  </div>
                </CardContent>
              </Card>
            </div>

            {/* Recent Entries */}
            <Card>
              <CardContent className="p-6">
                <h3 className="font-bold text-slate-900 mb-4">Recent Entries</h3>
                <div className="space-y-3">
                  {dashboard.recent_entries.slice(0, 5).map((entry) => {
                    const StatusIcon = statusConfig[entry.status]?.icon || Clock;
                    return (
                      <div key={entry.id} className="flex items-center justify-between p-3 bg-slate-50 rounded-lg">
                        <div className="flex items-center gap-3">
                          <div className="w-10 h-10 bg-teal-100 rounded-lg flex items-center justify-center">
                            <Receipt className="w-5 h-5 text-teal-600" />
                          </div>
                          <div>
                            <p className="font-medium text-slate-900">{entry.description || entry.category_name}</p>
                            <p className="text-xs text-slate-500">{entry.department} • {entry.payment_mode}</p>
                          </div>
                        </div>
                        <div className="text-right">
                          <p className="font-bold text-slate-900">₹{entry.amount.toLocaleString()}</p>
                          <span className={`text-xs px-2 py-0.5 rounded-full ${statusConfig[entry.status]?.color}`}>
                            {entry.status}
                          </span>
                        </div>
                      </div>
                    );
                  })}
                </div>
              </CardContent>
            </Card>
          </>
        )}

        {/* Entries Tab */}
        {activeTab === 'entries' && (
          <Card>
            <CardContent className="p-6">
              <div className="flex items-center justify-between mb-4">
                <h3 className="font-bold text-slate-900">All Expense Entries</h3>
                <div className="flex gap-2">
                  {['all', 'pending', 'approved', 'rejected'].map(status => (
                    <button
                      key={status}
                      onClick={() => setStatusFilter(status)}
                      className={`px-3 py-1 rounded-lg text-sm ${
                        statusFilter === status
                          ? 'bg-teal-600 text-white'
                          : 'bg-slate-100 text-slate-600'
                      }`}
                    >
                      {status.charAt(0).toUpperCase() + status.slice(1)}
                    </button>
                  ))}
                </div>
              </div>
              
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead>
                    <tr className="border-b border-slate-200">
                      <th className="text-left py-3 px-4 text-sm font-medium text-slate-600">Date</th>
                      <th className="text-left py-3 px-4 text-sm font-medium text-slate-600">Entry #</th>
                      <th className="text-left py-3 px-4 text-sm font-medium text-slate-600">Category</th>
                      <th className="text-left py-3 px-4 text-sm font-medium text-slate-600">Description</th>
                      <th className="text-right py-3 px-4 text-sm font-medium text-slate-600">Amount</th>
                      <th className="text-center py-3 px-4 text-sm font-medium text-slate-600">Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filteredEntries.map((entry) => (
                      <tr key={entry.id} className="border-b border-slate-100 hover:bg-slate-50">
                        <td className="py-3 px-4 text-sm">{entry.date}</td>
                        <td className="py-3 px-4 text-sm font-mono text-xs">{entry.entry_number}</td>
                        <td className="py-3 px-4 text-sm">{entry.category_name}</td>
                        <td className="py-3 px-4 text-sm text-slate-600">{entry.description}</td>
                        <td className="py-3 px-4 text-sm text-right font-bold">₹{entry.amount.toLocaleString()}</td>
                        <td className="py-3 px-4 text-center">
                          <span className={`text-xs px-2 py-1 rounded-full ${statusConfig[entry.status]?.color}`}>
                            {entry.status}
                          </span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {filteredEntries.length === 0 && (
                  <div className="text-center py-12 text-slate-400">
                    <Receipt className="w-12 h-12 mx-auto mb-2 opacity-50" />
                    <p>No expense entries found</p>
                  </div>
                )}
              </div>
            </CardContent>
          </Card>
        )}

        {/* Approvals Tab */}
        {activeTab === 'approvals' && (
          <Card>
            <CardContent className="p-6">
              <h3 className="font-bold text-slate-900 mb-4">Pending Approvals</h3>
              <div className="space-y-4">
                {entries.filter(e => e.status === 'pending' || e.status === 'supervisor_approved').map((entry) => (
                  <div key={entry.id} className="p-4 border border-slate-200 rounded-lg">
                    <div className="flex items-center justify-between mb-3">
                      <div>
                        <p className="font-bold text-slate-900">{entry.entry_number}</p>
                        <p className="text-sm text-slate-600">{entry.description || entry.category_name}</p>
                      </div>
                      <p className="text-2xl font-bold text-teal-600">₹{entry.amount.toLocaleString()}</p>
                    </div>
                    <div className="flex items-center justify-between text-sm text-slate-500 mb-3">
                      <span>{entry.date} • {entry.department} • {entry.payment_mode}</span>
                      <span>By: {entry.created_by_name}</span>
                    </div>
                    <div className="flex gap-2">
                      <Button
                        onClick={() => handleApprove(entry.id, 'approve')}
                        className="flex-1 bg-green-600 hover:bg-green-700"
                        size="sm"
                      >
                        <CheckCircle className="w-4 h-4 mr-2" />
                        Approve
                      </Button>
                      <Button
                        onClick={() => handleApprove(entry.id, 'reject')}
                        variant="outline"
                        className="flex-1 border-red-300 text-red-600 hover:bg-red-50"
                        size="sm"
                      >
                        <XCircle className="w-4 h-4 mr-2" />
                        Reject
                      </Button>
                    </div>
                  </div>
                ))}
                {entries.filter(e => e.status === 'pending' || e.status === 'supervisor_approved').length === 0 && (
                  <div className="text-center py-12 text-slate-400">
                    <CheckCircle className="w-12 h-12 mx-auto mb-2 opacity-50" />
                    <p>No pending approvals</p>
                  </div>
                )}
              </div>
            </CardContent>
          </Card>
        )}

        {/* Settings Tab */}
        {activeTab === 'settings' && settings && (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <Card>
              <CardContent className="p-6">
                <h3 className="font-bold text-slate-900 mb-4">Approval Settings</h3>
                <div className="space-y-4">
                  <div className="flex items-center justify-between p-3 bg-slate-50 rounded-lg">
                    <span>Approval Required</span>
                    <span className={`px-2 py-1 rounded text-sm ${settings.approval_enabled ? 'bg-green-100 text-green-700' : 'bg-slate-200'}`}>
                      {settings.approval_enabled ? 'Enabled' : 'Disabled'}
                    </span>
                  </div>
                  <div className="flex items-center justify-between p-3 bg-slate-50 rounded-lg">
                    <span>Approval Levels</span>
                    <span className="font-bold">{settings.approval_levels}</span>
                  </div>
                  <div className="flex items-center justify-between p-3 bg-slate-50 rounded-lg">
                    <span>Admin Direct Approval</span>
                    <span className={`px-2 py-1 rounded text-sm ${settings.admin_direct_approval ? 'bg-green-100 text-green-700' : 'bg-slate-200'}`}>
                      {settings.admin_direct_approval ? 'Yes' : 'No'}
                    </span>
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardContent className="p-6">
                <h3 className="font-bold text-slate-900 mb-4">Limits</h3>
                <div className="space-y-4">
                  <div className="flex items-center justify-between p-3 bg-slate-50 rounded-lg">
                    <span>Daily Limit</span>
                    <span className="font-bold">₹{settings.daily_limit?.toLocaleString()}</span>
                  </div>
                  <div className="flex items-center justify-between p-3 bg-slate-50 rounded-lg">
                    <span>Monthly Limit</span>
                    <span className="font-bold">₹{settings.monthly_limit?.toLocaleString()}</span>
                  </div>
                  <div className="flex items-center justify-between p-3 bg-slate-50 rounded-lg">
                    <span>Attachment Required Above</span>
                    <span className="font-bold">₹{settings.require_attachment_above?.toLocaleString()}</span>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>
        )}

        {/* Add Expense Modal */}
        {showAddModal && (
          <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
            <Card className="max-w-lg w-full max-h-[90vh] overflow-y-auto">
              <CardContent className="p-6">
                <h2 className="text-xl font-bold text-slate-900 mb-4">Add Expense Entry</h2>
                <form onSubmit={handleCreateExpense} className="space-y-4">
                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-1">Date</label>
                      <input
                        type="date"
                        value={newExpense.date}
                        onChange={(e) => setNewExpense({...newExpense, date: e.target.value})}
                        className="w-full h-10 rounded border border-slate-300 px-3"
                        required
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-1">Category</label>
                      <select
                        value={newExpense.category_id}
                        onChange={(e) => {
                          const cat = categories.find(c => c.id === e.target.value);
                          setNewExpense({
                            ...newExpense, 
                            category_id: e.target.value,
                            category_name: cat?.name || ''
                          });
                        }}
                        className="w-full h-10 rounded border border-slate-300 px-3"
                        required
                      >
                        <option value="">Select Category</option>
                        {categories.map(cat => (
                          <option key={cat.id} value={cat.id}>{cat.name}</option>
                        ))}
                      </select>
                    </div>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-1">Amount (₹)</label>
                    <input
                      type="number"
                      value={newExpense.amount}
                      onChange={(e) => setNewExpense({...newExpense, amount: e.target.value})}
                      className="w-full h-10 rounded border border-slate-300 px-3"
                      min="1"
                      required
                    />
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-1">Description</label>
                    <input
                      type="text"
                      value={newExpense.description}
                      onChange={(e) => setNewExpense({...newExpense, description: e.target.value})}
                      className="w-full h-10 rounded border border-slate-300 px-3"
                      placeholder="e.g., Office supplies purchase"
                      required
                    />
                  </div>

                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-1">Payment Mode</label>
                      <select
                        value={newExpense.payment_mode}
                        onChange={(e) => setNewExpense({...newExpense, payment_mode: e.target.value})}
                        className="w-full h-10 rounded border border-slate-300 px-3"
                      >
                        <option value="cash">Cash</option>
                        <option value="bank">Bank Transfer</option>
                        <option value="upi">UPI</option>
                        <option value="cheque">Cheque</option>
                        <option value="credit">Credit</option>
                      </select>
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-1">Department</label>
                      <select
                        value={newExpense.department}
                        onChange={(e) => setNewExpense({...newExpense, department: e.target.value})}
                        className="w-full h-10 rounded border border-slate-300 px-3"
                      >
                        {(settings?.departments || ['Production', 'Admin', 'Sales', 'HR', 'Store']).map(dept => (
                          <option key={dept} value={dept}>{dept}</option>
                        ))}
                      </select>
                    </div>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-1">Vendor Name (Optional)</label>
                    <input
                      type="text"
                      value={newExpense.vendor_name}
                      onChange={(e) => setNewExpense({...newExpense, vendor_name: e.target.value})}
                      className="w-full h-10 rounded border border-slate-300 px-3"
                      placeholder="e.g., Local Supplier"
                    />
                  </div>

                  <div className="flex gap-3 pt-2">
                    <Button type="submit" className="flex-1 bg-teal-600 hover:bg-teal-700">
                      Save Expense
                    </Button>
                    <Button
                      type="button"
                      variant="outline"
                      className="flex-1"
                      onClick={() => setShowAddModal(false)}
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
    </div>
  );
};

export default ExpenseDashboard;
