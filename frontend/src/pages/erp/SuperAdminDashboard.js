import React, { useState, useEffect } from 'react';
import { Card, CardContent } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { 
  Users, Shield, Activity, Clock, FileText, Settings,
  UserPlus, UserX, UserCheck, Key, Trash2, Eye, Download,
  Calendar, TrendingUp, AlertTriangle, CheckCircle, XCircle,
  RefreshCw, Search, Filter, BarChart3, PieChart
} from 'lucide-react';
import { toast } from 'sonner';
import axios from 'axios';
import { 
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  PieChart as RechartsPie, Pie, Cell, LineChart, Line
} from 'recharts';

const API_BASE = process.env.REACT_APP_BACKEND_URL;

const SuperAdminDashboard = () => {
  const [activeTab, setActiveTab] = useState('dashboard');
  const [dashboard, setDashboard] = useState(null);
  const [users, setUsers] = useState([]);
  const [auditLogs, setAuditLogs] = useState([]);
  const [dailyActivity, setDailyActivity] = useState(null);
  const [monthlyMIS, setMonthlyMIS] = useState(null);
  const [loading, setLoading] = useState(true);
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [selectedUser, setSelectedUser] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [roleFilter, setRoleFilter] = useState('');
  const [newUser, setNewUser] = useState({
    email: '', name: '', phone: '', password: '', role: 'operator'
  });

  const getAuthHeaders = () => {
    const token = localStorage.getItem('token');
    return { Authorization: `Bearer ${token}` };
  };

  useEffect(() => {
    fetchDashboard();
  }, []);

  useEffect(() => {
    if (activeTab === 'users') fetchUsers();
    if (activeTab === 'audit') fetchAuditLogs();
    if (activeTab === 'daily') fetchDailyActivity();
    if (activeTab === 'monthly') fetchMonthlyMIS();
  }, [activeTab]);

  const fetchDashboard = async () => {
    try {
      const res = await axios.get(`${API_BASE}/api/erp/superadmin/dashboard`, { headers: getAuthHeaders() });
      setDashboard(res.data);
    } catch (error) {
      console.error('Failed to fetch dashboard:', error);
      toast.error('Failed to load dashboard');
    } finally {
      setLoading(false);
    }
  };

  const fetchUsers = async () => {
    try {
      const params = {};
      if (roleFilter) params.role = roleFilter;
      if (searchTerm) params.search = searchTerm;
      const res = await axios.get(`${API_BASE}/api/erp/superadmin/users`, { headers: getAuthHeaders(), params });
      setUsers(res.data.users || []);
    } catch (error) {
      toast.error('Failed to load users');
    }
  };

  const fetchAuditLogs = async () => {
    try {
      const res = await axios.get(`${API_BASE}/api/erp/audit/logs`, { headers: getAuthHeaders(), params: { limit: 100 } });
      setAuditLogs(res.data.logs || []);
    } catch (error) {
      toast.error('Failed to load audit logs');
    }
  };

  const fetchDailyActivity = async () => {
    try {
      const res = await axios.get(`${API_BASE}/api/erp/audit/daily-activity`, { headers: getAuthHeaders() });
      setDailyActivity(res.data);
    } catch (error) {
      toast.error('Failed to load daily activity');
    }
  };

  const fetchMonthlyMIS = async () => {
    try {
      const res = await axios.get(`${API_BASE}/api/erp/audit/monthly-mis`, { headers: getAuthHeaders() });
      setMonthlyMIS(res.data);
    } catch (error) {
      toast.error('Failed to load MIS report');
    }
  };

  const handleCreateUser = async () => {
    if (!newUser.email || !newUser.name || !newUser.password) {
      toast.error('Please fill all required fields');
      return;
    }
    try {
      await axios.post(`${API_BASE}/api/erp/superadmin/users`, newUser, { headers: getAuthHeaders() });
      toast.success('User created successfully');
      setShowCreateModal(false);
      setNewUser({ email: '', name: '', phone: '', password: '', role: 'operator' });
      fetchUsers();
    } catch (error) {
      toast.error(error.response?.data?.detail || 'Failed to create user');
    }
  };

  const handleDisableUser = async (userId) => {
    if (!window.confirm('Are you sure you want to disable this user?')) return;
    try {
      await axios.post(`${API_BASE}/api/erp/superadmin/users/${userId}/disable`, {}, { headers: getAuthHeaders() });
      toast.success('User disabled');
      fetchUsers();
    } catch (error) {
      toast.error('Failed to disable user');
    }
  };

  const handleEnableUser = async (userId) => {
    try {
      await axios.post(`${API_BASE}/api/erp/superadmin/users/${userId}/enable`, {}, { headers: getAuthHeaders() });
      toast.success('User enabled');
      fetchUsers();
    } catch (error) {
      toast.error('Failed to enable user');
    }
  };

  const handleDeleteUser = async (userId) => {
    if (!window.confirm('Are you sure you want to DELETE this user? This action cannot be undone.')) return;
    try {
      await axios.delete(`${API_BASE}/api/erp/superadmin/users/${userId}`, { headers: getAuthHeaders() });
      toast.success('User deleted');
      fetchUsers();
    } catch (error) {
      toast.error(error.response?.data?.detail || 'Failed to delete user');
    }
  };

  const tabs = [
    { key: 'dashboard', label: 'Dashboard', icon: BarChart3 },
    { key: 'users', label: 'User Management', icon: Users },
    { key: 'audit', label: 'Audit Trail', icon: FileText },
    { key: 'daily', label: 'Daily Activity', icon: Activity },
    { key: 'monthly', label: 'MIS Report', icon: TrendingUp },
  ];

  const roleColors = {
    super_admin: 'bg-red-100 text-red-700',
    admin: 'bg-purple-100 text-purple-700',
    owner: 'bg-blue-100 text-blue-700',
    manager: 'bg-cyan-100 text-cyan-700',
    sales: 'bg-green-100 text-green-700',
    production_manager: 'bg-orange-100 text-orange-700',
    operator: 'bg-yellow-100 text-yellow-700',
    hr: 'bg-pink-100 text-pink-700',
    accountant: 'bg-indigo-100 text-indigo-700',
    store: 'bg-teal-100 text-teal-700',
    customer: 'bg-slate-100 text-slate-700',
    dealer: 'bg-emerald-100 text-emerald-700'
  };

  const actionColors = {
    create: 'bg-green-100 text-green-700',
    update: 'bg-blue-100 text-blue-700',
    delete: 'bg-red-100 text-red-700',
    approve: 'bg-emerald-100 text-emerald-700',
    reject: 'bg-orange-100 text-orange-700',
    login: 'bg-purple-100 text-purple-700',
    logout: 'bg-slate-100 text-slate-700'
  };

  const CHART_COLORS = ['#0d9488', '#f59e0b', '#ef4444', '#8b5cf6', '#3b82f6', '#10b981'];

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <RefreshCw className="w-8 h-8 animate-spin text-teal-600" />
      </div>
    );
  }

  return (
    <div className="space-y-6" data-testid="super-admin-dashboard">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-900 flex items-center gap-3">
            <Shield className="w-8 h-8 text-red-600" />
            Super Admin Panel
          </h1>
          <p className="text-slate-600 mt-1">Audit Trail & MIS Dashboard</p>
        </div>
        <Button onClick={() => setShowCreateModal(true)} className="bg-teal-600 hover:bg-teal-700">
          <UserPlus className="w-4 h-4 mr-2" /> Create User
        </Button>
      </div>

      {/* Tabs */}
      <div className="flex gap-2 border-b border-slate-200 pb-2 overflow-x-auto">
        {tabs.map((tab) => (
          <button
            key={tab.key}
            onClick={() => setActiveTab(tab.key)}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-all whitespace-nowrap ${
              activeTab === tab.key
                ? 'bg-teal-600 text-white'
                : 'text-slate-600 hover:bg-slate-100'
            }`}
          >
            <tab.icon className="w-4 h-4" />
            {tab.label}
          </button>
        ))}
      </div>

      {/* Dashboard Tab */}
      {activeTab === 'dashboard' && dashboard && (
        <div className="space-y-6">
          {/* Stats Cards */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <Card className="bg-gradient-to-br from-blue-500 to-blue-600 text-white">
              <CardContent className="p-6">
                <Users className="w-8 h-8 mb-2 opacity-80" />
                <p className="text-3xl font-bold">{dashboard.users?.total || 0}</p>
                <p className="text-blue-100">Total Users</p>
              </CardContent>
            </Card>
            <Card className="bg-gradient-to-br from-green-500 to-green-600 text-white">
              <CardContent className="p-6">
                <UserCheck className="w-8 h-8 mb-2 opacity-80" />
                <p className="text-3xl font-bold">{dashboard.users?.active || 0}</p>
                <p className="text-green-100">Active Users</p>
              </CardContent>
            </Card>
            <Card className="bg-gradient-to-br from-purple-500 to-purple-600 text-white">
              <CardContent className="p-6">
                <Activity className="w-8 h-8 mb-2 opacity-80" />
                <p className="text-3xl font-bold">{dashboard.today?.total_actions || 0}</p>
                <p className="text-purple-100">Today's Actions</p>
              </CardContent>
            </Card>
            <Card className="bg-gradient-to-br from-orange-500 to-orange-600 text-white">
              <CardContent className="p-6">
                <Clock className="w-8 h-8 mb-2 opacity-80" />
                <p className="text-3xl font-bold">{dashboard.today?.active_users || 0}</p>
                <p className="text-orange-100">Active Today</p>
              </CardContent>
            </Card>
          </div>

          {/* Charts Row */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Role Distribution */}
            <Card>
              <CardContent className="p-6">
                <h3 className="font-bold text-slate-900 mb-4">Users by Role</h3>
                <ResponsiveContainer width="100%" height={250}>
                  <RechartsPie>
                    <Pie
                      data={Object.entries(dashboard.users?.by_role || {}).filter(([_, v]) => v > 0).map(([name, value]) => ({ name, value }))}
                      cx="50%"
                      cy="50%"
                      innerRadius={60}
                      outerRadius={100}
                      dataKey="value"
                      label={({ name, value }) => `${name}: ${value}`}
                    >
                      {Object.entries(dashboard.users?.by_role || {}).map((_, i) => (
                        <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />
                      ))}
                    </Pie>
                    <Tooltip />
                  </RechartsPie>
                </ResponsiveContainer>
              </CardContent>
            </Card>

            {/* System Stats */}
            <Card>
              <CardContent className="p-6">
                <h3 className="font-bold text-slate-900 mb-4">System Data</h3>
                <div className="grid grid-cols-2 gap-4">
                  {Object.entries(dashboard.system || {}).map(([key, value]) => (
                    <div key={key} className="p-3 bg-slate-50 rounded-lg">
                      <p className="text-2xl font-bold text-slate-900">{value}</p>
                      <p className="text-sm text-slate-500 capitalize">{key.replace('_', ' ')}</p>
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>
          </div>

          {/* Recent Activity */}
          <Card>
            <CardContent className="p-6">
              <h3 className="font-bold text-slate-900 mb-4">Recent Activity</h3>
              <div className="space-y-3 max-h-80 overflow-y-auto">
                {(dashboard.recent_activity || []).slice(0, 10).map((log, idx) => (
                  <div key={idx} className="flex items-center justify-between p-3 bg-slate-50 rounded-lg">
                    <div className="flex items-center gap-3">
                      <span className={`px-2 py-1 rounded text-xs font-medium ${actionColors[log.action] || 'bg-slate-100'}`}>
                        {log.action}
                      </span>
                      <div>
                        <p className="text-sm font-medium text-slate-900">{log.user_name}</p>
                        <p className="text-xs text-slate-500">{log.module} - {JSON.stringify(log.details).slice(0, 50)}</p>
                      </div>
                    </div>
                    <span className="text-xs text-slate-400">{new Date(log.timestamp).toLocaleString()}</span>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        </div>
      )}

      {/* User Management Tab */}
      {activeTab === 'users' && (
        <div className="space-y-4">
          {/* Filters */}
          <div className="flex gap-4 flex-wrap">
            <div className="relative flex-1 min-w-[200px]">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
              <input
                type="text"
                placeholder="Search users..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && fetchUsers()}
                className="w-full pl-10 pr-4 py-2 border border-slate-300 rounded-lg"
              />
            </div>
            <select
              value={roleFilter}
              onChange={(e) => { setRoleFilter(e.target.value); }}
              className="px-4 py-2 border border-slate-300 rounded-lg"
            >
              <option value="">All Roles</option>
              {Object.keys(roleColors).map(role => (
                <option key={role} value={role}>{role.replace('_', ' ')}</option>
              ))}
            </select>
            <Button onClick={fetchUsers} variant="outline">
              <Filter className="w-4 h-4 mr-2" /> Apply
            </Button>
          </div>

          {/* Users Table */}
          <Card>
            <CardContent className="p-0">
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead className="bg-slate-50 border-b">
                    <tr>
                      <th className="text-left p-4 text-sm font-medium text-slate-600">User</th>
                      <th className="text-left p-4 text-sm font-medium text-slate-600">Email</th>
                      <th className="text-left p-4 text-sm font-medium text-slate-600">Role</th>
                      <th className="text-left p-4 text-sm font-medium text-slate-600">Status</th>
                      <th className="text-left p-4 text-sm font-medium text-slate-600">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {users.map((user) => (
                      <tr key={user.id} className="border-b hover:bg-slate-50">
                        <td className="p-4">
                          <p className="font-medium text-slate-900">{user.name}</p>
                          <p className="text-xs text-slate-500">{user.phone}</p>
                        </td>
                        <td className="p-4 text-sm text-slate-600">{user.email}</td>
                        <td className="p-4">
                          <span className={`px-2 py-1 rounded text-xs font-medium ${roleColors[user.role] || 'bg-slate-100'}`}>
                            {user.role}
                          </span>
                        </td>
                        <td className="p-4">
                          {user.is_active !== false ? (
                            <span className="flex items-center gap-1 text-green-600 text-sm">
                              <CheckCircle className="w-4 h-4" /> Active
                            </span>
                          ) : (
                            <span className="flex items-center gap-1 text-red-600 text-sm">
                              <XCircle className="w-4 h-4" /> Disabled
                            </span>
                          )}
                        </td>
                        <td className="p-4">
                          <div className="flex gap-2">
                            {user.is_active !== false ? (
                              <Button size="sm" variant="outline" onClick={() => handleDisableUser(user.id)} className="text-orange-600 border-orange-300">
                                <UserX className="w-4 h-4" />
                              </Button>
                            ) : (
                              <Button size="sm" variant="outline" onClick={() => handleEnableUser(user.id)} className="text-green-600 border-green-300">
                                <UserCheck className="w-4 h-4" />
                              </Button>
                            )}
                            <Button size="sm" variant="outline" onClick={() => handleDeleteUser(user.id)} className="text-red-600 border-red-300">
                              <Trash2 className="w-4 h-4" />
                            </Button>
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Audit Trail Tab */}
      {activeTab === 'audit' && (
        <Card>
          <CardContent className="p-6">
            <h3 className="font-bold text-slate-900 mb-4">Audit Trail - All Actions</h3>
            <div className="space-y-3 max-h-[600px] overflow-y-auto">
              {auditLogs.map((log, idx) => (
                <div key={idx} className="flex items-start justify-between p-4 bg-slate-50 rounded-lg">
                  <div className="flex items-start gap-4">
                    <span className={`px-3 py-1 rounded text-xs font-medium ${actionColors[log.action] || 'bg-slate-100'}`}>
                      {log.action.toUpperCase()}
                    </span>
                    <div>
                      <p className="font-medium text-slate-900">{log.user_name} <span className="text-slate-400">({log.user_role})</span></p>
                      <p className="text-sm text-slate-600">Module: <span className="font-medium">{log.module}</span></p>
                      <p className="text-xs text-slate-500 mt-1">{JSON.stringify(log.details)}</p>
                    </div>
                  </div>
                  <div className="text-right">
                    <p className="text-sm text-slate-600">{new Date(log.timestamp).toLocaleDateString()}</p>
                    <p className="text-xs text-slate-400">{new Date(log.timestamp).toLocaleTimeString()}</p>
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Daily Activity Tab */}
      {activeTab === 'daily' && dailyActivity && (
        <div className="space-y-6">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <Card>
              <CardContent className="p-4 text-center">
                <p className="text-3xl font-bold text-teal-600">{dailyActivity.summary?.total_actions || 0}</p>
                <p className="text-sm text-slate-500">Total Actions</p>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="p-4 text-center">
                <p className="text-3xl font-bold text-blue-600">{dailyActivity.summary?.active_users || 0}</p>
                <p className="text-sm text-slate-500">Active Users</p>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="p-4 text-center">
                <p className="text-3xl font-bold text-green-600">{dailyActivity.summary?.actions_breakdown?.create || 0}</p>
                <p className="text-sm text-slate-500">Creates</p>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="p-4 text-center">
                <p className="text-3xl font-bold text-red-600">{dailyActivity.summary?.actions_breakdown?.delete || 0}</p>
                <p className="text-sm text-slate-500">Deletes</p>
              </CardContent>
            </Card>
          </div>

          <Card>
            <CardContent className="p-6">
              <h3 className="font-bold text-slate-900 mb-4">User Activity - {dailyActivity.summary?.date}</h3>
              <div className="space-y-3">
                {(dailyActivity.user_activity || []).map((user, idx) => (
                  <div key={idx} className="flex items-center justify-between p-4 bg-slate-50 rounded-lg">
                    <div>
                      <p className="font-medium text-slate-900">{user.user_name}</p>
                      <p className="text-sm text-slate-500">{user.user_role} • {user.modules_accessed?.join(', ')}</p>
                    </div>
                    <div className="text-right">
                      <p className="text-2xl font-bold text-teal-600">{user.total_actions}</p>
                      <p className="text-xs text-slate-400">actions</p>
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Monthly MIS Tab */}
      {activeTab === 'monthly' && monthlyMIS && (
        <div className="space-y-6">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <Card>
              <CardContent className="p-4 text-center">
                <p className="text-3xl font-bold text-teal-600">{monthlyMIS.summary?.total_actions || 0}</p>
                <p className="text-sm text-slate-500">Total Actions</p>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="p-4 text-center">
                <p className="text-3xl font-bold text-blue-600">{monthlyMIS.summary?.total_users || 0}</p>
                <p className="text-sm text-slate-500">Total Users</p>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="p-4 text-center">
                <p className="text-3xl font-bold text-purple-600">{monthlyMIS.summary?.avg_actions_per_user || 0}</p>
                <p className="text-sm text-slate-500">Avg/User</p>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="p-4 text-center">
                <p className="text-3xl font-bold text-orange-600">{monthlyMIS.summary?.avg_actions_per_day || 0}</p>
                <p className="text-sm text-slate-500">Avg/Day</p>
              </CardContent>
            </Card>
          </div>

          {/* Daily Breakdown Chart */}
          <Card>
            <CardContent className="p-6">
              <h3 className="font-bold text-slate-900 mb-4">Daily Activity Trend - {monthlyMIS.month}</h3>
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={monthlyMIS.daily_breakdown || []}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="date" tickFormatter={(d) => d.slice(8)} />
                  <YAxis />
                  <Tooltip />
                  <Bar dataKey="total" fill="#0d9488" name="Actions" />
                </BarChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>

          {/* Top Performers */}
          <Card>
            <CardContent className="p-6">
              <h3 className="font-bold text-slate-900 mb-4">Top Performers</h3>
              <div className="space-y-3">
                {(monthlyMIS.top_performers || []).slice(0, 10).map((user, idx) => (
                  <div key={idx} className="flex items-center justify-between p-3 bg-slate-50 rounded-lg">
                    <div className="flex items-center gap-3">
                      <span className="w-8 h-8 flex items-center justify-center bg-teal-100 text-teal-700 rounded-full font-bold">
                        {idx + 1}
                      </span>
                      <div>
                        <p className="font-medium text-slate-900">{user.user_name}</p>
                        <p className="text-xs text-slate-500">{user.user_role}</p>
                      </div>
                    </div>
                    <div className="text-right">
                      <p className="text-xl font-bold text-teal-600">{user.total_actions}</p>
                      <p className="text-xs text-slate-400">{user.active_days} active days</p>
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Create User Modal */}
      {showCreateModal && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <Card className="w-full max-w-md mx-4">
            <CardContent className="p-6">
              <h3 className="text-lg font-bold text-slate-900 mb-4">Create New User</h3>
              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-1">Name *</label>
                  <input
                    type="text"
                    value={newUser.name}
                    onChange={(e) => setNewUser({ ...newUser, name: e.target.value })}
                    className="w-full px-3 py-2 border border-slate-300 rounded-lg"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-1">Email *</label>
                  <input
                    type="email"
                    value={newUser.email}
                    onChange={(e) => setNewUser({ ...newUser, email: e.target.value })}
                    className="w-full px-3 py-2 border border-slate-300 rounded-lg"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-1">Phone</label>
                  <input
                    type="tel"
                    value={newUser.phone}
                    onChange={(e) => setNewUser({ ...newUser, phone: e.target.value })}
                    className="w-full px-3 py-2 border border-slate-300 rounded-lg"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-1">Password *</label>
                  <input
                    type="password"
                    value={newUser.password}
                    onChange={(e) => setNewUser({ ...newUser, password: e.target.value })}
                    className="w-full px-3 py-2 border border-slate-300 rounded-lg"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-1">Role *</label>
                  <select
                    value={newUser.role}
                    onChange={(e) => setNewUser({ ...newUser, role: e.target.value })}
                    className="w-full px-3 py-2 border border-slate-300 rounded-lg"
                  >
                    {Object.keys(roleColors).map(role => (
                      <option key={role} value={role}>{role.replace('_', ' ')}</option>
                    ))}
                  </select>
                </div>
                <div className="flex gap-3 mt-6">
                  <Button onClick={() => setShowCreateModal(false)} variant="outline" className="flex-1">
                    Cancel
                  </Button>
                  <Button onClick={handleCreateUser} className="flex-1 bg-teal-600 hover:bg-teal-700">
                    Create User
                  </Button>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>
      )}
    </div>
  );
};

export default SuperAdminDashboard;
