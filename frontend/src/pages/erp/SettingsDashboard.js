import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { 
  Settings, IndianRupee, Percent, CreditCard, Save, 
  RefreshCw, Info, CheckCircle, AlertTriangle, Mail, 
  MessageSquare, Clock, Send, Bell
} from 'lucide-react';
import { toast } from 'sonner';

const SettingsDashboard = () => {
  const [activeTab, setActiveTab] = useState('payment');
  const [settings, setSettings] = useState({
    no_advance_upto: 2000,
    min_advance_percent_upto_5000: 50,
    min_advance_percent_above_5000: 25,
    credit_enabled: true
  });
  const [reportSettings, setReportSettings] = useState({
    enabled: true,
    email_enabled: true,
    whatsapp_enabled: true,
    report_time: '05:00',
    timezone: 'Asia/Kolkata',
    weekly_enabled: true,
    monthly_enabled: true
  });
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [sendingReport, setSendingReport] = useState(false);
  const [sendingWeekly, setSendingWeekly] = useState(false);
  const [sendingMonthly, setSendingMonthly] = useState(false);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [updatedBy, setUpdatedBy] = useState(null);

  useEffect(() => {
    fetchSettings();
    fetchReportSettings();
  }, []);

  const fetchSettings = async () => {
    try {
      setLoading(true);
      const token = localStorage.getItem('token');
      const response = await fetch(`${process.env.REACT_APP_BACKEND_URL}/api/settings/advance`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      const data = await response.json();
      if (response.ok) {
        setSettings({
          no_advance_upto: data.no_advance_upto || 2000,
          min_advance_percent_upto_5000: data.min_advance_percent_upto_5000 || 50,
          min_advance_percent_above_5000: data.min_advance_percent_above_5000 || 25,
          credit_enabled: data.credit_enabled !== false
        });
        setLastUpdated(data.updated_at);
        setUpdatedBy(data.updated_by);
      }
    } catch (error) {
      console.error('Failed to fetch settings:', error);
      toast.error('Failed to load settings');
    } finally {
      setLoading(false);
    }
  };

  const fetchReportSettings = async () => {
    try {
      const token = localStorage.getItem('token');
      const response = await fetch(`${process.env.REACT_APP_BACKEND_URL}/api/erp/cash/report-settings`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      const data = await response.json();
      if (response.ok) {
        setReportSettings({
          enabled: data.enabled !== false,
          email_enabled: data.email_enabled !== false,
          whatsapp_enabled: data.whatsapp_enabled !== false,
          report_time: data.report_time || '05:00',
          timezone: data.timezone || 'Asia/Kolkata',
          weekly_enabled: data.weekly_enabled !== false,
          monthly_enabled: data.monthly_enabled !== false
        });
      }
    } catch (error) {
      console.error('Failed to fetch report settings:', error);
    }
  };

  const handleSaveSettings = async () => {
    try {
      setSaving(true);
      const token = localStorage.getItem('token');
      const response = await fetch(`${process.env.REACT_APP_BACKEND_URL}/api/settings/advance`, {
        method: 'PUT',
        headers: { 
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(settings)
      });
      const data = await response.json();
      if (!response.ok) throw new Error(data.detail || 'Failed to save settings');
      
      toast.success('Settings saved successfully!');
      setLastUpdated(data.settings?.updated_at);
      setUpdatedBy(data.settings?.updated_by);
    } catch (error) {
      toast.error(error.message || 'Failed to save settings');
    } finally {
      setSaving(false);
    }
  };

  const handleSaveReportSettings = async () => {
    try {
      setSaving(true);
      const token = localStorage.getItem('token');
      const response = await fetch(`${process.env.REACT_APP_BACKEND_URL}/api/erp/cash/report-settings`, {
        method: 'PUT',
        headers: { 
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(reportSettings)
      });
      const data = await response.json();
      if (!response.ok) throw new Error(data.detail || 'Failed to save settings');
      toast.success('Report settings saved!');
    } catch (error) {
      toast.error(error.message || 'Failed to save report settings');
    } finally {
      setSaving(false);
    }
  };

  const handleSendNow = async () => {
    try {
      setSendingReport(true);
      const token = localStorage.getItem('token');
      const response = await fetch(`${process.env.REACT_APP_BACKEND_URL}/api/erp/cash/send-daily-report`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}` }
      });
      const data = await response.json();
      if (!response.ok) throw new Error(data.detail || 'Failed to send report');
      toast.success(`Daily report sent to ${data.recipients?.length || 0} recipients!`);
    } catch (error) {
      toast.error(error.message || 'Failed to send report');
    } finally {
      setSendingReport(false);
    }
  };

  const handleSendWeekly = async () => {
    try {
      setSendingWeekly(true);
      const token = localStorage.getItem('token');
      const response = await fetch(`${process.env.REACT_APP_BACKEND_URL}/api/erp/cash/send-weekly-report`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}` }
      });
      const data = await response.json();
      if (!response.ok) throw new Error(data.detail || 'Failed to send report');
      toast.success(`Weekly report sent! Period: ${data.period?.start} to ${data.period?.end}`);
    } catch (error) {
      toast.error(error.message || 'Failed to send weekly report');
    } finally {
      setSendingWeekly(false);
    }
  };

  const handleSendMonthly = async () => {
    try {
      setSendingMonthly(true);
      const token = localStorage.getItem('token');
      const response = await fetch(`${process.env.REACT_APP_BACKEND_URL}/api/erp/cash/send-monthly-report`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}` }
      });
      const data = await response.json();
      if (!response.ok) throw new Error(data.detail || 'Failed to send report');
      toast.success(`Monthly report sent! Period: ${data.period?.start} to ${data.period?.end}`);
    } catch (error) {
      toast.error(error.message || 'Failed to send monthly report');
    } finally {
      setSendingMonthly(false);
    }
  };

  const formatDate = (dateStr) => {
    if (!dateStr) return 'Never';
    return new Date(dateStr).toLocaleString('en-IN', {
      day: '2-digit', month: 'short', year: 'numeric',
      hour: '2-digit', minute: '2-digit'
    });
  };

  // Preview calculation
  const getPreviewRules = () => {
    const rules = [];
    rules.push({
      range: `₹0 - ₹${settings.no_advance_upto.toLocaleString()}`,
      rule: '100% payment required',
      options: ['100%'],
      color: 'bg-red-500/20 text-red-400 border-red-500/30'
    });
    rules.push({
      range: `₹${settings.no_advance_upto.toLocaleString()} - ₹5,000`,
      rule: `Minimum ${settings.min_advance_percent_upto_5000}% advance`,
      options: [25, 50, 75, 100].filter(p => p >= settings.min_advance_percent_upto_5000).map(p => `${p}%`),
      color: 'bg-amber-500/20 text-amber-400 border-amber-500/30'
    });
    rules.push({
      range: '> ₹5,000',
      rule: `Minimum ${settings.min_advance_percent_above_5000}% advance`,
      options: [25, 50, 75, 100].filter(p => p >= settings.min_advance_percent_above_5000).map(p => `${p}%`),
      color: 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30'
    });
    if (settings.credit_enabled) {
      rules.push({
        range: 'Admin Only',
        rule: 'Credit orders (0% advance)',
        options: ['Credit'],
        color: 'bg-purple-500/20 text-purple-400 border-purple-500/30'
      });
    }
    return rules;
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-slate-100 p-6 flex items-center justify-center">
        <RefreshCw className="w-8 h-8 text-teal-600 animate-spin" />
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-slate-100 p-6" data-testid="settings-dashboard">
      {/* Header */}
      <div className="mb-6">
        <div className="flex items-center gap-3 mb-2">
          <div className="w-12 h-12 bg-gradient-to-br from-teal-500 to-teal-600 rounded-xl flex items-center justify-center shadow-lg">
            <Settings className="w-6 h-6 text-white" />
          </div>
          <div>
            <h1 className="text-2xl font-bold text-slate-800">System Settings</h1>
            <p className="text-slate-500 text-sm">Configure payment rules and automated reports</p>
          </div>
        </div>
      </div>

      {/* Tabs */}
      <div className="flex gap-2 mb-6">
        <button
          onClick={() => setActiveTab('payment')}
          className={`px-4 py-2 rounded-lg font-medium transition-all flex items-center gap-2 ${
            activeTab === 'payment' 
              ? 'bg-teal-600 text-white shadow-lg shadow-teal-500/20' 
              : 'bg-white text-slate-600 hover:bg-slate-50'
          }`}
          data-testid="payment-tab"
        >
          <IndianRupee className="w-4 h-4" />
          Payment Rules
        </button>
        <button
          onClick={() => setActiveTab('reports')}
          className={`px-4 py-2 rounded-lg font-medium transition-all flex items-center gap-2 ${
            activeTab === 'reports' 
              ? 'bg-teal-600 text-white shadow-lg shadow-teal-500/20' 
              : 'bg-white text-slate-600 hover:bg-slate-50'
          }`}
          data-testid="reports-tab"
        >
          <Mail className="w-4 h-4" />
          Daily Reports
        </button>
      </div>

      {/* Payment Settings Tab */}
      {activeTab === 'payment' && (
        <>
          {lastUpdated && (
            <p className="text-xs text-slate-400 mb-4">
              Last updated: {formatDate(lastUpdated)} {updatedBy && `by ${updatedBy}`}
            </p>
          )}
          <div className="grid lg:grid-cols-2 gap-6">
        {/* Settings Form */}
        <Card className="bg-white shadow-lg border-0" data-testid="settings-form-card">
          <CardHeader className="border-b border-slate-100 bg-gradient-to-r from-slate-50 to-white">
            <CardTitle className="text-lg font-semibold text-slate-800 flex items-center gap-2">
              <IndianRupee className="w-5 h-5 text-teal-600" />
              Advance Payment Rules
            </CardTitle>
          </CardHeader>
          <CardContent className="p-6 space-y-6">
            {/* Rule 1: No Advance Threshold */}
            <div className="space-y-2">
              <label className="flex items-center gap-2 text-sm font-medium text-slate-700">
                <span className="w-6 h-6 bg-red-100 text-red-600 rounded-full flex items-center justify-center text-xs font-bold">1</span>
                Full Payment Required Up To (₹)
              </label>
              <div className="relative">
                <IndianRupee className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
                <input
                  type="number"
                  value={settings.no_advance_upto}
                  onChange={(e) => setSettings({...settings, no_advance_upto: parseFloat(e.target.value) || 0})}
                  className="w-full pl-9 pr-4 py-3 border border-slate-200 rounded-lg focus:ring-2 focus:ring-teal-500 focus:border-teal-500 bg-slate-50"
                  placeholder="2000"
                  data-testid="no-advance-upto-input"
                />
              </div>
              <p className="text-xs text-slate-500 flex items-start gap-1">
                <Info className="w-3 h-3 mt-0.5 flex-shrink-0" />
                Orders below this amount will require 100% payment upfront
              </p>
            </div>

            {/* Rule 2: Minimum % for orders up to 5000 */}
            <div className="space-y-2">
              <label className="flex items-center gap-2 text-sm font-medium text-slate-700">
                <span className="w-6 h-6 bg-amber-100 text-amber-600 rounded-full flex items-center justify-center text-xs font-bold">2</span>
                Minimum Advance % (Orders up to ₹5,000)
              </label>
              <div className="relative">
                <Percent className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
                <select
                  value={settings.min_advance_percent_upto_5000}
                  onChange={(e) => setSettings({...settings, min_advance_percent_upto_5000: parseInt(e.target.value)})}
                  className="w-full pl-9 pr-4 py-3 border border-slate-200 rounded-lg focus:ring-2 focus:ring-teal-500 focus:border-teal-500 bg-slate-50 appearance-none cursor-pointer"
                  data-testid="min-advance-5000-select"
                >
                  <option value={25}>25% Minimum</option>
                  <option value={50}>50% Minimum</option>
                  <option value={75}>75% Minimum</option>
                  <option value={100}>100% Required</option>
                </select>
              </div>
              <p className="text-xs text-slate-500 flex items-start gap-1">
                <Info className="w-3 h-3 mt-0.5 flex-shrink-0" />
                For orders between ₹{settings.no_advance_upto.toLocaleString()} and ₹5,000
              </p>
            </div>

            {/* Rule 3: Minimum % for orders above 5000 */}
            <div className="space-y-2">
              <label className="flex items-center gap-2 text-sm font-medium text-slate-700">
                <span className="w-6 h-6 bg-emerald-100 text-emerald-600 rounded-full flex items-center justify-center text-xs font-bold">3</span>
                Minimum Advance % (Orders above ₹5,000)
              </label>
              <div className="relative">
                <Percent className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
                <select
                  value={settings.min_advance_percent_above_5000}
                  onChange={(e) => setSettings({...settings, min_advance_percent_above_5000: parseInt(e.target.value)})}
                  className="w-full pl-9 pr-4 py-3 border border-slate-200 rounded-lg focus:ring-2 focus:ring-teal-500 focus:border-teal-500 bg-slate-50 appearance-none cursor-pointer"
                  data-testid="min-advance-above-5000-select"
                >
                  <option value={25}>25% Minimum</option>
                  <option value={50}>50% Minimum</option>
                  <option value={75}>75% Minimum</option>
                  <option value={100}>100% Required</option>
                </select>
              </div>
              <p className="text-xs text-slate-500 flex items-start gap-1">
                <Info className="w-3 h-3 mt-0.5 flex-shrink-0" />
                For high-value orders above ₹5,000
              </p>
            </div>

            {/* Credit Orders Toggle */}
            <div className="space-y-2 pt-4 border-t border-slate-100">
              <label className="flex items-center gap-2 text-sm font-medium text-slate-700">
                <span className="w-6 h-6 bg-purple-100 text-purple-600 rounded-full flex items-center justify-center text-xs font-bold">C</span>
                Credit Orders (Admin Only)
              </label>
              <div 
                className={`flex items-center justify-between p-4 rounded-lg border transition-all cursor-pointer ${
                  settings.credit_enabled 
                    ? 'bg-purple-50 border-purple-200' 
                    : 'bg-slate-50 border-slate-200'
                }`}
                onClick={() => setSettings({...settings, credit_enabled: !settings.credit_enabled})}
                data-testid="credit-toggle"
              >
                <div className="flex items-center gap-3">
                  <CreditCard className={`w-5 h-5 ${settings.credit_enabled ? 'text-purple-600' : 'text-slate-400'}`} />
                  <div>
                    <p className="font-medium text-slate-800">Allow Credit Sales</p>
                    <p className="text-xs text-slate-500">Admin can create orders with 0% advance</p>
                  </div>
                </div>
                <div className={`w-12 h-6 rounded-full relative transition-colors ${
                  settings.credit_enabled ? 'bg-purple-600' : 'bg-slate-300'
                }`}>
                  <div className={`w-5 h-5 bg-white rounded-full absolute top-0.5 transition-transform shadow ${
                    settings.credit_enabled ? 'translate-x-6' : 'translate-x-0.5'
                  }`} />
                </div>
              </div>
            </div>

            {/* Save Button */}
            <div className="pt-4">
              <Button
                onClick={handleSaveSettings}
                disabled={saving}
                className="w-full bg-gradient-to-r from-teal-600 to-teal-500 hover:from-teal-700 hover:to-teal-600 text-white py-3 rounded-lg font-medium shadow-lg shadow-teal-500/20"
                data-testid="save-settings-btn"
              >
                {saving ? (
                  <RefreshCw className="w-5 h-5 animate-spin mr-2" />
                ) : (
                  <Save className="w-5 h-5 mr-2" />
                )}
                {saving ? 'Saving...' : 'Save Settings'}
              </Button>
            </div>
          </CardContent>
        </Card>

        {/* Preview Panel */}
        <div className="space-y-6">
          {/* Rules Preview */}
          <Card className="bg-white shadow-lg border-0" data-testid="rules-preview-card">
            <CardHeader className="border-b border-slate-100 bg-gradient-to-r from-slate-50 to-white">
              <CardTitle className="text-lg font-semibold text-slate-800 flex items-center gap-2">
                <CheckCircle className="w-5 h-5 text-emerald-600" />
                Rules Preview
              </CardTitle>
            </CardHeader>
            <CardContent className="p-6 space-y-3">
              {getPreviewRules().map((rule, index) => (
                <div key={index} className={`p-4 rounded-lg border ${rule.color}`}>
                  <div className="flex items-center justify-between mb-2">
                    <span className="font-medium">{rule.range}</span>
                    <div className="flex gap-1">
                      {rule.options.map((opt, i) => (
                        <span key={i} className="px-2 py-0.5 bg-white/50 rounded text-xs font-medium">
                          {opt}
                        </span>
                      ))}
                    </div>
                  </div>
                  <p className="text-sm opacity-80">{rule.rule}</p>
                </div>
              ))}
            </CardContent>
          </Card>

          {/* Example Scenarios */}
          <Card className="bg-white shadow-lg border-0" data-testid="example-scenarios-card">
            <CardHeader className="border-b border-slate-100 bg-gradient-to-r from-slate-50 to-white">
              <CardTitle className="text-lg font-semibold text-slate-800 flex items-center gap-2">
                <AlertTriangle className="w-5 h-5 text-amber-500" />
                Example Scenarios
              </CardTitle>
            </CardHeader>
            <CardContent className="p-6">
              <div className="space-y-4 text-sm">
                <div className="p-3 bg-slate-50 rounded-lg">
                  <p className="font-medium text-slate-800">₹1,500 Order</p>
                  <p className="text-slate-600">
                    → Customer must pay <span className="text-red-600 font-semibold">100%</span> upfront (full payment)
                  </p>
                </div>
                <div className="p-3 bg-slate-50 rounded-lg">
                  <p className="font-medium text-slate-800">₹3,500 Order</p>
                  <p className="text-slate-600">
                    → Options: <span className="text-amber-600 font-semibold">
                      {[25, 50, 75, 100].filter(p => p >= settings.min_advance_percent_upto_5000).join('%, ')}%
                    </span> advance
                  </p>
                </div>
                <div className="p-3 bg-slate-50 rounded-lg">
                  <p className="font-medium text-slate-800">₹12,000 Order</p>
                  <p className="text-slate-600">
                    → Options: <span className="text-emerald-600 font-semibold">
                      {[25, 50, 75, 100].filter(p => p >= settings.min_advance_percent_above_5000).join('%, ')}%
                    </span> advance
                  </p>
                </div>
                {settings.credit_enabled && (
                  <div className="p-3 bg-purple-50 rounded-lg border border-purple-200">
                    <p className="font-medium text-slate-800">Admin Credit Order</p>
                    <p className="text-slate-600">
                      → Admin can approve <span className="text-purple-600 font-semibold">0% (Credit)</span> for any order
                    </p>
                  </div>
                )}
              </div>
            </CardContent>
          </Card>

          {/* Info Card */}
          <Card className="bg-gradient-to-br from-teal-600 to-teal-700 shadow-lg border-0 text-white">
            <CardContent className="p-6">
              <div className="flex items-start gap-4">
                <div className="w-10 h-10 bg-white/20 rounded-lg flex items-center justify-center flex-shrink-0">
                  <Info className="w-5 h-5" />
                </div>
                <div>
                  <h3 className="font-semibold mb-2">How It Works</h3>
                  <ul className="text-sm text-white/90 space-y-1">
                    <li>• These rules apply to the checkout page automatically</li>
                    <li>• Customers will only see allowed payment options</li>
                    <li>• Credit option is only visible to Admin users</li>
                    <li>• Changes take effect immediately after saving</li>
                  </ul>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
        </>
      )}

      {/* Daily Reports Tab */}
      {activeTab === 'reports' && (
        <div className="grid lg:grid-cols-2 gap-6">
          {/* Report Settings Card */}
          <Card className="bg-white shadow-lg border-0" data-testid="report-settings-card">
            <CardHeader className="border-b border-slate-100 bg-gradient-to-r from-slate-50 to-white">
              <CardTitle className="text-lg font-semibold text-slate-800 flex items-center gap-2">
                <Bell className="w-5 h-5 text-teal-600" />
                Daily P&L Report Settings
              </CardTitle>
            </CardHeader>
            <CardContent className="p-6 space-y-6">
              {/* Enable Reports Toggle */}
              <div 
                className={`flex items-center justify-between p-4 rounded-lg border transition-all cursor-pointer ${
                  reportSettings.enabled 
                    ? 'bg-emerald-50 border-emerald-200' 
                    : 'bg-slate-50 border-slate-200'
                }`}
                onClick={() => setReportSettings({...reportSettings, enabled: !reportSettings.enabled})}
                data-testid="enable-reports-toggle"
              >
                <div className="flex items-center gap-3">
                  <Clock className={`w-5 h-5 ${reportSettings.enabled ? 'text-emerald-600' : 'text-slate-400'}`} />
                  <div>
                    <p className="font-medium text-slate-800">Enable Daily Reports</p>
                    <p className="text-xs text-slate-500">Auto-send P&L reports to Admin/Finance users</p>
                  </div>
                </div>
                <div className={`w-12 h-6 rounded-full relative transition-colors ${
                  reportSettings.enabled ? 'bg-emerald-600' : 'bg-slate-300'
                }`}>
                  <div className={`w-5 h-5 bg-white rounded-full absolute top-0.5 transition-transform shadow ${
                    reportSettings.enabled ? 'translate-x-6' : 'translate-x-0.5'
                  }`} />
                </div>
              </div>

              {/* Email Toggle */}
              <div 
                className={`flex items-center justify-between p-4 rounded-lg border transition-all cursor-pointer ${
                  reportSettings.email_enabled 
                    ? 'bg-blue-50 border-blue-200' 
                    : 'bg-slate-50 border-slate-200'
                }`}
                onClick={() => setReportSettings({...reportSettings, email_enabled: !reportSettings.email_enabled})}
                data-testid="email-toggle"
              >
                <div className="flex items-center gap-3">
                  <Mail className={`w-5 h-5 ${reportSettings.email_enabled ? 'text-blue-600' : 'text-slate-400'}`} />
                  <div>
                    <p className="font-medium text-slate-800">Email Reports</p>
                    <p className="text-xs text-slate-500">Send detailed HTML report via email</p>
                  </div>
                </div>
                <div className={`w-12 h-6 rounded-full relative transition-colors ${
                  reportSettings.email_enabled ? 'bg-blue-600' : 'bg-slate-300'
                }`}>
                  <div className={`w-5 h-5 bg-white rounded-full absolute top-0.5 transition-transform shadow ${
                    reportSettings.email_enabled ? 'translate-x-6' : 'translate-x-0.5'
                  }`} />
                </div>
              </div>

              {/* WhatsApp Toggle */}
              <div 
                className={`flex items-center justify-between p-4 rounded-lg border transition-all cursor-pointer ${
                  reportSettings.whatsapp_enabled 
                    ? 'bg-green-50 border-green-200' 
                    : 'bg-slate-50 border-slate-200'
                }`}
                onClick={() => setReportSettings({...reportSettings, whatsapp_enabled: !reportSettings.whatsapp_enabled})}
                data-testid="whatsapp-toggle"
              >
                <div className="flex items-center gap-3">
                  <MessageSquare className={`w-5 h-5 ${reportSettings.whatsapp_enabled ? 'text-green-600' : 'text-slate-400'}`} />
                  <div>
                    <p className="font-medium text-slate-800">WhatsApp Reports</p>
                    <p className="text-xs text-slate-500">Send quick summary via WhatsApp</p>
                  </div>
                </div>
                <div className={`w-12 h-6 rounded-full relative transition-colors ${
                  reportSettings.whatsapp_enabled ? 'bg-green-600' : 'bg-slate-300'
                }`}>
                  <div className={`w-5 h-5 bg-white rounded-full absolute top-0.5 transition-transform shadow ${
                    reportSettings.whatsapp_enabled ? 'translate-x-6' : 'translate-x-0.5'
                  }`} />
                </div>
              </div>

              {/* Divider */}
              <div className="border-t border-slate-200 pt-4">
                <p className="text-sm font-medium text-slate-700 mb-3">📅 Report Frequency</p>
              </div>

              {/* Weekly Toggle */}
              <div 
                className={`flex items-center justify-between p-4 rounded-lg border transition-all cursor-pointer ${
                  reportSettings.weekly_enabled 
                    ? 'bg-purple-50 border-purple-200' 
                    : 'bg-slate-50 border-slate-200'
                }`}
                onClick={() => setReportSettings({...reportSettings, weekly_enabled: !reportSettings.weekly_enabled})}
                data-testid="weekly-toggle"
              >
                <div className="flex items-center gap-3">
                  <div className={`w-8 h-8 rounded-lg flex items-center justify-center ${reportSettings.weekly_enabled ? 'bg-purple-100' : 'bg-slate-100'}`}>
                    <span className="text-lg">📅</span>
                  </div>
                  <div>
                    <p className="font-medium text-slate-800">Weekly Reports</p>
                    <p className="text-xs text-slate-500">Every Monday at 5 AM IST</p>
                  </div>
                </div>
                <div className={`w-12 h-6 rounded-full relative transition-colors ${
                  reportSettings.weekly_enabled ? 'bg-purple-600' : 'bg-slate-300'
                }`}>
                  <div className={`w-5 h-5 bg-white rounded-full absolute top-0.5 transition-transform shadow ${
                    reportSettings.weekly_enabled ? 'translate-x-6' : 'translate-x-0.5'
                  }`} />
                </div>
              </div>

              {/* Monthly Toggle */}
              <div 
                className={`flex items-center justify-between p-4 rounded-lg border transition-all cursor-pointer ${
                  reportSettings.monthly_enabled 
                    ? 'bg-red-50 border-red-200' 
                    : 'bg-slate-50 border-slate-200'
                }`}
                onClick={() => setReportSettings({...reportSettings, monthly_enabled: !reportSettings.monthly_enabled})}
                data-testid="monthly-toggle"
              >
                <div className="flex items-center gap-3">
                  <div className={`w-8 h-8 rounded-lg flex items-center justify-center ${reportSettings.monthly_enabled ? 'bg-red-100' : 'bg-slate-100'}`}>
                    <span className="text-lg">📆</span>
                  </div>
                  <div>
                    <p className="font-medium text-slate-800">Monthly Reports</p>
                    <p className="text-xs text-slate-500">1st of every month at 5 AM IST</p>
                  </div>
                </div>
                <div className={`w-12 h-6 rounded-full relative transition-colors ${
                  reportSettings.monthly_enabled ? 'bg-red-600' : 'bg-slate-300'
                }`}>
                  <div className={`w-5 h-5 bg-white rounded-full absolute top-0.5 transition-transform shadow ${
                    reportSettings.monthly_enabled ? 'translate-x-6' : 'translate-x-0.5'
                  }`} />
                </div>
              </div>

              {/* Report Time */}
              <div className="space-y-2 pt-2">
                <label className="flex items-center gap-2 text-sm font-medium text-slate-700">
                  <Clock className="w-4 h-4 text-slate-400" />
                  Daily Report Time (IST)
                </label>
                <input
                  type="time"
                  value={reportSettings.report_time}
                  onChange={(e) => setReportSettings({...reportSettings, report_time: e.target.value})}
                  className="w-full px-4 py-3 border border-slate-200 rounded-lg focus:ring-2 focus:ring-teal-500 focus:border-teal-500 bg-slate-50"
                  data-testid="report-time-input"
                />
                <p className="text-xs text-slate-500">Daily reports will be sent at this time</p>
              </div>

              {/* Save Button */}
              <div className="pt-4">
                <Button
                  onClick={handleSaveReportSettings}
                  disabled={saving}
                  className="w-full bg-gradient-to-r from-teal-600 to-teal-500 hover:from-teal-700 hover:to-teal-600 text-white py-3 rounded-lg font-medium shadow-lg shadow-teal-500/20"
                  data-testid="save-report-settings-btn"
                >
                  {saving ? (
                    <RefreshCw className="w-5 h-5 animate-spin mr-2" />
                  ) : (
                    <Save className="w-5 h-5 mr-2" />
                  )}
                  {saving ? 'Saving...' : 'Save Settings'}
                </Button>
              </div>

              {/* Manual Send Buttons */}
              <div className="pt-2 border-t border-slate-200 mt-4">
                <p className="text-sm font-medium text-slate-700 mb-3">🚀 Send Report Now</p>
                <div className="grid grid-cols-3 gap-2">
                  <Button
                    onClick={handleSendNow}
                    disabled={sendingReport}
                    variant="outline"
                    className="py-2 border-2 border-teal-600 text-teal-600 hover:bg-teal-50 rounded-lg font-medium text-sm"
                    data-testid="send-daily-btn"
                  >
                    {sendingReport ? <RefreshCw className="w-4 h-4 animate-spin" /> : 'Daily'}
                  </Button>
                  <Button
                    onClick={handleSendWeekly}
                    disabled={sendingWeekly}
                    variant="outline"
                    className="py-2 border-2 border-purple-600 text-purple-600 hover:bg-purple-50 rounded-lg font-medium text-sm"
                    data-testid="send-weekly-btn"
                  >
                    {sendingWeekly ? <RefreshCw className="w-4 h-4 animate-spin" /> : 'Weekly'}
                  </Button>
                  <Button
                    onClick={handleSendMonthly}
                    disabled={sendingMonthly}
                    variant="outline"
                    className="py-2 border-2 border-red-600 text-red-600 hover:bg-red-50 rounded-lg font-medium text-sm"
                    data-testid="send-monthly-btn"
                  >
                    {sendingMonthly ? <RefreshCw className="w-4 h-4 animate-spin" /> : 'Monthly'}
                  </Button>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Info Panel */}
          <div className="space-y-6">
            {/* Recipients Info */}
            <Card className="bg-white shadow-lg border-0">
              <CardHeader className="border-b border-slate-100 bg-gradient-to-r from-slate-50 to-white">
                <CardTitle className="text-lg font-semibold text-slate-800 flex items-center gap-2">
                  <CheckCircle className="w-5 h-5 text-emerald-600" />
                  Recipients
                </CardTitle>
              </CardHeader>
              <CardContent className="p-6">
                <p className="text-sm text-slate-600 mb-4">
                  Reports are automatically sent to all users with these roles:
                </p>
                <div className="flex flex-wrap gap-2">
                  {['Super Admin', 'Admin', 'Owner', 'Finance', 'Accountant'].map(role => (
                    <span key={role} className="px-3 py-1 bg-teal-100 text-teal-700 rounded-full text-sm font-medium">
                      {role}
                    </span>
                  ))}
                </div>
              </CardContent>
            </Card>

            {/* Report Contents */}
            <Card className="bg-white shadow-lg border-0">
              <CardHeader className="border-b border-slate-100 bg-gradient-to-r from-slate-50 to-white">
                <CardTitle className="text-lg font-semibold text-slate-800 flex items-center gap-2">
                  <AlertTriangle className="w-5 h-5 text-amber-500" />
                  Report Contents
                </CardTitle>
              </CardHeader>
              <CardContent className="p-6">
                <ul className="space-y-3 text-sm">
                  <li className="flex items-center gap-2 text-slate-700">
                    <span className="w-2 h-2 bg-emerald-500 rounded-full"></span>
                    Total Revenue (Product Sales + Transport)
                  </li>
                  <li className="flex items-center gap-2 text-slate-700">
                    <span className="w-2 h-2 bg-blue-500 rounded-full"></span>
                    Collections (Online + Cash)
                  </li>
                  <li className="flex items-center gap-2 text-slate-700">
                    <span className="w-2 h-2 bg-red-500 rounded-full"></span>
                    Total Expenses
                  </li>
                  <li className="flex items-center gap-2 text-slate-700">
                    <span className="w-2 h-2 bg-purple-500 rounded-full"></span>
                    Gross Profit/Loss with Margin %
                  </li>
                  <li className="flex items-center gap-2 text-slate-700">
                    <span className="w-2 h-2 bg-amber-500 rounded-full"></span>
                    Current Cash-in-Hand Balance
                  </li>
                </ul>
              </CardContent>
            </Card>

            {/* Scheduler Info */}
            <Card className="bg-gradient-to-br from-teal-600 to-teal-700 shadow-lg border-0 text-white">
              <CardContent className="p-6">
                <div className="flex items-start gap-4">
                  <div className="w-10 h-10 bg-white/20 rounded-lg flex items-center justify-center flex-shrink-0">
                    <Clock className="w-5 h-5" />
                  </div>
                  <div>
                    <h3 className="font-semibold mb-2">Scheduler Active</h3>
                    <ul className="text-sm text-white/90 space-y-1">
                      <li>• <strong>Daily:</strong> Every day at 5 AM IST</li>
                      <li>• <strong>Weekly:</strong> Every Monday at 5 AM IST</li>
                      <li>• <strong>Monthly:</strong> 1st of every month at 5 AM IST</li>
                      <li>• Use buttons below to trigger reports manually</li>
                    </ul>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>
        </div>
      )}
    </div>
  );
};

export default SettingsDashboard;
