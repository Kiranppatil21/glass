import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { toast } from 'sonner';
import { erpApi } from '../../utils/erpApi';
import {
  Receipt, Settings, FileText, MapPin, Building2, Plus, Trash2, Save,
  Loader2, CheckCircle, XCircle, Search, Shield, Key
} from 'lucide-react';

const GSTDashboard = () => {
  const [activeTab, setActiveTab] = useState('settings');
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [settings, setSettings] = useState(null);
  const [states, setStates] = useState([]);
  const [hsnCodes, setHSNCodes] = useState([]);
  
  // Verify GSTIN
  const [verifyGSTIN, setVerifyGSTIN] = useState('');
  const [verifying, setVerifying] = useState(false);
  const [verifyResult, setVerifyResult] = useState(null);
  
  // New HSN Code
  const [newHSN, setNewHSN] = useState({ code: '', description: '', gst_rate: 18 });

  useEffect(() => {
    fetchData();
  }, []);

  const fetchData = async () => {
    setLoading(true);
    try {
      const [settingsRes, statesRes, hsnRes] = await Promise.all([
        erpApi.gst.getSettings(),
        erpApi.gst.getStates(),
        erpApi.gst.getHSNCodes()
      ]);
      setSettings(settingsRes.data);
      setStates(statesRes.data.states || []);
      setHSNCodes(hsnRes.data.hsn_codes || []);
    } catch (error) {
      console.error('Failed to fetch GST data:', error);
      toast.error('Failed to load GST settings');
    } finally {
      setLoading(false);
    }
  };

  const handleSaveSettings = async () => {
    setSaving(true);
    try {
      await erpApi.gst.updateSettings(settings);
      toast.success('GST settings saved!');
    } catch (error) {
      toast.error(error.response?.data?.detail || 'Failed to save settings');
    } finally {
      setSaving(false);
    }
  };

  const handleAddHSN = async () => {
    if (!newHSN.code || !newHSN.description) {
      toast.error('Enter HSN code and description');
      return;
    }
    try {
      await erpApi.gst.addHSNCode(newHSN);
      toast.success('HSN code added');
      setNewHSN({ code: '', description: '', gst_rate: 18 });
      fetchData();
    } catch (error) {
      toast.error(error.response?.data?.detail || 'Failed to add HSN code');
    }
  };

  const handleDeleteHSN = async (code) => {
    if (!window.confirm('Delete this HSN code?')) return;
    try {
      await erpApi.gst.deleteHSNCode(code);
      toast.success('HSN code deleted');
      fetchData();
    } catch (error) {
      toast.error('Failed to delete HSN code');
    }
  };

  const handleVerifyGSTIN = async () => {
    if (!verifyGSTIN.trim()) {
      toast.error('Enter GSTIN to verify');
      return;
    }
    setVerifying(true);
    setVerifyResult(null);
    try {
      const response = await erpApi.gst.verifyGSTIN(verifyGSTIN.toUpperCase());
      setVerifyResult(response.data);
    } catch (error) {
      toast.error(error.response?.data?.detail || 'Verification failed');
      setVerifyResult({ valid: false, error: error.response?.data?.detail });
    } finally {
      setVerifying(false);
    }
  };

  const tabs = [
    { id: 'settings', label: 'Company Settings', icon: Building2 },
    { id: 'hsn', label: 'HSN Codes', icon: FileText },
    { id: 'verify', label: 'Verify GSTIN', icon: Search },
    { id: 'api', label: 'API Settings', icon: Key },
  ];

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="w-8 h-8 animate-spin text-teal-600" />
      </div>
    );
  }

  return (
    <div className="space-y-6" data-testid="gst-dashboard">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-slate-900 flex items-center gap-2">
          <Receipt className="w-7 h-7 text-teal-600" />
          GST Management
        </h1>
      </div>

      {/* Tabs */}
      <div className="flex flex-wrap gap-2 border-b pb-2">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg font-medium transition-colors ${
              activeTab === tab.id
                ? 'bg-teal-100 text-teal-800'
                : 'text-slate-600 hover:bg-slate-100'
            }`}
          >
            <tab.icon className="w-4 h-4" />
            {tab.label}
          </button>
        ))}
      </div>

      {/* Company Settings Tab */}
      {activeTab === 'settings' && settings && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Building2 className="w-5 h-5 text-teal-600" />
              Company GST Details
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-6">
            <div className="grid md:grid-cols-2 gap-6">
              <div>
                <label className="block text-sm font-medium text-slate-700 mb-1">Company Name</label>
                <input
                  type="text"
                  value={settings.company_name}
                  onChange={(e) => setSettings({ ...settings, company_name: e.target.value })}
                  className="w-full h-10 rounded-lg border border-slate-300 px-3"
                />
              </div>
              
              <div>
                <label className="block text-sm font-medium text-slate-700 mb-1">Company GSTIN *</label>
                <input
                  type="text"
                  value={settings.company_gstin}
                  onChange={(e) => setSettings({ ...settings, company_gstin: e.target.value.toUpperCase() })}
                  className="w-full h-10 rounded-lg border border-slate-300 px-3 font-mono"
                  placeholder="22AAAAA0000A1Z5"
                  maxLength={15}
                />
                <p className="text-xs text-slate-500 mt-1">15-digit GST Identification Number</p>
              </div>
              
              <div>
                <label className="block text-sm font-medium text-slate-700 mb-1">Company State *</label>
                <select
                  value={settings.company_state_code}
                  onChange={(e) => setSettings({ ...settings, company_state_code: e.target.value })}
                  className="w-full h-10 rounded-lg border border-slate-300 px-3"
                >
                  {states.map((s) => (
                    <option key={s.code} value={s.code}>{s.name} ({s.code})</option>
                  ))}
                </select>
                <p className="text-xs text-slate-500 mt-1">Used to determine CGST+SGST vs IGST</p>
              </div>
              
              <div>
                <label className="block text-sm font-medium text-slate-700 mb-1">Default GST Rate (%)</label>
                <input
                  type="number"
                  value={settings.default_gst_rate}
                  onChange={(e) => setSettings({ ...settings, default_gst_rate: parseFloat(e.target.value) })}
                  className="w-full h-10 rounded-lg border border-slate-300 px-3"
                />
              </div>
              
              <div className="md:col-span-2">
                <label className="block text-sm font-medium text-slate-700 mb-1">Company Address</label>
                <textarea
                  value={settings.company_address}
                  onChange={(e) => setSettings({ ...settings, company_address: e.target.value })}
                  className="w-full rounded-lg border border-slate-300 px-3 py-2 h-20"
                  placeholder="Full registered address"
                />
              </div>
              
              <div>
                <label className="block text-sm font-medium text-slate-700 mb-1">Invoice Prefix</label>
                <input
                  type="text"
                  value={settings.invoice_prefix}
                  onChange={(e) => setSettings({ ...settings, invoice_prefix: e.target.value.toUpperCase() })}
                  className="w-full h-10 rounded-lg border border-slate-300 px-3"
                  placeholder="INV"
                />
              </div>
              
              <div>
                <label className="block text-sm font-medium text-slate-700 mb-1">Invoice Starting Number</label>
                <input
                  type="number"
                  value={settings.invoice_series}
                  onChange={(e) => setSettings({ ...settings, invoice_series: parseInt(e.target.value) })}
                  className="w-full h-10 rounded-lg border border-slate-300 px-3"
                />
              </div>
            </div>

            {/* GST Type Explanation */}
            <div className="bg-blue-50 p-4 rounded-lg">
              <p className="font-medium text-blue-900 mb-2">GST Calculation Rules</p>
              <div className="grid md:grid-cols-2 gap-4 text-sm text-blue-800">
                <div>
                  <p className="font-medium">Intra-State (Same State)</p>
                  <p>CGST: {settings.default_gst_rate / 2}% + SGST: {settings.default_gst_rate / 2}%</p>
                </div>
                <div>
                  <p className="font-medium">Inter-State (Different State)</p>
                  <p>IGST: {settings.default_gst_rate}%</p>
                </div>
              </div>
            </div>

            <div className="flex justify-end pt-4 border-t">
              <Button onClick={handleSaveSettings} disabled={saving} className="gap-2">
                {saving ? <Loader2 className="w-4 h-4 animate-spin" /> : <Save className="w-4 h-4" />}
                Save Settings
              </Button>
            </div>
          </CardContent>
        </Card>
      )}

      {/* HSN Codes Tab */}
      {activeTab === 'hsn' && (
        <div className="space-y-4">
          {/* Add New HSN */}
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Add New HSN Code</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="flex flex-wrap gap-3 items-end">
                <div className="flex-1 min-w-[100px]">
                  <label className="block text-sm font-medium text-slate-700 mb-1">HSN Code</label>
                  <input
                    type="text"
                    value={newHSN.code}
                    onChange={(e) => setNewHSN({ ...newHSN, code: e.target.value })}
                    className="w-full h-10 rounded-lg border border-slate-300 px-3"
                    placeholder="7007"
                  />
                </div>
                <div className="flex-[2] min-w-[200px]">
                  <label className="block text-sm font-medium text-slate-700 mb-1">Description</label>
                  <input
                    type="text"
                    value={newHSN.description}
                    onChange={(e) => setNewHSN({ ...newHSN, description: e.target.value })}
                    className="w-full h-10 rounded-lg border border-slate-300 px-3"
                    placeholder="Safety glass (toughened/laminated)"
                  />
                </div>
                <div className="w-24">
                  <label className="block text-sm font-medium text-slate-700 mb-1">GST %</label>
                  <input
                    type="number"
                    value={newHSN.gst_rate}
                    onChange={(e) => setNewHSN({ ...newHSN, gst_rate: parseFloat(e.target.value) })}
                    className="w-full h-10 rounded-lg border border-slate-300 px-3"
                  />
                </div>
                <Button onClick={handleAddHSN} className="gap-2">
                  <Plus className="w-4 h-4" /> Add
                </Button>
              </div>
            </CardContent>
          </Card>

          {/* HSN Codes List */}
          <Card>
            <CardHeader>
              <CardTitle>HSN Codes for Glass Products</CardTitle>
            </CardHeader>
            <CardContent className="p-0">
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead className="bg-slate-50">
                    <tr>
                      <th className="px-4 py-3 text-left text-xs font-medium text-slate-500">HSN Code</th>
                      <th className="px-4 py-3 text-left text-xs font-medium text-slate-500">Description</th>
                      <th className="px-4 py-3 text-left text-xs font-medium text-slate-500">GST Rate</th>
                      <th className="px-4 py-3 text-left text-xs font-medium text-slate-500">Actions</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y">
                    {hsnCodes.map((hsn) => (
                      <tr key={hsn.code} className="hover:bg-slate-50">
                        <td className="px-4 py-3 font-mono font-medium">{hsn.code}</td>
                        <td className="px-4 py-3">{hsn.description}</td>
                        <td className="px-4 py-3">{hsn.gst_rate}%</td>
                        <td className="px-4 py-3">
                          <Button
                            size="sm"
                            variant="outline"
                            className="text-red-600"
                            onClick={() => handleDeleteHSN(hsn.code)}
                          >
                            <Trash2 className="w-3 h-3" />
                          </Button>
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

      {/* Verify GSTIN Tab */}
      {activeTab === 'verify' && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Search className="w-5 h-5 text-teal-600" />
              Verify Customer GSTIN
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex gap-3">
              <input
                type="text"
                value={verifyGSTIN}
                onChange={(e) => setVerifyGSTIN(e.target.value.toUpperCase())}
                className="flex-1 h-11 rounded-lg border border-slate-300 px-4 font-mono text-lg"
                placeholder="Enter GSTIN (e.g., 27AAAAA0000A1Z5)"
                maxLength={15}
              />
              <Button onClick={handleVerifyGSTIN} disabled={verifying} className="gap-2 px-6">
                {verifying ? <Loader2 className="w-4 h-4 animate-spin" /> : <Search className="w-4 h-4" />}
                Verify
              </Button>
            </div>

            {verifyResult && (
              <div className={`p-4 rounded-lg ${verifyResult.valid ? 'bg-green-50 border border-green-200' : 'bg-red-50 border border-red-200'}`}>
                <div className="flex items-center gap-2 mb-3">
                  {verifyResult.valid ? (
                    <CheckCircle className="w-5 h-5 text-green-600" />
                  ) : (
                    <XCircle className="w-5 h-5 text-red-600" />
                  )}
                  <span className={`font-medium ${verifyResult.valid ? 'text-green-800' : 'text-red-800'}`}>
                    {verifyResult.valid ? 'Valid GSTIN' : 'Invalid GSTIN'}
                  </span>
                  {verifyResult.verified_via_api && (
                    <span className="text-xs bg-green-200 text-green-800 px-2 py-0.5 rounded">API Verified</span>
                  )}
                </div>
                
                {verifyResult.valid && (
                  <div className="grid md:grid-cols-2 gap-4 text-sm">
                    <div>
                      <p className="text-slate-500">GSTIN</p>
                      <p className="font-mono font-medium">{verifyResult.gstin}</p>
                    </div>
                    <div>
                      <p className="text-slate-500">State</p>
                      <p className="font-medium">{verifyResult.state_name} ({verifyResult.state_code})</p>
                    </div>
                    {verifyResult.legal_name && (
                      <div>
                        <p className="text-slate-500">Legal Name</p>
                        <p className="font-medium">{verifyResult.legal_name}</p>
                      </div>
                    )}
                    {verifyResult.trade_name && (
                      <div>
                        <p className="text-slate-500">Trade Name</p>
                        <p className="font-medium">{verifyResult.trade_name}</p>
                      </div>
                    )}
                    {verifyResult.status && (
                      <div>
                        <p className="text-slate-500">Status</p>
                        <p className="font-medium">{verifyResult.status}</p>
                      </div>
                    )}
                    {verifyResult.message && (
                      <div className="md:col-span-2">
                        <p className="text-amber-600 text-xs">{verifyResult.message}</p>
                      </div>
                    )}
                  </div>
                )}
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {/* API Settings Tab */}
      {activeTab === 'api' && settings && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Key className="w-5 h-5 text-teal-600" />
              GST API Configuration
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-6">
            <div className="bg-amber-50 p-4 rounded-lg">
              <p className="font-medium text-amber-900">GST Verification API</p>
              <p className="text-sm text-amber-700 mt-1">
                Configure API key to enable real-time GSTIN verification against GST portal.
                Without API, basic format validation will be used.
              </p>
            </div>

            <div className="space-y-4">
              <div>
                <label className="flex items-center gap-3 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={settings.gst_api_enabled}
                    onChange={(e) => setSettings({ ...settings, gst_api_enabled: e.target.checked })}
                    className="w-5 h-5 rounded border-slate-300"
                  />
                  <span className="font-medium text-slate-700">Enable GST API Verification</span>
                </label>
              </div>

              {settings.gst_api_enabled && (
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-1">
                    GST API Key
                  </label>
                  <input
                    type="password"
                    value={settings.gst_api_key || ''}
                    onChange={(e) => setSettings({ ...settings, gst_api_key: e.target.value })}
                    className="w-full h-10 rounded-lg border border-slate-300 px-3 font-mono"
                    placeholder="Enter your GST API key"
                  />
                  <p className="text-xs text-slate-500 mt-1">
                    Get API key from GST Suvidha Provider or authorized API providers
                  </p>
                </div>
              )}
            </div>

            <div className="flex justify-end pt-4 border-t">
              <Button onClick={handleSaveSettings} disabled={saving} className="gap-2">
                {saving ? <Loader2 className="w-4 h-4 animate-spin" /> : <Save className="w-4 h-4" />}
                Save API Settings
              </Button>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
};

export default GSTDashboard;
