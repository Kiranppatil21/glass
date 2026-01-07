import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { toast } from 'sonner';
import { erpApi } from '../../utils/erpApi';
import {
  Gift, Users, CreditCard, TrendingUp, Settings, UserPlus, Search,
  Plus, Minus, Save, Loader2, X, Award, Percent, IndianRupee
} from 'lucide-react';

const RewardsDashboard = () => {
  const [activeTab, setActiveTab] = useState('dashboard');
  const [loading, setLoading] = useState(true);
  const [dashboard, setDashboard] = useState(null);
  const [referrals, setReferrals] = useState([]);
  const [settings, setSettings] = useState(null);
  const [saving, setSaving] = useState(false);
  
  // Adjust credit modal
  const [showAdjustModal, setShowAdjustModal] = useState(false);
  const [searchUserId, setSearchUserId] = useState('');
  const [selectedUser, setSelectedUser] = useState(null);
  const [adjustForm, setAdjustForm] = useState({ amount: '', type: 'credit', reason: '' });

  useEffect(() => {
    fetchData();
  }, [activeTab]);

  const fetchData = async () => {
    setLoading(true);
    try {
      if (activeTab === 'dashboard') {
        const res = await erpApi.rewards.getDashboard();
        setDashboard(res.data);
      } else if (activeTab === 'referrals') {
        const res = await erpApi.rewards.getAllReferrals();
        setReferrals(res.data.referrals || []);
      } else if (activeTab === 'settings') {
        const res = await erpApi.rewards.getSettings();
        setSettings(res.data);
      }
    } catch (error) {
      console.error('Failed to fetch data:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleSaveSettings = async () => {
    setSaving(true);
    try {
      await erpApi.rewards.updateSettings(settings);
      toast.success('Settings saved!');
    } catch (error) {
      toast.error('Failed to save settings');
    } finally {
      setSaving(false);
    }
  };

  const handleSearchUser = async () => {
    if (!searchUserId.trim()) {
      toast.error('Enter user ID or email');
      return;
    }
    try {
      const res = await erpApi.rewards.getUserBalance(searchUserId);
      setSelectedUser(res.data);
    } catch (error) {
      toast.error('User not found');
      setSelectedUser(null);
    }
  };

  const handleAdjustCredit = async () => {
    if (!selectedUser || !adjustForm.amount || !adjustForm.reason) {
      toast.error('Fill all fields');
      return;
    }
    setSaving(true);
    try {
      await erpApi.rewards.adjustCredit({
        user_id: selectedUser.user?.id || searchUserId,
        amount: parseFloat(adjustForm.amount),
        type: adjustForm.type,
        reason: adjustForm.reason
      });
      toast.success(`Credit ${adjustForm.type === 'credit' ? 'added' : 'deducted'} successfully`);
      setShowAdjustModal(false);
      setSelectedUser(null);
      setAdjustForm({ amount: '', type: 'credit', reason: '' });
      fetchData();
    } catch (error) {
      toast.error(error.response?.data?.detail || 'Failed to adjust credit');
    } finally {
      setSaving(false);
    }
  };

  const tabs = [
    { id: 'dashboard', label: 'Dashboard', icon: TrendingUp },
    { id: 'referrals', label: 'Referrals', icon: UserPlus },
    { id: 'credits', label: 'Manage Credits', icon: CreditCard },
    { id: 'settings', label: 'Settings', icon: Settings },
  ];

  return (
    <div className="space-y-6" data-testid="rewards-dashboard">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-slate-900 flex items-center gap-2">
          <Gift className="w-7 h-7 text-purple-600" />
          Rewards & Referrals
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
                ? 'bg-purple-100 text-purple-800'
                : 'text-slate-600 hover:bg-slate-100'
            }`}
          >
            <tab.icon className="w-4 h-4" />
            {tab.label}
          </button>
        ))}
      </div>

      {loading ? (
        <div className="flex items-center justify-center h-64">
          <Loader2 className="w-8 h-8 animate-spin text-purple-600" />
        </div>
      ) : (
        <>
          {/* Dashboard Tab */}
          {activeTab === 'dashboard' && dashboard && (
            <div className="space-y-6">
              <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-4">
                <Card className="bg-gradient-to-br from-purple-500 to-purple-600 text-white">
                  <CardContent className="p-4">
                    <div className="flex items-center justify-between">
                      <div>
                        <p className="text-purple-100 text-sm">Total Credit Issued</p>
                        <p className="text-2xl font-bold">₹{dashboard.credit_stats.total_issued?.toLocaleString()}</p>
                      </div>
                      <CreditCard className="w-10 h-10 text-purple-200" />
                    </div>
                  </CardContent>
                </Card>

                <Card className="bg-gradient-to-br from-green-500 to-green-600 text-white">
                  <CardContent className="p-4">
                    <div className="flex items-center justify-between">
                      <div>
                        <p className="text-green-100 text-sm">Credit Redeemed</p>
                        <p className="text-2xl font-bold">₹{dashboard.credit_stats.total_redeemed?.toLocaleString()}</p>
                      </div>
                      <IndianRupee className="w-10 h-10 text-green-200" />
                    </div>
                  </CardContent>
                </Card>

                <Card className="bg-gradient-to-br from-amber-500 to-amber-600 text-white">
                  <CardContent className="p-4">
                    <div className="flex items-center justify-between">
                      <div>
                        <p className="text-amber-100 text-sm">Outstanding Credit</p>
                        <p className="text-2xl font-bold">₹{dashboard.credit_stats.outstanding?.toLocaleString()}</p>
                      </div>
                      <Award className="w-10 h-10 text-amber-200" />
                    </div>
                  </CardContent>
                </Card>

                <Card className="bg-gradient-to-br from-blue-500 to-blue-600 text-white">
                  <CardContent className="p-4">
                    <div className="flex items-center justify-between">
                      <div>
                        <p className="text-blue-100 text-sm">Total Referrals</p>
                        <p className="text-2xl font-bold">{dashboard.referral_stats.total}</p>
                      </div>
                      <Users className="w-10 h-10 text-blue-200" />
                    </div>
                    <p className="text-xs text-blue-100 mt-2">
                      {dashboard.referral_stats.completed} completed ({dashboard.referral_stats.conversion_rate}%)
                    </p>
                  </CardContent>
                </Card>
              </div>

              {/* Top Referrers */}
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Award className="w-5 h-5 text-amber-500" />
                    Top Referrers
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  {dashboard.top_referrers?.length === 0 ? (
                    <p className="text-slate-500 text-center py-4">No referrals yet</p>
                  ) : (
                    <div className="space-y-3">
                      {dashboard.top_referrers?.map((r, idx) => (
                        <div key={r._id} className="flex items-center justify-between p-3 bg-slate-50 rounded-lg">
                          <div className="flex items-center gap-3">
                            <span className={`w-8 h-8 rounded-full flex items-center justify-center font-bold ${
                              idx === 0 ? 'bg-amber-100 text-amber-700' :
                              idx === 1 ? 'bg-slate-200 text-slate-700' :
                              idx === 2 ? 'bg-orange-100 text-orange-700' :
                              'bg-slate-100 text-slate-600'
                            }`}>
                              {idx + 1}
                            </span>
                            <span className="font-medium">{r.name}</span>
                          </div>
                          <span className="font-bold text-purple-600">{r.count} referrals</span>
                        </div>
                      ))}
                    </div>
                  )}
                </CardContent>
              </Card>
            </div>
          )}

          {/* Referrals Tab */}
          {activeTab === 'referrals' && (
            <Card>
              <CardContent className="p-0">
                {referrals.length === 0 ? (
                  <div className="p-8 text-center text-slate-500">
                    No referrals yet
                  </div>
                ) : (
                  <div className="overflow-x-auto">
                    <table className="w-full">
                      <thead className="bg-slate-50">
                        <tr>
                          <th className="px-4 py-3 text-left text-xs font-medium text-slate-500">Referrer</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-slate-500">Referee</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-slate-500">Code</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-slate-500">Status</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-slate-500">Reward</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-slate-500">Date</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y">
                        {referrals.map((r) => (
                          <tr key={r.id} className="hover:bg-slate-50">
                            <td className="px-4 py-3 font-medium">{r.referrer_name}</td>
                            <td className="px-4 py-3">{r.referee_name}</td>
                            <td className="px-4 py-3 font-mono text-sm">{r.referral_code}</td>
                            <td className="px-4 py-3">
                              <span className={`px-2 py-1 rounded text-xs font-medium ${
                                r.status === 'completed' ? 'bg-green-100 text-green-800' : 'bg-amber-100 text-amber-800'
                              }`}>
                                {r.status}
                              </span>
                            </td>
                            <td className="px-4 py-3">
                              {r.reward_amount ? `₹${r.reward_amount}` : '-'}
                            </td>
                            <td className="px-4 py-3 text-sm text-slate-500">
                              {new Date(r.created_at).toLocaleDateString('en-IN')}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </CardContent>
            </Card>
          )}

          {/* Manage Credits Tab */}
          {activeTab === 'credits' && (
            <div className="space-y-6">
              <Card>
                <CardHeader>
                  <CardTitle>Search User & Adjust Credit</CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="flex gap-3">
                    <input
                      type="text"
                      value={searchUserId}
                      onChange={(e) => setSearchUserId(e.target.value)}
                      placeholder="Enter user ID or email"
                      className="flex-1 h-10 rounded-lg border border-slate-300 px-3"
                    />
                    <Button onClick={handleSearchUser} className="gap-2">
                      <Search className="w-4 h-4" /> Search
                    </Button>
                  </div>

                  {selectedUser && (
                    <div className="bg-slate-50 p-4 rounded-lg space-y-4">
                      <div className="flex items-center justify-between">
                        <div>
                          <p className="font-bold text-lg">{selectedUser.user?.name}</p>
                          <p className="text-sm text-slate-500">{selectedUser.user?.email}</p>
                        </div>
                        <div className="text-right">
                          <p className="text-2xl font-bold text-purple-600">₹{selectedUser.credit_balance?.toLocaleString()}</p>
                          <p className="text-sm text-slate-500">Credit Balance</p>
                          <p className="text-sm text-amber-600">{selectedUser.points_balance} points</p>
                        </div>
                      </div>

                      <div className="grid md:grid-cols-3 gap-4 pt-4 border-t">
                        <div>
                          <label className="block text-sm font-medium mb-1">Amount (₹)</label>
                          <input
                            type="number"
                            value={adjustForm.amount}
                            onChange={(e) => setAdjustForm({ ...adjustForm, amount: e.target.value })}
                            className="w-full h-10 rounded-lg border border-slate-300 px-3"
                            placeholder="Enter amount"
                          />
                        </div>
                        <div>
                          <label className="block text-sm font-medium mb-1">Type</label>
                          <select
                            value={adjustForm.type}
                            onChange={(e) => setAdjustForm({ ...adjustForm, type: e.target.value })}
                            className="w-full h-10 rounded-lg border border-slate-300 px-3"
                          >
                            <option value="credit">Add Credit</option>
                            <option value="debit">Deduct Credit</option>
                          </select>
                        </div>
                        <div>
                          <label className="block text-sm font-medium mb-1">Reason</label>
                          <input
                            type="text"
                            value={adjustForm.reason}
                            onChange={(e) => setAdjustForm({ ...adjustForm, reason: e.target.value })}
                            className="w-full h-10 rounded-lg border border-slate-300 px-3"
                            placeholder="Reason for adjustment"
                          />
                        </div>
                      </div>

                      <div className="flex justify-end">
                        <Button 
                          onClick={handleAdjustCredit} 
                          disabled={saving}
                          className={adjustForm.type === 'credit' ? 'bg-green-600 hover:bg-green-700' : 'bg-red-600 hover:bg-red-700'}
                        >
                          {saving ? <Loader2 className="w-4 h-4 animate-spin mr-2" /> : adjustForm.type === 'credit' ? <Plus className="w-4 h-4 mr-2" /> : <Minus className="w-4 h-4 mr-2" />}
                          {adjustForm.type === 'credit' ? 'Add Credit' : 'Deduct Credit'}
                        </Button>
                      </div>

                      {/* Recent Transactions */}
                      {selectedUser.transactions?.length > 0 && (
                        <div className="pt-4 border-t">
                          <p className="font-medium mb-2">Recent Transactions</p>
                          <div className="space-y-2 max-h-48 overflow-y-auto">
                            {selectedUser.transactions.map((t) => (
                              <div key={t.id} className="flex items-center justify-between text-sm p-2 bg-white rounded">
                                <div>
                                  <p className="font-medium">{t.description}</p>
                                  <p className="text-xs text-slate-500">{new Date(t.created_at).toLocaleString('en-IN')}</p>
                                </div>
                                <span className={`font-bold ${t.type.includes('credit') || t.type === 'referral_bonus' || t.type === 'order_reward' ? 'text-green-600' : 'text-red-600'}`}>
                                  {t.type.includes('credit') || t.type === 'referral_bonus' || t.type === 'order_reward' ? '+' : '-'}₹{t.amount}
                                </span>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                    </div>
                  )}
                </CardContent>
              </Card>
            </div>
          )}

          {/* Settings Tab */}
          {activeTab === 'settings' && settings && (
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Settings className="w-5 h-5" />
                  Referral & Rewards Settings
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-6">
                <div className="grid md:grid-cols-2 gap-6">
                  <div className="space-y-4">
                    <h3 className="font-medium text-slate-900 flex items-center gap-2">
                      <UserPlus className="w-4 h-4 text-purple-600" />
                      Referral Settings
                    </h3>
                    
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-1">
                        Referrer Reward (% of first order)
                      </label>
                      <div className="flex items-center gap-2">
                        <input
                          type="number"
                          value={settings.referrer_reward_percent}
                          onChange={(e) => setSettings({ ...settings, referrer_reward_percent: parseFloat(e.target.value) })}
                          className="w-full h-10 rounded-lg border border-slate-300 px-3"
                        />
                        <span className="text-slate-500">%</span>
                      </div>
                      <p className="text-xs text-slate-500 mt-1">Credit given to referrer when referee places first order</p>
                    </div>
                    
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-1">
                        Referee Discount (% on first order)
                      </label>
                      <div className="flex items-center gap-2">
                        <input
                          type="number"
                          value={settings.referee_discount_percent}
                          onChange={(e) => setSettings({ ...settings, referee_discount_percent: parseFloat(e.target.value) })}
                          className="w-full h-10 rounded-lg border border-slate-300 px-3"
                        />
                        <span className="text-slate-500">%</span>
                      </div>
                      <p className="text-xs text-slate-500 mt-1">Discount for new customers using referral code</p>
                    </div>
                    
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-1">
                        Min Order for Referral (₹)
                      </label>
                      <input
                        type="number"
                        value={settings.min_order_for_referral}
                        onChange={(e) => setSettings({ ...settings, min_order_for_referral: parseFloat(e.target.value) })}
                        className="w-full h-10 rounded-lg border border-slate-300 px-3"
                      />
                      <p className="text-xs text-slate-500 mt-1">Minimum order value to qualify for referral reward</p>
                    </div>
                    
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-1">
                        Max Referral Reward (₹)
                      </label>
                      <input
                        type="number"
                        value={settings.max_referral_reward}
                        onChange={(e) => setSettings({ ...settings, max_referral_reward: parseFloat(e.target.value) })}
                        className="w-full h-10 rounded-lg border border-slate-300 px-3"
                      />
                      <p className="text-xs text-slate-500 mt-1">Maximum reward per referral</p>
                    </div>
                  </div>

                  <div className="space-y-4">
                    <h3 className="font-medium text-slate-900 flex items-center gap-2">
                      <Award className="w-4 h-4 text-amber-600" />
                      Reward Points Settings
                    </h3>
                    
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-1">
                        Points per ₹1 spent
                      </label>
                      <input
                        type="number"
                        value={settings.reward_points_per_rupee}
                        onChange={(e) => setSettings({ ...settings, reward_points_per_rupee: parseFloat(e.target.value) })}
                        className="w-full h-10 rounded-lg border border-slate-300 px-3"
                      />
                      <p className="text-xs text-slate-500 mt-1">Points earned on each order</p>
                    </div>
                    
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-1">
                        Points to ₹ Ratio
                      </label>
                      <input
                        type="number"
                        value={settings.points_to_rupee_ratio}
                        onChange={(e) => setSettings({ ...settings, points_to_rupee_ratio: parseFloat(e.target.value) })}
                        className="w-full h-10 rounded-lg border border-slate-300 px-3"
                      />
                      <p className="text-xs text-slate-500 mt-1">How many points = ₹1 (e.g., 10 points = ₹1)</p>
                    </div>

                    <div className="bg-purple-50 p-4 rounded-lg">
                      <p className="font-medium text-purple-900">Example Calculation</p>
                      <p className="text-sm text-purple-700 mt-1">
                        ₹10,000 order = {10000 * settings.reward_points_per_rupee} points = ₹{(10000 * settings.reward_points_per_rupee / settings.points_to_rupee_ratio).toFixed(0)} value
                      </p>
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
        </>
      )}
    </div>
  );
};

export default RewardsDashboard;
