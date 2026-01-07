import React, { useState, useEffect } from 'react';
import { Card, CardContent } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { 
  Wallet, Gift, Users, Settings, TrendingUp, TrendingDown,
  Copy, CheckCircle, AlertCircle, RefreshCw, CreditCard, Percent,
  DollarSign, UserPlus, Clock, ChevronRight
} from 'lucide-react';
import { toast } from 'sonner';
import erpApi from '../../utils/erpApi';

const WalletDashboard = () => {
  const [activeTab, setActiveTab] = useState('overview');
  const [stats, setStats] = useState(null);
  const [settings, setSettings] = useState(null);
  const [wallets, setWallets] = useState([]);
  const [loading, setLoading] = useState(true);
  const [showCreditModal, setShowCreditModal] = useState(false);
  const [creditData, setCreditData] = useState({ user_id: '', amount: '', reason: '' });

  useEffect(() => {
    fetchData();
  }, []);

  const fetchData = async () => {
    try {
      const [statsRes, settingsRes, walletsRes] = await Promise.all([
        erpApi.wallet.getStats(),
        erpApi.wallet.getSettings(),
        erpApi.wallet.getAllWallets({ limit: 50 })
      ]);
      setStats(statsRes.data);
      setSettings(settingsRes.data);
      setWallets(walletsRes.data);
    } catch (error) {
      console.error('Failed to fetch wallet data:', error);
      toast.error('Failed to load wallet data');
    } finally {
      setLoading(false);
    }
  };

  const handleUpdateSettings = async (field, value) => {
    try {
      await erpApi.wallet.updateSettings({ [field]: value });
      setSettings({ ...settings, [field]: value });
      toast.success('Settings updated');
    } catch (error) {
      toast.error('Failed to update settings');
    }
  };

  const handleCreditWallet = async (e) => {
    e.preventDefault();
    try {
      await erpApi.wallet.creditWallet(
        creditData.user_id,
        parseFloat(creditData.amount),
        creditData.reason
      );
      toast.success('Wallet credited successfully');
      setShowCreditModal(false);
      setCreditData({ user_id: '', amount: '', reason: '' });
      fetchData();
    } catch (error) {
      toast.error('Failed to credit wallet');
    }
  };

  const tabs = [
    { id: 'overview', label: 'Overview', icon: TrendingUp },
    { id: 'users', label: 'User Wallets', icon: Users },
    { id: 'settings', label: 'Settings', icon: Settings },
  ];

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-slate-50">
        <RefreshCw className="w-8 h-8 animate-spin text-teal-600" />
      </div>
    );
  }

  return (
    <div className="min-h-screen py-8 bg-slate-50" data-testid="wallet-dashboard">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        {/* Header */}
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-3xl font-bold text-slate-900">Refer & Earn System</h1>
            <p className="text-slate-600">Manage wallet, referrals, and rewards</p>
          </div>
          <Button
            onClick={() => setShowCreditModal(true)}
            className="bg-teal-600 hover:bg-teal-700"
            data-testid="credit-wallet-btn"
          >
            <CreditCard className="w-4 h-4 mr-2" />
            Credit Wallet
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
              </button>
            );
          })}
        </div>

        {/* Overview Tab */}
        {activeTab === 'overview' && stats && (
          <>
            {/* Stats Cards */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
              <Card className="bg-gradient-to-br from-teal-500 to-teal-600 text-white">
                <CardContent className="p-5">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-teal-100 text-sm">Total Wallets</p>
                      <p className="text-3xl font-bold">{stats.total_wallets}</p>
                    </div>
                    <Wallet className="w-10 h-10 opacity-80" />
                  </div>
                </CardContent>
              </Card>

              <Card className="bg-gradient-to-br from-purple-500 to-purple-600 text-white">
                <CardContent className="p-5">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-purple-100 text-sm">Total Balance</p>
                      <p className="text-3xl font-bold">₹{stats.total_balance_outstanding?.toLocaleString()}</p>
                    </div>
                    <DollarSign className="w-10 h-10 opacity-80" />
                  </div>
                </CardContent>
              </Card>

              <Card className="bg-gradient-to-br from-green-500 to-green-600 text-white">
                <CardContent className="p-5">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-green-100 text-sm">Rewards Given</p>
                      <p className="text-3xl font-bold">₹{stats.total_rewards_given?.toLocaleString()}</p>
                    </div>
                    <Gift className="w-10 h-10 opacity-80" />
                  </div>
                </CardContent>
              </Card>

              <Card className="bg-gradient-to-br from-blue-500 to-blue-600 text-white">
                <CardContent className="p-5">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-blue-100 text-sm">Total Referrals</p>
                      <p className="text-3xl font-bold">{stats.total_referrals}</p>
                    </div>
                    <UserPlus className="w-10 h-10 opacity-80" />
                  </div>
                </CardContent>
              </Card>
            </div>

            {/* Recent Transactions */}
            <Card>
              <CardContent className="p-6">
                <h3 className="text-lg font-bold text-slate-900 mb-4">Recent Transactions</h3>
                <div className="space-y-3">
                  {stats.recent_transactions?.length > 0 ? stats.recent_transactions.map((txn) => (
                    <div key={txn.id} className="flex items-center justify-between p-3 bg-slate-50 rounded-lg">
                      <div className="flex items-center gap-3">
                        <div className={`w-10 h-10 rounded-full flex items-center justify-center ${
                          txn.type === 'credit' ? 'bg-green-100' : 'bg-red-100'
                        }`}>
                          {txn.type === 'credit' ? (
                            <TrendingUp className="w-5 h-5 text-green-600" />
                          ) : (
                            <TrendingDown className="w-5 h-5 text-red-600" />
                          )}
                        </div>
                        <div>
                          <p className="font-medium text-slate-900">{txn.description}</p>
                          <p className="text-xs text-slate-500">{txn.category} • {new Date(txn.created_at).toLocaleDateString()}</p>
                        </div>
                      </div>
                      <div className="text-right">
                        <p className={`font-bold ${txn.type === 'credit' ? 'text-green-600' : 'text-red-600'}`}>
                          {txn.type === 'credit' ? '+' : '-'}₹{txn.amount?.toLocaleString()}
                        </p>
                        <p className="text-xs text-slate-500">Bal: ₹{txn.balance_after?.toLocaleString()}</p>
                      </div>
                    </div>
                  )) : (
                    <div className="text-center py-8 text-slate-400">
                      <Clock className="w-12 h-12 mx-auto mb-2 opacity-50" />
                      <p>No transactions yet</p>
                    </div>
                  )}
                </div>
              </CardContent>
            </Card>
          </>
        )}

        {/* User Wallets Tab */}
        {activeTab === 'users' && (
          <Card>
            <CardContent className="p-6">
              <h3 className="text-lg font-bold text-slate-900 mb-4">User Wallets ({wallets.length})</h3>
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead>
                    <tr className="border-b border-slate-200">
                      <th className="text-left py-3 px-4 text-sm font-medium text-slate-600">User</th>
                      <th className="text-left py-3 px-4 text-sm font-medium text-slate-600">Referral Code</th>
                      <th className="text-right py-3 px-4 text-sm font-medium text-slate-600">Balance</th>
                      <th className="text-right py-3 px-4 text-sm font-medium text-slate-600">Earned</th>
                      <th className="text-right py-3 px-4 text-sm font-medium text-slate-600">Spent</th>
                      <th className="text-center py-3 px-4 text-sm font-medium text-slate-600">Referrals</th>
                    </tr>
                  </thead>
                  <tbody>
                    {wallets.map((wallet) => (
                      <tr key={wallet.id} className="border-b border-slate-100 hover:bg-slate-50">
                        <td className="py-3 px-4">
                          <div>
                            <p className="font-medium text-slate-900">{wallet.user_name || 'Unknown'}</p>
                            <p className="text-xs text-slate-500">{wallet.user_email}</p>
                          </div>
                        </td>
                        <td className="py-3 px-4">
                          <code className="bg-slate-100 px-2 py-1 rounded text-sm">{wallet.referral_code}</code>
                        </td>
                        <td className="py-3 px-4 text-right font-bold text-teal-600">
                          ₹{wallet.balance?.toLocaleString()}
                        </td>
                        <td className="py-3 px-4 text-right text-green-600">
                          ₹{wallet.total_earned?.toLocaleString()}
                        </td>
                        <td className="py-3 px-4 text-right text-slate-600">
                          ₹{wallet.total_spent?.toLocaleString()}
                        </td>
                        <td className="py-3 px-4 text-center">
                          <span className="bg-blue-100 text-blue-700 px-2 py-1 rounded-full text-sm">
                            {wallet.referral_count || 0}
                          </span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {wallets.length === 0 && (
                  <div className="text-center py-12 text-slate-400">
                    <Users className="w-12 h-12 mx-auto mb-2 opacity-50" />
                    <p>No user wallets yet</p>
                  </div>
                )}
              </div>
            </CardContent>
          </Card>
        )}

        {/* Settings Tab */}
        {activeTab === 'settings' && settings && (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Referral Settings */}
            <Card>
              <CardContent className="p-6">
                <div className="flex items-center gap-3 mb-6">
                  <div className="w-10 h-10 bg-purple-100 rounded-lg flex items-center justify-center">
                    <UserPlus className="w-5 h-5 text-purple-600" />
                  </div>
                  <div>
                    <h3 className="font-bold text-slate-900">Referral Settings</h3>
                    <p className="text-sm text-slate-500">Configure referral bonuses</p>
                  </div>
                </div>

                <div className="space-y-4">
                  <div className="flex items-center justify-between p-3 bg-slate-50 rounded-lg">
                    <span className="text-slate-700">Referral Program</span>
                    <button
                      onClick={() => handleUpdateSettings('referral_enabled', !settings.referral_enabled)}
                      className={`w-12 h-6 rounded-full transition-colors ${
                        settings.referral_enabled ? 'bg-teal-600' : 'bg-slate-300'
                      }`}
                    >
                      <div className={`w-5 h-5 bg-white rounded-full shadow transform transition-transform ${
                        settings.referral_enabled ? 'translate-x-6' : 'translate-x-0.5'
                      }`} />
                    </button>
                  </div>

                  <div className="p-3 bg-slate-50 rounded-lg">
                    <label className="block text-sm text-slate-600 mb-2">Flat Bonus (₹)</label>
                    <input
                      type="number"
                      value={settings.referral_flat_bonus}
                      onChange={(e) => handleUpdateSettings('referral_flat_bonus', parseFloat(e.target.value))}
                      className="w-full h-10 rounded border border-slate-300 px-3"
                    />
                  </div>

                  <div className="p-3 bg-slate-50 rounded-lg">
                    <label className="block text-sm text-slate-600 mb-2">Percentage Bonus (%)</label>
                    <input
                      type="number"
                      value={settings.referral_percentage_bonus}
                      onChange={(e) => handleUpdateSettings('referral_percentage_bonus', parseFloat(e.target.value))}
                      className="w-full h-10 rounded border border-slate-300 px-3"
                    />
                  </div>

                  <div className="p-3 bg-slate-50 rounded-lg">
                    <label className="block text-sm text-slate-600 mb-2">Bonus Cap (₹)</label>
                    <input
                      type="number"
                      value={settings.referral_bonus_cap}
                      onChange={(e) => handleUpdateSettings('referral_bonus_cap', parseFloat(e.target.value))}
                      className="w-full h-10 rounded border border-slate-300 px-3"
                    />
                  </div>

                  <div className="p-3 bg-slate-50 rounded-lg">
                    <label className="block text-sm text-slate-600 mb-2">New User Bonus (₹)</label>
                    <input
                      type="number"
                      value={settings.referee_bonus_amount}
                      onChange={(e) => handleUpdateSettings('referee_bonus_amount', parseFloat(e.target.value))}
                      className="w-full h-10 rounded border border-slate-300 px-3"
                    />
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Wallet Usage Settings */}
            <Card>
              <CardContent className="p-6">
                <div className="flex items-center gap-3 mb-6">
                  <div className="w-10 h-10 bg-teal-100 rounded-lg flex items-center justify-center">
                    <Wallet className="w-5 h-5 text-teal-600" />
                  </div>
                  <div>
                    <h3 className="font-bold text-slate-900">Wallet Usage Settings</h3>
                    <p className="text-sm text-slate-500">Configure wallet usage limits</p>
                  </div>
                </div>

                <div className="space-y-4">
                  <div className="flex items-center justify-between p-3 bg-slate-50 rounded-lg">
                    <span className="text-slate-700">Wallet Usage</span>
                    <button
                      onClick={() => handleUpdateSettings('wallet_enabled', !settings.wallet_enabled)}
                      className={`w-12 h-6 rounded-full transition-colors ${
                        settings.wallet_enabled ? 'bg-teal-600' : 'bg-slate-300'
                      }`}
                    >
                      <div className={`w-5 h-5 bg-white rounded-full shadow transform transition-transform ${
                        settings.wallet_enabled ? 'translate-x-6' : 'translate-x-0.5'
                      }`} />
                    </button>
                  </div>

                  <div className="p-3 bg-slate-50 rounded-lg">
                    <label className="block text-sm text-slate-600 mb-2">Usage Type</label>
                    <select
                      value={settings.wallet_usage_type}
                      onChange={(e) => handleUpdateSettings('wallet_usage_type', e.target.value)}
                      className="w-full h-10 rounded border border-slate-300 px-3"
                    >
                      <option value="percentage">Percentage of Order</option>
                      <option value="fixed">Fixed Amount</option>
                    </select>
                  </div>

                  <div className="p-3 bg-slate-50 rounded-lg">
                    <label className="block text-sm text-slate-600 mb-2">
                      Max {settings.wallet_usage_type === 'percentage' ? 'Percentage (%)' : 'Amount (₹)'}
                    </label>
                    <input
                      type="number"
                      value={settings.wallet_usage_type === 'percentage' ? settings.wallet_max_percentage : settings.wallet_max_fixed}
                      onChange={(e) => handleUpdateSettings(
                        settings.wallet_usage_type === 'percentage' ? 'wallet_max_percentage' : 'wallet_max_fixed',
                        parseFloat(e.target.value)
                      )}
                      className="w-full h-10 rounded border border-slate-300 px-3"
                    />
                  </div>

                  <div className="p-3 bg-slate-50 rounded-lg">
                    <label className="block text-sm text-slate-600 mb-2">Min Order for Wallet (₹)</label>
                    <input
                      type="number"
                      value={settings.min_order_for_wallet}
                      onChange={(e) => handleUpdateSettings('min_order_for_wallet', parseFloat(e.target.value))}
                      className="w-full h-10 rounded border border-slate-300 px-3"
                    />
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Cashback Settings */}
            <Card className="lg:col-span-2">
              <CardContent className="p-6">
                <div className="flex items-center gap-3 mb-6">
                  <div className="w-10 h-10 bg-green-100 rounded-lg flex items-center justify-center">
                    <Percent className="w-5 h-5 text-green-600" />
                  </div>
                  <div>
                    <h3 className="font-bold text-slate-900">Cashback Settings</h3>
                    <p className="text-sm text-slate-500">Configure order cashback rewards</p>
                  </div>
                </div>

                <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                  <div className="flex items-center justify-between p-3 bg-slate-50 rounded-lg">
                    <span className="text-slate-700">Cashback</span>
                    <button
                      onClick={() => handleUpdateSettings('cashback_enabled', !settings.cashback_enabled)}
                      className={`w-12 h-6 rounded-full transition-colors ${
                        settings.cashback_enabled ? 'bg-teal-600' : 'bg-slate-300'
                      }`}
                    >
                      <div className={`w-5 h-5 bg-white rounded-full shadow transform transition-transform ${
                        settings.cashback_enabled ? 'translate-x-6' : 'translate-x-0.5'
                      }`} />
                    </button>
                  </div>

                  <div className="p-3 bg-slate-50 rounded-lg">
                    <label className="block text-sm text-slate-600 mb-2">Cashback %</label>
                    <input
                      type="number"
                      value={settings.cashback_percentage}
                      onChange={(e) => handleUpdateSettings('cashback_percentage', parseFloat(e.target.value))}
                      className="w-full h-10 rounded border border-slate-300 px-3"
                    />
                  </div>

                  <div className="p-3 bg-slate-50 rounded-lg">
                    <label className="block text-sm text-slate-600 mb-2">Max Cashback (₹)</label>
                    <input
                      type="number"
                      value={settings.cashback_cap}
                      onChange={(e) => handleUpdateSettings('cashback_cap', parseFloat(e.target.value))}
                      className="w-full h-10 rounded border border-slate-300 px-3"
                    />
                  </div>

                  <div className="p-3 bg-slate-50 rounded-lg">
                    <label className="block text-sm text-slate-600 mb-2">Min Order (₹)</label>
                    <input
                      type="number"
                      value={settings.min_order_for_cashback}
                      onChange={(e) => handleUpdateSettings('min_order_for_cashback', parseFloat(e.target.value))}
                      className="w-full h-10 rounded border border-slate-300 px-3"
                    />
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>
        )}

        {/* Credit Wallet Modal */}
        {showCreditModal && (
          <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
            <Card className="max-w-md w-full">
              <CardContent className="p-6">
                <h2 className="text-xl font-bold text-slate-900 mb-4">Credit User Wallet</h2>
                <form onSubmit={handleCreditWallet} className="space-y-4">
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-2">User ID</label>
                    <select
                      value={creditData.user_id}
                      onChange={(e) => setCreditData({ ...creditData, user_id: e.target.value })}
                      className="w-full h-10 rounded border border-slate-300 px-3"
                      required
                    >
                      <option value="">Select User</option>
                      {wallets.map((w) => (
                        <option key={w.user_id} value={w.user_id}>
                          {w.user_name || w.user_email || w.user_id.slice(0, 8)}
                        </option>
                      ))}
                    </select>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-2">Amount (₹)</label>
                    <input
                      type="number"
                      value={creditData.amount}
                      onChange={(e) => setCreditData({ ...creditData, amount: e.target.value })}
                      className="w-full h-10 rounded border border-slate-300 px-3"
                      min="1"
                      required
                    />
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-2">Reason</label>
                    <input
                      type="text"
                      value={creditData.reason}
                      onChange={(e) => setCreditData({ ...creditData, reason: e.target.value })}
                      className="w-full h-10 rounded border border-slate-300 px-3"
                      placeholder="e.g., Promotional bonus"
                      required
                    />
                  </div>

                  <div className="flex gap-3 pt-2">
                    <Button type="submit" className="flex-1 bg-teal-600 hover:bg-teal-700">
                      Credit Wallet
                    </Button>
                    <Button
                      type="button"
                      variant="outline"
                      className="flex-1"
                      onClick={() => setShowCreditModal(false)}
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

export default WalletDashboard;
