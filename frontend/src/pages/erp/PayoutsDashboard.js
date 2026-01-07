import React, { useState, useEffect } from 'react';
import { Card, CardContent } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { Input } from '../../components/ui/input';
import { 
  CreditCard, Users, CheckCircle, Clock, AlertTriangle,
  Loader, Send, Building2, RefreshCw, DollarSign, X, Plus
} from 'lucide-react';
import { toast } from 'sonner';
import { erpApi } from '../../utils/erpApi';

const PayoutsDashboard = () => {
  const [dashboard, setDashboard] = useState(null);
  const [loading, setLoading] = useState(true);
  const [employees, setEmployees] = useState([]);
  const [fundAccounts, setFundAccounts] = useState([]);
  const [salaries, setSalaries] = useState([]);
  const [activeTab, setActiveTab] = useState('overview');
  const [processing, setProcessing] = useState(false);
  const [showBankModal, setShowBankModal] = useState(false);
  const [selectedEmployee, setSelectedEmployee] = useState(null);
  const [bankForm, setBankForm] = useState({
    account_holder_name: '',
    account_number: '',
    ifsc_code: ''
  });

  const currentMonth = new Date().toISOString().slice(0, 7);

  useEffect(() => {
    fetchAllData();
  }, []);

  const fetchAllData = async () => {
    try {
      const [dashRes, empRes, fundRes, salRes] = await Promise.all([
        erpApi.payouts.getDashboard(),
        erpApi.hr.getEmployees(),
        erpApi.payouts.getFundAccounts(),
        erpApi.hr.getSalaries({ month: currentMonth })
      ]);
      
      setDashboard(dashRes.data);
      setEmployees(empRes.data);
      setFundAccounts(fundRes.data);
      setSalaries(salRes.data);
    } catch (error) {
      console.error('Failed to fetch data:', error);
      toast.error('Failed to load payout data');
    } finally {
      setLoading(false);
    }
  };

  const handleAddBankAccount = async () => {
    if (!selectedEmployee || !bankForm.account_number || !bankForm.ifsc_code) {
      toast.error('Please fill all required fields');
      return;
    }

    setProcessing(true);
    try {
      await erpApi.payouts.createFundAccount({
        employee_id: selectedEmployee.id,
        ...bankForm
      });
      toast.success('Bank account linked successfully');
      setShowBankModal(false);
      setBankForm({ account_holder_name: '', account_number: '', ifsc_code: '' });
      setSelectedEmployee(null);
      fetchAllData();
    } catch (error) {
      toast.error(error.response?.data?.detail || 'Failed to link bank account');
    } finally {
      setProcessing(false);
    }
  };

  const handleProcessSinglePayout = async (salary) => {
    setProcessing(true);
    try {
      const response = await erpApi.payouts.processSalaryPayout({
        salary_id: salary.id,
        mode: 'IMPS'
      });
      toast.success(`Payout initiated: ₹${salary.net_salary.toLocaleString()}`);
      fetchAllData();
    } catch (error) {
      toast.error(error.response?.data?.detail || 'Payout failed');
    } finally {
      setProcessing(false);
    }
  };

  const handleBulkPayout = async () => {
    if (!window.confirm(`Process all approved salaries for ${currentMonth}?`)) {
      return;
    }

    setProcessing(true);
    try {
      const response = await erpApi.payouts.bulkProcessPayouts({
        month: currentMonth,
        mode: 'IMPS'
      });
      const { summary } = response.data;
      toast.success(`Bulk payout completed: ${summary.total_processed} processed, ${summary.total_failed} failed`);
      fetchAllData();
    } catch (error) {
      toast.error(error.response?.data?.detail || 'Bulk payout failed');
    } finally {
      setProcessing(false);
    }
  };

  const formatCurrency = (amount) => {
    return new Intl.NumberFormat('en-IN', {
      style: 'currency',
      currency: 'INR',
      minimumFractionDigits: 0
    }).format(amount);
  };

  const getStatusBadge = (status) => {
    const badges = {
      pending: 'bg-yellow-100 text-yellow-800',
      approved: 'bg-blue-100 text-blue-800',
      processing: 'bg-purple-100 text-purple-800',
      paid: 'bg-green-100 text-green-800',
      failed: 'bg-red-100 text-red-800'
    };
    return badges[status] || 'bg-gray-100 text-gray-800';
  };

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-slate-50">
        <Loader className="w-8 h-8 animate-spin text-teal-600" />
      </div>
    );
  }

  const employeesWithoutBank = employees.filter(e => !e.bank_linked);
  const approvedSalaries = salaries.filter(s => s.payment_status === 'approved');

  return (
    <div className="min-h-screen py-8 bg-slate-50" data-testid="payouts-dashboard">
      <div className="max-w-[1400px] mx-auto px-4 sm:px-6 lg:px-8">
        {/* Header */}
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-2xl font-bold text-slate-900">Salary Payouts</h1>
            <p className="text-slate-600">Manage salary disbursements via Razorpay</p>
          </div>
          <div className="flex gap-3">
            <Button
              variant="outline"
              onClick={fetchAllData}
              className="flex items-center gap-2"
            >
              <RefreshCw className="w-4 h-4" />
              Refresh
            </Button>
            <Button
              onClick={handleBulkPayout}
              disabled={processing || approvedSalaries.length === 0}
              className="flex items-center gap-2 bg-teal-600 hover:bg-teal-700"
            >
              {processing ? <Loader className="w-4 h-4 animate-spin" /> : <Send className="w-4 h-4" />}
              Pay All Approved ({approvedSalaries.length})
            </Button>
          </div>
        </div>

        {/* Metrics Cards */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
          <Card className="border-l-4 border-l-blue-500">
            <CardContent className="p-5">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-slate-600">Ready to Pay</p>
                  <p className="text-2xl font-bold text-blue-600">
                    {formatCurrency(dashboard?.amounts?.ready_to_pay || 0)}
                  </p>
                  <p className="text-xs text-slate-500">{approvedSalaries.length} employees</p>
                </div>
                <div className="w-12 h-12 bg-blue-100 rounded-full flex items-center justify-center">
                  <DollarSign className="w-6 h-6 text-blue-600" />
                </div>
              </div>
            </CardContent>
          </Card>

          <Card className="border-l-4 border-l-green-500">
            <CardContent className="p-5">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-slate-600">Paid This Month</p>
                  <p className="text-2xl font-bold text-green-600">
                    {formatCurrency(dashboard?.amounts?.paid_this_month || 0)}
                  </p>
                  <p className="text-xs text-slate-500">{dashboard?.salaries?.paid || 0} employees</p>
                </div>
                <div className="w-12 h-12 bg-green-100 rounded-full flex items-center justify-center">
                  <CheckCircle className="w-6 h-6 text-green-600" />
                </div>
              </div>
            </CardContent>
          </Card>

          <Card className="border-l-4 border-l-purple-500">
            <CardContent className="p-5">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-slate-600">Bank Linked</p>
                  <p className="text-2xl font-bold text-purple-600">
                    {dashboard?.employees?.bank_linked || 0}/{dashboard?.employees?.total || 0}
                  </p>
                  <p className="text-xs text-slate-500">Employees</p>
                </div>
                <div className="w-12 h-12 bg-purple-100 rounded-full flex items-center justify-center">
                  <Building2 className="w-6 h-6 text-purple-600" />
                </div>
              </div>
            </CardContent>
          </Card>

          <Card className="border-l-4 border-l-orange-500">
            <CardContent className="p-5">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-slate-600">Processing</p>
                  <p className="text-2xl font-bold text-orange-600">
                    {dashboard?.salaries?.processing || 0}
                  </p>
                  <p className="text-xs text-slate-500">In transit</p>
                </div>
                <div className="w-12 h-12 bg-orange-100 rounded-full flex items-center justify-center">
                  <Clock className="w-6 h-6 text-orange-600" />
                </div>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Tabs */}
        <div className="flex gap-2 mb-6 border-b">
          {['overview', 'bank-accounts', 'payouts'].map((tab) => (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              className={`px-4 py-2 font-medium transition-colors ${
                activeTab === tab
                  ? 'text-teal-600 border-b-2 border-teal-600'
                  : 'text-slate-500 hover:text-slate-700'
              }`}
            >
              {tab === 'overview' && 'Overview'}
              {tab === 'bank-accounts' && 'Bank Accounts'}
              {tab === 'payouts' && 'Payout History'}
            </button>
          ))}
        </div>

        {/* Tab Content */}
        {activeTab === 'overview' && (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Approved Salaries - Ready to Pay */}
            <Card>
              <CardContent className="p-6">
                <h3 className="text-lg font-bold text-slate-900 mb-4">
                  Approved Salaries - Ready to Pay
                </h3>
                <div className="space-y-3 max-h-[400px] overflow-y-auto">
                  {approvedSalaries.length > 0 ? approvedSalaries.map((salary) => {
                    const employee = employees.find(e => e.id === salary.employee_id);
                    const hasBankAccount = fundAccounts.some(f => f.employee_id === salary.employee_id);
                    
                    return (
                      <div key={salary.id} className="flex items-center justify-between p-3 bg-slate-50 rounded-lg">
                        <div className="flex items-center gap-3">
                          <div className="w-10 h-10 bg-teal-100 rounded-full flex items-center justify-center">
                            <Users className="w-5 h-5 text-teal-600" />
                          </div>
                          <div>
                            <p className="font-medium text-slate-900">{employee?.name || 'Unknown'}</p>
                            <p className="text-xs text-slate-500">{salary.month}</p>
                          </div>
                        </div>
                        <div className="flex items-center gap-3">
                          <p className="font-bold text-slate-900">{formatCurrency(salary.net_salary)}</p>
                          {hasBankAccount ? (
                            <Button
                              size="sm"
                              onClick={() => handleProcessSinglePayout(salary)}
                              disabled={processing}
                              className="bg-teal-600 hover:bg-teal-700"
                            >
                              {processing ? <Loader className="w-3 h-3 animate-spin" /> : <Send className="w-3 h-3" />}
                            </Button>
                          ) : (
                            <span className="text-xs text-red-500">No bank</span>
                          )}
                        </div>
                      </div>
                    );
                  }) : (
                    <div className="text-center py-8 text-slate-400">
                      <CheckCircle className="w-12 h-12 mx-auto mb-2 text-green-400" />
                      <p>No approved salaries pending</p>
                    </div>
                  )}
                </div>
              </CardContent>
            </Card>

            {/* Recent Payouts */}
            <Card>
              <CardContent className="p-6">
                <h3 className="text-lg font-bold text-slate-900 mb-4">Recent Payouts</h3>
                <div className="space-y-3 max-h-[400px] overflow-y-auto">
                  {dashboard?.recent_payouts?.length > 0 ? dashboard.recent_payouts.map((payout) => (
                    <div key={payout.id} className="flex items-center justify-between p-3 bg-slate-50 rounded-lg">
                      <div className="flex items-center gap-3">
                        <div className={`w-10 h-10 rounded-full flex items-center justify-center ${
                          payout.status === 'processed' ? 'bg-green-100' : 
                          payout.status === 'processing' ? 'bg-yellow-100' : 'bg-red-100'
                        }`}>
                          {payout.status === 'processed' ? (
                            <CheckCircle className="w-5 h-5 text-green-600" />
                          ) : payout.status === 'processing' ? (
                            <Clock className="w-5 h-5 text-yellow-600" />
                          ) : (
                            <AlertTriangle className="w-5 h-5 text-red-600" />
                          )}
                        </div>
                        <div>
                          <p className="font-medium text-slate-900">{payout.employee_name}</p>
                          <p className="text-xs text-slate-500">{payout.created_at?.slice(0, 10)}</p>
                        </div>
                      </div>
                      <div className="text-right">
                        <p className="font-bold text-slate-900">{formatCurrency(payout.amount)}</p>
                        <span className={`text-xs px-2 py-0.5 rounded-full ${getStatusBadge(payout.status)}`}>
                          {payout.status}
                        </span>
                      </div>
                    </div>
                  )) : (
                    <div className="text-center py-8 text-slate-400">
                      <CreditCard className="w-12 h-12 mx-auto mb-2 opacity-50" />
                      <p>No payouts yet</p>
                    </div>
                  )}
                </div>
              </CardContent>
            </Card>
          </div>
        )}

        {activeTab === 'bank-accounts' && (
          <Card>
            <CardContent className="p-6">
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-lg font-bold text-slate-900">Employee Bank Accounts</h3>
                {employeesWithoutBank.length > 0 && (
                  <span className="text-sm text-orange-600">
                    {employeesWithoutBank.length} employees need bank linking
                  </span>
                )}
              </div>
              <div className="space-y-3">
                {employees.map((employee) => {
                  const fundAccount = fundAccounts.find(f => f.employee_id === employee.id);
                  
                  return (
                    <div key={employee.id} className="flex items-center justify-between p-4 bg-slate-50 rounded-lg">
                      <div className="flex items-center gap-4">
                        <div className={`w-12 h-12 rounded-full flex items-center justify-center ${
                          fundAccount ? 'bg-green-100' : 'bg-orange-100'
                        }`}>
                          {fundAccount ? (
                            <Building2 className="w-6 h-6 text-green-600" />
                          ) : (
                            <AlertTriangle className="w-6 h-6 text-orange-600" />
                          )}
                        </div>
                        <div>
                          <p className="font-medium text-slate-900">{employee.name}</p>
                          <p className="text-sm text-slate-500">{employee.emp_code} • {employee.department}</p>
                          {fundAccount && (
                            <p className="text-xs text-slate-400">
                              {fundAccount.bank_name} ****{fundAccount.account_number_last4}
                            </p>
                          )}
                        </div>
                      </div>
                      {!fundAccount && (
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={() => {
                            setSelectedEmployee(employee);
                            setBankForm({ ...bankForm, account_holder_name: employee.name });
                            setShowBankModal(true);
                          }}
                          className="flex items-center gap-2"
                        >
                          <Plus className="w-4 h-4" />
                          Link Bank
                        </Button>
                      )}
                    </div>
                  );
                })}
              </div>
            </CardContent>
          </Card>
        )}

        {activeTab === 'payouts' && (
          <Card>
            <CardContent className="p-6">
              <h3 className="text-lg font-bold text-slate-900 mb-4">All Payouts</h3>
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead>
                    <tr className="border-b">
                      <th className="text-left py-3 px-4 text-sm font-medium text-slate-600">Date</th>
                      <th className="text-left py-3 px-4 text-sm font-medium text-slate-600">Employee</th>
                      <th className="text-left py-3 px-4 text-sm font-medium text-slate-600">Amount</th>
                      <th className="text-left py-3 px-4 text-sm font-medium text-slate-600">Mode</th>
                      <th className="text-left py-3 px-4 text-sm font-medium text-slate-600">Status</th>
                      <th className="text-left py-3 px-4 text-sm font-medium text-slate-600">UTR</th>
                    </tr>
                  </thead>
                  <tbody>
                    {dashboard?.recent_payouts?.map((payout) => (
                      <tr key={payout.id} className="border-b hover:bg-slate-50">
                        <td className="py-3 px-4 text-sm">{payout.created_at?.slice(0, 10)}</td>
                        <td className="py-3 px-4 text-sm font-medium">{payout.employee_name}</td>
                        <td className="py-3 px-4 text-sm font-bold">{formatCurrency(payout.amount)}</td>
                        <td className="py-3 px-4 text-sm">{payout.mode}</td>
                        <td className="py-3 px-4">
                          <span className={`text-xs px-2 py-1 rounded-full ${getStatusBadge(payout.status)}`}>
                            {payout.status}
                          </span>
                        </td>
                        <td className="py-3 px-4 text-sm text-slate-500">{payout.utr || '-'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {(!dashboard?.recent_payouts || dashboard.recent_payouts.length === 0) && (
                  <div className="text-center py-8 text-slate-400">
                    <CreditCard className="w-12 h-12 mx-auto mb-2 opacity-50" />
                    <p>No payout history</p>
                  </div>
                )}
              </div>
            </CardContent>
          </Card>
        )}
      </div>

      {/* Bank Account Modal */}
      {showBankModal && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <div className="bg-white rounded-xl p-6 w-full max-w-md mx-4">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-lg font-bold">Link Bank Account</h3>
              <button onClick={() => setShowBankModal(false)}>
                <X className="w-5 h-5 text-slate-400" />
              </button>
            </div>
            
            <p className="text-sm text-slate-600 mb-4">
              Adding bank account for: <strong>{selectedEmployee?.name}</strong>
            </p>
            
            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-slate-700 mb-1">
                  Account Holder Name
                </label>
                <Input
                  value={bankForm.account_holder_name}
                  onChange={(e) => setBankForm({ ...bankForm, account_holder_name: e.target.value })}
                  placeholder="As per bank records"
                />
              </div>
              
              <div>
                <label className="block text-sm font-medium text-slate-700 mb-1">
                  Account Number *
                </label>
                <Input
                  value={bankForm.account_number}
                  onChange={(e) => setBankForm({ ...bankForm, account_number: e.target.value })}
                  placeholder="Enter account number"
                />
              </div>
              
              <div>
                <label className="block text-sm font-medium text-slate-700 mb-1">
                  IFSC Code *
                </label>
                <Input
                  value={bankForm.ifsc_code}
                  onChange={(e) => setBankForm({ ...bankForm, ifsc_code: e.target.value.toUpperCase() })}
                  placeholder="e.g., HDFC0001234"
                />
              </div>
              
              <Button
                onClick={handleAddBankAccount}
                disabled={processing}
                className="w-full bg-teal-600 hover:bg-teal-700"
              >
                {processing ? (
                  <Loader className="w-4 h-4 animate-spin mr-2" />
                ) : (
                  <Building2 className="w-4 h-4 mr-2" />
                )}
                Link Bank Account
              </Button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default PayoutsDashboard;
