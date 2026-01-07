import React, { useState, useEffect } from 'react';
import { Card, CardContent } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { Users, DollarSign, CheckCircle, Clock, AlertCircle, Plus, Building, Phone, Mail, Calendar, UserCheck, UserX } from 'lucide-react';
import { toast } from 'sonner';
import erpApi from '../../utils/erpApi';

const HRDashboard = () => {
  const [employees, setEmployees] = useState([]);
  const [salaries, setSalaries] = useState([]);
  const [attendance, setAttendance] = useState([]);
  const [attendanceSummary, setAttendanceSummary] = useState([]);
  const [activeTab, setActiveTab] = useState('employees');
  const [loading, setLoading] = useState(true);
  const [showAddEmployee, setShowAddEmployee] = useState(false);
  const [showMarkAttendance, setShowMarkAttendance] = useState(false);
  const [selectedDate, setSelectedDate] = useState(new Date().toISOString().split('T')[0]);
  const [newEmployee, setNewEmployee] = useState({
    emp_code: '',
    name: '',
    email: '',
    phone: '',
    role: 'operator',
    department: 'Production',
    designation: '',
    date_of_joining: new Date().toISOString().split('T')[0],
    salary: 15000,
    bank_account: '',
    bank_ifsc: ''
  });

  useEffect(() => {
    fetchEmployees();
    fetchSalaries();
  }, []);

  useEffect(() => {
    if (activeTab === 'attendance') {
      fetchAttendance();
      fetchAttendanceSummary();
    }
  }, [activeTab, selectedDate]);

  const fetchEmployees = async () => {
    try {
      const response = await erpApi.hr.getEmployees();
      setEmployees(response.data || []);
    } catch (error) {
      console.error('Failed to fetch employees:', error);
    } finally {
      setLoading(false);
    }
  };

  const fetchSalaries = async () => {
    try {
      const response = await erpApi.hr.getSalaries();
      setSalaries(response.data || []);
    } catch (error) {
      console.error('Failed to fetch salaries:', error);
    }
  };

  const fetchAttendance = async () => {
    try {
      const response = await erpApi.hr.getAttendance({ date: selectedDate });
      setAttendance(response.data || []);
    } catch (error) {
      console.error('Failed to fetch attendance:', error);
    }
  };

  const fetchAttendanceSummary = async () => {
    try {
      const currentMonth = selectedDate.slice(0, 7);
      const response = await erpApi.hr.getAttendanceSummary(currentMonth);
      setAttendanceSummary(response.data || []);
    } catch (error) {
      console.error('Failed to fetch attendance summary:', error);
    }
  };

  const handleMarkAttendance = async (empId, status) => {
    try {
      await erpApi.hr.markAttendance({
        employee_id: empId,
        date: selectedDate,
        status: status,
        check_in: status === 'present' || status === 'half_day' ? '09:00' : '',
        check_out: status === 'present' ? '18:00' : status === 'half_day' ? '14:00' : ''
      });
      toast.success('Attendance marked!');
      fetchAttendance();
      fetchAttendanceSummary();
    } catch (error) {
      console.error('Failed to mark attendance:', error);
      toast.error('Failed to mark attendance');
    }
  };

  const handleAddEmployee = async (e) => {
    e.preventDefault();
    try {
      await erpApi.hr.createEmployee(newEmployee);
      toast.success('Employee added successfully!');
      setShowAddEmployee(false);
      setNewEmployee({
        emp_code: '',
        name: '',
        email: '',
        phone: '',
        role: 'operator',
        department: 'Production',
        designation: '',
        date_of_joining: new Date().toISOString().split('T')[0],
        salary: 15000,
        bank_account: '',
        bank_ifsc: ''
      });
      fetchEmployees();
    } catch (error) {
      console.error('Failed to add employee:', error);
      toast.error('Failed to add employee');
    }
  };

  const handleCalculateSalary = async (empId) => {
    try {
      const currentMonth = new Date().toISOString().slice(0, 7); // YYYY-MM
      await erpApi.hr.calculateSalary(empId, { month: currentMonth, overtime_pay: 0, deductions: 0 });
      toast.success('Salary calculated successfully!');
      fetchSalaries();
    } catch (error) {
      console.error('Failed to calculate salary:', error);
      toast.error('Failed to calculate salary');
    }
  };

  const handleApproveSalary = async (salaryId) => {
    try {
      await erpApi.hr.approveSalary(salaryId);
      toast.success('Salary approved for payment!');
      fetchSalaries();
    } catch (error) {
      console.error('Failed to approve salary:', error);
      toast.error('Failed to approve salary');
    }
  };

  const statusColors = {
    pending: 'bg-yellow-100 text-yellow-700',
    approved: 'bg-blue-100 text-blue-700',
    paid: 'bg-green-100 text-green-700'
  };

  return (
    <div className="min-h-screen py-20 bg-slate-50" data-testid="hr-dashboard">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between mb-8">
          <div>
            <h1 className="text-4xl font-bold text-slate-900 mb-2">HR & Payroll</h1>
            <p className="text-slate-600">Manage employees, attendance, and salary payments</p>
          </div>
          <Button 
            onClick={() => setShowAddEmployee(true)}
            className="bg-primary-700 hover:bg-primary-800"
            data-testid="add-employee-btn"
          >
            <Plus className="w-4 h-4 mr-2" />
            Add Employee
          </Button>
        </div>

        {/* Tabs */}
        <div className="flex gap-4 mb-8 border-b border-slate-200">
          <button
            onClick={() => setActiveTab('employees')}
            className={`pb-4 px-4 font-medium transition-colors border-b-2 ${
              activeTab === 'employees'
                ? 'border-primary-600 text-primary-600'
                : 'border-transparent text-slate-600 hover:text-slate-900'
            }`}
          >
            Employees
          </button>
          <button
            onClick={() => setActiveTab('attendance')}
            className={`pb-4 px-4 font-medium transition-colors border-b-2 ${
              activeTab === 'attendance'
                ? 'border-primary-600 text-primary-600'
                : 'border-transparent text-slate-600 hover:text-slate-900'
            }`}
          >
            Attendance
          </button>
          <button
            onClick={() => setActiveTab('salaries')}
            className={`pb-4 px-4 font-medium transition-colors border-b-2 ${
              activeTab === 'salaries'
                ? 'border-primary-600 text-primary-600'
                : 'border-transparent text-slate-600 hover:text-slate-900'
            }`}
          >
            Salary Management
          </button>
        </div>

        {/* Employees Tab */}
        {activeTab === 'employees' && (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {employees.map((emp) => (
              <Card key={emp.id} className="hover:shadow-lg transition-shadow">
                <CardContent className="p-6">
                  <div className="flex items-start justify-between mb-4">
                    <div className="flex items-center gap-3">
                      <div className="w-12 h-12 bg-primary-100 rounded-full flex items-center justify-center">
                        <Users className="w-6 h-6 text-primary-700" />
                      </div>
                      <div>
                        <h3 className="font-semibold text-slate-900">{emp.name}</h3>
                        <p className="text-sm text-slate-600">{emp.emp_code}</p>
                      </div>
                    </div>
                  </div>

                  <div className="space-y-2 mb-4">
                    <div className="flex justify-between text-sm">
                      <span className="text-slate-600">Role:</span>
                      <span className="font-medium capitalize">{emp.role}</span>
                    </div>
                    <div className="flex justify-between text-sm">
                      <span className="text-slate-600">Department:</span>
                      <span className="font-medium">{emp.department}</span>
                    </div>
                    <div className="flex justify-between text-sm">
                      <span className="text-slate-600">Salary:</span>
                      <span className="font-medium">₹{emp.salary.toLocaleString()}/mo</span>
                    </div>
                  </div>

                  <Button
                    onClick={() => handleCalculateSalary(emp.id)}
                    className="w-full bg-primary-700 hover:bg-primary-800"
                    size="sm"
                  >
                    <DollarSign className="w-4 h-4 mr-2" />
                    Calculate Salary
                  </Button>
                </CardContent>
              </Card>
            ))}

            {employees.length === 0 && !loading && (
              <div className="col-span-3 text-center py-12">
                <Users className="w-16 h-16 text-slate-300 mx-auto mb-4" />
                <p className="text-slate-600">No employees found.</p>
              </div>
            )}
          </div>
        )}

        {/* Attendance Tab */}
        {activeTab === 'attendance' && (
          <div>
            {/* Date Selector */}
            <div className="flex items-center justify-between mb-6">
              <div className="flex items-center gap-4">
                <Calendar className="w-5 h-5 text-slate-600" />
                <input
                  type="date"
                  value={selectedDate}
                  onChange={(e) => setSelectedDate(e.target.value)}
                  className="h-10 rounded-lg border border-slate-300 px-4"
                />
              </div>
              <div className="flex gap-4">
                <div className="text-center px-4 py-2 bg-green-100 rounded-lg">
                  <p className="text-xs text-green-700">Present</p>
                  <p className="text-xl font-bold text-green-700">
                    {attendance.filter(a => a.status === 'present').length}
                  </p>
                </div>
                <div className="text-center px-4 py-2 bg-red-100 rounded-lg">
                  <p className="text-xs text-red-700">Absent</p>
                  <p className="text-xl font-bold text-red-700">
                    {employees.length - attendance.filter(a => a.status !== 'absent').length}
                  </p>
                </div>
              </div>
            </div>

            {/* Attendance Grid */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {employees.map((emp) => {
                const empAttendance = attendance.find(a => a.employee_id === emp.id);
                const status = empAttendance?.status || 'not_marked';
                
                const statusConfig = {
                  present: { bg: 'bg-green-100 border-green-200', text: 'text-green-700', icon: UserCheck },
                  absent: { bg: 'bg-red-100 border-red-200', text: 'text-red-700', icon: UserX },
                  half_day: { bg: 'bg-yellow-100 border-yellow-200', text: 'text-yellow-700', icon: Clock },
                  leave: { bg: 'bg-blue-100 border-blue-200', text: 'text-blue-700', icon: Calendar },
                  not_marked: { bg: 'bg-slate-100 border-slate-200', text: 'text-slate-600', icon: Clock }
                };
                const config = statusConfig[status] || statusConfig.not_marked;
                const Icon = config.icon;

                return (
                  <Card key={emp.id} className={`${config.bg} border`}>
                    <CardContent className="p-4">
                      <div className="flex items-center justify-between mb-3">
                        <div className="flex items-center gap-3">
                          <div className={`w-10 h-10 rounded-full flex items-center justify-center ${config.bg}`}>
                            <Icon className={`w-5 h-5 ${config.text}`} />
                          </div>
                          <div>
                            <h4 className="font-medium text-slate-900">{emp.name}</h4>
                            <p className="text-xs text-slate-600">{emp.department}</p>
                          </div>
                        </div>
                        <span className={`px-2 py-1 rounded text-xs font-medium capitalize ${config.text} ${config.bg}`}>
                          {status === 'not_marked' ? 'Not Marked' : status.replace('_', ' ')}
                        </span>
                      </div>

                      {empAttendance && (
                        <div className="text-xs text-slate-600 mb-3">
                          {empAttendance.check_in && <span>In: {empAttendance.check_in}</span>}
                          {empAttendance.check_out && <span className="ml-3">Out: {empAttendance.check_out}</span>}
                          {empAttendance.overtime_hours > 0 && (
                            <span className="ml-3">OT: {empAttendance.overtime_hours}h</span>
                          )}
                        </div>
                      )}

                      <div className="flex gap-2">
                        <Button
                          size="sm"
                          variant={status === 'present' ? 'default' : 'outline'}
                          className={status === 'present' ? 'bg-green-600 hover:bg-green-700' : 'text-green-600 border-green-300 hover:bg-green-50'}
                          onClick={() => handleMarkAttendance(emp.id, 'present')}
                        >
                          P
                        </Button>
                        <Button
                          size="sm"
                          variant={status === 'absent' ? 'default' : 'outline'}
                          className={status === 'absent' ? 'bg-red-600 hover:bg-red-700' : 'text-red-600 border-red-300 hover:bg-red-50'}
                          onClick={() => handleMarkAttendance(emp.id, 'absent')}
                        >
                          A
                        </Button>
                        <Button
                          size="sm"
                          variant={status === 'half_day' ? 'default' : 'outline'}
                          className={status === 'half_day' ? 'bg-yellow-600 hover:bg-yellow-700' : 'text-yellow-600 border-yellow-300 hover:bg-yellow-50'}
                          onClick={() => handleMarkAttendance(emp.id, 'half_day')}
                        >
                          H
                        </Button>
                        <Button
                          size="sm"
                          variant={status === 'leave' ? 'default' : 'outline'}
                          className={status === 'leave' ? 'bg-blue-600 hover:bg-blue-700' : 'text-blue-600 border-blue-300 hover:bg-blue-50'}
                          onClick={() => handleMarkAttendance(emp.id, 'leave')}
                        >
                          L
                        </Button>
                      </div>
                    </CardContent>
                  </Card>
                );
              })}
            </div>

            {/* Monthly Summary */}
            {attendanceSummary.length > 0 && (
              <div className="mt-8">
                <h3 className="text-lg font-bold text-slate-900 mb-4">Monthly Summary - {selectedDate.slice(0, 7)}</h3>
                <Card>
                  <CardContent className="p-0">
                    <table className="w-full">
                      <thead className="bg-slate-50 border-b">
                        <tr>
                          <th className="px-4 py-3 text-left text-sm font-medium text-slate-700">Employee</th>
                          <th className="px-4 py-3 text-center text-sm font-medium text-green-600">Present</th>
                          <th className="px-4 py-3 text-center text-sm font-medium text-red-600">Absent</th>
                          <th className="px-4 py-3 text-center text-sm font-medium text-yellow-600">Half Day</th>
                          <th className="px-4 py-3 text-center text-sm font-medium text-blue-600">Leave</th>
                          <th className="px-4 py-3 text-center text-sm font-medium text-slate-700">Total Days</th>
                          <th className="px-4 py-3 text-center text-sm font-medium text-purple-600">Overtime</th>
                        </tr>
                      </thead>
                      <tbody>
                        {attendanceSummary.map((summary) => (
                          <tr key={summary.employee_id} className="border-b hover:bg-slate-50">
                            <td className="px-4 py-3">
                              <div>
                                <p className="font-medium text-slate-900">{summary.employee_name}</p>
                                <p className="text-xs text-slate-500">{summary.emp_code}</p>
                              </div>
                            </td>
                            <td className="px-4 py-3 text-center font-medium text-green-600">{summary.present}</td>
                            <td className="px-4 py-3 text-center font-medium text-red-600">{summary.absent}</td>
                            <td className="px-4 py-3 text-center font-medium text-yellow-600">{summary.half_day}</td>
                            <td className="px-4 py-3 text-center font-medium text-blue-600">{summary.leave}</td>
                            <td className="px-4 py-3 text-center font-bold text-slate-900">{summary.total_days}</td>
                            <td className="px-4 py-3 text-center font-medium text-purple-600">{summary.overtime_hours}h</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </CardContent>
                </Card>
              </div>
            )}
          </div>
        )}

        {/* Salaries Tab */}
        {activeTab === 'salaries' && (
          <div className="space-y-4">
            {salaries.map((salary) => {
              const employee = employees.find(e => e.id === salary.employee_id);
              
              return (
                <Card key={salary.id}>
                  <CardContent className="p-6">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-4">
                        <div className="w-12 h-12 bg-green-100 rounded-full flex items-center justify-center">
                          <DollarSign className="w-6 h-6 text-green-700" />
                        </div>
                        <div>
                          <h3 className="font-semibold text-slate-900">
                            {employee?.name || salary.employee_id.slice(0, 8)}
                          </h3>
                          <p className="text-sm text-slate-600">Month: {salary.month}</p>
                        </div>
                      </div>

                      <div className="flex items-center gap-6">
                        <div className="text-right">
                          <p className="text-sm text-slate-600">Net Salary</p>
                          <p className="text-2xl font-bold text-green-700">
                            ₹{salary.net_salary.toLocaleString()}
                          </p>
                        </div>

                        <span className={`px-4 py-2 rounded-full text-sm font-medium flex items-center gap-2 ${
                          statusColors[salary.payment_status]
                        }`}>
                          {salary.payment_status === 'paid' ? (
                            <CheckCircle className="w-4 h-4" />
                          ) : salary.payment_status === 'approved' ? (
                            <Clock className="w-4 h-4" />
                          ) : (
                            <AlertCircle className="w-4 h-4" />
                          )}
                          {salary.payment_status.charAt(0).toUpperCase() + salary.payment_status.slice(1)}
                        </span>

                        {salary.payment_status === 'pending' && (
                          <Button
                            onClick={() => handleApproveSalary(salary.id)}
                            className="bg-primary-700 hover:bg-primary-800"
                          >
                            Approve Payment
                          </Button>
                        )}
                      </div>
                    </div>

                    <div className="mt-4 grid grid-cols-4 gap-4 pt-4 border-t border-slate-200">
                      <div>
                        <p className="text-xs text-slate-600">Basic Salary</p>
                        <p className="font-medium">₹{salary.basic_salary.toLocaleString()}</p>
                      </div>
                      <div>
                        <p className="text-xs text-slate-600">Overtime</p>
                        <p className="font-medium">₹{salary.overtime_pay?.toLocaleString() || 0}</p>
                      </div>
                      <div>
                        <p className="text-xs text-slate-600">Deductions</p>
                        <p className="font-medium text-red-600">-₹{salary.deductions?.toLocaleString() || 0}</p>
                      </div>
                      <div>
                        <p className="text-xs text-slate-600">Created</p>
                        <p className="font-medium text-sm">{new Date(salary.created_at).toLocaleDateString()}</p>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              );
            })}

            {salaries.length === 0 && !loading && (
              <div className="text-center py-12">
                <DollarSign className="w-16 h-16 text-slate-300 mx-auto mb-4" />
                <p className="text-slate-600">No salary records found. Calculate salaries for employees first.</p>
              </div>
            )}
          </div>
        )}

        {/* Add Employee Modal */}
        {showAddEmployee && (
          <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
            <Card className="max-w-2xl w-full max-h-[90vh] overflow-y-auto">
              <CardContent className="p-8">
                <h2 className="text-2xl font-bold text-slate-900 mb-6">Add New Employee</h2>
                <form onSubmit={handleAddEmployee} className="space-y-4">
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Employee Code *</label>
                      <input
                        type="text"
                        value={newEmployee.emp_code}
                        onChange={(e) => setNewEmployee({...newEmployee, emp_code: e.target.value})}
                        className="w-full h-12 rounded-lg border border-slate-300 px-4"
                        placeholder="EMP001"
                        required
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Full Name *</label>
                      <input
                        type="text"
                        value={newEmployee.name}
                        onChange={(e) => setNewEmployee({...newEmployee, name: e.target.value})}
                        className="w-full h-12 rounded-lg border border-slate-300 px-4"
                        required
                      />
                    </div>
                  </div>

                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Email</label>
                      <input
                        type="email"
                        value={newEmployee.email}
                        onChange={(e) => setNewEmployee({...newEmployee, email: e.target.value})}
                        className="w-full h-12 rounded-lg border border-slate-300 px-4"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Phone *</label>
                      <input
                        type="tel"
                        value={newEmployee.phone}
                        onChange={(e) => setNewEmployee({...newEmployee, phone: e.target.value})}
                        className="w-full h-12 rounded-lg border border-slate-300 px-4"
                        required
                      />
                    </div>
                  </div>

                  <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Role</label>
                      <select
                        value={newEmployee.role}
                        onChange={(e) => setNewEmployee({...newEmployee, role: e.target.value})}
                        className="w-full h-12 rounded-lg border border-slate-300 px-4"
                      >
                        <option value="operator">Operator</option>
                        <option value="supervisor">Supervisor</option>
                        <option value="manager">Manager</option>
                        <option value="admin">Admin</option>
                        <option value="helper">Helper</option>
                      </select>
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Department</label>
                      <select
                        value={newEmployee.department}
                        onChange={(e) => setNewEmployee({...newEmployee, department: e.target.value})}
                        className="w-full h-12 rounded-lg border border-slate-300 px-4"
                      >
                        <option value="Production">Production</option>
                        <option value="Quality">Quality</option>
                        <option value="Sales">Sales</option>
                        <option value="Admin">Admin</option>
                        <option value="Accounts">Accounts</option>
                        <option value="Dispatch">Dispatch</option>
                      </select>
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Designation</label>
                      <input
                        type="text"
                        value={newEmployee.designation}
                        onChange={(e) => setNewEmployee({...newEmployee, designation: e.target.value})}
                        className="w-full h-12 rounded-lg border border-slate-300 px-4"
                        placeholder="Glass Cutter"
                      />
                    </div>
                  </div>

                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Date of Joining</label>
                      <input
                        type="date"
                        value={newEmployee.date_of_joining}
                        onChange={(e) => setNewEmployee({...newEmployee, date_of_joining: e.target.value})}
                        className="w-full h-12 rounded-lg border border-slate-300 px-4"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Monthly Salary (₹) *</label>
                      <input
                        type="number"
                        value={newEmployee.salary}
                        onChange={(e) => setNewEmployee({...newEmployee, salary: parseFloat(e.target.value) || 0})}
                        className="w-full h-12 rounded-lg border border-slate-300 px-4"
                        min="0"
                        required
                      />
                    </div>
                  </div>

                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Bank Account No.</label>
                      <input
                        type="text"
                        value={newEmployee.bank_account}
                        onChange={(e) => setNewEmployee({...newEmployee, bank_account: e.target.value})}
                        className="w-full h-12 rounded-lg border border-slate-300 px-4"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">IFSC Code</label>
                      <input
                        type="text"
                        value={newEmployee.bank_ifsc}
                        onChange={(e) => setNewEmployee({...newEmployee, bank_ifsc: e.target.value})}
                        className="w-full h-12 rounded-lg border border-slate-300 px-4"
                      />
                    </div>
                  </div>

                  <div className="flex gap-3 pt-4">
                    <Button type="submit" className="flex-1 bg-primary-700 hover:bg-primary-800">
                      Add Employee
                    </Button>
                    <Button 
                      type="button"
                      variant="outline"
                      onClick={() => setShowAddEmployee(false)}
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
    </div>
  );
};

export default HRDashboard;
