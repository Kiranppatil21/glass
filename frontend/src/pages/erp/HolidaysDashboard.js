import React, { useState, useEffect } from 'react';
import { Card, CardContent } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { 
  Calendar, Plus, Clock, CheckCircle, Settings, Users, DollarSign,
  RefreshCw, ChevronLeft, ChevronRight, Sun, Briefcase, Gift
} from 'lucide-react';
import { toast } from 'sonner';
import erpApi from '../../utils/erpApi';

const HolidaysDashboard = () => {
  const [activeTab, setActiveTab] = useState('calendar');
  const [selectedYear, setSelectedYear] = useState(new Date().getFullYear());
  const [selectedMonth, setSelectedMonth] = useState(new Date().getMonth() + 1);
  const [calendarData, setCalendarData] = useState(null);
  const [holidays, setHolidays] = useState([]);
  const [settings, setSettings] = useState(null);
  const [overtime, setOvertime] = useState([]);
  const [loading, setLoading] = useState(true);
  const [showAddModal, setShowAddModal] = useState(false);
  const [newHoliday, setNewHoliday] = useState({
    date: '', name: '', type: 'company', paid: true, description: ''
  });

  const monthNames = ['', 'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December'];

  useEffect(() => {
    fetchData();
  }, [selectedYear]);

  const fetchData = async () => {
    try {
      const [calRes, holidaysRes, settingsRes, overtimeRes] = await Promise.all([
        erpApi.holidays.getCalendar(selectedYear),
        erpApi.holidays.getAll({ year: selectedYear }),
        erpApi.holidays.getSettings(),
        erpApi.holidays.getOvertime({ limit: 50 })
      ]);
      setCalendarData(calRes.data);
      setHolidays(holidaysRes.data);
      setSettings(settingsRes.data);
      setOvertime(overtimeRes.data);
    } catch (error) {
      console.error('Failed to fetch holiday data:', error);
      toast.error('Failed to load holiday data');
    } finally {
      setLoading(false);
    }
  };

  const handleCreateHoliday = async (e) => {
    e.preventDefault();
    try {
      await erpApi.holidays.create(newHoliday);
      toast.success('Holiday added');
      setShowAddModal(false);
      setNewHoliday({ date: '', name: '', type: 'company', paid: true, description: '' });
      fetchData();
    } catch (error) {
      toast.error(error.response?.data?.detail || 'Failed to add holiday');
    }
  };

  const handleDeleteHoliday = async (id) => {
    if (!window.confirm('Delete this holiday?')) return;
    try {
      await erpApi.holidays.delete(id);
      toast.success('Holiday deleted');
      fetchData();
    } catch (error) {
      toast.error('Failed to delete holiday');
    }
  };

  const handleUpdateSettings = async (field, value) => {
    try {
      await erpApi.holidays.updateSettings({ [field]: value });
      setSettings({ ...settings, [field]: value });
      toast.success('Settings updated');
    } catch (error) {
      toast.error('Failed to update settings');
    }
  };

  const tabs = [
    { id: 'calendar', label: 'Calendar', icon: Calendar },
    { id: 'holidays', label: 'Holiday List', icon: Gift },
    { id: 'overtime', label: 'Overtime', icon: Clock },
    { id: 'settings', label: 'Settings', icon: Settings },
  ];

  const typeColors = {
    national: 'bg-red-100 text-red-700 border-red-300',
    state: 'bg-orange-100 text-orange-700 border-orange-300',
    company: 'bg-blue-100 text-blue-700 border-blue-300',
    optional: 'bg-purple-100 text-purple-700 border-purple-300',
    weekly_off: 'bg-slate-100 text-slate-600 border-slate-300',
  };

  const currentMonthData = calendarData?.months?.[selectedMonth - 1];

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-slate-50">
        <RefreshCw className="w-8 h-8 animate-spin text-teal-600" />
      </div>
    );
  }

  return (
    <div className="min-h-screen py-8 bg-slate-50" data-testid="holidays-dashboard">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        {/* Header */}
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-3xl font-bold text-slate-900">Holiday Calendar</h1>
            <p className="text-slate-600">Manage holidays, overtime & salary impact</p>
          </div>
          <Button
            onClick={() => setShowAddModal(true)}
            className="bg-teal-600 hover:bg-teal-700"
            data-testid="add-holiday-btn"
          >
            <Plus className="w-4 h-4 mr-2" />
            Add Holiday
          </Button>
        </div>

        {/* Year Stats */}
        {calendarData && (
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
            <Card className="bg-gradient-to-br from-teal-500 to-teal-600 text-white">
              <CardContent className="p-5">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-teal-100 text-sm">Working Days</p>
                    <p className="text-3xl font-bold">{calendarData.summary.working_days}</p>
                  </div>
                  <Briefcase className="w-10 h-10 opacity-80" />
                </div>
              </CardContent>
            </Card>

            <Card className="bg-gradient-to-br from-red-500 to-red-600 text-white">
              <CardContent className="p-5">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-red-100 text-sm">Holidays</p>
                    <p className="text-3xl font-bold">{calendarData.summary.holidays}</p>
                  </div>
                  <Gift className="w-10 h-10 opacity-80" />
                </div>
              </CardContent>
            </Card>

            <Card className="bg-gradient-to-br from-slate-500 to-slate-600 text-white">
              <CardContent className="p-5">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-slate-200 text-sm">Weekly Offs</p>
                    <p className="text-3xl font-bold">{calendarData.summary.weekly_offs}</p>
                  </div>
                  <Sun className="w-10 h-10 opacity-80" />
                </div>
              </CardContent>
            </Card>

            <Card className="bg-gradient-to-br from-purple-500 to-purple-600 text-white">
              <CardContent className="p-5">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-purple-100 text-sm">Total Days</p>
                    <p className="text-3xl font-bold">{calendarData.summary.total_days}</p>
                  </div>
                  <Calendar className="w-10 h-10 opacity-80" />
                </div>
              </CardContent>
            </Card>
          </div>
        )}

        {/* Tabs */}
        <div className="flex gap-2 mb-6 bg-white rounded-xl p-2 shadow-sm overflow-x-auto">
          {tabs.map((tab) => {
            const Icon = tab.icon;
            return (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`flex items-center gap-2 px-4 py-2 rounded-lg font-medium transition-all whitespace-nowrap ${
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

        {/* Calendar Tab */}
        {activeTab === 'calendar' && currentMonthData && (
          <Card>
            <CardContent className="p-6">
              {/* Month Navigator */}
              <div className="flex items-center justify-between mb-6">
                <button
                  onClick={() => {
                    if (selectedMonth === 1) {
                      setSelectedYear(selectedYear - 1);
                      setSelectedMonth(12);
                    } else {
                      setSelectedMonth(selectedMonth - 1);
                    }
                  }}
                  className="p-2 hover:bg-slate-100 rounded-lg"
                >
                  <ChevronLeft className="w-5 h-5" />
                </button>
                
                <div className="text-center">
                  <h2 className="text-2xl font-bold text-slate-900">
                    {monthNames[selectedMonth]} {selectedYear}
                  </h2>
                  <p className="text-sm text-slate-500">
                    {currentMonthData.working_days} working days • {currentMonthData.holidays} holidays
                  </p>
                </div>

                <button
                  onClick={() => {
                    if (selectedMonth === 12) {
                      setSelectedYear(selectedYear + 1);
                      setSelectedMonth(1);
                    } else {
                      setSelectedMonth(selectedMonth + 1);
                    }
                  }}
                  className="p-2 hover:bg-slate-100 rounded-lg"
                >
                  <ChevronRight className="w-5 h-5" />
                </button>
              </div>

              {/* Google Calendar Style Grid */}
              <div className="border border-slate-200 rounded-xl overflow-hidden">
                {/* Day Headers */}
                <div className="grid grid-cols-7 bg-slate-50 border-b border-slate-200">
                  {['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'].map(day => (
                    <div key={day} className="text-center py-3 text-sm font-semibold text-slate-600 border-r border-slate-200 last:border-r-0">
                      <span className="hidden sm:inline">{day}</span>
                      <span className="sm:hidden">{day.slice(0, 3)}</span>
                    </div>
                  ))}
                </div>
                
                {/* Calendar Cells */}
                <div className="grid grid-cols-7">
                  {/* Empty cells for days before month starts */}
                  {currentMonthData.days[0] && Array.from({ 
                    length: new Date(currentMonthData.days[0].date).getDay() 
                  }).map((_, i) => (
                    <div key={`empty-${i}`} className="min-h-[100px] bg-slate-50/50 border-r border-b border-slate-200" />
                  ))}
                  
                  {currentMonthData.days.map((day, idx) => {
                    const isToday = day.date === new Date().toISOString().split('T')[0];
                    const dayOfWeek = new Date(day.date).getDay();
                    const isLastCol = dayOfWeek === 6;
                    
                    return (
                      <div
                        key={day.date}
                        className={`min-h-[100px] p-2 border-b border-r border-slate-200 transition-all relative ${
                          isLastCol ? 'border-r-0' : ''
                        } ${
                          day.is_weekly_off ? 'bg-slate-50' :
                          day.is_holiday ? 'bg-gradient-to-br from-white to-slate-50' :
                          'bg-white hover:bg-slate-50'
                        }`}
                      >
                        {/* Date Number */}
                        <div className="flex items-start justify-between mb-1">
                          <span className={`inline-flex items-center justify-center w-7 h-7 rounded-full text-sm font-medium ${
                            isToday ? 'bg-teal-600 text-white' :
                            day.is_weekly_off ? 'text-slate-400' :
                            'text-slate-700'
                          }`}>
                            {day.day}
                          </span>
                          {day.is_weekly_off && !day.is_holiday && (
                            <span className="text-[10px] text-slate-400 font-medium">OFF</span>
                          )}
                        </div>
                        
                        {/* Holiday Name Badge */}
                        {day.is_holiday && day.holiday_name && (
                          <div 
                            className={`mt-1 px-2 py-1.5 rounded-md text-xs font-medium cursor-pointer transition-all hover:scale-[1.02] ${
                              day.type === 'national' ? 'bg-red-500 text-white shadow-sm shadow-red-200' :
                              day.type === 'state' ? 'bg-orange-500 text-white shadow-sm shadow-orange-200' :
                              day.type === 'company' ? 'bg-blue-500 text-white shadow-sm shadow-blue-200' :
                              day.type === 'optional' ? 'bg-purple-500 text-white shadow-sm shadow-purple-200' :
                              'bg-slate-500 text-white'
                            }`}
                            title={day.holiday_name}
                          >
                            <div className="truncate">{day.holiday_name}</div>
                          </div>
                        )}
                      </div>
                    );
                  })}
                  
                  {/* Empty cells to complete last row */}
                  {currentMonthData.days.length > 0 && (() => {
                    const lastDay = new Date(currentMonthData.days[currentMonthData.days.length - 1].date).getDay();
                    const emptyCells = lastDay === 6 ? 0 : 6 - lastDay;
                    return Array.from({ length: emptyCells }).map((_, i) => (
                      <div key={`empty-end-${i}`} className="min-h-[100px] bg-slate-50/50 border-b border-r border-slate-200 last:border-r-0" />
                    ));
                  })()}
                </div>
              </div>

              {/* Legend */}
              <div className="flex flex-wrap items-center gap-6 mt-6 pt-4 border-t border-slate-200">
                <span className="text-sm font-medium text-slate-500">Legend:</span>
                <div className="flex items-center gap-2">
                  <div className="w-4 h-4 bg-red-500 rounded shadow-sm" />
                  <span className="text-sm text-slate-600">National Holiday</span>
                </div>
                <div className="flex items-center gap-2">
                  <div className="w-4 h-4 bg-orange-500 rounded shadow-sm" />
                  <span className="text-sm text-slate-600">State Holiday</span>
                </div>
                <div className="flex items-center gap-2">
                  <div className="w-4 h-4 bg-blue-500 rounded shadow-sm" />
                  <span className="text-sm text-slate-600">Company Holiday</span>
                </div>
                <div className="flex items-center gap-2">
                  <div className="w-4 h-4 bg-purple-500 rounded shadow-sm" />
                  <span className="text-sm text-slate-600">Optional</span>
                </div>
                <div className="flex items-center gap-2">
                  <div className="w-4 h-4 bg-slate-200 rounded" />
                  <span className="text-sm text-slate-600">Weekly Off</span>
                </div>
              </div>
            </CardContent>
          </Card>
        )}

        {/* Holiday List Tab */}
        {activeTab === 'holidays' && (
          <Card>
            <CardContent className="p-6">
              <h3 className="font-bold text-slate-900 mb-4">
                Holidays in {selectedYear} ({holidays.length})
              </h3>
              
              <div className="space-y-3">
                {holidays.length > 0 ? holidays.map((holiday) => (
                  <div key={holiday.id} className="flex items-center justify-between p-4 bg-slate-50 rounded-lg">
                    <div className="flex items-center gap-4">
                      <div className={`w-12 h-12 rounded-lg flex items-center justify-center ${
                        typeColors[holiday.type]?.split(' ')[0] || 'bg-slate-100'
                      }`}>
                        <Calendar className="w-6 h-6" />
                      </div>
                      <div>
                        <h4 className="font-medium text-slate-900">{holiday.name}</h4>
                        <p className="text-sm text-slate-500">
                          {new Date(holiday.date).toLocaleDateString('en-IN', { 
                            weekday: 'long', day: 'numeric', month: 'long' 
                          })}
                        </p>
                      </div>
                    </div>
                    <div className="flex items-center gap-3">
                      <span className={`px-3 py-1 rounded-full text-sm ${typeColors[holiday.type]}`}>
                        {holiday.type}
                      </span>
                      {holiday.paid && (
                        <span className="px-2 py-1 bg-green-100 text-green-700 rounded text-xs">
                          Paid
                        </span>
                      )}
                      <Button 
                        variant="ghost" 
                        size="sm"
                        onClick={() => handleDeleteHoliday(holiday.id)}
                        className="text-red-600 hover:text-red-700 hover:bg-red-50"
                      >
                        Delete
                      </Button>
                    </div>
                  </div>
                )) : (
                  <div className="text-center py-12 text-slate-400">
                    <Calendar className="w-16 h-16 mx-auto mb-4 opacity-50" />
                    <p>No holidays added for {selectedYear}</p>
                    <Button className="mt-4" onClick={() => setShowAddModal(true)}>
                      Add Holiday
                    </Button>
                  </div>
                )}
              </div>
            </CardContent>
          </Card>
        )}

        {/* Overtime Tab */}
        {activeTab === 'overtime' && (
          <Card>
            <CardContent className="p-6">
              <div className="flex items-center justify-between mb-4">
                <h3 className="font-bold text-slate-900">Holiday Overtime Records</h3>
                <span className="text-sm text-slate-500">
                  Overtime Rate: {settings?.overtime_rate || 2}x
                </span>
              </div>
              
              <div className="space-y-3">
                {overtime.length > 0 ? overtime.map((record) => (
                  <div key={record.id} className="flex items-center justify-between p-4 bg-slate-50 rounded-lg">
                    <div className="flex items-center gap-4">
                      <div className="w-10 h-10 bg-orange-100 rounded-full flex items-center justify-center">
                        <Clock className="w-5 h-5 text-orange-600" />
                      </div>
                      <div>
                        <p className="font-medium text-slate-900">{record.employee_name}</p>
                        <p className="text-sm text-slate-500">
                          {record.date} • {record.hours_worked} hours
                        </p>
                      </div>
                    </div>
                    <div className="flex items-center gap-3">
                      <span className={`px-3 py-1 rounded-full text-sm ${
                        record.status === 'approved' ? 'bg-green-100 text-green-700' :
                        record.status === 'pending' ? 'bg-yellow-100 text-yellow-700' :
                        'bg-red-100 text-red-700'
                      }`}>
                        {record.status}
                      </span>
                      {record.comp_off_requested && (
                        <span className="text-xs bg-purple-100 text-purple-700 px-2 py-1 rounded">
                          Comp Off
                        </span>
                      )}
                    </div>
                  </div>
                )) : (
                  <div className="text-center py-12 text-slate-400">
                    <Clock className="w-16 h-16 mx-auto mb-4 opacity-50" />
                    <p>No overtime records</p>
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
                <h3 className="font-bold text-slate-900 mb-4">Weekly Off Settings</h3>
                <div className="space-y-4">
                  {['sunday', 'saturday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday'].map(day => (
                    <div key={day} className="flex items-center justify-between p-3 bg-slate-50 rounded-lg">
                      <span className="capitalize">{day}</span>
                      <button
                        onClick={() => {
                          const current = settings.weekly_offs || [];
                          const newOffs = current.includes(day) 
                            ? current.filter(d => d !== day)
                            : [...current, day];
                          handleUpdateSettings('weekly_offs', newOffs);
                        }}
                        className={`w-12 h-6 rounded-full transition-colors ${
                          settings.weekly_offs?.includes(day) ? 'bg-teal-600' : 'bg-slate-300'
                        }`}
                      >
                        <div className={`w-5 h-5 bg-white rounded-full shadow transform transition-transform ${
                          settings.weekly_offs?.includes(day) ? 'translate-x-6' : 'translate-x-0.5'
                        }`} />
                      </button>
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardContent className="p-6">
                <h3 className="font-bold text-slate-900 mb-4">Overtime & Comp Off</h3>
                <div className="space-y-4">
                  <div className="p-3 bg-slate-50 rounded-lg">
                    <label className="block text-sm text-slate-600 mb-2">Overtime Rate (multiplier)</label>
                    <input
                      type="number"
                      value={settings.overtime_rate}
                      onChange={(e) => handleUpdateSettings('overtime_rate', parseFloat(e.target.value))}
                      className="w-full h-10 rounded border px-3"
                      step="0.5"
                      min="1"
                    />
                  </div>

                  <div className="flex items-center justify-between p-3 bg-slate-50 rounded-lg">
                    <div>
                      <span className="text-slate-700">Comp Off Enabled</span>
                      <p className="text-xs text-slate-500">Allow comp off for holiday work</p>
                    </div>
                    <button
                      onClick={() => handleUpdateSettings('comp_off_enabled', !settings.comp_off_enabled)}
                      className={`w-12 h-6 rounded-full transition-colors ${
                        settings.comp_off_enabled ? 'bg-teal-600' : 'bg-slate-300'
                      }`}
                    >
                      <div className={`w-5 h-5 bg-white rounded-full shadow transform transition-transform ${
                        settings.comp_off_enabled ? 'translate-x-6' : 'translate-x-0.5'
                      }`} />
                    </button>
                  </div>

                  <div className="p-3 bg-slate-50 rounded-lg">
                    <label className="block text-sm text-slate-600 mb-2">Comp Off Validity (days)</label>
                    <input
                      type="number"
                      value={settings.comp_off_validity_days}
                      onChange={(e) => handleUpdateSettings('comp_off_validity_days', parseInt(e.target.value))}
                      className="w-full h-10 rounded border px-3"
                      min="1"
                    />
                  </div>

                  <div className="flex items-center justify-between p-3 bg-slate-50 rounded-lg">
                    <div>
                      <span className="text-slate-700">Auto Mark Attendance</span>
                      <p className="text-xs text-slate-500">Auto-mark holiday attendance</p>
                    </div>
                    <button
                      onClick={() => handleUpdateSettings('auto_mark_attendance', !settings.auto_mark_attendance)}
                      className={`w-12 h-6 rounded-full transition-colors ${
                        settings.auto_mark_attendance ? 'bg-teal-600' : 'bg-slate-300'
                      }`}
                    >
                      <div className={`w-5 h-5 bg-white rounded-full shadow transform transition-transform ${
                        settings.auto_mark_attendance ? 'translate-x-6' : 'translate-x-0.5'
                      }`} />
                    </button>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>
        )}

        {/* Add Holiday Modal */}
        {showAddModal && (
          <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
            <Card className="max-w-md w-full">
              <CardContent className="p-6">
                <h2 className="text-xl font-bold text-slate-900 mb-4">Add Holiday</h2>
                <form onSubmit={handleCreateHoliday} className="space-y-4">
                  <div>
                    <label className="block text-sm font-medium mb-1">Holiday Name *</label>
                    <input
                      type="text"
                      value={newHoliday.name}
                      onChange={(e) => setNewHoliday({...newHoliday, name: e.target.value})}
                      className="w-full h-10 rounded border px-3"
                      placeholder="e.g., Diwali"
                      required
                    />
                  </div>
                  
                  <div>
                    <label className="block text-sm font-medium mb-1">Date *</label>
                    <input
                      type="date"
                      value={newHoliday.date}
                      onChange={(e) => setNewHoliday({...newHoliday, date: e.target.value})}
                      className="w-full h-10 rounded border px-3"
                      required
                    />
                  </div>

                  <div>
                    <label className="block text-sm font-medium mb-1">Type</label>
                    <select
                      value={newHoliday.type}
                      onChange={(e) => setNewHoliday({...newHoliday, type: e.target.value})}
                      className="w-full h-10 rounded border px-3"
                    >
                      <option value="national">National Holiday</option>
                      <option value="state">State Holiday</option>
                      <option value="company">Company Holiday</option>
                      <option value="optional">Optional/Restricted</option>
                    </select>
                  </div>

                  <div className="flex items-center gap-2">
                    <input
                      type="checkbox"
                      id="paid"
                      checked={newHoliday.paid}
                      onChange={(e) => setNewHoliday({...newHoliday, paid: e.target.checked})}
                      className="w-4 h-4"
                    />
                    <label htmlFor="paid" className="text-sm">Paid Holiday</label>
                  </div>

                  <div className="flex gap-3 pt-2">
                    <Button type="submit" className="flex-1 bg-teal-600 hover:bg-teal-700">
                      Add Holiday
                    </Button>
                    <Button type="button" variant="outline" className="flex-1" onClick={() => setShowAddModal(false)}>
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

export default HolidaysDashboard;
