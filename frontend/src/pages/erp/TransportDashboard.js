import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { toast } from 'sonner';
import { erpApi } from '../../utils/erpApi';
import {
  Truck, User, Settings, MapPin, Package, Plus, Pencil, Trash2,
  Phone, Car, CheckCircle, Clock, Navigation, Save, X, Loader2
} from 'lucide-react';

const TransportDashboard = () => {
  const [activeTab, setActiveTab] = useState('dashboard');
  const [loading, setLoading] = useState(true);
  const [dashboard, setDashboard] = useState(null);
  const [vehicles, setVehicles] = useState([]);
  const [drivers, setDrivers] = useState([]);
  const [dispatches, setDispatches] = useState([]);
  const [settings, setSettings] = useState(null);
  
  // Modal states
  const [showVehicleModal, setShowVehicleModal] = useState(false);
  const [showDriverModal, setShowDriverModal] = useState(false);
  const [editingVehicle, setEditingVehicle] = useState(null);
  const [editingDriver, setEditingDriver] = useState(null);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    fetchData();
  }, [activeTab]);

  const fetchData = async () => {
    setLoading(true);
    try {
      if (activeTab === 'dashboard') {
        const res = await erpApi.transport.getDashboard();
        setDashboard(res.data);
      } else if (activeTab === 'vehicles') {
        const res = await erpApi.transport.getVehicles();
        setVehicles(res.data.vehicles || []);
      } else if (activeTab === 'drivers') {
        const res = await erpApi.transport.getDrivers();
        setDrivers(res.data.drivers || []);
      } else if (activeTab === 'dispatches') {
        const res = await erpApi.transport.getDispatches({});
        setDispatches(res.data.dispatches || []);
      } else if (activeTab === 'settings') {
        const res = await erpApi.transport.getSettings();
        setSettings(res.data);
      }
    } catch (error) {
      console.error('Failed to fetch data:', error);
      toast.error('Failed to load data');
    } finally {
      setLoading(false);
    }
  };

  const handleSaveSettings = async () => {
    setSaving(true);
    try {
      await erpApi.transport.updateSettings(settings);
      toast.success('Settings saved!');
    } catch (error) {
      toast.error('Failed to save settings');
    } finally {
      setSaving(false);
    }
  };

  const handleSaveVehicle = async (vehicleData) => {
    setSaving(true);
    try {
      if (editingVehicle?.id) {
        await erpApi.transport.updateVehicle(editingVehicle.id, vehicleData);
        toast.success('Vehicle updated');
      } else {
        await erpApi.transport.createVehicle(vehicleData);
        toast.success('Vehicle added');
      }
      setShowVehicleModal(false);
      setEditingVehicle(null);
      fetchData();
    } catch (error) {
      toast.error(error.response?.data?.detail || 'Failed to save vehicle');
    } finally {
      setSaving(false);
    }
  };

  const handleDeleteVehicle = async (id) => {
    if (!window.confirm('Delete this vehicle?')) return;
    try {
      await erpApi.transport.deleteVehicle(id);
      toast.success('Vehicle deleted');
      fetchData();
    } catch (error) {
      toast.error('Failed to delete vehicle');
    }
  };

  const handleSaveDriver = async (driverData) => {
    setSaving(true);
    try {
      if (editingDriver?.id) {
        await erpApi.transport.updateDriver(editingDriver.id, driverData);
        toast.success('Driver updated');
      } else {
        await erpApi.transport.createDriver(driverData);
        toast.success('Driver added');
      }
      setShowDriverModal(false);
      setEditingDriver(null);
      fetchData();
    } catch (error) {
      toast.error(error.response?.data?.detail || 'Failed to save driver');
    } finally {
      setSaving(false);
    }
  };

  const handleDeleteDriver = async (id) => {
    if (!window.confirm('Delete this driver?')) return;
    try {
      await erpApi.transport.deleteDriver(id);
      toast.success('Driver deleted');
      fetchData();
    } catch (error) {
      toast.error('Failed to delete driver');
    }
  };

  const handleUpdateDispatchStatus = async (dispatchId, status) => {
    try {
      await erpApi.transport.updateDispatchStatus(dispatchId, status);
      toast.success(`Dispatch marked as ${status}`);
      fetchData();
    } catch (error) {
      toast.error('Failed to update status');
    }
  };

  const tabs = [
    { id: 'dashboard', label: 'Dashboard', icon: Truck },
    { id: 'vehicles', label: 'Vehicles', icon: Car },
    { id: 'drivers', label: 'Drivers', icon: User },
    { id: 'dispatches', label: 'Dispatches', icon: Package },
    { id: 'settings', label: 'Settings', icon: Settings },
  ];

  const getStatusColor = (status) => {
    const colors = {
      available: 'bg-green-100 text-green-800',
      on_trip: 'bg-amber-100 text-amber-800',
      maintenance: 'bg-red-100 text-red-800',
      off_duty: 'bg-slate-100 text-slate-800',
      dispatched: 'bg-blue-100 text-blue-800',
      in_transit: 'bg-amber-100 text-amber-800',
      delivered: 'bg-green-100 text-green-800',
    };
    return colors[status] || 'bg-slate-100 text-slate-800';
  };

  return (
    <div className="space-y-6" data-testid="transport-dashboard">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-slate-900 flex items-center gap-2">
          <Truck className="w-7 h-7 text-amber-600" />
          Transport Management
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
                ? 'bg-amber-100 text-amber-800'
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
          <Loader2 className="w-8 h-8 animate-spin text-amber-600" />
        </div>
      ) : (
        <>
          {/* Dashboard Tab */}
          {activeTab === 'dashboard' && dashboard && (
            <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-4">
              <Card>
                <CardContent className="p-4">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-sm text-slate-500">Total Vehicles</p>
                      <p className="text-2xl font-bold text-slate-900">{dashboard.vehicles.total}</p>
                    </div>
                    <Car className="w-10 h-10 text-amber-500" />
                  </div>
                  <div className="mt-2 flex gap-2 text-xs">
                    <span className="text-green-600">{dashboard.vehicles.available} available</span>
                    <span className="text-amber-600">{dashboard.vehicles.on_trip} on trip</span>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardContent className="p-4">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-sm text-slate-500">Total Drivers</p>
                      <p className="text-2xl font-bold text-slate-900">{dashboard.drivers.total}</p>
                    </div>
                    <User className="w-10 h-10 text-blue-500" />
                  </div>
                  <div className="mt-2 flex gap-2 text-xs">
                    <span className="text-green-600">{dashboard.drivers.available} available</span>
                    <span className="text-amber-600">{dashboard.drivers.on_trip} on trip</span>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardContent className="p-4">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-sm text-slate-500">Today's Dispatches</p>
                      <p className="text-2xl font-bold text-slate-900">{dashboard.dispatches.today}</p>
                    </div>
                    <Package className="w-10 h-10 text-teal-500" />
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardContent className="p-4">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-sm text-slate-500">Pending Delivery</p>
                      <p className="text-2xl font-bold text-amber-600">{dashboard.dispatches.pending_delivery}</p>
                    </div>
                    <Clock className="w-10 h-10 text-amber-500" />
                  </div>
                </CardContent>
              </Card>

              {/* Recent Dispatches */}
              <Card className="md:col-span-2 lg:col-span-4">
                <CardHeader>
                  <CardTitle>Recent Dispatches</CardTitle>
                </CardHeader>
                <CardContent>
                  {dashboard.recent_dispatches.length === 0 ? (
                    <p className="text-slate-500 text-center py-4">No dispatches yet</p>
                  ) : (
                    <div className="space-y-3">
                      {dashboard.recent_dispatches.map((d) => (
                        <div key={d.id} className="flex items-center justify-between p-3 bg-slate-50 rounded-lg">
                          <div>
                            <p className="font-medium">Order #{d.order_number}</p>
                            <p className="text-sm text-slate-500">{d.driver_name} • {d.vehicle_number}</p>
                          </div>
                          <span className={`px-2 py-1 rounded text-xs font-medium ${getStatusColor(d.status)}`}>
                            {d.status}
                          </span>
                        </div>
                      ))}
                    </div>
                  )}
                </CardContent>
              </Card>
            </div>
          )}

          {/* Vehicles Tab */}
          {activeTab === 'vehicles' && (
            <div className="space-y-4">
              <div className="flex justify-end">
                <Button onClick={() => { setEditingVehicle({}); setShowVehicleModal(true); }} className="gap-2">
                  <Plus className="w-4 h-4" /> Add Vehicle
                </Button>
              </div>
              
              {vehicles.length === 0 ? (
                <Card>
                  <CardContent className="p-8 text-center text-slate-500">
                    No vehicles added yet. Click "Add Vehicle" to get started.
                  </CardContent>
                </Card>
              ) : (
                <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-4">
                  {vehicles.map((v) => (
                    <Card key={v.id}>
                      <CardContent className="p-4">
                        <div className="flex items-start justify-between">
                          <div>
                            <p className="font-bold text-lg">{v.vehicle_number}</p>
                            <p className="text-sm text-slate-500">{v.vehicle_type}</p>
                            <p className="text-xs text-slate-400">Capacity: {v.capacity_sqft} sq.ft</p>
                          </div>
                          <span className={`px-2 py-1 rounded text-xs font-medium ${getStatusColor(v.status)}`}>
                            {v.status}
                          </span>
                        </div>
                        {v.driver && (
                          <div className="mt-3 pt-3 border-t text-sm">
                            <p className="text-slate-600 flex items-center gap-1">
                              <User className="w-3 h-3" /> {v.driver.name}
                            </p>
                            <p className="text-slate-500 flex items-center gap-1">
                              <Phone className="w-3 h-3" /> {v.driver.phone}
                            </p>
                          </div>
                        )}
                        <div className="mt-3 flex gap-2">
                          <Button size="sm" variant="outline" onClick={() => { setEditingVehicle(v); setShowVehicleModal(true); }}>
                            <Pencil className="w-3 h-3" />
                          </Button>
                          <Button size="sm" variant="outline" className="text-red-600" onClick={() => handleDeleteVehicle(v.id)}>
                            <Trash2 className="w-3 h-3" />
                          </Button>
                        </div>
                      </CardContent>
                    </Card>
                  ))}
                </div>
              )}
            </div>
          )}

          {/* Drivers Tab */}
          {activeTab === 'drivers' && (
            <div className="space-y-4">
              <div className="flex justify-end">
                <Button onClick={() => { setEditingDriver({}); setShowDriverModal(true); }} className="gap-2">
                  <Plus className="w-4 h-4" /> Add Driver
                </Button>
              </div>
              
              {drivers.length === 0 ? (
                <Card>
                  <CardContent className="p-8 text-center text-slate-500">
                    No drivers added yet. Click "Add Driver" to get started.
                  </CardContent>
                </Card>
              ) : (
                <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-4">
                  {drivers.map((d) => (
                    <Card key={d.id}>
                      <CardContent className="p-4">
                        <div className="flex items-start justify-between">
                          <div>
                            <p className="font-bold text-lg">{d.name}</p>
                            <p className="text-sm text-slate-500 flex items-center gap-1">
                              <Phone className="w-3 h-3" /> {d.phone}
                            </p>
                            <p className="text-xs text-slate-400">License: {d.license_number}</p>
                          </div>
                          <span className={`px-2 py-1 rounded text-xs font-medium ${getStatusColor(d.status)}`}>
                            {d.status}
                          </span>
                        </div>
                        {d.vehicle && (
                          <div className="mt-3 pt-3 border-t text-sm">
                            <p className="text-slate-600 flex items-center gap-1">
                              <Car className="w-3 h-3" /> {d.vehicle.vehicle_number} ({d.vehicle.vehicle_type})
                            </p>
                          </div>
                        )}
                        <div className="mt-3 flex gap-2">
                          <Button size="sm" variant="outline" onClick={() => { setEditingDriver(d); setShowDriverModal(true); }}>
                            <Pencil className="w-3 h-3" />
                          </Button>
                          <Button size="sm" variant="outline" className="text-red-600" onClick={() => handleDeleteDriver(d.id)}>
                            <Trash2 className="w-3 h-3" />
                          </Button>
                        </div>
                      </CardContent>
                    </Card>
                  ))}
                </div>
              )}
            </div>
          )}

          {/* Dispatches Tab */}
          {activeTab === 'dispatches' && (
            <div className="space-y-4">
              {dispatches.length === 0 ? (
                <Card>
                  <CardContent className="p-8 text-center text-slate-500">
                    No dispatches yet. Dispatches are created from Order Management.
                  </CardContent>
                </Card>
              ) : (
                <Card>
                  <CardContent className="p-0">
                    <div className="overflow-x-auto">
                      <table className="w-full">
                        <thead className="bg-slate-50">
                          <tr>
                            <th className="px-4 py-3 text-left text-xs font-medium text-slate-500">Order #</th>
                            <th className="px-4 py-3 text-left text-xs font-medium text-slate-500">Customer</th>
                            <th className="px-4 py-3 text-left text-xs font-medium text-slate-500">Driver</th>
                            <th className="px-4 py-3 text-left text-xs font-medium text-slate-500">Vehicle</th>
                            <th className="px-4 py-3 text-left text-xs font-medium text-slate-500">Status</th>
                            <th className="px-4 py-3 text-left text-xs font-medium text-slate-500">Dispatched</th>
                            <th className="px-4 py-3 text-left text-xs font-medium text-slate-500">Actions</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y">
                          {dispatches.map((d) => (
                            <tr key={d.id} className="hover:bg-slate-50">
                              <td className="px-4 py-3 font-medium">{d.order_number}</td>
                              <td className="px-4 py-3">
                                <p>{d.customer_name}</p>
                                <p className="text-xs text-slate-500">{d.delivery_address?.substring(0, 30)}...</p>
                              </td>
                              <td className="px-4 py-3">
                                <p>{d.driver_name}</p>
                                <p className="text-xs text-slate-500">{d.driver_phone}</p>
                              </td>
                              <td className="px-4 py-3">{d.vehicle_number}</td>
                              <td className="px-4 py-3">
                                <span className={`px-2 py-1 rounded text-xs font-medium ${getStatusColor(d.status)}`}>
                                  {d.status}
                                </span>
                              </td>
                              <td className="px-4 py-3 text-sm text-slate-500">
                                {new Date(d.dispatched_at).toLocaleString('en-IN', { dateStyle: 'short', timeStyle: 'short' })}
                              </td>
                              <td className="px-4 py-3">
                                {d.status === 'dispatched' && (
                                  <Button size="sm" variant="outline" onClick={() => handleUpdateDispatchStatus(d.id, 'delivered')}>
                                    <CheckCircle className="w-3 h-3 mr-1" /> Delivered
                                  </Button>
                                )}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </CardContent>
                </Card>
              )}
            </div>
          )}

          {/* Settings Tab */}
          {activeTab === 'settings' && settings && (
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Settings className="w-5 h-5" />
                  Transport Pricing Settings
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-6">
                <div className="grid md:grid-cols-2 gap-6">
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-1">Base Charge (₹)</label>
                    <input
                      type="number"
                      value={settings.base_charge}
                      onChange={(e) => setSettings({ ...settings, base_charge: parseFloat(e.target.value) })}
                      className="w-full h-10 rounded-lg border border-slate-300 px-3"
                    />
                    <p className="text-xs text-slate-500 mt-1">Minimum transport charge</p>
                  </div>
                  
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-1">Base KM Included</label>
                    <input
                      type="number"
                      value={settings.base_km}
                      onChange={(e) => setSettings({ ...settings, base_km: parseFloat(e.target.value) })}
                      className="w-full h-10 rounded-lg border border-slate-300 px-3"
                    />
                    <p className="text-xs text-slate-500 mt-1">KM included in base charge</p>
                  </div>
                  
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-1">Per KM Rate (₹)</label>
                    <input
                      type="number"
                      value={settings.per_km_rate}
                      onChange={(e) => setSettings({ ...settings, per_km_rate: parseFloat(e.target.value) })}
                      className="w-full h-10 rounded-lg border border-slate-300 px-3"
                    />
                    <p className="text-xs text-slate-500 mt-1">Rate per km after base km</p>
                  </div>
                  
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-1">Per Sq.ft Rate (₹)</label>
                    <input
                      type="number"
                      value={settings.per_sqft_rate}
                      onChange={(e) => setSettings({ ...settings, per_sqft_rate: parseFloat(e.target.value) })}
                      className="w-full h-10 rounded-lg border border-slate-300 px-3"
                    />
                    <p className="text-xs text-slate-500 mt-1">Additional charge for heavy loads</p>
                  </div>
                  
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-1">Min Sq.ft for Load Charge</label>
                    <input
                      type="number"
                      value={settings.min_sqft_for_load_charge}
                      onChange={(e) => setSettings({ ...settings, min_sqft_for_load_charge: parseFloat(e.target.value) })}
                      className="w-full h-10 rounded-lg border border-slate-300 px-3"
                    />
                    <p className="text-xs text-slate-500 mt-1">Apply load charge above this sq.ft</p>
                  </div>
                  
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-1">GST Percent (%)</label>
                    <input
                      type="number"
                      value={settings.gst_percent}
                      onChange={(e) => setSettings({ ...settings, gst_percent: parseFloat(e.target.value) })}
                      className="w-full h-10 rounded-lg border border-slate-300 px-3"
                    />
                    <p className="text-xs text-slate-500 mt-1">GST on transport services</p>
                  </div>
                </div>

                {/* Factory Location Info */}
                {settings.factory_location && (
                  <div className="bg-slate-50 rounded-lg p-4">
                    <p className="font-medium text-slate-900 flex items-center gap-2">
                      <MapPin className="w-4 h-4 text-amber-600" />
                      Factory Location
                    </p>
                    <p className="text-sm text-slate-600 mt-1">{settings.factory_location.name}</p>
                    <p className="text-xs text-slate-500">{settings.factory_location.address}</p>
                    <p className="text-xs text-slate-400">Coordinates: {settings.factory_location.lat}, {settings.factory_location.lng}</p>
                  </div>
                )}

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

      {/* Vehicle Modal */}
      {showVehicleModal && (
        <VehicleModal
          vehicle={editingVehicle}
          drivers={drivers}
          onSave={handleSaveVehicle}
          onClose={() => { setShowVehicleModal(false); setEditingVehicle(null); }}
          saving={saving}
        />
      )}

      {/* Driver Modal */}
      {showDriverModal && (
        <DriverModal
          driver={editingDriver}
          vehicles={vehicles}
          onSave={handleSaveDriver}
          onClose={() => { setShowDriverModal(false); setEditingDriver(null); }}
          saving={saving}
        />
      )}
    </div>
  );
};

// Vehicle Modal Component
const VehicleModal = ({ vehicle, drivers, onSave, onClose, saving }) => {
  const [form, setForm] = useState({
    vehicle_number: vehicle?.vehicle_number || '',
    vehicle_type: vehicle?.vehicle_type || 'tempo',
    capacity_sqft: vehicle?.capacity_sqft || 500,
    driver_id: vehicle?.driver_id || '',
    status: vehicle?.status || 'available',
    notes: vehicle?.notes || ''
  });

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
      <div className="bg-white rounded-xl w-full max-w-md">
        <div className="flex items-center justify-between p-4 border-b">
          <h3 className="font-semibold">{vehicle?.id ? 'Edit Vehicle' : 'Add Vehicle'}</h3>
          <button onClick={onClose}><X className="w-5 h-5" /></button>
        </div>
        <div className="p-4 space-y-4">
          <div>
            <label className="block text-sm font-medium mb-1">Vehicle Number *</label>
            <input
              type="text"
              value={form.vehicle_number}
              onChange={(e) => setForm({ ...form, vehicle_number: e.target.value.toUpperCase() })}
              placeholder="MH12AB1234"
              className="w-full h-10 rounded-lg border border-slate-300 px-3"
            />
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Vehicle Type *</label>
            <select
              value={form.vehicle_type}
              onChange={(e) => setForm({ ...form, vehicle_type: e.target.value })}
              className="w-full h-10 rounded-lg border border-slate-300 px-3"
            >
              <option value="tempo">Tempo</option>
              <option value="mini-truck">Mini Truck</option>
              <option value="truck">Truck</option>
              <option value="pickup">Pickup</option>
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Capacity (sq.ft)</label>
            <input
              type="number"
              value={form.capacity_sqft}
              onChange={(e) => setForm({ ...form, capacity_sqft: parseFloat(e.target.value) })}
              className="w-full h-10 rounded-lg border border-slate-300 px-3"
            />
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Assigned Driver</label>
            <select
              value={form.driver_id}
              onChange={(e) => setForm({ ...form, driver_id: e.target.value })}
              className="w-full h-10 rounded-lg border border-slate-300 px-3"
            >
              <option value="">Not Assigned</option>
              {drivers.map((d) => (
                <option key={d.id} value={d.id}>{d.name} - {d.phone}</option>
              ))}
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Status</label>
            <select
              value={form.status}
              onChange={(e) => setForm({ ...form, status: e.target.value })}
              className="w-full h-10 rounded-lg border border-slate-300 px-3"
            >
              <option value="available">Available</option>
              <option value="on_trip">On Trip</option>
              <option value="maintenance">Maintenance</option>
            </select>
          </div>
        </div>
        <div className="flex justify-end gap-2 p-4 border-t">
          <Button variant="outline" onClick={onClose}>Cancel</Button>
          <Button onClick={() => onSave(form)} disabled={saving || !form.vehicle_number}>
            {saving ? 'Saving...' : 'Save'}
          </Button>
        </div>
      </div>
    </div>
  );
};

// Driver Modal Component
const DriverModal = ({ driver, vehicles, onSave, onClose, saving }) => {
  const [form, setForm] = useState({
    name: driver?.name || '',
    phone: driver?.phone || '',
    license_number: driver?.license_number || '',
    vehicle_id: driver?.vehicle_id || '',
    status: driver?.status || 'available',
    address: driver?.address || '',
    emergency_contact: driver?.emergency_contact || ''
  });

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
      <div className="bg-white rounded-xl w-full max-w-md max-h-[90vh] overflow-y-auto">
        <div className="flex items-center justify-between p-4 border-b sticky top-0 bg-white">
          <h3 className="font-semibold">{driver?.id ? 'Edit Driver' : 'Add Driver'}</h3>
          <button onClick={onClose}><X className="w-5 h-5" /></button>
        </div>
        <div className="p-4 space-y-4">
          <div>
            <label className="block text-sm font-medium mb-1">Driver Name *</label>
            <input
              type="text"
              value={form.name}
              onChange={(e) => setForm({ ...form, name: e.target.value })}
              className="w-full h-10 rounded-lg border border-slate-300 px-3"
            />
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Phone Number *</label>
            <input
              type="tel"
              value={form.phone}
              onChange={(e) => setForm({ ...form, phone: e.target.value })}
              placeholder="9876543210"
              className="w-full h-10 rounded-lg border border-slate-300 px-3"
            />
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">License Number *</label>
            <input
              type="text"
              value={form.license_number}
              onChange={(e) => setForm({ ...form, license_number: e.target.value.toUpperCase() })}
              className="w-full h-10 rounded-lg border border-slate-300 px-3"
            />
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Assigned Vehicle</label>
            <select
              value={form.vehicle_id}
              onChange={(e) => setForm({ ...form, vehicle_id: e.target.value })}
              className="w-full h-10 rounded-lg border border-slate-300 px-3"
            >
              <option value="">Not Assigned</option>
              {vehicles.map((v) => (
                <option key={v.id} value={v.id}>{v.vehicle_number} ({v.vehicle_type})</option>
              ))}
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Status</label>
            <select
              value={form.status}
              onChange={(e) => setForm({ ...form, status: e.target.value })}
              className="w-full h-10 rounded-lg border border-slate-300 px-3"
            >
              <option value="available">Available</option>
              <option value="on_trip">On Trip</option>
              <option value="off_duty">Off Duty</option>
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Address</label>
            <textarea
              value={form.address}
              onChange={(e) => setForm({ ...form, address: e.target.value })}
              className="w-full rounded-lg border border-slate-300 px-3 py-2 h-20"
            />
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Emergency Contact</label>
            <input
              type="tel"
              value={form.emergency_contact}
              onChange={(e) => setForm({ ...form, emergency_contact: e.target.value })}
              className="w-full h-10 rounded-lg border border-slate-300 px-3"
            />
          </div>
        </div>
        <div className="flex justify-end gap-2 p-4 border-t sticky bottom-0 bg-white">
          <Button variant="outline" onClick={onClose}>Cancel</Button>
          <Button onClick={() => onSave(form)} disabled={saving || !form.name || !form.phone || !form.license_number}>
            {saving ? 'Saving...' : 'Save'}
          </Button>
        </div>
      </div>
    </div>
  );
};

export default TransportDashboard;
