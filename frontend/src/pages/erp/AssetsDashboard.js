import React, { useState, useEffect } from 'react';
import { Card, CardContent } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { 
  Package, Plus, Truck, Users, Settings, TrendingDown, Building,
  Wrench, Calendar, AlertCircle, RefreshCw, DollarSign, Clock,
  FileText, CheckCircle, XCircle, User, Clipboard, Car, Monitor, Sofa
} from 'lucide-react';
import { toast } from 'sonner';
import erpApi from '../../utils/erpApi';

const AssetsDashboard = () => {
  const [activeTab, setActiveTab] = useState('owned');
  const [ownedAssets, setOwnedAssets] = useState([]);
  const [rentedAssets, setRentedAssets] = useState([]);
  const [handovers, setHandovers] = useState([]);
  const [assetTypes, setAssetTypes] = useState([]);
  const [loading, setLoading] = useState(true);
  const [showAddModal, setShowAddModal] = useState(false);
  const [modalType, setModalType] = useState('owned');
  const [newAsset, setNewAsset] = useState({
    name: '', asset_type: 'machine', description: '', purchase_date: '',
    purchase_price: '', useful_life_years: 5, depreciation_method: 'straight_line',
    salvage_value: '', location: '', department: 'Production',
    serial_number: '', manufacturer: ''
  });
  const [newRented, setNewRented] = useState({
    name: '', asset_type: 'machine', vendor_name: '', vendor_contact: '',
    rent_type: 'monthly', rent_amount: '', rent_start_date: '', rent_end_date: '',
    security_deposit: '', department: '', location: ''
  });

  useEffect(() => {
    fetchData();
  }, []);

  const fetchData = async () => {
    try {
      const [typesRes, ownedRes, rentedRes, handoversRes] = await Promise.all([
        erpApi.assets.getTypes(),
        erpApi.assets.getOwned(),
        erpApi.assets.getRented(),
        erpApi.assets.getHandovers({ limit: 50 })
      ]);
      setAssetTypes(typesRes.data);
      setOwnedAssets(ownedRes.data);
      setRentedAssets(rentedRes.data);
      setHandovers(handoversRes.data);
    } catch (error) {
      console.error('Failed to fetch assets:', error);
      toast.error('Failed to load assets');
    } finally {
      setLoading(false);
    }
  };

  const handleCreateOwned = async (e) => {
    e.preventDefault();
    try {
      await erpApi.assets.createOwned({
        ...newAsset,
        purchase_price: parseFloat(newAsset.purchase_price) || 0,
        salvage_value: parseFloat(newAsset.salvage_value) || 0,
        useful_life_years: parseInt(newAsset.useful_life_years) || 5
      });
      toast.success('Asset created successfully');
      setShowAddModal(false);
      fetchData();
    } catch (error) {
      toast.error('Failed to create asset');
    }
  };

  const handleCreateRented = async (e) => {
    e.preventDefault();
    try {
      await erpApi.assets.createRented({
        ...newRented,
        rent_amount: parseFloat(newRented.rent_amount) || 0,
        security_deposit: parseFloat(newRented.security_deposit) || 0
      });
      toast.success('Rented asset added');
      setShowAddModal(false);
      fetchData();
    } catch (error) {
      toast.error('Failed to add rented asset');
    }
  };

  const getAssetIcon = (type) => {
    const icons = {
      machine: Package, vehicle: Car, tool: Wrench,
      it_asset: Monitor, furniture: Sofa, building: Building, other: Package
    };
    return icons[type] || Package;
  };

  const tabs = [
    { id: 'owned', label: 'Company Assets', icon: Building, count: ownedAssets.length },
    { id: 'rented', label: 'Rented Assets', icon: Truck, count: rentedAssets.length },
    { id: 'handover', label: 'Handovers', icon: Users, count: handovers.filter(h => h.status === 'issued').length },
    { id: 'reports', label: 'Reports', icon: FileText },
  ];

  // Calculate totals
  const totalPurchaseValue = ownedAssets.reduce((sum, a) => sum + (a.purchase_price || 0), 0);
  const totalBookValue = ownedAssets.reduce((sum, a) => sum + (a.book_value || 0), 0);
  const totalDepreciation = totalPurchaseValue - totalBookValue;
  const monthlyRentLiability = rentedAssets.filter(a => a.status === 'active')
    .reduce((sum, a) => sum + (a.rent_amount || 0), 0);

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-slate-50">
        <RefreshCw className="w-8 h-8 animate-spin text-teal-600" />
      </div>
    );
  }

  return (
    <div className="min-h-screen py-8 bg-slate-50" data-testid="assets-dashboard">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        {/* Header */}
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-3xl font-bold text-slate-900">Asset Management</h1>
            <p className="text-slate-600">Track company & rented assets, depreciation, handovers</p>
          </div>
          <div className="flex gap-2">
            <Button
              variant="outline"
              onClick={() => { setModalType('rented'); setShowAddModal(true); }}
            >
              <Truck className="w-4 h-4 mr-2" />
              Add Rented
            </Button>
            <Button
              onClick={() => { setModalType('owned'); setShowAddModal(true); }}
              className="bg-teal-600 hover:bg-teal-700"
              data-testid="add-asset-btn"
            >
              <Plus className="w-4 h-4 mr-2" />
              Add Asset
            </Button>
          </div>
        </div>

        {/* Stats Cards */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
          <Card className="bg-gradient-to-br from-teal-500 to-teal-600 text-white">
            <CardContent className="p-5">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-teal-100 text-sm">Total Assets</p>
                  <p className="text-3xl font-bold">{ownedAssets.length}</p>
                </div>
                <Building className="w-10 h-10 opacity-80" />
              </div>
            </CardContent>
          </Card>

          <Card className="bg-gradient-to-br from-blue-500 to-blue-600 text-white">
            <CardContent className="p-5">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-blue-100 text-sm">Book Value</p>
                  <p className="text-3xl font-bold">₹{(totalBookValue/100000).toFixed(1)}L</p>
                </div>
                <DollarSign className="w-10 h-10 opacity-80" />
              </div>
            </CardContent>
          </Card>

          <Card className="bg-gradient-to-br from-orange-500 to-orange-600 text-white">
            <CardContent className="p-5">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-orange-100 text-sm">Depreciation</p>
                  <p className="text-3xl font-bold">₹{(totalDepreciation/100000).toFixed(1)}L</p>
                </div>
                <TrendingDown className="w-10 h-10 opacity-80" />
              </div>
            </CardContent>
          </Card>

          <Card className="bg-gradient-to-br from-purple-500 to-purple-600 text-white">
            <CardContent className="p-5">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-purple-100 text-sm">Monthly Rent</p>
                  <p className="text-3xl font-bold">₹{monthlyRentLiability.toLocaleString()}</p>
                </div>
                <Truck className="w-10 h-10 opacity-80" />
              </div>
            </CardContent>
          </Card>
        </div>

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
                {tab.count !== undefined && (
                  <span className={`ml-1 px-2 py-0.5 rounded-full text-xs ${
                    activeTab === tab.id ? 'bg-white/20' : 'bg-slate-200'
                  }`}>
                    {tab.count}
                  </span>
                )}
              </button>
            );
          })}
        </div>

        {/* Owned Assets Tab */}
        {activeTab === 'owned' && (
          <div className="grid gap-4">
            {ownedAssets.length > 0 ? ownedAssets.map((asset) => {
              const AssetIcon = getAssetIcon(asset.asset_type);
              const depPercent = asset.purchase_price > 0 
                ? ((asset.accumulated_depreciation / asset.purchase_price) * 100).toFixed(0)
                : 0;
              return (
                <Card key={asset.id} className="hover:shadow-md transition-shadow">
                  <CardContent className="p-5">
                    <div className="flex items-start justify-between">
                      <div className="flex items-start gap-4">
                        <div className="w-12 h-12 bg-teal-100 rounded-lg flex items-center justify-center">
                          <AssetIcon className="w-6 h-6 text-teal-600" />
                        </div>
                        <div>
                          <div className="flex items-center gap-2">
                            <h3 className="font-bold text-slate-900">{asset.name}</h3>
                            <span className="text-xs bg-slate-100 px-2 py-0.5 rounded">
                              {asset.asset_code}
                            </span>
                          </div>
                          <p className="text-sm text-slate-500 mt-1">
                            {asset.manufacturer} • {asset.location} • {asset.department}
                          </p>
                          <div className="flex items-center gap-4 mt-2 text-sm">
                            <span className="text-slate-600">
                              Purchase: ₹{asset.purchase_price?.toLocaleString()}
                            </span>
                            <span className="text-teal-600 font-medium">
                              Book Value: ₹{asset.book_value?.toLocaleString()}
                            </span>
                            <span className={`px-2 py-0.5 rounded text-xs ${
                              asset.condition === 'excellent' ? 'bg-green-100 text-green-700' :
                              asset.condition === 'good' ? 'bg-blue-100 text-blue-700' :
                              asset.condition === 'fair' ? 'bg-yellow-100 text-yellow-700' :
                              'bg-red-100 text-red-700'
                            }`}>
                              {asset.condition}
                            </span>
                          </div>
                        </div>
                      </div>
                      <div className="text-right">
                        <p className="text-sm text-slate-500">Depreciation</p>
                        <p className="text-lg font-bold text-orange-600">
                          ₹{asset.accumulated_depreciation?.toLocaleString()}
                        </p>
                        <div className="w-24 bg-slate-200 rounded-full h-2 mt-2">
                          <div 
                            className="bg-orange-500 rounded-full h-2" 
                            style={{ width: `${Math.min(depPercent, 100)}%` }}
                          />
                        </div>
                        <p className="text-xs text-slate-500 mt-1">{depPercent}% depreciated</p>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              );
            }) : (
              <Card>
                <CardContent className="p-12 text-center text-slate-400">
                  <Building className="w-16 h-16 mx-auto mb-4 opacity-50" />
                  <p className="text-lg">No company assets yet</p>
                  <Button className="mt-4" onClick={() => { setModalType('owned'); setShowAddModal(true); }}>
                    Add First Asset
                  </Button>
                </CardContent>
              </Card>
            )}
          </div>
        )}

        {/* Rented Assets Tab */}
        {activeTab === 'rented' && (
          <div className="grid gap-4">
            {rentedAssets.length > 0 ? rentedAssets.map((asset) => (
              <Card key={asset.id} className={`hover:shadow-md transition-shadow ${
                asset.days_remaining >= 0 && asset.days_remaining <= 30 ? 'border-orange-300' : ''
              }`}>
                <CardContent className="p-5">
                  <div className="flex items-start justify-between">
                    <div className="flex items-start gap-4">
                      <div className="w-12 h-12 bg-purple-100 rounded-lg flex items-center justify-center">
                        <Truck className="w-6 h-6 text-purple-600" />
                      </div>
                      <div>
                        <div className="flex items-center gap-2">
                          <h3 className="font-bold text-slate-900">{asset.name}</h3>
                          <span className={`text-xs px-2 py-0.5 rounded ${
                            asset.status === 'active' ? 'bg-green-100 text-green-700' :
                            asset.status === 'returned' ? 'bg-slate-100 text-slate-700' :
                            'bg-yellow-100 text-yellow-700'
                          }`}>
                            {asset.status}
                          </span>
                        </div>
                        <p className="text-sm text-slate-500 mt-1">
                          Vendor: {asset.vendor_name} • {asset.location}
                        </p>
                        <div className="flex items-center gap-4 mt-2 text-sm">
                          <span className="text-slate-600">
                            {asset.rent_type}: ₹{asset.rent_amount?.toLocaleString()}
                          </span>
                          <span className="text-slate-600">
                            Deposit: ₹{asset.security_deposit?.toLocaleString()}
                          </span>
                        </div>
                      </div>
                    </div>
                    <div className="text-right">
                      {asset.days_remaining >= 0 ? (
                        <>
                          <p className="text-sm text-slate-500">Expires in</p>
                          <p className={`text-2xl font-bold ${
                            asset.days_remaining <= 7 ? 'text-red-600' :
                            asset.days_remaining <= 30 ? 'text-orange-600' :
                            'text-green-600'
                          }`}>
                            {asset.days_remaining} days
                          </p>
                        </>
                      ) : (
                        <span className="text-sm text-slate-500">No end date</span>
                      )}
                      <p className="text-xs text-slate-500 mt-1">
                        Start: {asset.rent_start_date}
                      </p>
                    </div>
                  </div>
                </CardContent>
              </Card>
            )) : (
              <Card>
                <CardContent className="p-12 text-center text-slate-400">
                  <Truck className="w-16 h-16 mx-auto mb-4 opacity-50" />
                  <p className="text-lg">No rented assets</p>
                  <Button className="mt-4" onClick={() => { setModalType('rented'); setShowAddModal(true); }}>
                    Add Rented Asset
                  </Button>
                </CardContent>
              </Card>
            )}
          </div>
        )}

        {/* Handovers Tab */}
        {activeTab === 'handover' && (
          <div className="grid gap-4">
            {handovers.length > 0 ? handovers.map((ho) => (
              <Card key={ho.id}>
                <CardContent className="p-5">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-4">
                      <div className={`w-10 h-10 rounded-full flex items-center justify-center ${
                        ho.status === 'issued' ? 'bg-green-100' :
                        ho.status === 'pending' ? 'bg-yellow-100' :
                        ho.status === 'returned' ? 'bg-blue-100' :
                        'bg-slate-100'
                      }`}>
                        <User className={`w-5 h-5 ${
                          ho.status === 'issued' ? 'text-green-600' :
                          ho.status === 'pending' ? 'text-yellow-600' :
                          ho.status === 'returned' ? 'text-blue-600' :
                          'text-slate-600'
                        }`} />
                      </div>
                      <div>
                        <p className="font-medium text-slate-900">{ho.asset_name}</p>
                        <p className="text-sm text-slate-500">
                          {ho.employee_name} • {ho.department}
                        </p>
                      </div>
                    </div>
                    <div className="flex items-center gap-4">
                      <span className={`px-3 py-1 rounded-full text-sm ${
                        ho.status === 'issued' ? 'bg-green-100 text-green-700' :
                        ho.status === 'pending' ? 'bg-yellow-100 text-yellow-700' :
                        ho.status === 'approved' ? 'bg-blue-100 text-blue-700' :
                        ho.status === 'returned' ? 'bg-slate-100 text-slate-700' :
                        'bg-red-100 text-red-700'
                      }`}>
                        {ho.status}
                      </span>
                      <span className="text-sm text-slate-500">{ho.request_number}</span>
                    </div>
                  </div>
                </CardContent>
              </Card>
            )) : (
              <Card>
                <CardContent className="p-12 text-center text-slate-400">
                  <Users className="w-16 h-16 mx-auto mb-4 opacity-50" />
                  <p className="text-lg">No asset handovers</p>
                </CardContent>
              </Card>
            )}
          </div>
        )}

        {/* Reports Tab */}
        {activeTab === 'reports' && (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <Card>
              <CardContent className="p-6">
                <h3 className="font-bold text-slate-900 mb-4">Asset Register Summary</h3>
                <div className="space-y-3">
                  <div className="flex justify-between p-3 bg-slate-50 rounded">
                    <span>Total Assets</span>
                    <span className="font-bold">{ownedAssets.length}</span>
                  </div>
                  <div className="flex justify-between p-3 bg-slate-50 rounded">
                    <span>Total Purchase Value</span>
                    <span className="font-bold">₹{totalPurchaseValue.toLocaleString()}</span>
                  </div>
                  <div className="flex justify-between p-3 bg-slate-50 rounded">
                    <span>Current Book Value</span>
                    <span className="font-bold text-teal-600">₹{totalBookValue.toLocaleString()}</span>
                  </div>
                  <div className="flex justify-between p-3 bg-slate-50 rounded">
                    <span>Total Depreciation</span>
                    <span className="font-bold text-orange-600">₹{totalDepreciation.toLocaleString()}</span>
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardContent className="p-6">
                <h3 className="font-bold text-slate-900 mb-4">Rent Liability</h3>
                <div className="space-y-3">
                  <div className="flex justify-between p-3 bg-slate-50 rounded">
                    <span>Active Rentals</span>
                    <span className="font-bold">{rentedAssets.filter(a => a.status === 'active').length}</span>
                  </div>
                  <div className="flex justify-between p-3 bg-slate-50 rounded">
                    <span>Monthly Rent</span>
                    <span className="font-bold text-purple-600">₹{monthlyRentLiability.toLocaleString()}</span>
                  </div>
                  <div className="flex justify-between p-3 bg-slate-50 rounded">
                    <span>Annual Rent</span>
                    <span className="font-bold">₹{(monthlyRentLiability * 12).toLocaleString()}</span>
                  </div>
                  <div className="flex justify-between p-3 bg-slate-50 rounded">
                    <span>Expiring Soon (30 days)</span>
                    <span className="font-bold text-orange-600">
                      {rentedAssets.filter(a => a.days_remaining >= 0 && a.days_remaining <= 30).length}
                    </span>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>
        )}

        {/* Add Modal */}
        {showAddModal && (
          <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
            <Card className="max-w-2xl w-full max-h-[90vh] overflow-y-auto">
              <CardContent className="p-6">
                <h2 className="text-xl font-bold text-slate-900 mb-4">
                  {modalType === 'owned' ? 'Add Company Asset' : 'Add Rented Asset'}
                </h2>
                
                {modalType === 'owned' ? (
                  <form onSubmit={handleCreateOwned} className="space-y-4">
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <label className="block text-sm font-medium mb-1">Asset Name *</label>
                        <input
                          type="text"
                          value={newAsset.name}
                          onChange={(e) => setNewAsset({...newAsset, name: e.target.value})}
                          className="w-full h-10 rounded border px-3"
                          required
                        />
                      </div>
                      <div>
                        <label className="block text-sm font-medium mb-1">Type *</label>
                        <select
                          value={newAsset.asset_type}
                          onChange={(e) => setNewAsset({...newAsset, asset_type: e.target.value})}
                          className="w-full h-10 rounded border px-3"
                        >
                          {assetTypes.map(t => <option key={t.id} value={t.id}>{t.name}</option>)}
                        </select>
                      </div>
                    </div>
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <label className="block text-sm font-medium mb-1">Purchase Date</label>
                        <input
                          type="date"
                          value={newAsset.purchase_date}
                          onChange={(e) => setNewAsset({...newAsset, purchase_date: e.target.value})}
                          className="w-full h-10 rounded border px-3"
                        />
                      </div>
                      <div>
                        <label className="block text-sm font-medium mb-1">Purchase Price (₹) *</label>
                        <input
                          type="number"
                          value={newAsset.purchase_price}
                          onChange={(e) => setNewAsset({...newAsset, purchase_price: e.target.value})}
                          className="w-full h-10 rounded border px-3"
                          required
                        />
                      </div>
                    </div>
                    <div className="grid grid-cols-3 gap-4">
                      <div>
                        <label className="block text-sm font-medium mb-1">Useful Life (Years)</label>
                        <input
                          type="number"
                          value={newAsset.useful_life_years}
                          onChange={(e) => setNewAsset({...newAsset, useful_life_years: e.target.value})}
                          className="w-full h-10 rounded border px-3"
                        />
                      </div>
                      <div>
                        <label className="block text-sm font-medium mb-1">Depreciation Method</label>
                        <select
                          value={newAsset.depreciation_method}
                          onChange={(e) => setNewAsset({...newAsset, depreciation_method: e.target.value})}
                          className="w-full h-10 rounded border px-3"
                        >
                          <option value="straight_line">Straight Line</option>
                          <option value="wdv">Written Down Value</option>
                        </select>
                      </div>
                      <div>
                        <label className="block text-sm font-medium mb-1">Salvage Value (₹)</label>
                        <input
                          type="number"
                          value={newAsset.salvage_value}
                          onChange={(e) => setNewAsset({...newAsset, salvage_value: e.target.value})}
                          className="w-full h-10 rounded border px-3"
                        />
                      </div>
                    </div>
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <label className="block text-sm font-medium mb-1">Location</label>
                        <input
                          type="text"
                          value={newAsset.location}
                          onChange={(e) => setNewAsset({...newAsset, location: e.target.value})}
                          className="w-full h-10 rounded border px-3"
                        />
                      </div>
                      <div>
                        <label className="block text-sm font-medium mb-1">Department</label>
                        <select
                          value={newAsset.department}
                          onChange={(e) => setNewAsset({...newAsset, department: e.target.value})}
                          className="w-full h-10 rounded border px-3"
                        >
                          <option>Production</option>
                          <option>Admin</option>
                          <option>Sales</option>
                          <option>HR</option>
                          <option>Store</option>
                        </select>
                      </div>
                    </div>
                    <div className="flex gap-3 pt-2">
                      <Button type="submit" className="flex-1 bg-teal-600 hover:bg-teal-700">Create Asset</Button>
                      <Button type="button" variant="outline" className="flex-1" onClick={() => setShowAddModal(false)}>Cancel</Button>
                    </div>
                  </form>
                ) : (
                  <form onSubmit={handleCreateRented} className="space-y-4">
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <label className="block text-sm font-medium mb-1">Asset Name *</label>
                        <input
                          type="text"
                          value={newRented.name}
                          onChange={(e) => setNewRented({...newRented, name: e.target.value})}
                          className="w-full h-10 rounded border px-3"
                          required
                        />
                      </div>
                      <div>
                        <label className="block text-sm font-medium mb-1">Vendor Name *</label>
                        <input
                          type="text"
                          value={newRented.vendor_name}
                          onChange={(e) => setNewRented({...newRented, vendor_name: e.target.value})}
                          className="w-full h-10 rounded border px-3"
                          required
                        />
                      </div>
                    </div>
                    <div className="grid grid-cols-3 gap-4">
                      <div>
                        <label className="block text-sm font-medium mb-1">Rent Type</label>
                        <select
                          value={newRented.rent_type}
                          onChange={(e) => setNewRented({...newRented, rent_type: e.target.value})}
                          className="w-full h-10 rounded border px-3"
                        >
                          <option value="monthly">Monthly</option>
                          <option value="daily">Daily</option>
                          <option value="hourly">Hourly</option>
                        </select>
                      </div>
                      <div>
                        <label className="block text-sm font-medium mb-1">Rent Amount (₹) *</label>
                        <input
                          type="number"
                          value={newRented.rent_amount}
                          onChange={(e) => setNewRented({...newRented, rent_amount: e.target.value})}
                          className="w-full h-10 rounded border px-3"
                          required
                        />
                      </div>
                      <div>
                        <label className="block text-sm font-medium mb-1">Security Deposit</label>
                        <input
                          type="number"
                          value={newRented.security_deposit}
                          onChange={(e) => setNewRented({...newRented, security_deposit: e.target.value})}
                          className="w-full h-10 rounded border px-3"
                        />
                      </div>
                    </div>
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <label className="block text-sm font-medium mb-1">Start Date *</label>
                        <input
                          type="date"
                          value={newRented.rent_start_date}
                          onChange={(e) => setNewRented({...newRented, rent_start_date: e.target.value})}
                          className="w-full h-10 rounded border px-3"
                          required
                        />
                      </div>
                      <div>
                        <label className="block text-sm font-medium mb-1">End Date</label>
                        <input
                          type="date"
                          value={newRented.rent_end_date}
                          onChange={(e) => setNewRented({...newRented, rent_end_date: e.target.value})}
                          className="w-full h-10 rounded border px-3"
                        />
                      </div>
                    </div>
                    <div className="flex gap-3 pt-2">
                      <Button type="submit" className="flex-1 bg-teal-600 hover:bg-teal-700">Add Rented Asset</Button>
                      <Button type="button" variant="outline" className="flex-1" onClick={() => setShowAddModal(false)}>Cancel</Button>
                    </div>
                  </form>
                )}
              </CardContent>
            </Card>
          </div>
        )}
      </div>
    </div>
  );
};

export default AssetsDashboard;
