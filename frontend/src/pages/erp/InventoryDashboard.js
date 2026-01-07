import React, { useState, useEffect } from 'react';
import { Card, CardContent } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { 
  Package, Plus, AlertTriangle, ArrowUpCircle, ArrowDownCircle,
  Search, Filter, Loader, Box, TrendingDown, RefreshCw
} from 'lucide-react';
import { toast } from 'sonner';
import erpApi from '../../utils/erpApi';

const InventoryDashboard = () => {
  const [materials, setMaterials] = useState([]);
  const [transactions, setTransactions] = useState([]);
  const [activeTab, setActiveTab] = useState('materials');
  const [loading, setLoading] = useState(true);
  const [showAddMaterial, setShowAddMaterial] = useState(false);
  const [showTransaction, setShowTransaction] = useState(false);
  const [selectedMaterial, setSelectedMaterial] = useState(null);
  const [categoryFilter, setCategoryFilter] = useState('all');
  const [showLowStock, setShowLowStock] = useState(false);
  
  const [newMaterial, setNewMaterial] = useState({
    name: '',
    category: 'glass',
    unit: 'pcs',
    current_stock: 0,
    minimum_stock: 10,
    unit_price: 0,
    location: 'Main Store'
  });

  const [newTransaction, setNewTransaction] = useState({
    material_id: '',
    type: 'IN',
    quantity: 0,
    reference: '',
    notes: ''
  });

  useEffect(() => {
    fetchMaterials();
    fetchTransactions();
  }, []);

  const fetchMaterials = async () => {
    try {
      const response = await erpApi.inventory.getMaterials();
      setMaterials(response.data || []);
    } catch (error) {
      console.error('Failed to fetch materials:', error);
    } finally {
      setLoading(false);
    }
  };

  const fetchTransactions = async () => {
    try {
      const response = await erpApi.inventory.getTransactions();
      setTransactions(response.data || []);
    } catch (error) {
      console.error('Failed to fetch transactions:', error);
    }
  };

  const handleAddMaterial = async (e) => {
    e.preventDefault();
    try {
      await erpApi.inventory.createMaterial(newMaterial);
      toast.success('Material added successfully!');
      setShowAddMaterial(false);
      setNewMaterial({
        name: '',
        category: 'glass',
        unit: 'pcs',
        current_stock: 0,
        minimum_stock: 10,
        unit_price: 0,
        location: 'Main Store'
      });
      fetchMaterials();
    } catch (error) {
      console.error('Failed to add material:', error);
      toast.error('Failed to add material');
    }
  };

  const handleTransaction = async (e) => {
    e.preventDefault();
    try {
      const response = await erpApi.inventory.addTransaction(newTransaction);
      toast.success(`Stock ${newTransaction.type === 'IN' ? 'added' : 'removed'}! New stock: ${response.data.new_stock}`);
      setShowTransaction(false);
      setNewTransaction({ material_id: '', type: 'IN', quantity: 0, reference: '', notes: '' });
      setSelectedMaterial(null);
      fetchMaterials();
      fetchTransactions();
    } catch (error) {
      console.error('Failed to record transaction:', error);
      toast.error(error.response?.data?.detail || 'Failed to record transaction');
    }
  };

  const openTransactionModal = (material, type) => {
    setSelectedMaterial(material);
    setNewTransaction({
      material_id: material.id,
      type: type,
      quantity: 0,
      reference: '',
      notes: ''
    });
    setShowTransaction(true);
  };

  const categories = [
    { value: 'all', label: 'All Categories' },
    { value: 'glass', label: 'Glass' },
    { value: 'chemical', label: 'Chemicals' },
    { value: 'packing', label: 'Packing' },
    { value: 'spare', label: 'Spare Parts' }
  ];

  const filteredMaterials = materials.filter(m => {
    if (categoryFilter !== 'all' && m.category !== categoryFilter) return false;
    if (showLowStock && m.current_stock > m.minimum_stock) return false;
    return true;
  });

  const lowStockCount = materials.filter(m => m.current_stock <= m.minimum_stock).length;
  const totalValue = materials.reduce((sum, m) => sum + (m.current_stock * m.unit_price), 0);

  return (
    <div className="min-h-screen py-20 bg-slate-50" data-testid="inventory-dashboard">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between mb-8">
          <div>
            <h1 className="text-4xl font-bold text-slate-900 mb-2">Inventory Management</h1>
            <p className="text-slate-600">Track raw materials and stock levels</p>
          </div>
          <Button 
            onClick={() => setShowAddMaterial(true)}
            className="bg-primary-700 hover:bg-primary-800"
            data-testid="add-material-btn"
          >
            <Plus className="w-4 h-4 mr-2" />
            Add Material
          </Button>
        </div>

        {/* Stats */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-8">
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-slate-600">Total Items</p>
                  <p className="text-3xl font-bold text-slate-900">{materials.length}</p>
                </div>
                <Package className="w-10 h-10 text-blue-500" />
              </div>
            </CardContent>
          </Card>
          <Card className={lowStockCount > 0 ? 'border-red-200 bg-red-50' : ''}>
            <CardContent className="p-4">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-slate-600">Low Stock</p>
                  <p className={`text-3xl font-bold ${lowStockCount > 0 ? 'text-red-600' : 'text-slate-900'}`}>
                    {lowStockCount}
                  </p>
                </div>
                <AlertTriangle className={`w-10 h-10 ${lowStockCount > 0 ? 'text-red-500' : 'text-slate-300'}`} />
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-slate-600">Stock Value</p>
                  <p className="text-2xl font-bold text-green-600">₹{totalValue.toLocaleString()}</p>
                </div>
                <TrendingDown className="w-10 h-10 text-green-500" />
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-slate-600">Transactions</p>
                  <p className="text-3xl font-bold text-slate-900">{transactions.length}</p>
                </div>
                <RefreshCw className="w-10 h-10 text-purple-500" />
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Tabs */}
        <div className="flex gap-4 mb-6 border-b border-slate-200">
          <button
            onClick={() => setActiveTab('materials')}
            className={`pb-4 px-4 font-medium transition-colors border-b-2 ${
              activeTab === 'materials'
                ? 'border-primary-600 text-primary-600'
                : 'border-transparent text-slate-600 hover:text-slate-900'
            }`}
          >
            Materials
          </button>
          <button
            onClick={() => setActiveTab('transactions')}
            className={`pb-4 px-4 font-medium transition-colors border-b-2 ${
              activeTab === 'transactions'
                ? 'border-primary-600 text-primary-600'
                : 'border-transparent text-slate-600 hover:text-slate-900'
            }`}
          >
            Transactions
          </button>
        </div>

        {/* Materials Tab */}
        {activeTab === 'materials' && (
          <>
            {/* Filters */}
            <div className="flex items-center gap-4 mb-6">
              <Filter className="w-5 h-5 text-slate-500" />
              <div className="flex gap-2">
                {categories.map(cat => (
                  <button
                    key={cat.value}
                    onClick={() => setCategoryFilter(cat.value)}
                    className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                      categoryFilter === cat.value
                        ? 'bg-primary-700 text-white'
                        : 'bg-white text-slate-700 hover:bg-slate-100'
                    }`}
                  >
                    {cat.label}
                  </button>
                ))}
              </div>
              <button
                onClick={() => setShowLowStock(!showLowStock)}
                className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ml-auto ${
                  showLowStock
                    ? 'bg-red-600 text-white'
                    : 'bg-white text-slate-700 hover:bg-slate-100 border border-slate-200'
                }`}
              >
                <AlertTriangle className="w-4 h-4 inline mr-2" />
                Low Stock Only
              </button>
            </div>

            {/* Materials Grid */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {filteredMaterials.map((material) => {
                const isLowStock = material.current_stock <= material.minimum_stock;
                
                return (
                  <Card key={material.id} className={`hover:shadow-lg transition-shadow ${isLowStock ? 'border-red-300 bg-red-50' : ''}`}>
                    <CardContent className="p-6">
                      <div className="flex items-start justify-between mb-4">
                        <div className="flex items-center gap-3">
                          <div className={`w-12 h-12 rounded-lg flex items-center justify-center ${
                            material.category === 'glass' ? 'bg-blue-100' :
                            material.category === 'chemical' ? 'bg-yellow-100' :
                            material.category === 'packing' ? 'bg-green-100' : 'bg-slate-100'
                          }`}>
                            <Box className={`w-6 h-6 ${
                              material.category === 'glass' ? 'text-blue-600' :
                              material.category === 'chemical' ? 'text-yellow-600' :
                              material.category === 'packing' ? 'text-green-600' : 'text-slate-600'
                            }`} />
                          </div>
                          <div>
                            <h3 className="font-semibold text-slate-900">{material.name}</h3>
                            <p className="text-xs text-slate-500 capitalize">{material.category} • {material.location}</p>
                          </div>
                        </div>
                        {isLowStock && (
                          <span className="px-2 py-1 bg-red-100 text-red-700 text-xs font-bold rounded">
                            LOW
                          </span>
                        )}
                      </div>

                      <div className="grid grid-cols-2 gap-4 mb-4">
                        <div className="bg-slate-100 rounded-lg p-3">
                          <p className="text-xs text-slate-600">Current Stock</p>
                          <p className={`text-xl font-bold ${isLowStock ? 'text-red-600' : 'text-slate-900'}`}>
                            {material.current_stock} <span className="text-sm font-normal">{material.unit}</span>
                          </p>
                        </div>
                        <div className="bg-slate-100 rounded-lg p-3">
                          <p className="text-xs text-slate-600">Min. Level</p>
                          <p className="text-xl font-bold text-slate-900">
                            {material.minimum_stock} <span className="text-sm font-normal">{material.unit}</span>
                          </p>
                        </div>
                      </div>

                      <div className="text-sm text-slate-600 mb-4">
                        Unit Price: <span className="font-medium">₹{material.unit_price}</span>
                        <span className="float-right">
                          Value: <span className="font-medium text-green-600">₹{(material.current_stock * material.unit_price).toLocaleString()}</span>
                        </span>
                      </div>

                      <div className="flex gap-2">
                        <Button
                          variant="outline"
                          size="sm"
                          className="flex-1 border-green-500 text-green-600 hover:bg-green-50"
                          onClick={() => openTransactionModal(material, 'IN')}
                        >
                          <ArrowUpCircle className="w-4 h-4 mr-1" />
                          Stock In
                        </Button>
                        <Button
                          variant="outline"
                          size="sm"
                          className="flex-1 border-red-500 text-red-600 hover:bg-red-50"
                          onClick={() => openTransactionModal(material, 'OUT')}
                        >
                          <ArrowDownCircle className="w-4 h-4 mr-1" />
                          Stock Out
                        </Button>
                      </div>
                    </CardContent>
                  </Card>
                );
              })}
            </div>

            {filteredMaterials.length === 0 && !loading && (
              <div className="text-center py-12">
                <Package className="w-16 h-16 text-slate-300 mx-auto mb-4" />
                <p className="text-slate-600">No materials found. Add your first material.</p>
              </div>
            )}
          </>
        )}

        {/* Transactions Tab */}
        {activeTab === 'transactions' && (
          <div className="space-y-4">
            {transactions.map((txn) => (
              <Card key={txn.id}>
                <CardContent className="p-4">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-4">
                      <div className={`w-10 h-10 rounded-full flex items-center justify-center ${
                        txn.type === 'IN' ? 'bg-green-100' : txn.type === 'OUT' ? 'bg-red-100' : 'bg-blue-100'
                      }`}>
                        {txn.type === 'IN' ? (
                          <ArrowUpCircle className="w-5 h-5 text-green-600" />
                        ) : txn.type === 'OUT' ? (
                          <ArrowDownCircle className="w-5 h-5 text-red-600" />
                        ) : (
                          <RefreshCw className="w-5 h-5 text-blue-600" />
                        )}
                      </div>
                      <div>
                        <h4 className="font-medium text-slate-900">{txn.material_name}</h4>
                        <p className="text-sm text-slate-500">
                          {txn.reference && <span className="mr-2">Ref: {txn.reference}</span>}
                          {txn.notes}
                        </p>
                      </div>
                    </div>
                    <div className="text-right">
                      <p className={`text-lg font-bold ${txn.type === 'IN' ? 'text-green-600' : 'text-red-600'}`}>
                        {txn.type === 'IN' ? '+' : '-'}{txn.quantity}
                      </p>
                      <p className="text-xs text-slate-500">
                        {txn.previous_stock} → {txn.new_stock}
                      </p>
                    </div>
                    <div className="text-right ml-6">
                      <p className="text-sm text-slate-600">
                        {new Date(txn.created_at).toLocaleDateString()}
                      </p>
                      <p className="text-xs text-slate-500">
                        {new Date(txn.created_at).toLocaleTimeString()}
                      </p>
                    </div>
                  </div>
                </CardContent>
              </Card>
            ))}

            {transactions.length === 0 && (
              <div className="text-center py-12">
                <RefreshCw className="w-16 h-16 text-slate-300 mx-auto mb-4" />
                <p className="text-slate-600">No transactions yet.</p>
              </div>
            )}
          </div>
        )}

        {/* Add Material Modal */}
        {showAddMaterial && (
          <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
            <Card className="max-w-lg w-full">
              <CardContent className="p-8">
                <h2 className="text-2xl font-bold text-slate-900 mb-6">Add New Material</h2>
                <form onSubmit={handleAddMaterial} className="space-y-4">
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-2">Material Name *</label>
                    <input
                      type="text"
                      value={newMaterial.name}
                      onChange={(e) => setNewMaterial({...newMaterial, name: e.target.value})}
                      className="w-full h-12 rounded-lg border border-slate-300 px-4"
                      placeholder="e.g., Clear Float Glass 6mm"
                      required
                    />
                  </div>

                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Category</label>
                      <select
                        value={newMaterial.category}
                        onChange={(e) => setNewMaterial({...newMaterial, category: e.target.value})}
                        className="w-full h-12 rounded-lg border border-slate-300 px-4"
                      >
                        <option value="glass">Glass</option>
                        <option value="chemical">Chemical</option>
                        <option value="packing">Packing</option>
                        <option value="spare">Spare Parts</option>
                      </select>
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Unit</label>
                      <select
                        value={newMaterial.unit}
                        onChange={(e) => setNewMaterial({...newMaterial, unit: e.target.value})}
                        className="w-full h-12 rounded-lg border border-slate-300 px-4"
                      >
                        <option value="pcs">Pieces</option>
                        <option value="sqft">Sq. Ft</option>
                        <option value="kg">KG</option>
                        <option value="ltr">Liters</option>
                        <option value="box">Boxes</option>
                      </select>
                    </div>
                  </div>

                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Current Stock</label>
                      <input
                        type="number"
                        value={newMaterial.current_stock}
                        onChange={(e) => setNewMaterial({...newMaterial, current_stock: parseFloat(e.target.value) || 0})}
                        className="w-full h-12 rounded-lg border border-slate-300 px-4"
                        min="0"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Minimum Stock</label>
                      <input
                        type="number"
                        value={newMaterial.minimum_stock}
                        onChange={(e) => setNewMaterial({...newMaterial, minimum_stock: parseFloat(e.target.value) || 0})}
                        className="w-full h-12 rounded-lg border border-slate-300 px-4"
                        min="0"
                      />
                    </div>
                  </div>

                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Unit Price (₹)</label>
                      <input
                        type="number"
                        value={newMaterial.unit_price}
                        onChange={(e) => setNewMaterial({...newMaterial, unit_price: parseFloat(e.target.value) || 0})}
                        className="w-full h-12 rounded-lg border border-slate-300 px-4"
                        min="0"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Location</label>
                      <input
                        type="text"
                        value={newMaterial.location}
                        onChange={(e) => setNewMaterial({...newMaterial, location: e.target.value})}
                        className="w-full h-12 rounded-lg border border-slate-300 px-4"
                        placeholder="Main Store"
                      />
                    </div>
                  </div>

                  <div className="flex gap-3 pt-4">
                    <Button type="submit" className="flex-1 bg-primary-700 hover:bg-primary-800">
                      Add Material
                    </Button>
                    <Button 
                      type="button"
                      variant="outline"
                      onClick={() => setShowAddMaterial(false)}
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

        {/* Transaction Modal */}
        {showTransaction && selectedMaterial && (
          <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
            <Card className="max-w-md w-full">
              <CardContent className="p-8">
                <h2 className="text-2xl font-bold text-slate-900 mb-2">
                  {newTransaction.type === 'IN' ? 'Stock In' : 'Stock Out'}
                </h2>
                <p className="text-slate-600 mb-6">{selectedMaterial.name}</p>
                
                <div className="bg-slate-100 rounded-lg p-4 mb-6">
                  <p className="text-sm text-slate-600">Current Stock</p>
                  <p className="text-2xl font-bold text-slate-900">
                    {selectedMaterial.current_stock} {selectedMaterial.unit}
                  </p>
                </div>

                <form onSubmit={handleTransaction} className="space-y-4">
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-2">Quantity *</label>
                    <input
                      type="number"
                      value={newTransaction.quantity}
                      onChange={(e) => setNewTransaction({...newTransaction, quantity: parseFloat(e.target.value) || 0})}
                      className="w-full h-12 rounded-lg border border-slate-300 px-4"
                      min="0.01"
                      step="0.01"
                      required
                    />
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-2">Reference (PO/Job Card)</label>
                    <input
                      type="text"
                      value={newTransaction.reference}
                      onChange={(e) => setNewTransaction({...newTransaction, reference: e.target.value})}
                      className="w-full h-12 rounded-lg border border-slate-300 px-4"
                      placeholder="Optional"
                    />
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-2">Notes</label>
                    <input
                      type="text"
                      value={newTransaction.notes}
                      onChange={(e) => setNewTransaction({...newTransaction, notes: e.target.value})}
                      className="w-full h-12 rounded-lg border border-slate-300 px-4"
                      placeholder="Optional"
                    />
                  </div>

                  <div className={`rounded-lg p-4 ${newTransaction.type === 'IN' ? 'bg-green-50' : 'bg-red-50'}`}>
                    <p className="text-sm text-slate-600">New Stock After Transaction</p>
                    <p className={`text-2xl font-bold ${newTransaction.type === 'IN' ? 'text-green-600' : 'text-red-600'}`}>
                      {newTransaction.type === 'IN' 
                        ? selectedMaterial.current_stock + (newTransaction.quantity || 0)
                        : selectedMaterial.current_stock - (newTransaction.quantity || 0)
                      } {selectedMaterial.unit}
                    </p>
                  </div>

                  <div className="flex gap-3 pt-4">
                    <Button 
                      type="submit" 
                      className={`flex-1 ${newTransaction.type === 'IN' ? 'bg-green-600 hover:bg-green-700' : 'bg-red-600 hover:bg-red-700'}`}
                    >
                      {newTransaction.type === 'IN' ? 'Add Stock' : 'Remove Stock'}
                    </Button>
                    <Button 
                      type="button"
                      variant="outline"
                      onClick={() => {
                        setShowTransaction(false);
                        setSelectedMaterial(null);
                      }}
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

export default InventoryDashboard;
