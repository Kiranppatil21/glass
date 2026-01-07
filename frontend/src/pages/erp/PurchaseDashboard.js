import React, { useState, useEffect } from 'react';
import { Card, CardContent } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { 
  ShoppingCart, Plus, Clock, CheckCircle, Truck, XCircle,
  Building, Package, Filter, Loader, FileText, IndianRupee
} from 'lucide-react';
import { toast } from 'sonner';
import erpApi from '../../utils/erpApi';

const PurchaseDashboard = () => {
  const [orders, setOrders] = useState([]);
  const [suppliers, setSuppliers] = useState([]);
  const [materials, setMaterials] = useState([]);
  const [activeTab, setActiveTab] = useState('orders');
  const [loading, setLoading] = useState(true);
  const [statusFilter, setStatusFilter] = useState('all');
  const [showCreatePO, setShowCreatePO] = useState(false);
  const [showAddSupplier, setShowAddSupplier] = useState(false);
  
  const [newPO, setNewPO] = useState({
    supplier_id: '',
    supplier_name: '',
    items: [],
    expected_delivery: '',
    notes: ''
  });

  const [newSupplier, setNewSupplier] = useState({
    name: '',
    contact_person: '',
    email: '',
    phone: '',
    address: '',
    gst_number: '',
    payment_terms: 'Net 30'
  });

  const [newItem, setNewItem] = useState({
    material_id: '',
    material_name: '',
    quantity: 0,
    unit_price: 0
  });

  useEffect(() => {
    fetchOrders();
    fetchSuppliers();
    fetchMaterials();
  }, []);

  const fetchOrders = async () => {
    try {
      const response = await erpApi.purchase.getPOs();
      setOrders(response.data || []);
    } catch (error) {
      console.error('Failed to fetch POs:', error);
    } finally {
      setLoading(false);
    }
  };

  const fetchSuppliers = async () => {
    try {
      const response = await erpApi.purchase.getSuppliers();
      setSuppliers(response.data || []);
    } catch (error) {
      console.error('Failed to fetch suppliers:', error);
    }
  };

  const fetchMaterials = async () => {
    try {
      const response = await erpApi.inventory.getMaterials();
      setMaterials(response.data || []);
    } catch (error) {
      console.error('Failed to fetch materials:', error);
    }
  };

  const handleCreatePO = async (e) => {
    e.preventDefault();
    if (newPO.items.length === 0) {
      toast.error('Add at least one item to the PO');
      return;
    }
    try {
      const response = await erpApi.purchase.createPO(newPO);
      toast.success(`PO ${response.data.po_number} created!`);
      setShowCreatePO(false);
      setNewPO({ supplier_id: '', supplier_name: '', items: [], expected_delivery: '', notes: '' });
      fetchOrders();
    } catch (error) {
      console.error('Failed to create PO:', error);
      toast.error('Failed to create PO');
    }
  };

  const handleAddSupplier = async (e) => {
    e.preventDefault();
    try {
      await erpApi.purchase.createSupplier(newSupplier);
      toast.success('Supplier added!');
      setShowAddSupplier(false);
      setNewSupplier({
        name: '',
        contact_person: '',
        email: '',
        phone: '',
        address: '',
        gst_number: '',
        payment_terms: 'Net 30'
      });
      fetchSuppliers();
    } catch (error) {
      console.error('Failed to add supplier:', error);
      toast.error('Failed to add supplier');
    }
  };

  const handleStatusUpdate = async (poId, newStatus) => {
    try {
      await erpApi.purchase.updatePOStatus(poId, newStatus);
      toast.success(`PO status updated to ${newStatus}`);
      fetchOrders();
      if (newStatus === 'received') {
        // Refresh inventory after receiving
        fetchMaterials();
      }
    } catch (error) {
      console.error('Failed to update status:', error);
      toast.error('Failed to update status');
    }
  };

  const addItemToPO = () => {
    if (!newItem.material_id || newItem.quantity <= 0) {
      toast.error('Select material and enter quantity');
      return;
    }
    setNewPO({
      ...newPO,
      items: [...newPO.items, { ...newItem }]
    });
    setNewItem({ material_id: '', material_name: '', quantity: 0, unit_price: 0 });
  };

  const removeItemFromPO = (index) => {
    setNewPO({
      ...newPO,
      items: newPO.items.filter((_, i) => i !== index)
    });
  };

  const calculatePOTotal = () => {
    const subtotal = newPO.items.reduce((sum, item) => sum + (item.quantity * item.unit_price), 0);
    const gst = subtotal * 0.18;
    return { subtotal, gst, total: subtotal + gst };
  };

  const statusConfig = {
    pending: { color: 'bg-yellow-100 text-yellow-700', icon: Clock, label: 'Pending' },
    approved: { color: 'bg-blue-100 text-blue-700', icon: CheckCircle, label: 'Approved' },
    ordered: { color: 'bg-purple-100 text-purple-700', icon: ShoppingCart, label: 'Ordered' },
    received: { color: 'bg-green-100 text-green-700', icon: Package, label: 'Received' },
    cancelled: { color: 'bg-red-100 text-red-700', icon: XCircle, label: 'Cancelled' }
  };

  const filteredOrders = statusFilter === 'all' 
    ? orders 
    : orders.filter(o => o.status === statusFilter);

  const stats = {
    total: orders.length,
    pending: orders.filter(o => o.status === 'pending').length,
    ordered: orders.filter(o => o.status === 'ordered').length,
    totalValue: orders.filter(o => o.status !== 'cancelled').reduce((sum, o) => sum + o.total, 0)
  };

  return (
    <div className="min-h-screen py-20 bg-slate-50" data-testid="purchase-dashboard">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between mb-8">
          <div>
            <h1 className="text-4xl font-bold text-slate-900 mb-2">Purchase Orders</h1>
            <p className="text-slate-600">Manage suppliers and purchase orders</p>
          </div>
          <div className="flex gap-3">
            <Button 
              variant="outline"
              onClick={() => setShowAddSupplier(true)}
            >
              <Building className="w-4 h-4 mr-2" />
              Add Supplier
            </Button>
            <Button 
              onClick={() => setShowCreatePO(true)}
              className="bg-primary-700 hover:bg-primary-800"
              data-testid="create-po-btn"
            >
              <Plus className="w-4 h-4 mr-2" />
              Create PO
            </Button>
          </div>
        </div>

        {/* Stats */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-8">
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-slate-600">Total POs</p>
                  <p className="text-3xl font-bold text-slate-900">{stats.total}</p>
                </div>
                <FileText className="w-10 h-10 text-blue-500" />
              </div>
            </CardContent>
          </Card>
          <Card className={stats.pending > 0 ? 'border-yellow-200 bg-yellow-50' : ''}>
            <CardContent className="p-4">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-slate-600">Pending Approval</p>
                  <p className={`text-3xl font-bold ${stats.pending > 0 ? 'text-yellow-600' : 'text-slate-900'}`}>
                    {stats.pending}
                  </p>
                </div>
                <Clock className="w-10 h-10 text-yellow-500" />
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-slate-600">In Transit</p>
                  <p className="text-3xl font-bold text-purple-600">{stats.ordered}</p>
                </div>
                <Truck className="w-10 h-10 text-purple-500" />
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-slate-600">Total Value</p>
                  <p className="text-2xl font-bold text-green-600">₹{stats.totalValue.toLocaleString()}</p>
                </div>
                <IndianRupee className="w-10 h-10 text-green-500" />
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Tabs */}
        <div className="flex gap-4 mb-6 border-b border-slate-200">
          <button
            onClick={() => setActiveTab('orders')}
            className={`pb-4 px-4 font-medium transition-colors border-b-2 ${
              activeTab === 'orders'
                ? 'border-primary-600 text-primary-600'
                : 'border-transparent text-slate-600 hover:text-slate-900'
            }`}
          >
            Purchase Orders
          </button>
          <button
            onClick={() => setActiveTab('suppliers')}
            className={`pb-4 px-4 font-medium transition-colors border-b-2 ${
              activeTab === 'suppliers'
                ? 'border-primary-600 text-primary-600'
                : 'border-transparent text-slate-600 hover:text-slate-900'
            }`}
          >
            Suppliers ({suppliers.length})
          </button>
        </div>

        {/* Orders Tab */}
        {activeTab === 'orders' && (
          <>
            {/* Status Filter */}
            <div className="flex items-center gap-3 mb-6">
              <Filter className="w-5 h-5 text-slate-500" />
              {['all', 'pending', 'approved', 'ordered', 'received'].map(status => (
                <button
                  key={status}
                  onClick={() => setStatusFilter(status)}
                  className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                    statusFilter === status
                      ? 'bg-primary-700 text-white'
                      : 'bg-white text-slate-700 hover:bg-slate-100'
                  }`}
                >
                  {status.charAt(0).toUpperCase() + status.slice(1)}
                </button>
              ))}
            </div>

            {/* Orders List */}
            <div className="space-y-4">
              {filteredOrders.map((po) => {
                const statusInfo = statusConfig[po.status];
                const StatusIcon = statusInfo?.icon || Clock;

                return (
                  <Card key={po.id} className="hover:shadow-lg transition-shadow">
                    <CardContent className="p-6">
                      <div className="flex items-start justify-between mb-4">
                        <div>
                          <div className="flex items-center gap-3 mb-2">
                            <h3 className="text-xl font-bold text-slate-900">{po.po_number}</h3>
                            <span className={`px-3 py-1 rounded-full text-sm font-medium flex items-center gap-1 ${statusInfo?.color}`}>
                              <StatusIcon className="w-4 h-4" />
                              {statusInfo?.label}
                            </span>
                          </div>
                          <p className="text-slate-600 flex items-center gap-2">
                            <Building className="w-4 h-4" />
                            {po.supplier_name || 'Unknown Supplier'}
                          </p>
                        </div>
                        <div className="text-right">
                          <p className="text-sm text-slate-600">Total Amount</p>
                          <p className="text-2xl font-bold text-green-600">₹{po.total?.toLocaleString()}</p>
                        </div>
                      </div>

                      {/* Items */}
                      <div className="bg-slate-50 rounded-lg p-4 mb-4">
                        <p className="text-sm font-medium text-slate-700 mb-2">Items ({po.items?.length || 0})</p>
                        <div className="space-y-2">
                          {po.items?.slice(0, 3).map((item, idx) => (
                            <div key={idx} className="flex justify-between text-sm">
                              <span>{item.material_name}</span>
                              <span className="font-medium">{item.quantity} × ₹{item.unit_price}</span>
                            </div>
                          ))}
                          {po.items?.length > 3 && (
                            <p className="text-xs text-slate-500">+{po.items.length - 3} more items</p>
                          )}
                        </div>
                      </div>

                      {/* Actions */}
                      <div className="flex items-center justify-between">
                        <p className="text-sm text-slate-500">
                          Created: {new Date(po.created_at).toLocaleDateString()}
                          {po.expected_delivery && ` • Expected: ${po.expected_delivery}`}
                        </p>
                        <div className="flex gap-2">
                          {po.status === 'pending' && (
                            <>
                              <Button
                                size="sm"
                                className="bg-green-600 hover:bg-green-700"
                                onClick={() => handleStatusUpdate(po.id, 'approved')}
                              >
                                Approve
                              </Button>
                              <Button
                                size="sm"
                                variant="outline"
                                className="border-red-500 text-red-500"
                                onClick={() => handleStatusUpdate(po.id, 'cancelled')}
                              >
                                Cancel
                              </Button>
                            </>
                          )}
                          {po.status === 'approved' && (
                            <Button
                              size="sm"
                              className="bg-purple-600 hover:bg-purple-700"
                              onClick={() => handleStatusUpdate(po.id, 'ordered')}
                            >
                              Mark as Ordered
                            </Button>
                          )}
                          {po.status === 'ordered' && (
                            <Button
                              size="sm"
                              className="bg-green-600 hover:bg-green-700"
                              onClick={() => handleStatusUpdate(po.id, 'received')}
                            >
                              <Package className="w-4 h-4 mr-1" />
                              Mark Received
                            </Button>
                          )}
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                );
              })}
            </div>

            {filteredOrders.length === 0 && !loading && (
              <div className="text-center py-12">
                <FileText className="w-16 h-16 text-slate-300 mx-auto mb-4" />
                <p className="text-slate-600">No purchase orders found.</p>
              </div>
            )}
          </>
        )}

        {/* Suppliers Tab */}
        {activeTab === 'suppliers' && (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {suppliers.map((supplier) => (
              <Card key={supplier.id} className="hover:shadow-lg transition-shadow">
                <CardContent className="p-6">
                  <div className="flex items-start gap-4 mb-4">
                    <div className="w-12 h-12 bg-primary-100 rounded-full flex items-center justify-center">
                      <Building className="w-6 h-6 text-primary-700" />
                    </div>
                    <div>
                      <h3 className="font-semibold text-slate-900">{supplier.name}</h3>
                      {supplier.contact_person && (
                        <p className="text-sm text-slate-600">{supplier.contact_person}</p>
                      )}
                    </div>
                  </div>

                  <div className="space-y-2 text-sm">
                    {supplier.phone && (
                      <p className="text-slate-600">📞 {supplier.phone}</p>
                    )}
                    {supplier.email && (
                      <p className="text-slate-600">📧 {supplier.email}</p>
                    )}
                    {supplier.gst_number && (
                      <p className="text-slate-600">GST: {supplier.gst_number}</p>
                    )}
                    <p className="text-slate-600">Terms: {supplier.payment_terms}</p>
                  </div>
                </CardContent>
              </Card>
            ))}

            {suppliers.length === 0 && (
              <div className="col-span-3 text-center py-12">
                <Building className="w-16 h-16 text-slate-300 mx-auto mb-4" />
                <p className="text-slate-600">No suppliers yet. Add your first supplier.</p>
              </div>
            )}
          </div>
        )}

        {/* Create PO Modal */}
        {showCreatePO && (
          <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
            <Card className="max-w-2xl w-full max-h-[90vh] overflow-y-auto">
              <CardContent className="p-8">
                <h2 className="text-2xl font-bold text-slate-900 mb-6">Create Purchase Order</h2>
                <form onSubmit={handleCreatePO} className="space-y-6">
                  {/* Supplier Selection */}
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-2">Supplier *</label>
                    <select
                      value={newPO.supplier_id}
                      onChange={(e) => {
                        const supplier = suppliers.find(s => s.id === e.target.value);
                        setNewPO({
                          ...newPO,
                          supplier_id: e.target.value,
                          supplier_name: supplier?.name || ''
                        });
                      }}
                      className="w-full h-12 rounded-lg border border-slate-300 px-4"
                      required
                    >
                      <option value="">Select Supplier</option>
                      {suppliers.map(s => (
                        <option key={s.id} value={s.id}>{s.name}</option>
                      ))}
                    </select>
                  </div>

                  {/* Add Items */}
                  <div className="border rounded-lg p-4">
                    <h3 className="font-medium text-slate-900 mb-4">Add Items</h3>
                    <div className="grid grid-cols-4 gap-3 mb-3">
                      <select
                        value={newItem.material_id}
                        onChange={(e) => {
                          const material = materials.find(m => m.id === e.target.value);
                          setNewItem({
                            ...newItem,
                            material_id: e.target.value,
                            material_name: material?.name || '',
                            unit_price: material?.unit_price || 0
                          });
                        }}
                        className="col-span-2 h-10 rounded border border-slate-300 px-3 text-sm"
                      >
                        <option value="">Select Material</option>
                        {materials.map(m => (
                          <option key={m.id} value={m.id}>{m.name}</option>
                        ))}
                      </select>
                      <input
                        type="number"
                        placeholder="Qty"
                        value={newItem.quantity || ''}
                        onChange={(e) => setNewItem({...newItem, quantity: parseFloat(e.target.value) || 0})}
                        className="h-10 rounded border border-slate-300 px-3 text-sm"
                        min="1"
                      />
                      <Button type="button" size="sm" onClick={addItemToPO}>
                        <Plus className="w-4 h-4" />
                      </Button>
                    </div>

                    {/* Items List */}
                    {newPO.items.length > 0 && (
                      <div className="bg-slate-50 rounded-lg p-3 space-y-2">
                        {newPO.items.map((item, idx) => (
                          <div key={idx} className="flex justify-between items-center text-sm">
                            <span>{item.material_name}</span>
                            <div className="flex items-center gap-3">
                              <span>{item.quantity} × ₹{item.unit_price} = ₹{(item.quantity * item.unit_price).toLocaleString()}</span>
                              <button
                                type="button"
                                onClick={() => removeItemFromPO(idx)}
                                className="text-red-500 hover:text-red-700"
                              >
                                <XCircle className="w-4 h-4" />
                              </button>
                            </div>
                          </div>
                        ))}
                        <div className="border-t pt-2 mt-2">
                          <div className="flex justify-between text-sm">
                            <span>Subtotal:</span>
                            <span>₹{calculatePOTotal().subtotal.toLocaleString()}</span>
                          </div>
                          <div className="flex justify-between text-sm">
                            <span>GST (18%):</span>
                            <span>₹{calculatePOTotal().gst.toLocaleString()}</span>
                          </div>
                          <div className="flex justify-between font-bold text-lg mt-1">
                            <span>Total:</span>
                            <span className="text-green-600">₹{calculatePOTotal().total.toLocaleString()}</span>
                          </div>
                        </div>
                      </div>
                    )}
                  </div>

                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Expected Delivery</label>
                      <input
                        type="date"
                        value={newPO.expected_delivery}
                        onChange={(e) => setNewPO({...newPO, expected_delivery: e.target.value})}
                        className="w-full h-12 rounded-lg border border-slate-300 px-4"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Notes</label>
                      <input
                        type="text"
                        value={newPO.notes}
                        onChange={(e) => setNewPO({...newPO, notes: e.target.value})}
                        className="w-full h-12 rounded-lg border border-slate-300 px-4"
                        placeholder="Optional"
                      />
                    </div>
                  </div>

                  <div className="flex gap-3 pt-4">
                    <Button type="submit" className="flex-1 bg-primary-700 hover:bg-primary-800">
                      Create Purchase Order
                    </Button>
                    <Button 
                      type="button"
                      variant="outline"
                      onClick={() => setShowCreatePO(false)}
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

        {/* Add Supplier Modal */}
        {showAddSupplier && (
          <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
            <Card className="max-w-lg w-full">
              <CardContent className="p-8">
                <h2 className="text-2xl font-bold text-slate-900 mb-6">Add Supplier</h2>
                <form onSubmit={handleAddSupplier} className="space-y-4">
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-2">Company Name *</label>
                    <input
                      type="text"
                      value={newSupplier.name}
                      onChange={(e) => setNewSupplier({...newSupplier, name: e.target.value})}
                      className="w-full h-12 rounded-lg border border-slate-300 px-4"
                      required
                    />
                  </div>

                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Contact Person</label>
                      <input
                        type="text"
                        value={newSupplier.contact_person}
                        onChange={(e) => setNewSupplier({...newSupplier, contact_person: e.target.value})}
                        className="w-full h-12 rounded-lg border border-slate-300 px-4"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Phone *</label>
                      <input
                        type="tel"
                        value={newSupplier.phone}
                        onChange={(e) => setNewSupplier({...newSupplier, phone: e.target.value})}
                        className="w-full h-12 rounded-lg border border-slate-300 px-4"
                        required
                      />
                    </div>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-2">Email</label>
                    <input
                      type="email"
                      value={newSupplier.email}
                      onChange={(e) => setNewSupplier({...newSupplier, email: e.target.value})}
                      className="w-full h-12 rounded-lg border border-slate-300 px-4"
                    />
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-2">GST Number</label>
                    <input
                      type="text"
                      value={newSupplier.gst_number}
                      onChange={(e) => setNewSupplier({...newSupplier, gst_number: e.target.value})}
                      className="w-full h-12 rounded-lg border border-slate-300 px-4"
                      placeholder="e.g., 27AABCU9603R1ZX"
                    />
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-2">Payment Terms</label>
                    <select
                      value={newSupplier.payment_terms}
                      onChange={(e) => setNewSupplier({...newSupplier, payment_terms: e.target.value})}
                      className="w-full h-12 rounded-lg border border-slate-300 px-4"
                    >
                      <option value="Advance">Advance</option>
                      <option value="COD">Cash on Delivery</option>
                      <option value="Net 15">Net 15 Days</option>
                      <option value="Net 30">Net 30 Days</option>
                      <option value="Net 45">Net 45 Days</option>
                    </select>
                  </div>

                  <div className="flex gap-3 pt-4">
                    <Button type="submit" className="flex-1 bg-primary-700 hover:bg-primary-800">
                      Add Supplier
                    </Button>
                    <Button 
                      type="button"
                      variant="outline"
                      onClick={() => setShowAddSupplier(false)}
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

export default PurchaseDashboard;
