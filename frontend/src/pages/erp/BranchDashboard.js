import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { Building2, Plus, Edit, Trash2, MapPin, Phone, Mail, Users, Package, ArrowRightLeft } from 'lucide-react';
import erpApi from '../../utils/erpApi';
import { toast } from 'sonner';

const BranchDashboard = () => {
  const [branches, setBranches] = useState([]);
  const [loading, setLoading] = useState(true);
  const [showModal, setShowModal] = useState(false);
  const [editBranch, setEditBranch] = useState(null);
  const [formData, setFormData] = useState({
    name: '', code: '', address: '', city: '', state: '', pincode: '',
    phone: '', email: '', is_warehouse: false, is_active: true
  });

  useEffect(() => { fetchBranches(); }, []);

  const fetchBranches = async () => {
    try {
      const res = await erpApi.branches.getAll();
      setBranches(res.data.branches || []);
    } catch (e) { console.error(e); }
    finally { setLoading(false); }
  };

  const handleSave = async () => {
    try {
      if (editBranch) await erpApi.branches.update(editBranch.id, formData);
      else await erpApi.branches.create(formData);
      toast.success(editBranch ? 'Branch updated' : 'Branch created');
      setShowModal(false);
      fetchBranches();
    } catch (e) { toast.error('Failed to save'); }
  };

  const handleDelete = async (id) => {
    if (!window.confirm('Delete this branch?')) return;
    try {
      await erpApi.branches.delete(id);
      toast.success('Branch deleted');
      fetchBranches();
    } catch (e) { toast.error('Failed to delete'); }
  };

  const openCreate = () => {
    setEditBranch(null);
    setFormData({ name: '', code: '', address: '', city: '', state: '', pincode: '', phone: '', email: '', is_warehouse: false, is_active: true });
    setShowModal(true);
  };

  const openEdit = (branch) => {
    setEditBranch(branch);
    setFormData({ ...branch });
    setShowModal(true);
  };

  if (loading) return <div className="p-6 text-center">Loading...</div>;

  return (
    <div className="p-6 space-y-6" data-testid="branch-dashboard">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-slate-900 flex items-center gap-3">
            <Building2 className="w-8 h-8 text-teal-600" />
            Branch Management
          </h1>
          <p className="text-slate-600">Manage multiple factory locations and warehouses</p>
        </div>
        <Button onClick={openCreate} className="bg-teal-600 hover:bg-teal-700 gap-2">
          <Plus className="w-4 h-4" /> Add Branch
        </Button>
      </div>

      {branches.length === 0 ? (
        <Card className="p-12 text-center">
          <Building2 className="w-16 h-16 text-slate-300 mx-auto mb-4" />
          <p className="text-slate-500">No branches yet. Add your first branch!</p>
        </Card>
      ) : (
        <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-6">
          {branches.map((branch) => (
            <Card key={branch.id} className={`hover:shadow-lg transition-shadow ${!branch.is_active ? 'opacity-60' : ''}`}>
              <CardHeader className="pb-2">
                <div className="flex justify-between items-start">
                  <div>
                    <CardTitle className="text-lg">{branch.name}</CardTitle>
                    <p className="text-sm text-slate-500 font-mono">{branch.code}</p>
                  </div>
                  <div className="flex gap-1">
                    {branch.is_warehouse && <span className="text-xs bg-purple-100 text-purple-700 px-2 py-1 rounded">Warehouse</span>}
                    <span className={`text-xs px-2 py-1 rounded ${branch.is_active ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'}`}>
                      {branch.is_active ? 'Active' : 'Inactive'}
                    </span>
                  </div>
                </div>
              </CardHeader>
              <CardContent>
                <div className="space-y-2 text-sm text-slate-600 mb-4">
                  <p className="flex items-center gap-2"><MapPin className="w-4 h-4" />{branch.address}, {branch.city}</p>
                  {branch.phone && <p className="flex items-center gap-2"><Phone className="w-4 h-4" />{branch.phone}</p>}
                  {branch.email && <p className="flex items-center gap-2"><Mail className="w-4 h-4" />{branch.email}</p>}
                </div>
                <div className="flex gap-2">
                  <Button onClick={() => openEdit(branch)} variant="outline" size="sm" className="flex-1">
                    <Edit className="w-4 h-4 mr-1" /> Edit
                  </Button>
                  <Button onClick={() => handleDelete(branch.id)} variant="outline" size="sm" className="text-red-600">
                    <Trash2 className="w-4 h-4" />
                  </Button>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      {showModal && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-xl max-w-lg w-full p-6">
            <h2 className="text-xl font-bold mb-4">{editBranch ? 'Edit Branch' : 'Add Branch'}</h2>
            <div className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium mb-1">Branch Name *</label>
                  <input type="text" value={formData.name} onChange={(e) => setFormData({ ...formData, name: e.target.value })} className="w-full px-3 py-2 border rounded-lg" />
                </div>
                <div>
                  <label className="block text-sm font-medium mb-1">Code *</label>
                  <input type="text" value={formData.code} onChange={(e) => setFormData({ ...formData, code: e.target.value.toUpperCase() })} className="w-full px-3 py-2 border rounded-lg" placeholder="e.g., MUM01" />
                </div>
              </div>
              <div>
                <label className="block text-sm font-medium mb-1">Address *</label>
                <input type="text" value={formData.address} onChange={(e) => setFormData({ ...formData, address: e.target.value })} className="w-full px-3 py-2 border rounded-lg" />
              </div>
              <div className="grid grid-cols-3 gap-4">
                <div>
                  <label className="block text-sm font-medium mb-1">City *</label>
                  <input type="text" value={formData.city} onChange={(e) => setFormData({ ...formData, city: e.target.value })} className="w-full px-3 py-2 border rounded-lg" />
                </div>
                <div>
                  <label className="block text-sm font-medium mb-1">State *</label>
                  <input type="text" value={formData.state} onChange={(e) => setFormData({ ...formData, state: e.target.value })} className="w-full px-3 py-2 border rounded-lg" />
                </div>
                <div>
                  <label className="block text-sm font-medium mb-1">Pincode *</label>
                  <input type="text" value={formData.pincode} onChange={(e) => setFormData({ ...formData, pincode: e.target.value })} className="w-full px-3 py-2 border rounded-lg" />
                </div>
              </div>
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium mb-1">Phone</label>
                  <input type="text" value={formData.phone} onChange={(e) => setFormData({ ...formData, phone: e.target.value })} className="w-full px-3 py-2 border rounded-lg" />
                </div>
                <div>
                  <label className="block text-sm font-medium mb-1">Email</label>
                  <input type="email" value={formData.email} onChange={(e) => setFormData({ ...formData, email: e.target.value })} className="w-full px-3 py-2 border rounded-lg" />
                </div>
              </div>
              <div className="flex gap-4">
                <label className="flex items-center gap-2">
                  <input type="checkbox" checked={formData.is_warehouse} onChange={(e) => setFormData({ ...formData, is_warehouse: e.target.checked })} />
                  <span className="text-sm">Is Warehouse</span>
                </label>
                <label className="flex items-center gap-2">
                  <input type="checkbox" checked={formData.is_active} onChange={(e) => setFormData({ ...formData, is_active: e.target.checked })} />
                  <span className="text-sm">Active</span>
                </label>
              </div>
            </div>
            <div className="flex justify-end gap-2 mt-6">
              <Button variant="outline" onClick={() => setShowModal(false)}>Cancel</Button>
              <Button onClick={handleSave} className="bg-teal-600 hover:bg-teal-700">Save Branch</Button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default BranchDashboard;
