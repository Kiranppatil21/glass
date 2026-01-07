import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { Settings2, Plus, Trash2, Edit, Palette, Layers, Ruler } from 'lucide-react';
import erpApi from '../../utils/erpApi';
import { toast } from 'sonner';

const ProductConfigDashboard = () => {
  const [activeTab, setActiveTab] = useState('thickness');
  const [loading, setLoading] = useState(true);
  const [thickness, setThickness] = useState([]);
  const [glassTypes, setGlassTypes] = useState([]);
  const [colors, setColors] = useState([]);
  const [showModal, setShowModal] = useState(false);
  const [formData, setFormData] = useState({});

  useEffect(() => { fetchConfig(); }, []);

  const fetchConfig = async () => {
    try {
      const res = await erpApi.config.getAll();
      setThickness(res.data.thickness_options || []);
      setGlassTypes(res.data.glass_types || []);
      setColors(res.data.colors || []);
    } catch (e) { console.error(e); }
    finally { setLoading(false); }
  };

  const handleSaveThickness = async () => {
    try {
      await erpApi.config.addThickness(formData);
      toast.success('Thickness saved');
      setShowModal(false);
      fetchConfig();
    } catch (e) { toast.error('Failed to save'); }
  };

  const handleDeleteThickness = async (value) => {
    if (!window.confirm('Delete this thickness?')) return;
    try {
      await erpApi.config.deleteThickness(value);
      toast.success('Deleted');
      fetchConfig();
    } catch (e) { toast.error('Failed to delete'); }
  };

  const handleSaveGlassType = async () => {
    try {
      await erpApi.config.addGlassType(formData);
      toast.success('Glass type saved');
      setShowModal(false);
      fetchConfig();
    } catch (e) { toast.error('Failed to save'); }
  };

  const handleDeleteGlassType = async (id) => {
    if (!window.confirm('Delete this glass type?')) return;
    try {
      await erpApi.config.deleteGlassType(id);
      toast.success('Deleted');
      fetchConfig();
    } catch (e) { toast.error('Failed to delete'); }
  };

  const handleSaveColor = async () => {
    try {
      await erpApi.config.addColor(formData);
      toast.success('Color saved');
      setShowModal(false);
      fetchConfig();
    } catch (e) { toast.error('Failed to save'); }
  };

  const handleDeleteColor = async (id) => {
    if (!window.confirm('Delete this color?')) return;
    try {
      await erpApi.config.deleteColor(id);
      toast.success('Deleted');
      fetchConfig();
    } catch (e) { toast.error('Failed to delete'); }
  };

  const openAddThickness = () => {
    setFormData({ value: '', label: '', active: true });
    setShowModal('thickness');
  };

  const openAddGlassType = () => {
    setFormData({ name: '', code: '', description: '', base_price_per_sqft: 0, thickness_options: [4,5,6,8,10,12], active: true });
    setShowModal('glass');
  };

  const openAddColor = () => {
    setFormData({ name: '', code: '', hex_code: '#000000', price_multiplier: 1.0, active: true });
    setShowModal('color');
  };

  if (loading) return <div className="p-6 text-center">Loading...</div>;

  return (
    <div className="p-6 space-y-6" data-testid="product-config">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-slate-900 flex items-center gap-3">
            <Settings2 className="w-8 h-8 text-teal-600" />
            Product Configuration
          </h1>
          <p className="text-slate-600">Manage glass thickness, types, and colors</p>
        </div>
      </div>

      {/* Tabs */}
      <div className="flex gap-2 border-b pb-2">
        <button onClick={() => setActiveTab('thickness')} className={`flex items-center gap-2 px-4 py-2 rounded-t-lg ${activeTab === 'thickness' ? 'bg-teal-600 text-white' : 'bg-slate-100'}`}>
          <Ruler className="w-4 h-4" /> Thickness
        </button>
        <button onClick={() => setActiveTab('glass')} className={`flex items-center gap-2 px-4 py-2 rounded-t-lg ${activeTab === 'glass' ? 'bg-teal-600 text-white' : 'bg-slate-100'}`}>
          <Layers className="w-4 h-4" /> Glass Types
        </button>
        <button onClick={() => setActiveTab('colors')} className={`flex items-center gap-2 px-4 py-2 rounded-t-lg ${activeTab === 'colors' ? 'bg-teal-600 text-white' : 'bg-slate-100'}`}>
          <Palette className="w-4 h-4" /> Colors
        </button>
      </div>

      {/* Thickness Tab */}
      {activeTab === 'thickness' && (
        <Card>
          <CardHeader>
            <div className="flex justify-between items-center">
              <CardTitle>Glass Thickness Options</CardTitle>
              <Button onClick={openAddThickness} className="bg-teal-600 hover:bg-teal-700 gap-2">
                <Plus className="w-4 h-4" /> Add Thickness
              </Button>
            </div>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-4">
              {thickness.map((t) => (
                <div key={t.value} className={`p-4 rounded-lg border-2 text-center ${t.active ? 'border-teal-200 bg-teal-50' : 'border-slate-200 bg-slate-50 opacity-50'}`}>
                  <p className="text-2xl font-bold text-slate-900">{t.value}mm</p>
                  <p className="text-sm text-slate-500">{t.label}</p>
                  <Button onClick={() => handleDeleteThickness(t.value)} variant="ghost" size="sm" className="mt-2 text-red-500">
                    <Trash2 className="w-4 h-4" />
                  </Button>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Glass Types Tab */}
      {activeTab === 'glass' && (
        <Card>
          <CardHeader>
            <div className="flex justify-between items-center">
              <CardTitle>Glass Types</CardTitle>
              <Button onClick={openAddGlassType} className="bg-teal-600 hover:bg-teal-700 gap-2">
                <Plus className="w-4 h-4" /> Add Glass Type
              </Button>
            </div>
          </CardHeader>
          <CardContent>
            {glassTypes.length === 0 ? (
              <p className="text-slate-500 text-center py-8">No glass types configured. Products from database will be used.</p>
            ) : (
              <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-4">
                {glassTypes.map((g) => (
                  <div key={g.id} className={`p-4 rounded-lg border ${g.active ? 'border-teal-200' : 'border-slate-200 opacity-50'}`}>
                    <div className="flex justify-between items-start">
                      <div>
                        <h3 className="font-bold text-slate-900">{g.name}</h3>
                        <p className="text-sm text-slate-500 font-mono">{g.code}</p>
                      </div>
                      <Button onClick={() => handleDeleteGlassType(g.id)} variant="ghost" size="sm" className="text-red-500">
                        <Trash2 className="w-4 h-4" />
                      </Button>
                    </div>
                    <p className="text-sm text-slate-600 mt-2">{g.description}</p>
                    <p className="text-sm font-medium text-teal-600 mt-2">₹{g.base_price_per_sqft}/sqft</p>
                    <div className="flex flex-wrap gap-1 mt-2">
                      {g.thickness_options?.map(t => (
                        <span key={t} className="text-xs bg-slate-100 px-2 py-1 rounded">{t}mm</span>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {/* Colors Tab */}
      {activeTab === 'colors' && (
        <Card>
          <CardHeader>
            <div className="flex justify-between items-center">
              <CardTitle>Color Options</CardTitle>
              <Button onClick={openAddColor} className="bg-teal-600 hover:bg-teal-700 gap-2">
                <Plus className="w-4 h-4" /> Add Color
              </Button>
            </div>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
              {colors.map((c) => (
                <div key={c.id} className={`p-4 rounded-lg border text-center ${c.active ? '' : 'opacity-50'}`}>
                  <div className="w-16 h-16 rounded-full mx-auto mb-2 border-4 border-white shadow-lg" style={{ backgroundColor: c.hex_code }}></div>
                  <p className="font-bold text-slate-900">{c.name}</p>
                  <p className="text-xs text-slate-500 font-mono">{c.code}</p>
                  <p className="text-xs text-teal-600 mt-1">{c.price_multiplier}x price</p>
                  <Button onClick={() => handleDeleteColor(c.id)} variant="ghost" size="sm" className="mt-2 text-red-500">
                    <Trash2 className="w-4 h-4" />
                  </Button>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Modal */}
      {showModal && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-xl max-w-md w-full p-6">
            <h2 className="text-xl font-bold mb-4">
              {showModal === 'thickness' ? 'Add Thickness' : showModal === 'glass' ? 'Add Glass Type' : 'Add Color'}
            </h2>
            
            {showModal === 'thickness' && (
              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium mb-1">Value (mm)</label>
                  <input type="number" step="0.5" value={formData.value} onChange={(e) => setFormData({ ...formData, value: parseFloat(e.target.value), label: `${e.target.value}mm` })} className="w-full px-3 py-2 border rounded-lg" />
                </div>
                <div className="flex gap-2 justify-end">
                  <Button variant="outline" onClick={() => setShowModal(false)}>Cancel</Button>
                  <Button onClick={handleSaveThickness} className="bg-teal-600">Save</Button>
                </div>
              </div>
            )}

            {showModal === 'glass' && (
              <div className="space-y-4">
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium mb-1">Name</label>
                    <input type="text" value={formData.name} onChange={(e) => setFormData({ ...formData, name: e.target.value })} className="w-full px-3 py-2 border rounded-lg" />
                  </div>
                  <div>
                    <label className="block text-sm font-medium mb-1">Code</label>
                    <input type="text" value={formData.code} onChange={(e) => setFormData({ ...formData, code: e.target.value.toUpperCase() })} className="w-full px-3 py-2 border rounded-lg" />
                  </div>
                </div>
                <div>
                  <label className="block text-sm font-medium mb-1">Description</label>
                  <input type="text" value={formData.description} onChange={(e) => setFormData({ ...formData, description: e.target.value })} className="w-full px-3 py-2 border rounded-lg" />
                </div>
                <div>
                  <label className="block text-sm font-medium mb-1">Base Price/sqft</label>
                  <input type="number" value={formData.base_price_per_sqft} onChange={(e) => setFormData({ ...formData, base_price_per_sqft: parseFloat(e.target.value) })} className="w-full px-3 py-2 border rounded-lg" />
                </div>
                <div className="flex gap-2 justify-end">
                  <Button variant="outline" onClick={() => setShowModal(false)}>Cancel</Button>
                  <Button onClick={handleSaveGlassType} className="bg-teal-600">Save</Button>
                </div>
              </div>
            )}

            {showModal === 'color' && (
              <div className="space-y-4">
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium mb-1">Name</label>
                    <input type="text" value={formData.name} onChange={(e) => setFormData({ ...formData, name: e.target.value })} className="w-full px-3 py-2 border rounded-lg" />
                  </div>
                  <div>
                    <label className="block text-sm font-medium mb-1">Code</label>
                    <input type="text" value={formData.code} onChange={(e) => setFormData({ ...formData, code: e.target.value.toUpperCase() })} className="w-full px-3 py-2 border rounded-lg" />
                  </div>
                </div>
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium mb-1">Hex Color</label>
                    <input type="color" value={formData.hex_code} onChange={(e) => setFormData({ ...formData, hex_code: e.target.value })} className="w-full h-10 border rounded-lg" />
                  </div>
                  <div>
                    <label className="block text-sm font-medium mb-1">Price Multiplier</label>
                    <input type="number" step="0.05" value={formData.price_multiplier} onChange={(e) => setFormData({ ...formData, price_multiplier: parseFloat(e.target.value) })} className="w-full px-3 py-2 border rounded-lg" />
                  </div>
                </div>
                <div className="flex gap-2 justify-end">
                  <Button variant="outline" onClick={() => setShowModal(false)}>Cancel</Button>
                  <Button onClick={handleSaveColor} className="bg-teal-600">Save</Button>
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

export default ProductConfigDashboard;
