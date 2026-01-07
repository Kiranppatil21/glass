import React, { useState, useEffect } from 'react';
import { Card, CardContent } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { 
  Plus, Phone, Mail, Building, User, Calendar, 
  TrendingUp, DollarSign, CheckCircle, XCircle,
  Clock, Filter, ChevronDown
} from 'lucide-react';
import { toast } from 'sonner';
import erpApi from '../../utils/erpApi';

const CRMDashboard = () => {
  const [leads, setLeads] = useState([]);
  const [selectedStatus, setSelectedStatus] = useState('all');
  const [showAddLead, setShowAddLead] = useState(false);
  const [loading, setLoading] = useState(true);
  const [openStatusDropdown, setOpenStatusDropdown] = useState(null);
  const [newLead, setNewLead] = useState({
    name: '',
    email: '',
    phone: '',
    company: '',
    customer_type: 'retail',
    source: 'website',
    enquiry_details: '',
    expected_value: 0
  });

  useEffect(() => {
    fetchLeads();
  }, [selectedStatus]);

  const fetchLeads = async () => {
    try {
      const params = selectedStatus !== 'all' ? { status: selectedStatus } : {};
      const response = await erpApi.crm.getLeads(params);
      setLeads(response.data || []);
    } catch (error) {
      console.error('Failed to fetch leads:', error);
      toast.error('Failed to load leads');
    } finally {
      setLoading(false);
    }
  };

  const handleCreateLead = async (e) => {
    e.preventDefault();
    try {
      await erpApi.crm.createLead(newLead);
      toast.success('Lead created successfully!');
      setShowAddLead(false);
      setNewLead({
        name: '',
        email: '',
        phone: '',
        company: '',
        customer_type: 'retail',
        source: 'website',
        enquiry_details: '',
        expected_value: 0
      });
      fetchLeads();
    } catch (error) {
      console.error('Failed to create lead:', error);
      toast.error('Failed to create lead');
    }
  };

  const handleStatusChange = async (leadId, newStatus) => {
    try {
      await erpApi.crm.updateLeadStatus(leadId, newStatus);
      toast.success(`Status updated to ${newStatus}`);
      setOpenStatusDropdown(null);
      fetchLeads();
    } catch (error) {
      console.error('Failed to update status:', error);
      toast.error('Failed to update status');
    }
  };

  const statusColors = {
    new: 'bg-blue-100 text-blue-700 border-blue-200',
    contacted: 'bg-yellow-100 text-yellow-700 border-yellow-200',
    quoted: 'bg-purple-100 text-purple-700 border-purple-200',
    negotiation: 'bg-orange-100 text-orange-700 border-orange-200',
    won: 'bg-green-100 text-green-700 border-green-200',
    lost: 'bg-red-100 text-red-700 border-red-200'
  };

  const statusIcons = {
    new: Clock,
    contacted: Phone,
    quoted: DollarSign,
    negotiation: TrendingUp,
    won: CheckCircle,
    lost: XCircle
  };

  const allStatuses = ['new', 'contacted', 'quoted', 'negotiation', 'won', 'lost'];

  const customerTypeIcons = {
    retail: User,
    dealer: Building,
    project: Building,
    architect: User
  };

  const stats = {
    total: leads.length,
    new: leads.filter(l => l.status === 'new').length,
    quoted: leads.filter(l => l.status === 'quoted').length,
    won: leads.filter(l => l.status === 'won').length,
    totalValue: leads.reduce((sum, l) => sum + (l.expected_value || 0), 0)
  };

  return (
    <div className="min-h-screen py-20 bg-slate-50" data-testid="crm-dashboard">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between mb-8">
          <div>
            <h1 className="text-4xl font-bold text-slate-900 mb-2">CRM & Sales Pipeline</h1>
            <p className="text-slate-600">Manage leads and track conversions</p>
          </div>
          <Button 
            onClick={() => setShowAddLead(true)}
            className="bg-primary-700 hover:bg-primary-800"
            data-testid="add-lead-btn"
          >
            <Plus className="w-4 h-4 mr-2" />
            Add Lead
          </Button>
        </div>

        {/* Stats */}
        <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mb-8">
          <Card>
            <CardContent className="p-4 text-center">
              <p className="text-sm text-slate-600 mb-1">Total Leads</p>
              <p className="text-3xl font-bold text-slate-900">{stats.total}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4 text-center">
              <p className="text-sm text-slate-600 mb-1">New</p>
              <p className="text-3xl font-bold text-blue-600">{stats.new}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4 text-center">
              <p className="text-sm text-slate-600 mb-1">Quoted</p>
              <p className="text-3xl font-bold text-purple-600">{stats.quoted}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4 text-center">
              <p className="text-sm text-slate-600 mb-1">Won</p>
              <p className="text-3xl font-bold text-green-600">{stats.won}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4 text-center">
              <p className="text-sm text-slate-600 mb-1">Pipeline Value</p>
              <p className="text-2xl font-bold text-primary-700">₹{stats.totalValue.toLocaleString()}</p>
            </CardContent>
          </Card>
        </div>

        {/* Filters */}
        <div className="flex items-center gap-3 mb-6">
          <Filter className="w-5 h-5 text-slate-600" />
          <div className="flex gap-2">
            {['all', 'new', 'contacted', 'quoted', 'negotiation', 'won', 'lost'].map(status => (
              <button
                key={status}
                onClick={() => setSelectedStatus(status)}
                className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                  selectedStatus === status
                    ? 'bg-primary-700 text-white'
                    : 'bg-white text-slate-700 hover:bg-slate-100'
                }`}
              >
                {status.charAt(0).toUpperCase() + status.slice(1)}
              </button>
            ))}
          </div>
        </div>

        {/* Leads List */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {leads.map((lead) => {
            const StatusIcon = statusIcons[lead.status];
            const CustomerIcon = customerTypeIcons[lead.customer_type];
            
            return (
              <Card key={lead.id} className="hover:shadow-lg transition-shadow">
                <CardContent className="p-6">
                  <div className="flex items-start justify-between mb-4">
                    <div className="flex items-center gap-3">
                      <div className="w-12 h-12 bg-primary-100 rounded-full flex items-center justify-center">
                        <CustomerIcon className="w-6 h-6 text-primary-700" />
                      </div>
                      <div>
                        <h3 className="font-semibold text-slate-900">{lead.name}</h3>
                        <p className="text-sm text-slate-600 capitalize">{lead.customer_type}</p>
                      </div>
                    </div>
                    {/* Status Dropdown */}
                    <div className="relative">
                      <button
                        onClick={() => setOpenStatusDropdown(openStatusDropdown === lead.id ? null : lead.id)}
                        className={`px-3 py-1 rounded-full text-xs font-medium flex items-center gap-1 border cursor-pointer hover:opacity-80 transition-opacity ${statusColors[lead.status]}`}
                        data-testid={`status-btn-${lead.id}`}
                      >
                        <StatusIcon className="w-3 h-3" />
                        {lead.status}
                        <ChevronDown className="w-3 h-3 ml-1" />
                      </button>
                      
                      {openStatusDropdown === lead.id && (
                        <div className="absolute right-0 top-full mt-1 w-40 bg-white rounded-lg shadow-lg border z-10">
                          {allStatuses.map((status) => {
                            const Icon = statusIcons[status];
                            return (
                              <button
                                key={status}
                                onClick={() => handleStatusChange(lead.id, status)}
                                className={`w-full px-3 py-2 text-left text-sm flex items-center gap-2 hover:bg-slate-50 first:rounded-t-lg last:rounded-b-lg ${
                                  lead.status === status ? 'bg-slate-100 font-medium' : ''
                                }`}
                              >
                                <Icon className="w-4 h-4" />
                                <span className="capitalize">{status}</span>
                              </button>
                            );
                          })}
                        </div>
                      )}
                    </div>
                  </div>

                  {lead.company && (
                    <div className="flex items-center gap-2 text-sm text-slate-600 mb-2">
                      <Building className="w-4 h-4" />
                      {lead.company}
                    </div>
                  )}

                  <div className="flex items-center gap-2 text-sm text-slate-600 mb-2">
                    <Phone className="w-4 h-4" />
                    {lead.phone}
                  </div>

                  {lead.email && (
                    <div className="flex items-center gap-2 text-sm text-slate-600 mb-4">
                      <Mail className="w-4 h-4" />
                      {lead.email}
                    </div>
                  )}

                  {lead.expected_value > 0 && (
                    <div className="bg-green-50 rounded-lg p-3 mb-4">
                      <p className="text-xs text-slate-600 mb-1">Expected Value</p>
                      <p className="text-lg font-bold text-green-700">₹{lead.expected_value.toLocaleString()}</p>
                    </div>
                  )}

                  {lead.enquiry_details && (
                    <p className="text-sm text-slate-600 mb-4 line-clamp-2">{lead.enquiry_details}</p>
                  )}

                  <div className="flex items-center gap-2 text-xs text-slate-500">
                    <Calendar className="w-3 h-3" />
                    {new Date(lead.created_at).toLocaleDateString()}
                  </div>
                </CardContent>
              </Card>
            );
          })}
        </div>

        {leads.length === 0 && !loading && (
          <div className="text-center py-12">
            <User className="w-16 h-16 text-slate-300 mx-auto mb-4" />
            <p className="text-slate-600">No leads found. Start by adding a new lead.</p>
          </div>
        )}

        {/* Add Lead Modal */}
        {showAddLead && (
          <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
            <Card className="max-w-2xl w-full max-h-[90vh] overflow-y-auto">
              <CardContent className="p-8">
                <h2 className="text-2xl font-bold text-slate-900 mb-6">Add New Lead</h2>
                <form onSubmit={handleCreateLead} className="space-y-4">
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Name *</label>
                      <input
                        type="text"
                        value={newLead.name}
                        onChange={(e) => setNewLead({...newLead, name: e.target.value})}
                        className="w-full h-12 rounded-lg border-slate-300 px-4"
                        required
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Phone *</label>
                      <input
                        type="tel"
                        value={newLead.phone}
                        onChange={(e) => setNewLead({...newLead, phone: e.target.value})}
                        className="w-full h-12 rounded-lg border-slate-300 px-4"
                        required
                      />
                    </div>
                  </div>

                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Email</label>
                      <input
                        type="email"
                        value={newLead.email}
                        onChange={(e) => setNewLead({...newLead, email: e.target.value})}
                        className="w-full h-12 rounded-lg border-slate-300 px-4"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Company</label>
                      <input
                        type="text"
                        value={newLead.company}
                        onChange={(e) => setNewLead({...newLead, company: e.target.value})}
                        className="w-full h-12 rounded-lg border-slate-300 px-4"
                      />
                    </div>
                  </div>

                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Customer Type</label>
                      <select
                        value={newLead.customer_type}
                        onChange={(e) => setNewLead({...newLead, customer_type: e.target.value})}
                        className="w-full h-12 rounded-lg border-slate-300 px-4"
                      >
                        <option value="retail">Retail</option>
                        <option value="dealer">Dealer</option>
                        <option value="project">Project</option>
                        <option value="architect">Architect</option>
                      </select>
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Expected Value (₹)</label>
                      <input
                        type="number"
                        value={newLead.expected_value}
                        onChange={(e) => setNewLead({...newLead, expected_value: parseFloat(e.target.value) || 0})}
                        className="w-full h-12 rounded-lg border-slate-300 px-4"
                      />
                    </div>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-2">Enquiry Details</label>
                    <textarea
                      value={newLead.enquiry_details}
                      onChange={(e) => setNewLead({...newLead, enquiry_details: e.target.value})}
                      className="w-full h-24 rounded-lg border-slate-300 px-4 py-3"
                      placeholder="Customer requirements, project details, etc."
                    />
                  </div>

                  <div className="flex gap-3 pt-4">
                    <Button type="submit" className="flex-1 bg-primary-700 hover:bg-primary-800">
                      Create Lead
                    </Button>
                    <Button 
                      type="button"
                      variant="outline"
                      onClick={() => setShowAddLead(false)}
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

export default CRMDashboard;
