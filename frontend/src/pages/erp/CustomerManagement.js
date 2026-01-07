import React, { useState, useEffect, useCallback } from 'react';
import { 
  Users, Plus, Search, Building2, User, Phone, Mail, MapPin, 
  CreditCard, FileText, Shield, Edit, Eye, Ban, RefreshCw, 
  ChevronDown, ChevronRight, IndianRupee, Calendar, CheckCircle2,
  AlertCircle, Clock, Truck, X, Save, Loader2
} from 'lucide-react';
import { erpApi } from '../../utils/erpApi';

// =============== CONSTANTS ===============

const CUSTOMER_TYPES = [
  { value: 'individual', label: 'Individual / Retail' },
  { value: 'proprietor', label: 'Proprietor' },
  { value: 'partnership', label: 'Partnership' },
  { value: 'pvt_ltd', label: 'Pvt Ltd' },
  { value: 'ltd', label: 'Ltd' },
  { value: 'builder', label: 'Builder' },
  { value: 'dealer', label: 'Dealer' },
  { value: 'architect', label: 'Architect' },
];

const GST_TYPES = [
  { value: 'regular', label: 'Regular' },
  { value: 'composition', label: 'Composition' },
  { value: 'unregistered', label: 'Unregistered' },
];

const CUSTOMER_CATEGORIES = [
  { value: 'retail', label: 'Retail' },
  { value: 'builder', label: 'Builder' },
  { value: 'dealer', label: 'Dealer' },
  { value: 'project', label: 'Project' },
];

const CREDIT_TYPES = [
  { value: 'advance_only', label: 'Advance Only' },
  { value: 'credit_allowed', label: 'Credit Allowed' },
];

const CREDIT_DAYS_OPTIONS = [0, 7, 15, 30, 45, 60, 90];

const KYC_STATUS_COLORS = {
  pending: 'bg-yellow-100 text-yellow-800',
  verified: 'bg-green-100 text-green-800',
  rejected: 'bg-red-100 text-red-800',
};


// =============== MAIN COMPONENT ===============

const CustomerManagement = () => {
  const [activeTab, setActiveTab] = useState('list');
  const [customers, setCustomers] = useState([]);
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(true);
  const [selectedCustomer, setSelectedCustomer] = useState(null);
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [showViewModal, setShowViewModal] = useState(false);
  const [showEditModal, setShowEditModal] = useState(false);
  
  // Filters
  const [search, setSearch] = useState('');
  const [filterType, setFilterType] = useState('');
  const [filterCategory, setFilterCategory] = useState('');
  const [filterStatus, setFilterStatus] = useState('active');
  const [page, setPage] = useState(1);
  const [pagination, setPagination] = useState({ total: 0, total_pages: 1 });
  
  // States list
  const [states, setStates] = useState([]);

  // =============== DATA FETCHING ===============

  const fetchCustomers = useCallback(async () => {
    try {
      setLoading(true);
      const params = { page, limit: 20 };
      if (search) params.search = search;
      if (filterType) params.customer_type = filterType;
      if (filterCategory) params.category = filterCategory;
      if (filterStatus) params.status = filterStatus;
      
      const response = await erpApi.customerMaster.list(params);
      setCustomers(response.data.profiles || []);
      setPagination({
        total: response.data.total,
        total_pages: response.data.total_pages
      });
    } catch (error) {
      console.error('Error fetching customers:', error);
    } finally {
      setLoading(false);
    }
  }, [page, search, filterType, filterCategory, filterStatus]);

  const fetchStats = async () => {
    try {
      const response = await erpApi.customerMaster.getStats();
      setStats(response.data);
    } catch (error) {
      console.error('Error fetching stats:', error);
    }
  };

  const fetchStates = async () => {
    try {
      const response = await erpApi.customerMaster.getStates();
      setStates(response.data || []);
    } catch (error) {
      console.error('Error fetching states:', error);
    }
  };

  useEffect(() => {
    fetchCustomers();
    fetchStats();
    fetchStates();
  }, [fetchCustomers]);

  // =============== HANDLERS ===============

  const handleViewCustomer = async (customerId) => {
    try {
      const response = await erpApi.customerMaster.get(customerId);
      setSelectedCustomer(response.data);
      setShowViewModal(true);
    } catch (error) {
      console.error('Error fetching customer details:', error);
    }
  };

  const handleEditCustomer = (customer) => {
    setSelectedCustomer(customer);
    setShowEditModal(true);
  };

  const handleDeactivate = async (customerId) => {
    if (!window.confirm('Are you sure you want to deactivate this customer?')) return;
    try {
      await erpApi.customerMaster.deactivate(customerId);
      fetchCustomers();
      fetchStats();
    } catch (error) {
      alert(error.response?.data?.detail || 'Error deactivating customer');
    }
  };

  const handleReactivate = async (customerId) => {
    try {
      await erpApi.customerMaster.reactivate(customerId);
      fetchCustomers();
      fetchStats();
    } catch (error) {
      alert(error.response?.data?.detail || 'Error reactivating customer');
    }
  };

  const handleMigrate = async () => {
    if (!window.confirm('This will migrate existing customer data. Continue?')) return;
    try {
      setLoading(true);
      const response = await erpApi.customerMaster.migrate();
      alert(`Migration complete: ${response.data.migrated} migrated, ${response.data.skipped} skipped`);
      fetchCustomers();
      fetchStats();
    } catch (error) {
      alert(error.response?.data?.detail || 'Error during migration');
    } finally {
      setLoading(false);
    }
  };

  // =============== RENDER ===============

  return (
    <div className="p-6 space-y-6" data-testid="customer-management">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900 flex items-center gap-2">
            <Users className="w-7 h-7 text-blue-600" />
            Customer Profile / Master
          </h1>
          <p className="text-gray-500 text-sm mt-1">
            Comprehensive customer data management with GST, billing, and credit control
          </p>
        </div>
        <div className="flex gap-2">
          <button
            onClick={handleMigrate}
            className="px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 flex items-center gap-2"
            data-testid="migrate-btn"
          >
            <RefreshCw className="w-4 h-4" />
            Migrate Existing
          </button>
          <button
            onClick={() => setShowCreateModal(true)}
            className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 flex items-center gap-2"
            data-testid="add-customer-btn"
          >
            <Plus className="w-4 h-4" />
            Add Customer
          </button>
        </div>
      </div>

      {/* Stats Cards */}
      {stats && (
        <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-4">
          <StatCard 
            title="Total Customers" 
            value={stats.total_active} 
            icon={Users} 
            color="blue"
          />
          <StatCard 
            title="B2B Customers" 
            value={stats.invoice_type?.b2b || 0} 
            icon={Building2} 
            color="purple"
          />
          <StatCard 
            title="B2C Customers" 
            value={stats.invoice_type?.b2c || 0} 
            icon={User} 
            color="green"
          />
          <StatCard 
            title="Credit Customers" 
            value={stats.credit_customers} 
            icon={CreditCard} 
            color="orange"
          />
          <StatCard 
            title="KYC Verified" 
            value={stats.kyc?.verified || 0} 
            icon={CheckCircle2} 
            color="teal"
          />
          <StatCard 
            title="KYC Pending" 
            value={stats.kyc?.pending || 0} 
            icon={Clock} 
            color="yellow"
          />
        </div>
      )}

      {/* Filters */}
      <div className="bg-white rounded-lg shadow p-4">
        <div className="flex flex-wrap gap-4">
          <div className="flex-1 min-w-[200px]">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
              <input
                type="text"
                placeholder="Search by name, mobile, GSTIN, code..."
                value={search}
                onChange={(e) => { setSearch(e.target.value); setPage(1); }}
                className="w-full pl-10 pr-4 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500"
                data-testid="search-input"
              />
            </div>
          </div>
          <select
            value={filterType}
            onChange={(e) => { setFilterType(e.target.value); setPage(1); }}
            className="px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500"
            data-testid="filter-type"
          >
            <option value="">All Types</option>
            {CUSTOMER_TYPES.map(t => (
              <option key={t.value} value={t.value}>{t.label}</option>
            ))}
          </select>
          <select
            value={filterCategory}
            onChange={(e) => { setFilterCategory(e.target.value); setPage(1); }}
            className="px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500"
            data-testid="filter-category"
          >
            <option value="">All Categories</option>
            {CUSTOMER_CATEGORIES.map(c => (
              <option key={c.value} value={c.value}>{c.label}</option>
            ))}
          </select>
          <select
            value={filterStatus}
            onChange={(e) => { setFilterStatus(e.target.value); setPage(1); }}
            className="px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500"
            data-testid="filter-status"
          >
            <option value="active">Active</option>
            <option value="inactive">Inactive</option>
            <option value="">All</option>
          </select>
        </div>
      </div>

      {/* Customer List */}
      <div className="bg-white rounded-lg shadow overflow-hidden">
        {loading ? (
          <div className="p-8 text-center">
            <Loader2 className="w-8 h-8 animate-spin mx-auto text-blue-600" />
            <p className="mt-2 text-gray-500">Loading customers...</p>
          </div>
        ) : customers.length === 0 ? (
          <div className="p-8 text-center text-gray-500">
            No customers found. Click "Add Customer" to create one.
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full" data-testid="customer-table">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Code</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Customer</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Contact</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">GSTIN</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Category</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Credit</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">KYC</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Actions</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {customers.map((customer) => (
                  <tr key={customer.id} className="hover:bg-gray-50">
                    <td className="px-4 py-3">
                      <span className="font-mono text-sm text-blue-600">{customer.customer_code}</span>
                    </td>
                    <td className="px-4 py-3">
                      <div className="flex items-center gap-2">
                        {customer.invoice_type === 'B2B' ? (
                          <Building2 className="w-4 h-4 text-purple-500" />
                        ) : (
                          <User className="w-4 h-4 text-green-500" />
                        )}
                        <div>
                          <p className="font-medium text-gray-900">{customer.display_name}</p>
                          <p className="text-xs text-gray-500 capitalize">{customer.customer_type?.replace('_', ' ')}</p>
                        </div>
                      </div>
                    </td>
                    <td className="px-4 py-3">
                      <div className="text-sm">
                        <p className="flex items-center gap-1">
                          <Phone className="w-3 h-3 text-gray-400" />
                          {customer.mobile}
                        </p>
                        {customer.email && (
                          <p className="flex items-center gap-1 text-gray-500">
                            <Mail className="w-3 h-3 text-gray-400" />
                            {customer.email}
                          </p>
                        )}
                      </div>
                    </td>
                    <td className="px-4 py-3">
                      {customer.gstin ? (
                        <span className="font-mono text-sm">{customer.gstin}</span>
                      ) : (
                        <span className="text-gray-400 text-sm">N/A</span>
                      )}
                    </td>
                    <td className="px-4 py-3">
                      <span className={`px-2 py-1 text-xs rounded-full capitalize ${
                        customer.customer_category === 'dealer' ? 'bg-purple-100 text-purple-700' :
                        customer.customer_category === 'builder' ? 'bg-blue-100 text-blue-700' :
                        customer.customer_category === 'project' ? 'bg-orange-100 text-orange-700' :
                        'bg-gray-100 text-gray-700'
                      }`}>
                        {customer.customer_category}
                      </span>
                    </td>
                    <td className="px-4 py-3">
                      <div className="text-sm">
                        {customer.credit_type === 'credit_allowed' ? (
                          <>
                            <p className="text-green-600 font-medium">₹{customer.credit_limit?.toLocaleString()}</p>
                            <p className="text-xs text-gray-500">{customer.credit_days} days</p>
                          </>
                        ) : (
                          <span className="text-gray-500">Advance Only</span>
                        )}
                      </div>
                    </td>
                    <td className="px-4 py-3">
                      <span className={`px-2 py-1 text-xs rounded-full capitalize ${
                        KYC_STATUS_COLORS[customer.compliance?.kyc_status] || 'bg-gray-100'
                      }`}>
                        {customer.compliance?.kyc_status || 'pending'}
                      </span>
                    </td>
                    <td className="px-4 py-3">
                      <div className="flex items-center gap-1">
                        <button
                          onClick={() => handleViewCustomer(customer.id)}
                          className="p-1 hover:bg-blue-100 rounded text-blue-600"
                          title="View Details"
                          data-testid={`view-btn-${customer.id}`}
                        >
                          <Eye className="w-4 h-4" />
                        </button>
                        <button
                          onClick={() => handleEditCustomer(customer)}
                          className="p-1 hover:bg-green-100 rounded text-green-600"
                          title="Edit"
                          data-testid={`edit-btn-${customer.id}`}
                        >
                          <Edit className="w-4 h-4" />
                        </button>
                        {customer.status === 'active' ? (
                          <button
                            onClick={() => handleDeactivate(customer.id)}
                            className="p-1 hover:bg-red-100 rounded text-red-600"
                            title="Deactivate"
                            data-testid={`deactivate-btn-${customer.id}`}
                          >
                            <Ban className="w-4 h-4" />
                          </button>
                        ) : (
                          <button
                            onClick={() => handleReactivate(customer.id)}
                            className="p-1 hover:bg-green-100 rounded text-green-600"
                            title="Reactivate"
                            data-testid={`reactivate-btn-${customer.id}`}
                          >
                            <RefreshCw className="w-4 h-4" />
                          </button>
                        )}
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {/* Pagination */}
        {pagination.total_pages > 1 && (
          <div className="px-4 py-3 border-t flex items-center justify-between">
            <p className="text-sm text-gray-500">
              Showing {((page - 1) * 20) + 1} - {Math.min(page * 20, pagination.total)} of {pagination.total}
            </p>
            <div className="flex gap-2">
              <button
                onClick={() => setPage(p => Math.max(1, p - 1))}
                disabled={page === 1}
                className="px-3 py-1 border rounded disabled:opacity-50"
              >
                Previous
              </button>
              <button
                onClick={() => setPage(p => Math.min(pagination.total_pages, p + 1))}
                disabled={page >= pagination.total_pages}
                className="px-3 py-1 border rounded disabled:opacity-50"
              >
                Next
              </button>
            </div>
          </div>
        )}
      </div>

      {/* Modals */}
      {showCreateModal && (
        <CustomerFormModal
          onClose={() => setShowCreateModal(false)}
          onSuccess={() => { setShowCreateModal(false); fetchCustomers(); fetchStats(); }}
          states={states}
        />
      )}

      {showViewModal && selectedCustomer && (
        <CustomerViewModal
          customer={selectedCustomer}
          onClose={() => { setShowViewModal(false); setSelectedCustomer(null); }}
          onEdit={() => { setShowViewModal(false); setShowEditModal(true); }}
        />
      )}

      {showEditModal && selectedCustomer && (
        <CustomerFormModal
          customer={selectedCustomer}
          onClose={() => { setShowEditModal(false); setSelectedCustomer(null); }}
          onSuccess={() => { setShowEditModal(false); setSelectedCustomer(null); fetchCustomers(); fetchStats(); }}
          states={states}
        />
      )}
    </div>
  );
};


// =============== STAT CARD ===============

const StatCard = ({ title, value, icon: Icon, color }) => {
  const colors = {
    blue: 'bg-blue-50 text-blue-600',
    purple: 'bg-purple-50 text-purple-600',
    green: 'bg-green-50 text-green-600',
    orange: 'bg-orange-50 text-orange-600',
    teal: 'bg-teal-50 text-teal-600',
    yellow: 'bg-yellow-50 text-yellow-600',
  };

  return (
    <div className="bg-white rounded-lg shadow p-4">
      <div className="flex items-center justify-between">
        <div>
          <p className="text-sm text-gray-500">{title}</p>
          <p className="text-2xl font-bold text-gray-900">{value}</p>
        </div>
        <div className={`p-2 rounded-lg ${colors[color]}`}>
          <Icon className="w-5 h-5" />
        </div>
      </div>
    </div>
  );
};


// =============== CUSTOMER FORM MODAL ===============

const CustomerFormModal = ({ customer, onClose, onSuccess, states }) => {
  const isEdit = !!customer;
  const [saving, setSaving] = useState(false);
  const [activeSection, setActiveSection] = useState('basic');
  const [errors, setErrors] = useState({});
  
  // Form state
  const [form, setForm] = useState({
    customer_type: customer?.customer_type || 'individual',
    company_name: customer?.company_name || '',
    individual_name: customer?.individual_name || '',
    contact_person: customer?.contact_person || '',
    mobile: customer?.mobile || '',
    email: customer?.email || '',
    needs_gst_invoice: customer?.needs_gst_invoice || !!customer?.gstin || false,
    gstin: customer?.gstin || '',
    pan: customer?.pan || '',
    place_of_supply: customer?.place_of_supply || '',
    gst_type: customer?.gst_type || 'unregistered',
    billing_address: customer?.billing_address || {
      address_line1: '',
      address_line2: '',
      city: '',
      state: '',
      state_code: '',
      pin_code: '',
      country: 'India'
    },
    customer_category: customer?.customer_category || 'retail',
    credit_type: customer?.credit_type || 'advance_only',
    credit_limit: customer?.credit_limit || 0,
    credit_days: customer?.credit_days || 0,
    bank_details: customer?.bank_details || {
      account_holder_name: '',
      bank_name: '',
      account_number: '',
      ifsc_code: '',
      upi_id: ''
    },
    invoice_preferences: customer?.invoice_preferences || {
      language: 'english',
      email_invoice: true,
      whatsapp_invoice: false,
      po_mandatory: false
    },
    compliance: customer?.compliance || {
      gst_declaration: false,
      data_consent: false,
      terms_accepted: false,
      kyc_status: 'pending'
    },
    crm_details: customer?.crm_details || {
      sales_person_id: '',
      sales_person_name: '',
      source: '',
      notes: ''
    }
  });

  // Determine if company type (always B2B)
  const isCompanyType = ['pvt_ltd', 'ltd'].includes(form.customer_type);
  // Show GST checkbox for non-company types
  const showGstCheckbox = !isCompanyType;
  // Show GST fields when checkbox checked or company type
  const showGstFields = form.needs_gst_invoice || isCompanyType;
  // Show individual name when not needing GST and not company type
  const showIndividualName = !form.needs_gst_invoice && !isCompanyType;

  const handleChange = (field, value) => {
    setForm(prev => ({ ...prev, [field]: value }));
    setErrors(prev => ({ ...prev, [field]: null }));
  };

  const handleNestedChange = (parent, field, value) => {
    setForm(prev => ({
      ...prev,
      [parent]: { ...prev[parent], [field]: value }
    }));
  };

  const validateForm = () => {
    const newErrors = {};
    
    // Mobile validation
    if (!form.mobile || !/^[6-9]\d{9}$/.test(form.mobile)) {
      newErrors.mobile = 'Valid 10-digit mobile required';
    }
    
    // Name validation based on GST requirement
    if (showGstFields) {
      // B2B - Company name required
      if (!form.company_name) {
        newErrors.company_name = 'Company name required for GST Invoice';
      }
      if (!form.gstin) {
        newErrors.gstin = 'GSTIN required for GST Invoice';
      }
      if (!form.pan) {
        newErrors.pan = 'PAN required for GST Invoice';
      }
    } else {
      // B2C - Individual name required
      if (!form.individual_name) {
        newErrors.individual_name = 'Customer name required';
      }
    }
    
    // GSTIN validation
    if (form.gstin && !/^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z]{1}[1-9A-Z]{1}Z[0-9A-Z]{1}$/i.test(form.gstin)) {
      newErrors.gstin = 'Invalid GSTIN format';
    }
    
    // PAN validation
    if (form.pan && !/^[A-Z]{5}[0-9]{4}[A-Z]{1}$/i.test(form.pan)) {
      newErrors.pan = 'Invalid PAN format';
    }
    
    // Billing address
    if (!form.billing_address?.address_line1) {
      newErrors.billing_address_line1 = 'Billing address required';
    }
    if (!form.billing_address?.city) {
      newErrors.billing_city = 'City required';
    }
    if (!form.billing_address?.state) {
      newErrors.billing_state = 'State required';
    }
    if (!form.billing_address?.pin_code) {
      newErrors.billing_pin = 'PIN code required';
    }
    
    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleSubmit = async () => {
    if (!validateForm()) return;
    
    setSaving(true);
    try {
      if (isEdit) {
        await erpApi.customerMaster.update(customer.id, form);
      } else {
        await erpApi.customerMaster.create(form);
      }
      onSuccess();
    } catch (error) {
      const msg = error.response?.data?.detail || 'Error saving customer';
      alert(msg);
    } finally {
      setSaving(false);
    }
  };

  const sections = [
    { id: 'basic', label: '1. Basic Identity', icon: User },
    { id: 'gst', label: '2. GST & Tax', icon: FileText },
    { id: 'address', label: '3. Addresses', icon: MapPin },
    { id: 'credit', label: '4. Credit Control', icon: CreditCard },
    { id: 'bank', label: '5. Bank Details', icon: IndianRupee },
    { id: 'invoice', label: '6. Invoice Prefs', icon: FileText },
    { id: 'compliance', label: '7. Compliance', icon: Shield },
    { id: 'crm', label: '8. CRM', icon: Users },
  ];

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4" data-testid="customer-form-modal">
      <div className="bg-white rounded-xl w-full max-w-4xl max-h-[90vh] overflow-hidden flex flex-col">
        {/* Header */}
        <div className="px-6 py-4 border-b flex items-center justify-between bg-gradient-to-r from-blue-600 to-blue-700">
          <h2 className="text-xl font-semibold text-white">
            {isEdit ? 'Edit Customer Profile' : 'Add New Customer'}
          </h2>
          <button onClick={onClose} className="text-white/80 hover:text-white">
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Section Tabs */}
        <div className="border-b bg-gray-50 px-4 py-2 overflow-x-auto">
          <div className="flex gap-1">
            {sections.map(section => (
              <button
                key={section.id}
                onClick={() => setActiveSection(section.id)}
                className={`px-3 py-2 text-sm rounded-lg whitespace-nowrap flex items-center gap-1 transition-colors ${
                  activeSection === section.id
                    ? 'bg-blue-600 text-white'
                    : 'text-gray-600 hover:bg-gray-200'
                }`}
              >
                <section.icon className="w-4 h-4" />
                <span className="hidden sm:inline">{section.label}</span>
              </button>
            ))}
          </div>
        </div>

        {/* Form Content */}
        <div className="flex-1 overflow-y-auto p-6">
          {/* Section 1: Basic Identity */}
          {activeSection === 'basic' && (
            <div className="space-y-4">
              <h3 className="font-semibold text-gray-800 border-b pb-2">Basic Identity Details</h3>
              
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Customer Type <span className="text-red-500">*</span>
                  </label>
                  <select
                    value={form.customer_type}
                    onChange={(e) => handleChange('customer_type', e.target.value)}
                    className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500"
                    data-testid="customer-type-select"
                  >
                    {CUSTOMER_TYPES.map(t => (
                      <option key={t.value} value={t.value}>{t.label}</option>
                    ))}
                  </select>
                </div>

                {/* Company Name - Show when GST needed OR company type */}
                {showGstFields && (
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Company / Firm Name <span className="text-red-500">*</span>
                    </label>
                    <input
                      type="text"
                      value={form.company_name}
                      onChange={(e) => handleChange('company_name', e.target.value)}
                      className={`w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 ${errors.company_name ? 'border-red-500' : ''}`}
                      placeholder="Company Name"
                      data-testid="company-name-input"
                    />
                    {errors.company_name && <p className="text-red-500 text-xs mt-1">{errors.company_name}</p>}
                  </div>
                )}

                {/* Individual Name - Show when NOT needing GST */}
                {showIndividualName && (
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Customer Name <span className="text-red-500">*</span>
                    </label>
                    <input
                      type="text"
                      value={form.individual_name}
                      onChange={(e) => handleChange('individual_name', e.target.value)}
                      className={`w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 ${errors.individual_name ? 'border-red-500' : ''}`}
                      placeholder="Customer Name"
                      data-testid="individual-name-input"
                    />
                    {errors.individual_name && <p className="text-red-500 text-xs mt-1">{errors.individual_name}</p>}
                  </div>
                )}

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Contact Person</label>
                  <input
                    type="text"
                    value={form.contact_person}
                    onChange={(e) => handleChange('contact_person', e.target.value)}
                    className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500"
                    placeholder="Contact Person Name"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Mobile Number <span className="text-red-500">*</span>
                  </label>
                  <input
                    type="tel"
                    value={form.mobile}
                    onChange={(e) => handleChange('mobile', e.target.value.replace(/\D/g, '').slice(0, 10))}
                    className={`w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 ${errors.mobile ? 'border-red-500' : ''}`}
                    placeholder="10-digit mobile"
                    data-testid="mobile-input"
                  />
                  {errors.mobile && <p className="text-red-500 text-xs mt-1">{errors.mobile}</p>}
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Email ID</label>
                  <input
                    type="email"
                    value={form.email}
                    onChange={(e) => handleChange('email', e.target.value)}
                    className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500"
                    placeholder="email@example.com"
                    data-testid="email-input"
                  />
                </div>
              </div>

              <div className="mt-4 p-3 bg-blue-50 rounded-lg text-sm text-blue-800">
                <p><strong>Note:</strong> Company name is auto-picked for invoices. If GSTIN is provided, invoice will be B2B.</p>
              </div>
            </div>
          )}

          {/* Section 2: GST & Tax */}
          {activeSection === 'gst' && (
            <div className="space-y-4">
              <h3 className="font-semibold text-gray-800 border-b pb-2">GST & Tax Details</h3>
              
              {/* GST Checkbox - Show for non-company types */}
              {showGstCheckbox && (
                <div className="p-4 bg-blue-50 rounded-lg border border-blue-200">
                  <label className="flex items-center gap-3 cursor-pointer">
                    <input
                      type="checkbox"
                      checked={form.needs_gst_invoice || false}
                      onChange={(e) => {
                        const checked = e.target.checked;
                        handleChange('needs_gst_invoice', checked);
                        if (checked) {
                          handleChange('gst_type', 'regular');
                        } else {
                          handleChange('gst_type', 'unregistered');
                        }
                      }}
                      className="w-5 h-5 rounded border-blue-400 text-blue-600 focus:ring-blue-500"
                      data-testid="needs-gst-checkbox"
                    />
                    <div>
                      <span className="font-medium text-blue-800">Do you have GSTIN / Do you need GST Invoice?</span>
                      <p className="text-xs text-blue-600 mt-0.5">
                        {form.needs_gst_invoice 
                          ? '✓ B2B Invoice - GST details required' 
                          : 'B2C Invoice - Individual name billing'}
                      </p>
                    </div>
                  </label>
                </div>
              )}

              {/* Company type note */}
              {isCompanyType && (
                <div className="p-3 bg-purple-50 rounded-lg text-sm text-purple-700">
                  <strong>Company Account:</strong> GST Invoice (B2B) is mandatory for Pvt Ltd / Ltd companies
                </div>
              )}

              {/* GST Fields - Show only when needed */}
              {showGstFields ? (
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4 p-4 bg-gray-50 rounded-lg border">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">GSTIN Number <span className="text-red-500">*</span></label>
                    <input
                      type="text"
                      value={form.gstin}
                      onChange={(e) => handleChange('gstin', e.target.value.toUpperCase())}
                      className={`w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 font-mono ${errors.gstin ? 'border-red-500' : ''}`}
                      placeholder="22AAAAA0000A1Z5"
                      maxLength={15}
                      data-testid="gstin-input"
                    />
                    {errors.gstin && <p className="text-red-500 text-xs mt-1">{errors.gstin}</p>}
                    <p className="text-xs text-gray-500 mt-1">Format: State Code (2) + PAN (10) + Entity + Z + Checksum</p>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">PAN Number <span className="text-red-500">*</span></label>
                    <input
                      type="text"
                      value={form.pan}
                      onChange={(e) => handleChange('pan', e.target.value.toUpperCase())}
                      className={`w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 font-mono ${errors.pan ? 'border-red-500' : ''}`}
                      placeholder="AAAAA0000A"
                      maxLength={10}
                      data-testid="pan-input"
                    />
                    {errors.pan && <p className="text-red-500 text-xs mt-1">{errors.pan}</p>}
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">GST Type</label>
                    <select
                      value={form.gst_type}
                      onChange={(e) => handleChange('gst_type', e.target.value)}
                      className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500"
                      data-testid="gst-type-select"
                    >
                      <option value="regular">Regular</option>
                      <option value="composition">Composition</option>
                    </select>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Place of Supply</label>
                    <select
                      value={form.place_of_supply}
                      onChange={(e) => handleChange('place_of_supply', e.target.value)}
                      className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500"
                    >
                      <option value="">Select State</option>
                      {states.map(s => (
                        <option key={s.code} value={s.name}>{s.name}</option>
                      ))}
                    </select>
                  </div>
                </div>
              ) : (
                <div className="p-3 bg-green-50 rounded-lg text-sm text-green-700">
                  <strong>Retail Customer (B2C):</strong> Invoice will be generated in customer's individual name. GST included in price.
                </div>
              )}
            </div>
          )}

          {/* Section 3: Addresses */}
          {activeSection === 'address' && (
            <div className="space-y-6">
              <h3 className="font-semibold text-gray-800 border-b pb-2">Billing Address (Mandatory)</h3>
              
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div className="md:col-span-2">
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Address Line 1 <span className="text-red-500">*</span>
                  </label>
                  <input
                    type="text"
                    value={form.billing_address?.address_line1 || ''}
                    onChange={(e) => handleNestedChange('billing_address', 'address_line1', e.target.value)}
                    className={`w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 ${errors.billing_address_line1 ? 'border-red-500' : ''}`}
                    placeholder="Street Address"
                    data-testid="billing-address1-input"
                  />
                  {errors.billing_address_line1 && <p className="text-red-500 text-xs mt-1">{errors.billing_address_line1}</p>}
                </div>

                <div className="md:col-span-2">
                  <label className="block text-sm font-medium text-gray-700 mb-1">Address Line 2</label>
                  <input
                    type="text"
                    value={form.billing_address?.address_line2 || ''}
                    onChange={(e) => handleNestedChange('billing_address', 'address_line2', e.target.value)}
                    className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500"
                    placeholder="Area, Landmark"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    City <span className="text-red-500">*</span>
                  </label>
                  <input
                    type="text"
                    value={form.billing_address?.city || ''}
                    onChange={(e) => handleNestedChange('billing_address', 'city', e.target.value)}
                    className={`w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 ${errors.billing_city ? 'border-red-500' : ''}`}
                    placeholder="City"
                    data-testid="billing-city-input"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    State <span className="text-red-500">*</span>
                  </label>
                  <select
                    value={form.billing_address?.state || ''}
                    onChange={(e) => {
                      const selected = states.find(s => s.name === e.target.value);
                      handleNestedChange('billing_address', 'state', e.target.value);
                      handleNestedChange('billing_address', 'state_code', selected?.code || '');
                    }}
                    className={`w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 ${errors.billing_state ? 'border-red-500' : ''}`}
                    data-testid="billing-state-select"
                  >
                    <option value="">Select State</option>
                    {states.map(s => (
                      <option key={s.code} value={s.name}>{s.name}</option>
                    ))}
                  </select>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    PIN Code <span className="text-red-500">*</span>
                  </label>
                  <input
                    type="text"
                    value={form.billing_address?.pin_code || ''}
                    onChange={(e) => handleNestedChange('billing_address', 'pin_code', e.target.value.replace(/\D/g, '').slice(0, 6))}
                    className={`w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 ${errors.billing_pin ? 'border-red-500' : ''}`}
                    placeholder="6-digit PIN"
                    maxLength={6}
                    data-testid="billing-pin-input"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Country</label>
                  <input
                    type="text"
                    value={form.billing_address?.country || 'India'}
                    onChange={(e) => handleNestedChange('billing_address', 'country', e.target.value)}
                    className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500"
                    placeholder="Country"
                  />
                </div>
              </div>

              <div className="mt-4 p-3 bg-gray-50 rounded-lg text-sm text-gray-600">
                <p><strong>Shipping Addresses:</strong> You can add multiple shipping/site addresses after creating the customer.</p>
              </div>
            </div>
          )}

          {/* Section 4: Credit Control */}
          {activeSection === 'credit' && (
            <div className="space-y-4">
              <h3 className="font-semibold text-gray-800 border-b pb-2">Business & Credit Control</h3>
              
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Customer Category</label>
                  <select
                    value={form.customer_category}
                    onChange={(e) => handleChange('customer_category', e.target.value)}
                    className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500"
                    data-testid="category-select"
                  >
                    {CUSTOMER_CATEGORIES.map(c => (
                      <option key={c.value} value={c.value}>{c.label}</option>
                    ))}
                  </select>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Credit Type</label>
                  <select
                    value={form.credit_type}
                    onChange={(e) => handleChange('credit_type', e.target.value)}
                    className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500"
                    data-testid="credit-type-select"
                  >
                    {CREDIT_TYPES.map(c => (
                      <option key={c.value} value={c.value}>{c.label}</option>
                    ))}
                  </select>
                </div>

                {form.credit_type === 'credit_allowed' && (
                  <>
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">Credit Limit (₹)</label>
                      <input
                        type="number"
                        value={form.credit_limit}
                        onChange={(e) => handleChange('credit_limit', parseFloat(e.target.value) || 0)}
                        className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500"
                        placeholder="e.g., 100000"
                        min={0}
                        data-testid="credit-limit-input"
                      />
                    </div>

                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">Credit Days</label>
                      <select
                        value={form.credit_days}
                        onChange={(e) => handleChange('credit_days', parseInt(e.target.value) || 0)}
                        className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500"
                        data-testid="credit-days-select"
                      >
                        {CREDIT_DAYS_OPTIONS.map(d => (
                          <option key={d} value={d}>{d} days</option>
                        ))}
                      </select>
                    </div>
                  </>
                )}
              </div>

              <div className="mt-4 p-3 bg-orange-50 rounded-lg text-sm text-orange-800">
                <p><strong>Market Credit Control:</strong> Credit limit and days help control udhaari and outstanding balances.</p>
              </div>
            </div>
          )}

          {/* Section 5: Bank Details */}
          {activeSection === 'bank' && (
            <div className="space-y-4">
              <h3 className="font-semibold text-gray-800 border-b pb-2">Bank Details (For Refunds)</h3>
              <p className="text-sm text-gray-500">These details are for internal use only. Not printed on invoices.</p>
              
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Account Holder Name</label>
                  <input
                    type="text"
                    value={form.bank_details?.account_holder_name || ''}
                    onChange={(e) => handleNestedChange('bank_details', 'account_holder_name', e.target.value)}
                    className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500"
                    placeholder="Name as per bank"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Bank Name</label>
                  <input
                    type="text"
                    value={form.bank_details?.bank_name || ''}
                    onChange={(e) => handleNestedChange('bank_details', 'bank_name', e.target.value)}
                    className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500"
                    placeholder="e.g., HDFC Bank"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Account Number</label>
                  <input
                    type="text"
                    value={form.bank_details?.account_number || ''}
                    onChange={(e) => handleNestedChange('bank_details', 'account_number', e.target.value)}
                    className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500"
                    placeholder="Account Number"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">IFSC Code</label>
                  <input
                    type="text"
                    value={form.bank_details?.ifsc_code || ''}
                    onChange={(e) => handleNestedChange('bank_details', 'ifsc_code', e.target.value.toUpperCase())}
                    className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500"
                    placeholder="e.g., HDFC0001234"
                    maxLength={11}
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">UPI ID (Optional)</label>
                  <input
                    type="text"
                    value={form.bank_details?.upi_id || ''}
                    onChange={(e) => handleNestedChange('bank_details', 'upi_id', e.target.value)}
                    className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500"
                    placeholder="e.g., name@upi"
                  />
                </div>
              </div>
            </div>
          )}

          {/* Section 6: Invoice Preferences */}
          {activeSection === 'invoice' && (
            <div className="space-y-4">
              <h3 className="font-semibold text-gray-800 border-b pb-2">Invoice Preferences</h3>
              
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Invoice Language</label>
                  <select
                    value={form.invoice_preferences?.language || 'english'}
                    onChange={(e) => handleNestedChange('invoice_preferences', 'language', e.target.value)}
                    className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500"
                  >
                    <option value="english">English</option>
                    <option value="hindi">Hindi</option>
                  </select>
                </div>

                <div className="space-y-3">
                  <label className="flex items-center gap-2 cursor-pointer">
                    <input
                      type="checkbox"
                      checked={form.invoice_preferences?.email_invoice || false}
                      onChange={(e) => handleNestedChange('invoice_preferences', 'email_invoice', e.target.checked)}
                      className="w-4 h-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                    />
                    <span className="text-sm text-gray-700">Email Invoice Auto-Send</span>
                  </label>

                  <label className="flex items-center gap-2 cursor-pointer">
                    <input
                      type="checkbox"
                      checked={form.invoice_preferences?.whatsapp_invoice || false}
                      onChange={(e) => handleNestedChange('invoice_preferences', 'whatsapp_invoice', e.target.checked)}
                      className="w-4 h-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                    />
                    <span className="text-sm text-gray-700">WhatsApp Invoice Sharing</span>
                  </label>

                  <label className="flex items-center gap-2 cursor-pointer">
                    <input
                      type="checkbox"
                      checked={form.invoice_preferences?.po_mandatory || false}
                      onChange={(e) => handleNestedChange('invoice_preferences', 'po_mandatory', e.target.checked)}
                      className="w-4 h-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                    />
                    <span className="text-sm text-gray-700">PO Number Mandatory</span>
                  </label>
                </div>
              </div>

              <div className="mt-4 p-3 bg-blue-50 rounded-lg text-sm text-blue-800">
                <p><strong>Auto-Apply:</strong> These preferences will be automatically applied to all orders from this customer.</p>
              </div>
            </div>
          )}

          {/* Section 7: Compliance */}
          {activeSection === 'compliance' && (
            <div className="space-y-4">
              <h3 className="font-semibold text-gray-800 border-b pb-2">Compliance & Declaration</h3>
              
              <div className="space-y-3">
                <label className="flex items-center gap-2 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={form.compliance?.gst_declaration || false}
                    onChange={(e) => handleNestedChange('compliance', 'gst_declaration', e.target.checked)}
                    className="w-4 h-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                  />
                  <span className="text-sm text-gray-700">GST Declaration Accepted</span>
                </label>

                <label className="flex items-center gap-2 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={form.compliance?.data_consent || false}
                    onChange={(e) => handleNestedChange('compliance', 'data_consent', e.target.checked)}
                    className="w-4 h-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                  />
                  <span className="text-sm text-gray-700">Data Consent Given</span>
                </label>

                <label className="flex items-center gap-2 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={form.compliance?.terms_accepted || false}
                    onChange={(e) => handleNestedChange('compliance', 'terms_accepted', e.target.checked)}
                    className="w-4 h-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                  />
                  <span className="text-sm text-gray-700">Terms & Conditions Accepted</span>
                </label>
              </div>

              <div className="mt-4 p-3 bg-purple-50 rounded-lg text-sm text-purple-800">
                <p><strong>CA & Legal Safety:</strong> These checkboxes help maintain compliance records for audits.</p>
              </div>
            </div>
          )}

          {/* Section 8: CRM */}
          {activeSection === 'crm' && (
            <div className="space-y-4">
              <h3 className="font-semibold text-gray-800 border-b pb-2">CRM & Sales Tracking</h3>
              
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Sales Person Name</label>
                  <input
                    type="text"
                    value={form.crm_details?.sales_person_name || ''}
                    onChange={(e) => handleNestedChange('crm_details', 'sales_person_name', e.target.value)}
                    className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500"
                    placeholder="Assigned Sales Person"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Source</label>
                  <select
                    value={form.crm_details?.source || ''}
                    onChange={(e) => handleNestedChange('crm_details', 'source', e.target.value)}
                    className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500"
                  >
                    <option value="">Select Source</option>
                    <option value="reference">Reference</option>
                    <option value="google">Google</option>
                    <option value="dealer">Dealer</option>
                    <option value="website">Website</option>
                    <option value="social_media">Social Media</option>
                    <option value="walk_in">Walk-in</option>
                    <option value="other">Other</option>
                  </select>
                </div>

                <div className="md:col-span-2">
                  <label className="block text-sm font-medium text-gray-700 mb-1">Notes / Remarks</label>
                  <textarea
                    value={form.crm_details?.notes || ''}
                    onChange={(e) => handleNestedChange('crm_details', 'notes', e.target.value)}
                    className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500"
                    rows={3}
                    placeholder="Any special notes about this customer..."
                  />
                </div>
              </div>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="px-6 py-4 border-t bg-gray-50 flex justify-between">
          <button
            onClick={onClose}
            className="px-4 py-2 text-gray-600 hover:text-gray-800"
          >
            Cancel
          </button>
          <div className="flex gap-2">
            {activeSection !== 'basic' && (
              <button
                onClick={() => {
                  const idx = sections.findIndex(s => s.id === activeSection);
                  if (idx > 0) setActiveSection(sections[idx - 1].id);
                }}
                className="px-4 py-2 border rounded-lg hover:bg-gray-100"
              >
                Previous
              </button>
            )}
            {activeSection !== 'crm' ? (
              <button
                onClick={() => {
                  const idx = sections.findIndex(s => s.id === activeSection);
                  if (idx < sections.length - 1) setActiveSection(sections[idx + 1].id);
                }}
                className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
              >
                Next
              </button>
            ) : (
              <button
                onClick={handleSubmit}
                disabled={saving}
                className="px-6 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50 flex items-center gap-2"
                data-testid="save-customer-btn"
              >
                {saving ? <Loader2 className="w-4 h-4 animate-spin" /> : <Save className="w-4 h-4" />}
                {isEdit ? 'Update Customer' : 'Create Customer'}
              </button>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};


// =============== VIEW MODAL ===============

const CustomerViewModal = ({ customer, onClose, onEdit }) => {
  const [expandedSection, setExpandedSection] = useState('linked');

  const Section = ({ title, children, id }) => (
    <div className="border rounded-lg overflow-hidden mb-3">
      <button
        onClick={() => setExpandedSection(expandedSection === id ? null : id)}
        className="w-full px-4 py-3 bg-gray-50 flex items-center justify-between text-left"
      >
        <span className="font-medium text-gray-800">{title}</span>
        {expandedSection === id ? <ChevronDown className="w-4 h-4" /> : <ChevronRight className="w-4 h-4" />}
      </button>
      {expandedSection === id && (
        <div className="p-4 bg-white">{children}</div>
      )}
    </div>
  );

  const InfoRow = ({ label, value }) => (
    <div className="flex justify-between py-1 text-sm">
      <span className="text-gray-500">{label}</span>
      <span className="font-medium text-gray-900">{value || 'N/A'}</span>
    </div>
  );

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4" data-testid="customer-view-modal">
      <div className="bg-white rounded-xl w-full max-w-3xl max-h-[90vh] overflow-hidden flex flex-col">
        {/* Header */}
        <div className="px-6 py-4 border-b bg-gradient-to-r from-blue-600 to-purple-600">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-blue-100 text-sm font-mono">{customer.customer_code}</p>
              <h2 className="text-xl font-semibold text-white">{customer.display_name}</h2>
            </div>
            <div className="flex items-center gap-2">
              <span className={`px-2 py-1 text-xs rounded-full ${
                customer.invoice_type === 'B2B' ? 'bg-purple-200 text-purple-800' : 'bg-green-200 text-green-800'
              }`}>
                {customer.invoice_type}
              </span>
              <button onClick={onClose} className="text-white/80 hover:text-white ml-4">
                <X className="w-5 h-5" />
              </button>
            </div>
          </div>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-4">
          {/* Quick Info */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
            <div className="bg-blue-50 rounded-lg p-3 text-center">
              <p className="text-xs text-blue-600">Category</p>
              <p className="font-semibold text-blue-800 capitalize">{customer.customer_category}</p>
            </div>
            <div className="bg-green-50 rounded-lg p-3 text-center">
              <p className="text-xs text-green-600">Credit Type</p>
              <p className="font-semibold text-green-800 capitalize">{customer.credit_type?.replace('_', ' ')}</p>
            </div>
            <div className="bg-orange-50 rounded-lg p-3 text-center">
              <p className="text-xs text-orange-600">Credit Limit</p>
              <p className="font-semibold text-orange-800">₹{customer.credit_limit?.toLocaleString()}</p>
            </div>
            <div className="bg-purple-50 rounded-lg p-3 text-center">
              <p className="text-xs text-purple-600">KYC Status</p>
              <p className={`font-semibold capitalize ${
                customer.compliance?.kyc_status === 'verified' ? 'text-green-800' : 'text-yellow-800'
              }`}>
                {customer.compliance?.kyc_status || 'Pending'}
              </p>
            </div>
          </div>

          {/* Basic Info */}
          <Section title="Basic Information" id="basic">
            <div className="grid grid-cols-2 gap-4">
              <InfoRow label="Customer Type" value={customer.customer_type?.replace('_', ' ')} />
              <InfoRow label="Mobile" value={customer.mobile} />
              <InfoRow label="Email" value={customer.email} />
              <InfoRow label="Contact Person" value={customer.contact_person} />
              {customer.company_name && <InfoRow label="Company" value={customer.company_name} />}
              {customer.individual_name && <InfoRow label="Individual" value={customer.individual_name} />}
            </div>
          </Section>

          {/* GST Info */}
          <Section title="GST & Tax Details" id="gst">
            <div className="grid grid-cols-2 gap-4">
              <InfoRow label="GSTIN" value={customer.gstin} />
              <InfoRow label="PAN" value={customer.pan} />
              <InfoRow label="GST Type" value={customer.gst_type} />
              <InfoRow label="State" value={`${customer.state_name || ''} (${customer.state_code || ''})`} />
              <InfoRow label="Place of Supply" value={customer.place_of_supply} />
            </div>
          </Section>

          {/* Billing Address */}
          {customer.billing_address && (
            <Section title="Billing Address" id="billing">
              <div>
                <p className="text-gray-800">{customer.billing_address.address_line1}</p>
                {customer.billing_address.address_line2 && <p className="text-gray-600">{customer.billing_address.address_line2}</p>}
                <p className="text-gray-600">
                  {customer.billing_address.city}, {customer.billing_address.state} - {customer.billing_address.pin_code}
                </p>
                <p className="text-gray-600">{customer.billing_address.country}</p>
              </div>
            </Section>
          )}

          {/* Linked Data (System Generated) */}
          {customer.linked_data && (
            <Section title="Linked Data (Auto-Generated)" id="linked">
              <div className="space-y-4">
                {/* Outstanding */}
                <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                  <div className="bg-gray-50 rounded p-2 text-center">
                    <p className="text-xs text-gray-500">Total Orders</p>
                    <p className="font-bold text-gray-800">{customer.linked_data.total_orders}</p>
                  </div>
                  <div className="bg-gray-50 rounded p-2 text-center">
                    <p className="text-xs text-gray-500">Total Spent</p>
                    <p className="font-bold text-gray-800">₹{customer.linked_data.total_spent?.toLocaleString()}</p>
                  </div>
                  <div className="bg-red-50 rounded p-2 text-center">
                    <p className="text-xs text-red-600">Outstanding</p>
                    <p className="font-bold text-red-800">₹{customer.linked_data.outstanding_balance?.toLocaleString()}</p>
                  </div>
                </div>

                {/* Ageing */}
                {customer.linked_data.ageing && (
                  <div>
                    <p className="text-sm font-medium text-gray-700 mb-2">Ageing Analysis</p>
                    <div className="grid grid-cols-4 gap-2">
                      <div className="bg-green-50 rounded p-2 text-center">
                        <p className="text-xs text-green-600">0-30 Days</p>
                        <p className="font-semibold text-green-800">₹{customer.linked_data.ageing['0_30']?.toLocaleString()}</p>
                      </div>
                      <div className="bg-yellow-50 rounded p-2 text-center">
                        <p className="text-xs text-yellow-600">31-60 Days</p>
                        <p className="font-semibold text-yellow-800">₹{customer.linked_data.ageing['31_60']?.toLocaleString()}</p>
                      </div>
                      <div className="bg-orange-50 rounded p-2 text-center">
                        <p className="text-xs text-orange-600">61-90 Days</p>
                        <p className="font-semibold text-orange-800">₹{customer.linked_data.ageing['61_90']?.toLocaleString()}</p>
                      </div>
                      <div className="bg-red-50 rounded p-2 text-center">
                        <p className="text-xs text-red-600">90+ Days</p>
                        <p className="font-semibold text-red-800">₹{customer.linked_data.ageing['over_90']?.toLocaleString()}</p>
                      </div>
                    </div>
                  </div>
                )}

                {/* Recent Orders */}
                {customer.linked_data.orders?.length > 0 && (
                  <div>
                    <p className="text-sm font-medium text-gray-700 mb-2">Recent Orders</p>
                    <div className="space-y-1">
                      {customer.linked_data.orders.slice(0, 5).map(order => (
                        <div key={order.id} className="flex justify-between text-sm py-1 border-b">
                          <span className="font-mono text-blue-600">{order.order_number || order.id?.slice(0, 8)}</span>
                          <span>₹{order.total_price?.toLocaleString()}</span>
                          <span className={`px-2 py-0.5 rounded text-xs ${
                            order.payment_status === 'paid' ? 'bg-green-100 text-green-700' : 'bg-yellow-100 text-yellow-700'
                          }`}>
                            {order.payment_status}
                          </span>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </Section>
          )}
        </div>

        {/* Footer */}
        <div className="px-6 py-4 border-t bg-gray-50 flex justify-between">
          <button onClick={onClose} className="px-4 py-2 text-gray-600 hover:text-gray-800">
            Close
          </button>
          <button
            onClick={onEdit}
            className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 flex items-center gap-2"
          >
            <Edit className="w-4 h-4" />
            Edit Customer
          </button>
        </div>
      </div>
    </div>
  );
};


export default CustomerManagement;
