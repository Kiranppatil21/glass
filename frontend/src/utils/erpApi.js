import axios from 'axios';

const API_BASE = `${process.env.REACT_APP_BACKEND_URL}/api/erp`;

const getAuthHeaders = () => {
  const token = localStorage.getItem('token');
  return token ? { Authorization: `Bearer ${token}` } : {};
};

export const erpApi = {
  // Admin Dashboard
  admin: {
    getDashboard: () => axios.get(`${API_BASE}/admin/dashboard`),
    getRevenueChart: (months = 6) => axios.get(`${API_BASE}/admin/charts/revenue`, { params: { months } }),
    getProductionChart: () => axios.get(`${API_BASE}/admin/charts/production`),
    getExpenseChart: () => axios.get(`${API_BASE}/admin/charts/expenses`),
    getTopCustomers: () => axios.get(`${API_BASE}/admin/charts/top-customers`),
  },
  
  // CRM
  crm: {
    createLead: (data) => axios.post(`${API_BASE}/crm/leads`, data),
    getLeads: (params) => axios.get(`${API_BASE}/crm/leads`, { params }),
    updateLead: (id, data) => axios.put(`${API_BASE}/crm/leads/${id}`, data),
    updateLeadStatus: (id, status) => axios.patch(`${API_BASE}/crm/leads/${id}/status`, { status }),
  },
  
  // Production
  production: {
    createOrder: (data) => axios.post(`${API_BASE}/production/orders`, data),
    getOrders: (params) => axios.get(`${API_BASE}/production/orders`, { params }),
    updateStatus: (id, stage) => axios.patch(`${API_BASE}/production/orders/${id}/stage`, { stage }),
    createBreakage: (data) => axios.post(`${API_BASE}/production/breakage`, data),
    getBreakageAnalytics: (params) => axios.get(`${API_BASE}/production/breakage/analytics`, { params }),
  },
  
  // HR
  hr: {
    createEmployee: (data) => axios.post(`${API_BASE}/hr/employees`, data),
    getEmployees: () => axios.get(`${API_BASE}/hr/employees`),
    getSalaries: (params) => axios.get(`${API_BASE}/hr/salary`, { params }),
    calculateSalary: (empId, data) => axios.post(`${API_BASE}/hr/salary/calculate/${empId}`, data),
    approveSalary: (salaryId) => axios.post(`${API_BASE}/hr/salary/approve/${salaryId}`),
    // Attendance
    markAttendance: (data) => axios.post(`${API_BASE}/hr/attendance`, data),
    getAttendance: (params) => axios.get(`${API_BASE}/hr/attendance`, { params }),
    getAttendanceSummary: (month) => axios.get(`${API_BASE}/hr/attendance/summary`, { params: { month } }),
  },

  // Accounts
  accounts: {
    getDashboard: () => axios.get(`${API_BASE}/accounts/dashboard`),
    createInvoice: (data) => axios.post(`${API_BASE}/accounts/invoices`, data),
    getInvoices: (params) => axios.get(`${API_BASE}/accounts/invoices`, { params }),
    getInvoice: (id) => axios.get(`${API_BASE}/accounts/invoices/${id}`),
    recordPayment: (invoiceId, data) => axios.post(`${API_BASE}/accounts/invoices/${invoiceId}/payment`, data),
    getLedger: (params) => axios.get(`${API_BASE}/accounts/ledger`, { params }),
    getGSTReport: (month) => axios.get(`${API_BASE}/accounts/gst-report`, { params: { month } }),
    getProfitLoss: (startDate, endDate) => axios.get(`${API_BASE}/accounts/profit-loss`, { params: { start_date: startDate, end_date: endDate } }),
  },
  
  // Inventory
  inventory: {
    getMaterials: (params) => axios.get(`${API_BASE}/inventory/materials`, { params }),
    getMaterial: (id) => axios.get(`${API_BASE}/inventory/materials/${id}`),
    createMaterial: (data) => axios.post(`${API_BASE}/inventory/materials`, data),
    updateMaterial: (id, data) => axios.patch(`${API_BASE}/inventory/materials/${id}`, data),
    addTransaction: (data) => axios.post(`${API_BASE}/inventory/transactions`, data),
    getTransactions: (params) => axios.get(`${API_BASE}/inventory/transactions`, { params }),
    getLowStock: () => axios.get(`${API_BASE}/inventory/low-stock`),
  },
  
  // Purchase
  purchase: {
    createPO: (data) => axios.post(`${API_BASE}/purchase/orders`, data),
    getPOs: (params) => axios.get(`${API_BASE}/purchase/orders`, { params }),
    getPO: (id) => axios.get(`${API_BASE}/purchase/orders/${id}`),
    updatePOStatus: (id, status) => axios.patch(`${API_BASE}/purchase/orders/${id}/status`, { status }),
    createSupplier: (data) => axios.post(`${API_BASE}/purchase/suppliers`, data),
    getSuppliers: () => axios.get(`${API_BASE}/purchase/suppliers`),
  },

  // Payouts (Salary Disbursement)
  payouts: {
    getDashboard: () => axios.get(`${API_BASE}/payouts/dashboard`),
    // Fund Accounts (Employee Bank Details)
    createFundAccount: (data) => axios.post(`${API_BASE}/payouts/fund-accounts`, data),
    getFundAccounts: (params) => axios.get(`${API_BASE}/payouts/fund-accounts`, { params }),
    // Salary Payouts
    processSalaryPayout: (data) => axios.post(`${API_BASE}/payouts/salary/process`, data),
    bulkProcessPayouts: (data) => axios.post(`${API_BASE}/payouts/salary/bulk-process`, data),
    getPayoutStatus: (salaryId) => axios.get(`${API_BASE}/payouts/salary/status/${salaryId}`),
    // History
    getPayoutHistory: (params) => axios.get(`${API_BASE}/payouts/history`, { params }),
  },

  // QR Code & Print
  qr: {
    getJobCardQR: (jobCardNumber) => `${API_BASE}/qr/job-card/${jobCardNumber}`,
    getJobCardBarcode: (jobCardNumber) => `${API_BASE}/qr/job-card/${jobCardNumber}/barcode`,
    getJobCardPrintData: (jobCardNumber) => axios.get(`${API_BASE}/qr/job-card/${jobCardNumber}/print-data`),
    getInvoiceQR: (invoiceNumber) => `${API_BASE}/qr/invoice/${invoiceNumber}`,
    getOrderQR: (orderId) => `${API_BASE}/qr/order/${orderId}`,
  },

  // SMS & WhatsApp Notifications
  notifications: {
    getStatus: () => axios.get(`${API_BASE}/notifications/status`),
    getSettings: () => axios.get(`${API_BASE}/notifications/settings`),
    updateSettings: (data) => axios.put(`${API_BASE}/notifications/settings`, data),
    getLogs: (params) => axios.get(`${API_BASE}/notifications/logs`, { params }),
    sendSMS: (data) => axios.post(`${API_BASE}/notifications/send-sms`, data),
    sendWhatsApp: (data) => axios.post(`${API_BASE}/notifications/send-whatsapp`, data),
    testWhatsApp: (data) => axios.post(`${API_BASE}/notifications/test-whatsapp`, data),
    sendWhatsAppTemplate: (data) => axios.post(`${API_BASE}/notifications/send-whatsapp-template`, data),
  },

  // Wallet & Referral System
  wallet: {
    // Customer endpoints
    getBalance: () => axios.get(`${API_BASE}/wallet/balance`),
    getTransactions: (params) => axios.get(`${API_BASE}/wallet/transactions`, { params }),
    applyReferral: (code) => axios.post(`${API_BASE}/wallet/apply-referral`, { referral_code: code }),
    calculateUsage: (orderAmount) => axios.post(`${API_BASE}/wallet/calculate-usage`, { order_amount: orderAmount }),
    useBalance: (orderId, amount) => axios.post(`${API_BASE}/wallet/use`, { order_id: orderId, amount }),
    // Admin endpoints
    getSettings: () => axios.get(`${API_BASE}/wallet/settings`),
    updateSettings: (data) => axios.put(`${API_BASE}/wallet/settings`, data),
    getStats: () => axios.get(`${API_BASE}/wallet/admin/stats`),
    getAllWallets: (params) => axios.get(`${API_BASE}/wallet/admin/users`, { params }),
    creditWallet: (userId, amount, reason) => axios.post(`${API_BASE}/wallet/admin/credit`, { user_id: userId, amount, reason }),
    // Rewards processing
    processReferralBonus: (data) => axios.post(`${API_BASE}/wallet/process-referral-bonus`, data),
    processCashback: (data) => axios.post(`${API_BASE}/wallet/process-cashback`, data),
  },

  // Daily Expense Management
  expenses: {
    getDashboard: () => axios.get(`${API_BASE}/expenses/dashboard`),
    getCategories: () => axios.get(`${API_BASE}/expenses/categories`),
    createCategory: (data) => axios.post(`${API_BASE}/expenses/categories`, data),
    getSettings: () => axios.get(`${API_BASE}/expenses/settings`),
    updateSettings: (data) => axios.put(`${API_BASE}/expenses/settings`, data),
    getEntries: (params) => axios.get(`${API_BASE}/expenses/entries`, { params }),
    getEntry: (id) => axios.get(`${API_BASE}/expenses/entries/${id}`),
    create: (data) => axios.post(`${API_BASE}/expenses/entries`, data),
    approve: (id, data) => axios.post(`${API_BASE}/expenses/entries/${id}/approve`, data),
    uploadAttachment: (file) => {
      const formData = new FormData();
      formData.append('file', file);
      return axios.post(`${API_BASE}/expenses/upload`, formData);
    },
    getVarianceReport: (month) => axios.get(`${API_BASE}/expenses/reports/variance`, { params: { month } }),
    getCashFlowReport: (params) => axios.get(`${API_BASE}/expenses/reports/cash-flow`, { params }),
  },

  // Asset Management
  assets: {
    getTypes: () => axios.get(`${API_BASE}/assets/types`),
    // Owned Assets
    createOwned: (data) => axios.post(`${API_BASE}/assets/owned`, data),
    getOwned: (params) => axios.get(`${API_BASE}/assets/owned`, { params }),
    getOwnedById: (id) => axios.get(`${API_BASE}/assets/owned/${id}`),
    updateOwned: (id, data) => axios.put(`${API_BASE}/assets/owned/${id}`, data),
    addMaintenance: (id, data) => axios.post(`${API_BASE}/assets/owned/${id}/maintenance`, data),
    disposeAsset: (id, data) => axios.post(`${API_BASE}/assets/owned/${id}/dispose`, data),
    // Rented Assets
    createRented: (data) => axios.post(`${API_BASE}/assets/rented`, data),
    getRented: (params) => axios.get(`${API_BASE}/assets/rented`, { params }),
    getRentedById: (id) => axios.get(`${API_BASE}/assets/rented/${id}`),
    recordRentPayment: (id, data) => axios.post(`${API_BASE}/assets/rented/${id}/payment`, data),
    returnRented: (id, data) => axios.post(`${API_BASE}/assets/rented/${id}/return`, data),
    // Handover
    createHandover: (data) => axios.post(`${API_BASE}/assets/handover/request`, data),
    getHandovers: (params) => axios.get(`${API_BASE}/assets/handover/requests`, { params }),
    approveHandover: (id, data) => axios.post(`${API_BASE}/assets/handover/${id}/approve`, data),
    issueAsset: (id, data) => axios.post(`${API_BASE}/assets/handover/${id}/issue`, data),
    returnAsset: (id, data) => axios.post(`${API_BASE}/assets/handover/${id}/return`, data),
    getEmployeeHoldings: (empId) => axios.get(`${API_BASE}/assets/employee/${empId}/holdings`),
    // Reports
    getAssetRegister: () => axios.get(`${API_BASE}/assets/reports/register`),
    getDepreciationReport: (year) => axios.get(`${API_BASE}/assets/reports/depreciation`, { params: { year } }),
    getRentLiability: () => axios.get(`${API_BASE}/assets/reports/rent-liability`),
  },

  // Holidays & Calendar
  holidays: {
    getTypes: () => axios.get(`${API_BASE}/holidays/types`),
    getSettings: () => axios.get(`${API_BASE}/holidays/settings`),
    updateSettings: (data) => axios.put(`${API_BASE}/holidays/settings`, data),
    create: (data) => axios.post(`${API_BASE}/holidays/`, data),
    getAll: (params) => axios.get(`${API_BASE}/holidays/`, { params }),
    getCalendar: (year) => axios.get(`${API_BASE}/holidays/calendar/${year}`),
    delete: (id) => axios.delete(`${API_BASE}/holidays/${id}`),
    // Overtime
    recordOvertime: (data) => axios.post(`${API_BASE}/holidays/overtime`, data),
    getOvertime: (params) => axios.get(`${API_BASE}/holidays/overtime`, { params }),
    approveOvertime: (id, data) => axios.post(`${API_BASE}/holidays/overtime/${id}/approve`, data),
    // Salary Impact
    getSalaryImpact: (empId, month) => axios.get(`${API_BASE}/holidays/salary-impact/${empId}`, { params: { month } }),
    getSalaryPreview: (month) => axios.get(`${API_BASE}/holidays/salary-preview/${month}`),
    // Reports
    getYearlyReport: (year) => axios.get(`${API_BASE}/holidays/reports/yearly/${year}`),
    getOvertimeSummary: (month) => axios.get(`${API_BASE}/holidays/reports/overtime-summary/${month}`),
  },

  // AI Demand Forecasting
  forecast: {
    getDemandForecast: (days = 90) => axios.get(`${API_BASE}/forecast/demand`, { params: { days } }),
    getOrderStats: () => axios.get(`${API_BASE}/forecast/stats`),
  },

  // CMS - Content Management
  cms: {
    // Pages
    getPages: (status) => axios.get(`${API_BASE}/cms/pages`, { params: { status } }),
    createPage: (data) => axios.post(`${API_BASE}/cms/pages`, data),
    updatePage: (id, data) => axios.put(`${API_BASE}/cms/pages/${id}`, data),
    deletePage: (id) => axios.delete(`${API_BASE}/cms/pages/${id}`),
    // Banners
    getBanners: (position) => axios.get(`${API_BASE}/cms/banners`, { params: { position } }),
    createBanner: (data) => axios.post(`${API_BASE}/cms/banners`, data),
    updateBanner: (id, data) => axios.put(`${API_BASE}/cms/banners/${id}`, data),
    deleteBanner: (id) => axios.delete(`${API_BASE}/cms/banners/${id}`),
    // Menu
    getMenu: (location) => axios.get(`${API_BASE}/cms/menu`, { params: { location } }),
    createMenuItem: (data) => axios.post(`${API_BASE}/cms/menu`, data),
    updateMenuItem: (id, data) => axios.put(`${API_BASE}/cms/menu/${id}`, data),
    deleteMenuItem: (id) => axios.delete(`${API_BASE}/cms/menu/${id}`),
    // Blog
    getBlogPosts: (params) => axios.get(`${API_BASE}/cms/blog`, { params }),
    createBlogPost: (data) => axios.post(`${API_BASE}/cms/blog`, data),
    updateBlogPost: (id, data) => axios.put(`${API_BASE}/cms/blog/${id}`, data),
    deleteBlogPost: (id) => axios.delete(`${API_BASE}/cms/blog/${id}`),
    getBlogCategories: () => axios.get(`${API_BASE}/cms/blog/categories`),
    // Contact Info
    getContactInfo: () => axios.get(`${API_BASE}/cms/contact-info`),
    updateContactInfo: (data) => axios.put(`${API_BASE}/cms/contact-info`, data),
    // SEO & Sitemap
    getSitemap: () => axios.get(`${API_BASE}/cms/sitemap`),
  },

  // Branch Management
  branches: {
    getAll: (params) => axios.get(`${API_BASE}/branches`, { params }),
    get: (id) => axios.get(`${API_BASE}/branches/${id}`),
    create: (data) => axios.post(`${API_BASE}/branches`, data),
    update: (id, data) => axios.put(`${API_BASE}/branches/${id}`, data),
    delete: (id) => axios.delete(`${API_BASE}/branches/${id}`),
    getStats: (id) => axios.get(`${API_BASE}/branches/${id}/stats`),
    transferInventory: (fromId, toId, productId, qty) => 
      axios.post(`${API_BASE}/branches/${fromId}/transfer-inventory?target_branch_id=${toId}&product_id=${productId}&quantity=${qty}`),
  },

  // Product Configuration
  config: {
    getAll: () => axios.get(`${API_BASE}/config/all`),
    addThickness: (data) => axios.post(`${API_BASE}/config/thickness`, data),
    deleteThickness: (value) => axios.delete(`${API_BASE}/config/thickness/${value}`),
    addGlassType: (data) => axios.post(`${API_BASE}/config/glass-types`, data),
    deleteGlassType: (id) => axios.delete(`${API_BASE}/config/glass-types/${id}`),
    addColor: (data) => axios.post(`${API_BASE}/config/colors`, data),
    deleteColor: (id) => axios.delete(`${API_BASE}/config/colors/${id}`),
  },

  // Transport Management
  transport: {
    // Settings
    getSettings: () => axios.get(`${API_BASE}/transport/settings`),
    updateSettings: (data) => axios.put(`${API_BASE}/transport/settings`, data),
    // Distance & Cost
    calculateDistance: (location) => axios.post(`${API_BASE}/transport/calculate-distance`, location),
    calculateCost: (data) => axios.post(`${API_BASE}/transport/calculate-cost`, data),
    // Vehicles
    getVehicles: (status) => axios.get(`${API_BASE}/transport/vehicles`, { params: { status } }),
    createVehicle: (data) => axios.post(`${API_BASE}/transport/vehicles`, data),
    updateVehicle: (id, data) => axios.put(`${API_BASE}/transport/vehicles/${id}`, data),
    deleteVehicle: (id) => axios.delete(`${API_BASE}/transport/vehicles/${id}`),
    // Drivers
    getDrivers: (status) => axios.get(`${API_BASE}/transport/drivers`, { params: { status } }),
    createDriver: (data) => axios.post(`${API_BASE}/transport/drivers`, data),
    updateDriver: (id, data) => axios.put(`${API_BASE}/transport/drivers/${id}`, data),
    deleteDriver: (id) => axios.delete(`${API_BASE}/transport/drivers/${id}`),
    // Dispatch
    createDispatch: (data) => axios.post(`${API_BASE}/transport/dispatch`, data),
    getDispatches: (params) => axios.get(`${API_BASE}/transport/dispatches`, { params }),
    updateDispatchStatus: (id, status) => axios.patch(`${API_BASE}/transport/dispatches/${id}/status`, null, { params: { status } }),
    // Dashboard
    getDashboard: () => axios.get(`${API_BASE}/transport/dashboard`),
  },

  // Rewards & Referrals
  rewards: {
    // Settings
    getSettings: () => axios.get(`${API_BASE}/rewards/settings`),
    updateSettings: (data) => axios.put(`${API_BASE}/rewards/settings`, data),
    // Referral
    getMyReferralCode: () => axios.get(`${API_BASE}/rewards/my-referral-code`),
    applyReferral: (code) => axios.post(`${API_BASE}/rewards/apply-referral`, null, { params: { referral_code: code } }),
    // Balance
    getMyBalance: () => axios.get(`${API_BASE}/rewards/my-balance`),
    getTransactions: (limit) => axios.get(`${API_BASE}/rewards/transactions`, { params: { limit } }),
    redeemCredit: (data) => axios.post(`${API_BASE}/rewards/redeem`, data),
    // Admin
    getUserBalance: (userId) => axios.get(`${API_BASE}/rewards/admin/user/${userId}/balance`),
    adjustCredit: (data) => axios.post(`${API_BASE}/rewards/admin/adjust-credit`, data),
    getAllReferrals: (status) => axios.get(`${API_BASE}/rewards/admin/referrals`, { params: { status } }),
    getDashboard: () => axios.get(`${API_BASE}/rewards/admin/dashboard`),
  },

  // GST Management
  gst: {
    // Settings
    getSettings: () => axios.get(`${API_BASE}/gst/settings`),
    updateSettings: (data) => axios.put(`${API_BASE}/gst/settings`, data),
    // HSN Codes
    getHSNCodes: () => axios.get(`${API_BASE}/gst/hsn-codes`),
    addHSNCode: (data) => axios.post(`${API_BASE}/gst/hsn-codes`, data),
    deleteHSNCode: (code) => axios.delete(`${API_BASE}/gst/hsn-codes/${code}`),
    // States
    getStates: () => axios.get(`${API_BASE}/gst/states`),
    // Calculation
    calculateGST: (data) => axios.post(`${API_BASE}/gst/calculate`, data),
    // Verification
    verifyGSTIN: (gstin) => axios.post(`${API_BASE}/gst/verify`, { gstin }),
    // Public
    getCompanyInfo: () => axios.get(`${API_BASE}/gst/company-info`),
  },

  // Job Work Module
  jobWork: {
    // Public
    getLabourRates: () => axios.get(`${API_BASE}/job-work/labour-rates`),
    calculateCost: (items) => axios.post(`${API_BASE}/job-work/calculate`, items),
    getDisclaimer: () => axios.get(`${API_BASE}/job-work/disclaimer`),
    // Customer
    createOrder: (data) => axios.post(`${API_BASE}/job-work/orders`, data, { headers: getAuthHeaders() }),
    getMyOrders: () => axios.get(`${API_BASE}/job-work/my-orders`, { headers: getAuthHeaders() }),
    getOrderById: (id) => axios.get(`${API_BASE}/job-work/orders/${id}`, { headers: getAuthHeaders() }),
    initiatePayment: (id) => axios.post(`${API_BASE}/job-work/orders/${id}/initiate-payment`, {}, { headers: getAuthHeaders() }),
    verifyPayment: (id, data) => axios.post(`${API_BASE}/job-work/orders/${id}/verify-payment`, data, { headers: getAuthHeaders() }),
    setCashPreference: (id) => axios.post(`${API_BASE}/job-work/orders/${id}/set-cash-preference`, {}, { headers: getAuthHeaders() }),
    // Admin
    getOrders: (status) => axios.get(`${API_BASE}/job-work/orders${status ? `?status=${status}` : ''}`, { headers: getAuthHeaders() }),
    getDashboard: () => axios.get(`${API_BASE}/job-work/dashboard`, { headers: getAuthHeaders() }),
    updateStatus: (id, data) => axios.patch(`${API_BASE}/job-work/orders/${id}/status`, data, { headers: getAuthHeaders() }),
    markCashPayment: (id, data) => axios.post(`${API_BASE}/job-work/orders/${id}/cash-payment`, data, { headers: getAuthHeaders() }),
    getRevenueStats: (startDate, endDate) => axios.get(`${API_BASE}/job-work/revenue-stats?start_date=${startDate}&end_date=${endDate}`, { headers: getAuthHeaders() }),
    getSettings: () => axios.get(`${API_BASE}/job-work/settings`, { headers: getAuthHeaders() }),
    updateSettings: (data) => axios.put(`${API_BASE}/job-work/settings`, data, { headers: getAuthHeaders() }),
  },

  // Customer Master / Profile Module
  customerMaster: {
    // States
    getStates: () => axios.get(`${API_BASE}/customer-master/states`, { headers: getAuthHeaders() }),
    // Stats
    getStats: () => axios.get(`${API_BASE}/customer-master/stats`, { headers: getAuthHeaders() }),
    // CRUD
    create: (data) => axios.post(`${API_BASE}/customer-master/`, data, { headers: getAuthHeaders() }),
    list: (params) => axios.get(`${API_BASE}/customer-master/`, { params, headers: getAuthHeaders() }),
    get: (id) => axios.get(`${API_BASE}/customer-master/${id}`, { headers: getAuthHeaders() }),
    update: (id, data) => axios.put(`${API_BASE}/customer-master/${id}`, data, { headers: getAuthHeaders() }),
    deactivate: (id) => axios.patch(`${API_BASE}/customer-master/${id}/deactivate`, {}, { headers: getAuthHeaders() }),
    reactivate: (id) => axios.patch(`${API_BASE}/customer-master/${id}/reactivate`, {}, { headers: getAuthHeaders() }),
    // Shipping Addresses
    addShippingAddress: (customerId, data) => axios.post(`${API_BASE}/customer-master/${customerId}/shipping-addresses`, data, { headers: getAuthHeaders() }),
    updateShippingAddress: (customerId, addressId, data) => axios.put(`${API_BASE}/customer-master/${customerId}/shipping-addresses/${addressId}`, data, { headers: getAuthHeaders() }),
    deleteShippingAddress: (customerId, addressId) => axios.delete(`${API_BASE}/customer-master/${customerId}/shipping-addresses/${addressId}`, { headers: getAuthHeaders() }),
    // KYC
    updateKYC: (customerId, status, notes) => axios.patch(`${API_BASE}/customer-master/${customerId}/kyc?status=${status}${notes ? `&notes=${notes}` : ''}`, {}, { headers: getAuthHeaders() }),
    // Migration
    migrate: () => axios.post(`${API_BASE}/customer-master/migrate-existing`, {}, { headers: getAuthHeaders() }),
    // Search for invoicing
    searchForInvoice: (q) => axios.get(`${API_BASE}/customer-master/search/for-invoice`, { params: { q }, headers: getAuthHeaders() }),
  },
};

export default erpApi;
