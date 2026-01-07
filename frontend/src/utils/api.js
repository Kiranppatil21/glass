import axios from 'axios';

const API_BASE = `${process.env.REACT_APP_BACKEND_URL}/api`;

export const api = {
  products: {
    getAll: () => axios.get(`${API_BASE}/products`),
    getOne: (id) => axios.get(`${API_BASE}/products/${id}`),
  },
  pricing: {
    calculate: (data) => axios.post(`${API_BASE}/pricing/calculate`, data),
  },
  orders: {
    create: (data) => axios.post(`${API_BASE}/orders`, data),
    getMyOrders: () => axios.get(`${API_BASE}/orders/my-orders`),
    track: (orderId) => axios.get(`${API_BASE}/erp/customer/orders/${orderId}/track`),
    uploadFile: (orderId, file) => {
      const formData = new FormData();
      formData.append('file', file);
      return axios.post(`${API_BASE}/orders/${orderId}/upload`, formData);
    },
    verifyPayment: (orderId, paymentData) => {
      const formData = new FormData();
      formData.append('razorpay_payment_id', paymentData.razorpay_payment_id);
      formData.append('razorpay_signature', paymentData.razorpay_signature);
      return axios.post(`${API_BASE}/orders/${orderId}/payment`, formData);
    },
  },
  admin: {
    getOrders: () => axios.get(`${API_BASE}/admin/orders`),
    updateOrderStatus: (orderId, status) => {
      const formData = new FormData();
      formData.append('status', status);
      return axios.patch(`${API_BASE}/admin/orders/${orderId}/status`, formData);
    },
  },
  contact: {
    submit: (data) => axios.post(`${API_BASE}/contact`, data),
  },
};

export default api;