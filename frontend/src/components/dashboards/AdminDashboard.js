import React, { useState, useEffect } from 'react';
import { useAuth } from '../../contexts/AuthContext';
import { useNavigate } from 'react-router-dom';
import { Card, CardContent } from '../ui/card';
import { Button } from '../ui/button';
import { toast } from 'sonner';
import api from '../../utils/api';

const AdminDashboard = () => {
  const { user, loading: authLoading } = useAuth();
  const navigate = useNavigate();
  const [orders, setOrders] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!authLoading) {
      if (!user || user.role !== 'admin') {
        navigate('/login');
      } else {
        fetchOrders();
      }
    }
  }, [user, authLoading, navigate]);

  const fetchOrders = async () => {
    try {
      const response = await api.admin.getOrders();
      setOrders(response.data);
    } catch (error) {
      console.error('Failed to fetch orders:', error);
    } finally {
      setLoading(false);
    }
  };

  const updateStatus = async (orderId, newStatus) => {
    try {
      await api.admin.updateOrderStatus(orderId, newStatus);
      toast.success('Order status updated');
      fetchOrders();
    } catch (error) {
      console.error('Failed to update status:', error);
      toast.error('Failed to update status');
    }
  };

  if (authLoading || loading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-xl text-slate-600">Loading...</div>
      </div>
    );
  }

  return (
    <div className="min-h-screen py-20 bg-slate-50" data-testid="admin-dashboard">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-slate-900 mb-2">Admin Dashboard</h1>
          <p className="text-slate-600">Manage all orders and production</p>
        </div>

        <Card>
          <CardContent className="p-6">
            <h2 className="text-xl font-bold text-slate-900 mb-6">All Orders</h2>
            {orders.length === 0 ? (
              <p className="text-slate-600 text-center py-8">No orders yet</p>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead>
                    <tr className="border-b border-slate-200">
                      <th className="text-left py-3 px-4 text-slate-700">Order ID</th>
                      <th className="text-left py-3 px-4 text-slate-700">Product</th>
                      <th className="text-left py-3 px-4 text-slate-700">Customer</th>
                      <th className="text-left py-3 px-4 text-slate-700">Amount</th>
                      <th className="text-left py-3 px-4 text-slate-700">Status</th>
                      <th className="text-left py-3 px-4 text-slate-700">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {orders.map((order) => (
                      <tr key={order.id} className="border-b border-slate-100">
                        <td className="py-3 px-4 font-mono text-sm">{order.id.slice(0, 8)}</td>
                        <td className="py-3 px-4">{order.product_name}</td>
                        <td className="py-3 px-4">{order.user_id.slice(0, 8)}</td>
                        <td className="py-3 px-4">₹{order.total_price.toLocaleString()}</td>
                        <td className="py-3 px-4">
                          <span className="px-3 py-1 rounded-full text-sm bg-blue-100 text-blue-700">
                            {order.status}
                          </span>
                        </td>
                        <td className="py-3 px-4">
                          <select
                            value={order.status}
                            onChange={(e) => updateStatus(order.id, e.target.value)}
                            className="text-sm border-slate-300 rounded px-2 py-1"
                          >
                            <option value="pending">Pending</option>
                            <option value="confirmed">Confirmed</option>
                            <option value="production">In Production</option>
                            <option value="quality_check">Quality Check</option>
                            <option value="dispatched">Dispatched</option>
                            <option value="delivered">Delivered</option>
                          </select>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
};

export default AdminDashboard;
