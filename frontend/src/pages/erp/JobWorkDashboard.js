import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { 
  Hammer, Package, Clock, CheckCircle, Truck, AlertTriangle,
  Search, Filter, RefreshCw, Eye, ChevronDown, X, FileText,
  AlertCircle, Factory, ShieldCheck, Loader2, Share2
} from 'lucide-react';
import { toast } from 'sonner';
import { erpApi } from '../../utils/erpApi';
import ShareModal from '../../components/ShareModal';

const JobWorkDashboard = () => {
  const [orders, setOrders] = useState([]);
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(true);
  const [selectedOrder, setSelectedOrder] = useState(null);
  const [statusFilter, setStatusFilter] = useState('all');
  const [searchQuery, setSearchQuery] = useState('');
  const [updatingStatus, setUpdatingStatus] = useState(null);
  const [shareModalOrder, setShareModalOrder] = useState(null);

  useEffect(() => {
    fetchData();
  }, [statusFilter]);

  const fetchData = async () => {
    setLoading(true);
    try {
      const [ordersRes, dashRes] = await Promise.all([
        erpApi.jobWork.getOrders(statusFilter === 'all' ? null : statusFilter),
        erpApi.jobWork.getDashboard()
      ]);
      setOrders(ordersRes.data || []);
      setStats(dashRes.data);
    } catch (error) {
      toast.error('Failed to load job work data');
    } finally {
      setLoading(false);
    }
  };

  const statusConfig = {
    pending: { color: 'bg-yellow-100 text-yellow-700', label: 'Pending Acceptance', icon: Clock },
    accepted: { color: 'bg-blue-100 text-blue-700', label: 'Job Accepted', icon: CheckCircle },
    material_received: { color: 'bg-purple-100 text-purple-700', label: 'Material Received', icon: Package },
    in_process: { color: 'bg-orange-100 text-orange-700', label: 'In Process', icon: Factory },
    completed: { color: 'bg-cyan-100 text-cyan-700', label: 'Work Completed', icon: ShieldCheck },
    ready_for_delivery: { color: 'bg-green-100 text-green-700', label: 'Ready for Delivery', icon: Truck },
    delivered: { color: 'bg-emerald-100 text-emerald-700', label: 'Delivered', icon: CheckCircle },
    cancelled: { color: 'bg-red-100 text-red-700', label: 'Cancelled', icon: X }
  };

  const statusFlow = ['pending', 'accepted', 'material_received', 'in_process', 'completed', 'ready_for_delivery', 'delivered'];

  const getNextStatus = (currentStatus) => {
    const currentIndex = statusFlow.indexOf(currentStatus);
    if (currentIndex < statusFlow.length - 1) {
      return statusFlow[currentIndex + 1];
    }
    return null;
  };

  const updateStatus = async (orderId, newStatus, notes = '', breakageCount = 0, breakageNotes = '') => {
    setUpdatingStatus(orderId);
    try {
      await erpApi.jobWork.updateStatus(orderId, {
        status: newStatus,
        notes,
        breakage_count: breakageCount,
        breakage_notes: breakageNotes
      });
      toast.success(`Status updated to ${statusConfig[newStatus]?.label}`);
      fetchData();
      if (selectedOrder?.id === orderId) {
        setSelectedOrder(null);
      }
    } catch (error) {
      toast.error('Failed to update status');
    } finally {
      setUpdatingStatus(null);
    }
  };

  const filteredOrders = orders.filter(order => {
    if (searchQuery) {
      const query = searchQuery.toLowerCase();
      return (
        order.job_work_number?.toLowerCase().includes(query) ||
        order.customer_name?.toLowerCase().includes(query) ||
        order.phone?.includes(query)
      );
    }
    return true;
  });

  if (loading && !stats) {
    return (
      <div className="flex items-center justify-center h-64">
        <RefreshCw className="w-8 h-8 animate-spin text-orange-600" />
      </div>
    );
  }

  return (
    <div className="space-y-6" data-testid="job-work-dashboard">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-900 flex items-center gap-2">
            <Hammer className="w-7 h-7 text-orange-600" />
            Job Work Management
          </h1>
          <p className="text-slate-600">Manage customer's own glass toughening orders</p>
        </div>
        <Button onClick={fetchData} variant="outline" className="gap-2">
          <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
          Refresh
        </Button>
      </div>

      {/* Stats Cards */}
      {stats && (
        <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
          <Card className="bg-gradient-to-br from-orange-500 to-orange-600 text-white">
            <CardContent className="p-4">
              <p className="text-orange-100 text-sm">Total Orders</p>
              <p className="text-3xl font-bold">{stats.total_orders}</p>
            </CardContent>
          </Card>
          <Card className="bg-gradient-to-br from-yellow-500 to-yellow-600 text-white">
            <CardContent className="p-4">
              <p className="text-yellow-100 text-sm">Pending</p>
              <p className="text-3xl font-bold">{stats.pending_orders}</p>
            </CardContent>
          </Card>
          <Card className="bg-gradient-to-br from-purple-500 to-purple-600 text-white">
            <CardContent className="p-4">
              <p className="text-purple-100 text-sm">This Month</p>
              <p className="text-3xl font-bold">{stats.this_month}</p>
            </CardContent>
          </Card>
          <Card className="bg-gradient-to-br from-red-500 to-red-600 text-white">
            <CardContent className="p-4">
              <p className="text-red-100 text-sm">Total Breakage</p>
              <p className="text-3xl font-bold">{stats.total_breakage}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <p className="text-slate-600 text-sm">By Status</p>
              <div className="flex flex-wrap gap-1 mt-2">
                {stats.by_status && Object.entries(stats.by_status).map(([status, count]) => (
                  <span 
                    key={status} 
                    className={`text-xs px-2 py-0.5 rounded ${statusConfig[status]?.color || 'bg-slate-100'}`}
                  >
                    {count}
                  </span>
                ))}
              </div>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Filters */}
      <Card>
        <CardContent className="p-4">
          <div className="flex flex-wrap gap-4">
            <div className="flex-1 min-w-[200px]">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
                <input
                  type="text"
                  placeholder="Search by JW number, customer, phone..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="w-full h-10 pl-10 pr-4 rounded-lg border border-slate-300"
                />
              </div>
            </div>
            <div className="flex items-center gap-2">
              <Filter className="w-4 h-4 text-slate-500" />
              <select
                value={statusFilter}
                onChange={(e) => setStatusFilter(e.target.value)}
                className="h-10 px-3 rounded-lg border border-slate-300"
              >
                <option value="all">All Status</option>
                {statusFlow.map(status => (
                  <option key={status} value={status}>
                    {statusConfig[status]?.label}
                  </option>
                ))}
                <option value="cancelled">Cancelled</option>
              </select>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Orders List */}
      <Card>
        <CardHeader>
          <CardTitle>Job Work Orders ({filteredOrders.length})</CardTitle>
        </CardHeader>
        <CardContent>
          {filteredOrders.length === 0 ? (
            <div className="text-center py-12 text-slate-400">
              <Hammer className="w-12 h-12 mx-auto mb-3 opacity-50" />
              <p>No job work orders found</p>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b border-slate-200">
                    <th className="text-left py-3 px-2 text-sm font-medium text-slate-700">JW Number</th>
                    <th className="text-left py-3 px-2 text-sm font-medium text-slate-700">Customer</th>
                    <th className="text-left py-3 px-2 text-sm font-medium text-slate-700">Items</th>
                    <th className="text-left py-3 px-2 text-sm font-medium text-slate-700">Amount</th>
                    <th className="text-left py-3 px-2 text-sm font-medium text-slate-700">Status</th>
                    <th className="text-left py-3 px-2 text-sm font-medium text-slate-700">Date</th>
                    <th className="text-left py-3 px-2 text-sm font-medium text-slate-700">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredOrders.map((order) => {
                    const status = statusConfig[order.status] || statusConfig.pending;
                    const StatusIcon = status.icon;
                    const nextStatus = getNextStatus(order.status);
                    
                    return (
                      <tr key={order.id} className="border-b border-slate-100 hover:bg-slate-50">
                        <td className="py-3 px-2">
                          <span className="font-mono font-bold text-orange-700">
                            {order.job_work_number}
                          </span>
                        </td>
                        <td className="py-3 px-2">
                          <p className="font-medium text-slate-900">{order.customer_name}</p>
                          <p className="text-xs text-slate-500">{order.phone}</p>
                        </td>
                        <td className="py-3 px-2">
                          <p className="text-sm">
                            {order.summary?.total_pieces} pcs • {order.summary?.total_sqft?.toFixed(1)} sqft
                          </p>
                          {order.breakage_count > 0 && (
                            <span className="text-xs text-red-600">
                              <AlertTriangle className="w-3 h-3 inline mr-1" />
                              {order.breakage_count} breakage
                            </span>
                          )}
                        </td>
                        <td className="py-3 px-2">
                          <p className="font-medium">₹{order.summary?.grand_total?.toLocaleString()}</p>
                          <p className={`text-xs ${order.payment_status === 'completed' ? 'text-green-600' : 'text-amber-600'}`}>
                            {order.payment_status === 'completed' ? 'Paid' : 'Pending'}
                          </p>
                        </td>
                        <td className="py-3 px-2">
                          <span className={`inline-flex items-center gap-1 px-2 py-1 rounded-full text-xs font-medium ${status.color}`}>
                            <StatusIcon className="w-3 h-3" />
                            {status.label}
                          </span>
                        </td>
                        <td className="py-3 px-2 text-sm text-slate-600">
                          {new Date(order.created_at).toLocaleDateString('en-IN')}
                        </td>
                        <td className="py-3 px-2">
                          <div className="flex items-center gap-2">
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => setSelectedOrder(order)}
                              className="h-8"
                            >
                              <Eye className="w-4 h-4" />
                            </Button>
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => setShareModalOrder({ order, isJobWork: true })}
                              className="h-8 text-purple-600 border-purple-200 hover:bg-purple-50"
                            >
                              <Share2 className="w-4 h-4" />
                            </Button>
                            {nextStatus && order.status !== 'delivered' && order.status !== 'cancelled' && (
                              <Button
                                size="sm"
                                className="h-8 bg-orange-600 hover:bg-orange-700"
                                onClick={() => updateStatus(order.id, nextStatus)}
                                disabled={updatingStatus === order.id}
                              >
                                {updatingStatus === order.id ? (
                                  <Loader2 className="w-4 h-4 animate-spin" />
                                ) : (
                                  <>→ {statusConfig[nextStatus]?.label}</>
                                )}
                              </Button>
                            )}
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Share Modal */}
      <ShareModal 
        isOpen={!!shareModalOrder}
        onClose={() => setShareModalOrder(null)}
        order={shareModalOrder?.order}
        isJobWork={shareModalOrder?.isJobWork}
      />

      {/* Order Details Modal */}
      {selectedOrder && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
          <Card className="max-w-3xl w-full max-h-[90vh] overflow-y-auto">
            <CardHeader className="flex flex-row items-center justify-between">
              <CardTitle className="flex items-center gap-2">
                <Hammer className="w-5 h-5 text-orange-600" />
                {selectedOrder.job_work_number}
              </CardTitle>
              <button onClick={() => setSelectedOrder(null)} className="text-slate-400 hover:text-slate-600">
                <X className="w-5 h-5" />
              </button>
            </CardHeader>
            <CardContent className="space-y-6">
              {/* Customer Info */}
              <div className="grid md:grid-cols-2 gap-4">
                <div>
                  <label className="text-xs text-slate-500">Customer</label>
                  <p className="font-medium">{selectedOrder.customer_name}</p>
                  {selectedOrder.company_name && (
                    <p className="text-sm text-slate-600">{selectedOrder.company_name}</p>
                  )}
                </div>
                <div>
                  <label className="text-xs text-slate-500">Contact</label>
                  <p className="font-medium">{selectedOrder.phone}</p>
                  <p className="text-sm text-slate-600">{selectedOrder.email}</p>
                </div>
                <div className="md:col-span-2">
                  <label className="text-xs text-slate-500">Delivery Address</label>
                  <p className="font-medium">{selectedOrder.delivery_address}</p>
                </div>
              </div>

              {/* Items Table */}
              <div>
                <h4 className="font-medium mb-2">Glass Items</h4>
                <div className="border rounded-lg overflow-hidden">
                  <table className="w-full text-sm">
                    <thead className="bg-slate-50">
                      <tr>
                        <th className="text-left py-2 px-3">Thickness</th>
                        <th className="text-left py-2 px-3">Size (inch)</th>
                        <th className="text-left py-2 px-3">Qty</th>
                        <th className="text-left py-2 px-3">Sq.ft</th>
                        <th className="text-left py-2 px-3">Rate</th>
                        <th className="text-right py-2 px-3">Amount</th>
                      </tr>
                    </thead>
                    <tbody>
                      {selectedOrder.item_details?.map((item, idx) => (
                        <tr key={idx} className="border-t">
                          <td className="py-2 px-3">{item.thickness_mm}mm</td>
                          <td className="py-2 px-3">{item.width_inch} × {item.height_inch}</td>
                          <td className="py-2 px-3">{item.quantity}</td>
                          <td className="py-2 px-3">{item.total_sqft}</td>
                          <td className="py-2 px-3">₹{item.labour_rate}/sqft</td>
                          <td className="py-2 px-3 text-right font-medium">₹{item.labour_cost?.toLocaleString()}</td>
                        </tr>
                      ))}
                    </tbody>
                    <tfoot className="bg-slate-50 font-medium">
                      <tr className="border-t">
                        <td colSpan={3} className="py-2 px-3">Total</td>
                        <td className="py-2 px-3">{selectedOrder.summary?.total_sqft} sqft</td>
                        <td className="py-2 px-3"></td>
                        <td className="py-2 px-3 text-right">₹{selectedOrder.summary?.labour_charges?.toLocaleString()}</td>
                      </tr>
                      <tr>
                        <td colSpan={5} className="py-2 px-3">GST ({selectedOrder.summary?.gst_rate}%)</td>
                        <td className="py-2 px-3 text-right">₹{selectedOrder.summary?.gst_amount?.toLocaleString()}</td>
                      </tr>
                      <tr className="text-orange-700">
                        <td colSpan={5} className="py-2 px-3 font-bold">Grand Total</td>
                        <td className="py-2 px-3 text-right font-bold">₹{selectedOrder.summary?.grand_total?.toLocaleString()}</td>
                      </tr>
                    </tfoot>
                  </table>
                </div>
              </div>

              {/* Status Timeline */}
              <div>
                <h4 className="font-medium mb-3">Status History</h4>
                <div className="space-y-3">
                  {selectedOrder.status_history?.map((entry, idx) => (
                    <div key={idx} className="flex items-start gap-3">
                      <div className={`w-8 h-8 rounded-full flex items-center justify-center ${
                        statusConfig[entry.status]?.color || 'bg-slate-100'
                      }`}>
                        {idx + 1}
                      </div>
                      <div>
                        <p className="font-medium">{statusConfig[entry.status]?.label || entry.status}</p>
                        <p className="text-xs text-slate-500">
                          {new Date(entry.timestamp).toLocaleString('en-IN')} • {entry.by}
                        </p>
                        {entry.notes && <p className="text-sm text-slate-600">{entry.notes}</p>}
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              {/* Breakage Info */}
              {selectedOrder.breakage_count > 0 && (
                <div className="bg-red-50 rounded-lg p-4">
                  <h4 className="font-medium text-red-900 flex items-center gap-2">
                    <AlertTriangle className="w-4 h-4" />
                    Breakage Record
                  </h4>
                  <p className="text-red-800 mt-1">
                    {selectedOrder.breakage_count} pieces broke during processing
                  </p>
                  {selectedOrder.breakage_notes && (
                    <p className="text-sm text-red-700 mt-1">{selectedOrder.breakage_notes}</p>
                  )}
                </div>
              )}

              {/* Update Status with Breakage */}
              {selectedOrder.status !== 'delivered' && selectedOrder.status !== 'cancelled' && (
                <div className="bg-orange-50 rounded-lg p-4">
                  <h4 className="font-medium text-orange-900 mb-3">Update Status</h4>
                  <div className="flex flex-wrap gap-2">
                    {statusFlow.slice(statusFlow.indexOf(selectedOrder.status) + 1).map(status => (
                      <Button
                        key={status}
                        size="sm"
                        variant={status === getNextStatus(selectedOrder.status) ? 'default' : 'outline'}
                        className={status === getNextStatus(selectedOrder.status) ? 'bg-orange-600 hover:bg-orange-700' : ''}
                        onClick={() => updateStatus(selectedOrder.id, status)}
                        disabled={updatingStatus === selectedOrder.id}
                      >
                        {statusConfig[status]?.label}
                      </Button>
                    ))}
                    <Button
                      size="sm"
                      variant="outline"
                      className="text-red-600 border-red-300 hover:bg-red-50"
                      onClick={() => {
                        const count = prompt('Enter breakage count:');
                        if (count && parseInt(count) > 0) {
                          const notes = prompt('Enter breakage notes (optional):');
                          updateStatus(selectedOrder.id, selectedOrder.status, '', parseInt(count), notes || '');
                        }
                      }}
                    >
                      <AlertTriangle className="w-4 h-4 mr-1" />
                      Record Breakage
                    </Button>
                  </div>
                </div>
              )}

              {/* Disclaimer Confirmation */}
              {selectedOrder.disclaimer_accepted && (
                <div className="bg-slate-50 rounded-lg p-4 text-sm">
                  <p className="text-slate-600">
                    <CheckCircle className="w-4 h-4 inline mr-1 text-green-600" />
                    Disclaimer accepted on {new Date(selectedOrder.disclaimer_accepted_at).toLocaleString('en-IN')}
                  </p>
                </div>
              )}

              <Button onClick={() => setSelectedOrder(null)} className="w-full">
                Close
              </Button>
            </CardContent>
          </Card>
        </div>
      )}
    </div>
  );
};

export default JobWorkDashboard;
