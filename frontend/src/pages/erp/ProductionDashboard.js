import React, { useState, useEffect } from 'react';
import { Card, CardContent } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { 
  Plus, QrCode, Clock, CheckCircle, AlertCircle,
  ArrowRight, Loader, Box, Filter, Printer, X, Download
} from 'lucide-react';
import { toast } from 'sonner';
import erpApi from '../../utils/erpApi';

const ProductionDashboard = () => {
  const [orders, setOrders] = useState([]);
  const [selectedStage, setSelectedStage] = useState('all');
  const [showAddOrder, setShowAddOrder] = useState(false);
  const [loading, setLoading] = useState(true);
  const [showQRModal, setShowQRModal] = useState(false);
  const [selectedOrder, setSelectedOrder] = useState(null);
  const [printData, setPrintData] = useState(null);
  const [newOrder, setNewOrder] = useState({
    glass_type: 'Toughened Glass',
    thickness: 6,
    width: 24,
    height: 36,
    quantity: 1,
    priority: 1,
    customer_order_id: ''
  });

  useEffect(() => {
    fetchOrders();
  }, [selectedStage]);

  const fetchOrders = async () => {
    try {
      const params = selectedStage !== 'all' ? { stage: selectedStage } : {};
      const response = await erpApi.production.getOrders(params);
      setOrders(response.data);
    } catch (error) {
      console.error('Failed to fetch orders:', error);
      toast.error('Failed to load production orders');
    } finally {
      setLoading(false);
    }
  };

  const handleCreateOrder = async (e) => {
    e.preventDefault();
    try {
      const response = await erpApi.production.createOrder(newOrder);
      toast.success(`Job Card ${response.data.job_card} created!`);
      setShowAddOrder(false);
      setNewOrder({
        glass_type: 'Toughened Glass',
        thickness: 6,
        width: 24,
        height: 36,
        quantity: 1,
        priority: 1,
        customer_order_id: ''
      });
      fetchOrders();
    } catch (error) {
      console.error('Failed to create order:', error);
      toast.error('Failed to create production order');
    }
  };

  const handleStageUpdate = async (orderId, newStage) => {
    try {
      await erpApi.production.updateStatus(orderId, newStage);
      toast.success('Stage updated successfully!');
      fetchOrders();
    } catch (error) {
      console.error('Failed to update stage:', error);
      toast.error('Failed to update stage');
    }
  };

  const handleShowQR = async (order) => {
    setSelectedOrder(order);
    try {
      const response = await erpApi.qr.getJobCardPrintData(order.job_card_number);
      setPrintData(response.data);
      setShowQRModal(true);
    } catch (error) {
      console.error('Failed to fetch QR data:', error);
      toast.error('Failed to load QR code');
    }
  };

  const handlePrintJobCard = () => {
    if (!printData) return;
    
    const printWindow = window.open('', '_blank');
    printWindow.document.write(`
      <!DOCTYPE html>
      <html>
      <head>
        <title>Job Card - ${printData.job_card_number}</title>
        <style>
          body { font-family: Arial, sans-serif; padding: 20px; }
          .job-card { border: 2px solid #333; padding: 20px; max-width: 400px; margin: 0 auto; }
          .header { text-align: center; border-bottom: 2px solid #333; padding-bottom: 15px; margin-bottom: 15px; }
          .company { font-size: 24px; font-weight: bold; color: #0d9488; }
          .job-number { font-size: 20px; font-weight: bold; margin-top: 10px; }
          .details { margin: 15px 0; }
          .detail-row { display: flex; justify-content: space-between; padding: 5px 0; border-bottom: 1px dashed #ccc; }
          .label { color: #666; }
          .value { font-weight: bold; }
          .codes { display: flex; justify-content: space-around; margin-top: 20px; padding-top: 15px; border-top: 2px solid #333; }
          .code-section { text-align: center; }
          .code-label { font-size: 12px; color: #666; margin-top: 5px; }
          .qr-img { width: 120px; height: 120px; }
          .barcode-img { width: 150px; height: 60px; }
          .priority-badge { display: inline-block; padding: 4px 12px; border-radius: 20px; font-size: 12px; font-weight: bold; }
          .priority-high { background: #fef3c7; color: #d97706; }
          .priority-urgent { background: #fee2e2; color: #dc2626; }
          @media print { body { padding: 0; } .job-card { border: 2px solid #000; } }
        </style>
      </head>
      <body>
        <div class="job-card">
          <div class="header">
            <div class="company">LUCUMAA GLASS</div>
            <div class="job-number">${printData.job_card_number}</div>
            ${printData.priority > 1 ? `<span class="priority-badge ${printData.priority === 3 ? 'priority-urgent' : 'priority-high'}">${printData.priority === 3 ? 'URGENT' : 'HIGH PRIORITY'}</span>` : ''}
          </div>
          <div class="details">
            <div class="detail-row"><span class="label">Glass Type:</span><span class="value">${printData.glass_type}</span></div>
            <div class="detail-row"><span class="label">Thickness:</span><span class="value">${printData.thickness}mm</span></div>
            <div class="detail-row"><span class="label">Dimensions:</span><span class="value">${printData.dimensions}</span></div>
            <div class="detail-row"><span class="label">Quantity:</span><span class="value">${printData.quantity} pcs</span></div>
            <div class="detail-row"><span class="label">Current Stage:</span><span class="value">${printData.current_stage?.toUpperCase()}</span></div>
            <div class="detail-row"><span class="label">Date:</span><span class="value">${printData.created_at}</span></div>
          </div>
          <div class="codes">
            <div class="code-section">
              <img src="${printData.qr_code_base64}" alt="QR Code" class="qr-img" />
              <div class="code-label">Scan to Track</div>
            </div>
            <div class="code-section">
              <img src="${printData.barcode_base64}" alt="Barcode" class="barcode-img" />
              <div class="code-label">${printData.job_card_number}</div>
            </div>
          </div>
        </div>
        <script>window.onload = function() { window.print(); }</script>
      </body>
      </html>
    `);
    printWindow.document.close();
  };

  const handlePrintAllOrders = () => {
    const printWindow = window.open('', '_blank');
    printWindow.document.write(`
      <!DOCTYPE html>
      <html>
      <head>
        <title>Production Orders Report</title>
        <style>
          body { font-family: Arial, sans-serif; padding: 20px; }
          h1 { color: #0d9488; border-bottom: 2px solid #0d9488; padding-bottom: 10px; }
          table { width: 100%; border-collapse: collapse; margin-top: 20px; }
          th, td { border: 1px solid #ddd; padding: 10px; text-align: left; }
          th { background: #0d9488; color: white; }
          tr:nth-child(even) { background: #f9fafb; }
          .priority-high { color: #d97706; font-weight: bold; }
          .priority-urgent { color: #dc2626; font-weight: bold; }
          .stage-badge { padding: 4px 8px; border-radius: 4px; font-size: 12px; }
          .footer { margin-top: 20px; text-align: right; color: #666; font-size: 12px; }
          @media print { th { background: #333 !important; -webkit-print-color-adjust: exact; } }
        </style>
      </head>
      <body>
        <h1>Production Orders Report</h1>
        <p>Generated: ${new Date().toLocaleString()} | Filter: ${selectedStage === 'all' ? 'All Stages' : selectedStage.toUpperCase()}</p>
        <table>
          <thead>
            <tr>
              <th>Job Card</th>
              <th>Glass Type</th>
              <th>Size</th>
              <th>Qty</th>
              <th>Stage</th>
              <th>Priority</th>
              <th>Created</th>
            </tr>
          </thead>
          <tbody>
            ${orders.map(order => `
              <tr>
                <td><strong>${order.job_card_number}</strong></td>
                <td>${order.glass_type}</td>
                <td>${order.width}" × ${order.height}" × ${order.thickness}mm</td>
                <td>${order.quantity}</td>
                <td>${order.current_stage?.toUpperCase()}</td>
                <td class="${order.priority === 3 ? 'priority-urgent' : order.priority === 2 ? 'priority-high' : ''}">${order.priority === 3 ? 'URGENT' : order.priority === 2 ? 'HIGH' : 'NORMAL'}</td>
                <td>${new Date(order.created_at).toLocaleDateString()}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
        <div class="footer">Total Orders: ${orders.length} | Lucumaa Glass ERP</div>
        <script>window.onload = function() { window.print(); }</script>
      </body>
      </html>
    `);
    printWindow.document.close();
  };

  const stages = [
    { value: 'all', label: 'All', color: 'bg-slate-500' },
    { value: 'pending', label: 'Pending', color: 'bg-yellow-500' },
    { value: 'cutting', label: 'Cutting', color: 'bg-blue-500' },
    { value: 'polishing', label: 'Polishing', color: 'bg-purple-500' },
    { value: 'grinding', label: 'Grinding', color: 'bg-indigo-500' },
    { value: 'toughening', label: 'Toughening', color: 'bg-orange-500' },
    { value: 'quality_check', label: 'Quality Check', color: 'bg-green-500' },
    { value: 'packing', label: 'Packing', color: 'bg-teal-500' },
    { value: 'dispatched', label: 'Dispatched', color: 'bg-cyan-500' }
  ];

  const priorityColors = {
    1: 'border-slate-200',
    2: 'border-orange-400 border-2',
    3: 'border-red-500 border-2'
  };

  return (
    <div className="min-h-screen py-20 bg-slate-50" data-testid="production-dashboard">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between mb-8">
          <div>
            <h1 className="text-4xl font-bold text-slate-900 mb-2">Production Orders</h1>
            <p className="text-slate-600">Track and manage job cards through production stages</p>
          </div>
          <div className="flex gap-3">
            <Button 
              variant="outline"
              onClick={handlePrintAllOrders}
              data-testid="print-all-orders-btn"
            >
              <Printer className="w-4 h-4 mr-2" />
              Print Report
            </Button>
            <Button 
              onClick={() => setShowAddOrder(true)}
              className="bg-primary-700 hover:bg-primary-800"
              data-testid="add-production-order-btn"
            >
              <Plus className="w-4 h-4 mr-2" />
              New Job Card
            </Button>
          </div>
        </div>

        {/* Stage Filters */}
        <div className="flex items-center gap-3 mb-6 overflow-x-auto pb-2">
          <Filter className="w-5 h-5 text-slate-600 flex-shrink-0" />
          <div className="flex gap-2">
            {stages.map(stage => (
              <button
                key={stage.value}
                onClick={() => setSelectedStage(stage.value)}
                className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors whitespace-nowrap ${
                  selectedStage === stage.value
                    ? 'bg-primary-700 text-white'
                    : 'bg-white text-slate-700 hover:bg-slate-100'
                }`}
              >
                {stage.label}
              </button>
            ))}
          </div>
        </div>

        {/* Orders Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {orders.map((order) => {
            const currentStageData = stages.find(s => s.value === order.current_stage);
            
            return (
              <Card key={order.id} className={`hover:shadow-lg transition-shadow ${priorityColors[order.priority]}`}>
                <CardContent className="p-6">
                  <div className="flex items-start justify-between mb-4">
                    <div className="flex items-center gap-3">
                      <div className={`w-12 h-12 ${currentStageData?.color || 'bg-slate-500'} rounded-lg flex items-center justify-center`}>
                        <Box className="w-6 h-6 text-white" />
                      </div>
                      <div>
                        <h3 className="font-bold text-slate-900">{order.job_card_number}</h3>
                        <p className="text-xs text-slate-600">{order.glass_type}</p>
                      </div>
                    </div>
                    {order.priority > 1 && (
                      <span className={`px-2 py-1 rounded text-xs font-bold ${
                        order.priority === 3 ? 'bg-red-100 text-red-700' : 'bg-orange-100 text-orange-700'
                      }`}>
                        {order.priority === 3 ? 'URGENT' : 'HIGH'}
                      </span>
                    )}
                  </div>

                  <div className="space-y-2 mb-4">
                    <div className="flex justify-between text-sm">
                      <span className="text-slate-600">Size:</span>
                      <span className="font-medium">{order.width}" × {order.height}"</span>
                    </div>
                    <div className="flex justify-between text-sm">
                      <span className="text-slate-600">Thickness:</span>
                      <span className="font-medium">{order.thickness}mm</span>
                    </div>
                    <div className="flex justify-between text-sm">
                      <span className="text-slate-600">Quantity:</span>
                      <span className="font-medium">{order.quantity} pcs</span>
                    </div>
                  </div>

                  <div className={`px-3 py-2 rounded-lg text-center text-sm font-medium mb-4 ${
                    currentStageData?.color || 'bg-slate-500'
                  } bg-opacity-20`}>
                    <div className="flex items-center justify-center gap-2">
                      {order.current_stage === 'dispatched' ? (
                        <CheckCircle className="w-4 h-4" />
                      ) : (
                        <Clock className="w-4 h-4" />
                      )}
                      <span>{currentStageData?.label || order.current_stage}</span>
                    </div>
                  </div>

                  <div className="flex gap-2">
                    <Button
                      variant="outline"
                      size="sm"
                      className="flex-1"
                      onClick={() => handleShowQR(order)}
                      data-testid={`qr-btn-${order.job_card_number}`}
                    >
                      <QrCode className="w-4 h-4 mr-1" />
                      QR
                    </Button>
                    <Button
                      size="sm"
                      className="flex-1 bg-primary-700 hover:bg-primary-800"
                      onClick={() => {
                        const nextStageIndex = stages.findIndex(s => s.value === order.current_stage);
                        const nextStage = stages[nextStageIndex + 1];
                        if (nextStage && nextStage.value !== 'all') {
                          handleStageUpdate(order.id, nextStage.value);
                        }
                      }}
                      disabled={order.current_stage === 'dispatched'}
                    >
                      {order.current_stage === 'dispatched' ? 'Complete' : 'Next Stage'}
                      {order.current_stage !== 'dispatched' && <ArrowRight className="w-4 h-4 ml-1" />}
                    </Button>
                  </div>

                  <div className="mt-3 text-xs text-slate-500">
                    Created: {new Date(order.created_at).toLocaleDateString()}
                  </div>
                </CardContent>
              </Card>
            );
          })}
        </div>

        {orders.length === 0 && !loading && (
          <div className="text-center py-12">
            <Box className="w-16 h-16 text-slate-300 mx-auto mb-4" />
            <p className="text-slate-600">No production orders found. Create your first job card.</p>
          </div>
        )}

        {/* Add Order Modal */}
        {showAddOrder && (
          <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
            <Card className="max-w-2xl w-full">
              <CardContent className="p-8">
                <h2 className="text-2xl font-bold text-slate-900 mb-6">Create Production Order</h2>
                <form onSubmit={handleCreateOrder} className="space-y-4">
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Glass Type *</label>
                      <select
                        value={newOrder.glass_type}
                        onChange={(e) => setNewOrder({...newOrder, glass_type: e.target.value})}
                        className="w-full h-12 rounded-lg border-slate-300 px-4"
                        required
                      >
                        <option>Toughened Glass</option>
                        <option>Laminated Glass</option>
                        <option>Insulated Glass (DGU)</option>
                        <option>Frosted Glass</option>
                      </select>
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Thickness (mm) *</label>
                      <input
                        type="number"
                        value={newOrder.thickness}
                        onChange={(e) => setNewOrder({...newOrder, thickness: parseFloat(e.target.value)})}
                        className="w-full h-12 rounded-lg border-slate-300 px-4"
                        step="0.1"
                        required
                      />
                    </div>
                  </div>

                  <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Width (inches) *</label>
                      <input
                        type="number"
                        value={newOrder.width}
                        onChange={(e) => setNewOrder({...newOrder, width: parseFloat(e.target.value)})}
                        className="w-full h-12 rounded-lg border-slate-300 px-4"
                        required
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Height (inches) *</label>
                      <input
                        type="number"
                        value={newOrder.height}
                        onChange={(e) => setNewOrder({...newOrder, height: parseFloat(e.target.value)})}
                        className="w-full h-12 rounded-lg border-slate-300 px-4"
                        required
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Quantity *</label>
                      <input
                        type="number"
                        value={newOrder.quantity}
                        onChange={(e) => setNewOrder({...newOrder, quantity: parseInt(e.target.value)})}
                        className="w-full h-12 rounded-lg border-slate-300 px-4"
                        min="1"
                        required
                      />
                    </div>
                  </div>

                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Priority</label>
                      <select
                        value={newOrder.priority}
                        onChange={(e) => setNewOrder({...newOrder, priority: parseInt(e.target.value)})}
                        className="w-full h-12 rounded-lg border-slate-300 px-4"
                      >
                        <option value="1">Normal</option>
                        <option value="2">High</option>
                        <option value="3">Urgent</option>
                      </select>
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Customer Order ID</label>
                      <input
                        type="text"
                        value={newOrder.customer_order_id}
                        onChange={(e) => setNewOrder({...newOrder, customer_order_id: e.target.value})}
                        className="w-full h-12 rounded-lg border-slate-300 px-4"
                        placeholder="Optional"
                      />
                    </div>
                  </div>

                  <div className="flex gap-3 pt-4">
                    <Button type="submit" className="flex-1 bg-primary-700 hover:bg-primary-800">
                      Create Job Card
                    </Button>
                    <Button 
                      type="button"
                      variant="outline"
                      onClick={() => setShowAddOrder(false)}
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

        {/* QR Code Modal */}
        {showQRModal && printData && (
          <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
            <Card className="max-w-md w-full" data-testid="qr-modal">
              <CardContent className="p-6">
                <div className="flex items-center justify-between mb-4">
                  <h2 className="text-xl font-bold text-slate-900">Job Card QR & Barcode</h2>
                  <button 
                    onClick={() => { setShowQRModal(false); setSelectedOrder(null); setPrintData(null); }}
                    className="text-slate-400 hover:text-slate-600"
                  >
                    <X className="w-5 h-5" />
                  </button>
                </div>

                <div className="text-center mb-6">
                  <div className="inline-block bg-teal-100 text-teal-800 px-4 py-2 rounded-lg font-bold text-lg">
                    {printData.job_card_number}
                  </div>
                </div>

                <div className="grid grid-cols-2 gap-4 mb-6 text-sm">
                  <div className="bg-slate-50 p-3 rounded-lg">
                    <p className="text-slate-500">Glass Type</p>
                    <p className="font-medium">{printData.glass_type}</p>
                  </div>
                  <div className="bg-slate-50 p-3 rounded-lg">
                    <p className="text-slate-500">Dimensions</p>
                    <p className="font-medium">{printData.dimensions}</p>
                  </div>
                  <div className="bg-slate-50 p-3 rounded-lg">
                    <p className="text-slate-500">Thickness</p>
                    <p className="font-medium">{printData.thickness}mm</p>
                  </div>
                  <div className="bg-slate-50 p-3 rounded-lg">
                    <p className="text-slate-500">Quantity</p>
                    <p className="font-medium">{printData.quantity} pcs</p>
                  </div>
                </div>

                <div className="flex justify-center gap-8 mb-6">
                  <div className="text-center">
                    <img 
                      src={printData.qr_code_base64} 
                      alt="QR Code" 
                      className="w-32 h-32 mx-auto border rounded-lg"
                      data-testid="qr-code-image"
                    />
                    <p className="text-xs text-slate-500 mt-2">Scan to Track</p>
                  </div>
                  <div className="text-center">
                    <img 
                      src={printData.barcode_base64} 
                      alt="Barcode" 
                      className="w-40 h-16 mx-auto border rounded"
                      data-testid="barcode-image"
                    />
                    <p className="text-xs text-slate-500 mt-2">Barcode</p>
                  </div>
                </div>

                <div className="flex gap-3">
                  <Button 
                    className="flex-1 bg-teal-600 hover:bg-teal-700"
                    onClick={handlePrintJobCard}
                    data-testid="print-job-card-btn"
                  >
                    <Printer className="w-4 h-4 mr-2" />
                    Print Job Card
                  </Button>
                  <Button 
                    variant="outline"
                    className="flex-1"
                    onClick={() => {
                      const link = document.createElement('a');
                      link.href = printData.qr_code_base64;
                      link.download = `qr_${printData.job_card_number}.png`;
                      link.click();
                    }}
                  >
                    <Download className="w-4 h-4 mr-2" />
                    Download QR
                  </Button>
                </div>
              </CardContent>
            </Card>
          </div>
        )}
      </div>
    </div>
  );
};

export default ProductionDashboard;