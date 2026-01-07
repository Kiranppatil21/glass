import React, { useState, useEffect } from 'react';
import { Card, CardContent } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { 
  Box, CheckCircle, AlertTriangle, ArrowRight, 
  Clock, User, Loader, QrCode
} from 'lucide-react';
import { toast } from 'sonner';
import erpApi from '../../utils/erpApi';

const OperatorDashboard = () => {
  const [orders, setOrders] = useState([]);
  const [loading, setLoading] = useState(true);
  const [selectedStage, setSelectedStage] = useState('');
  const [showBreakageModal, setShowBreakageModal] = useState(false);
  const [selectedOrder, setSelectedOrder] = useState(null);
  const [breakageData, setBreakageData] = useState({
    quantity_broken: 1,
    reason: '',
    cost_per_unit: 100
  });

  const stages = [
    { value: 'cutting', label: 'Cutting', color: 'bg-blue-500' },
    { value: 'polishing', label: 'Polishing', color: 'bg-purple-500' },
    { value: 'grinding', label: 'Grinding', color: 'bg-indigo-500' },
    { value: 'toughening', label: 'Toughening', color: 'bg-orange-500' },
    { value: 'quality_check', label: 'QC', color: 'bg-green-500' },
    { value: 'packing', label: 'Packing', color: 'bg-teal-500' }
  ];

  useEffect(() => {
    if (selectedStage) {
      fetchOrders();
    }
  }, [selectedStage]);

  const fetchOrders = async () => {
    setLoading(true);
    try {
      const response = await erpApi.production.getOrders({ stage: selectedStage });
      setOrders(response.data || []);
    } catch (error) {
      console.error('Failed to fetch orders:', error);
      toast.error('Failed to load orders');
    } finally {
      setLoading(false);
    }
  };

  const handleCompleteStage = async (order) => {
    const stageIndex = stages.findIndex(s => s.value === order.current_stage);
    const nextStage = stages[stageIndex + 1];
    
    if (!nextStage) {
      toast.info('This is the final stage');
      return;
    }

    try {
      await erpApi.production.updateStatus(order.id, nextStage.value);
      toast.success(`Moved to ${nextStage.label}!`);
      fetchOrders();
    } catch (error) {
      console.error('Failed to update stage:', error);
      toast.error('Failed to complete stage');
    }
  };

  const handleReportBreakage = async (e) => {
    e.preventDefault();
    if (!selectedOrder) return;

    try {
      await erpApi.production.createBreakage({
        production_order_id: selectedOrder.id,
        job_card_number: selectedOrder.job_card_number,
        stage: selectedOrder.current_stage,
        operator_id: 'operator-1', // In real app, get from auth
        glass_type: selectedOrder.glass_type,
        size: `${selectedOrder.width}" × ${selectedOrder.height}"`,
        ...breakageData
      });
      toast.success('Breakage reported successfully');
      setShowBreakageModal(false);
      setBreakageData({ quantity_broken: 1, reason: '', cost_per_unit: 100 });
      fetchOrders();
    } catch (error) {
      console.error('Failed to report breakage:', error);
      toast.error('Failed to report breakage');
    }
  };

  if (!selectedStage) {
    return (
      <div className="min-h-screen py-20 bg-slate-900" data-testid="operator-dashboard">
        <div className="max-w-4xl mx-auto px-4">
          <div className="text-center mb-12">
            <h1 className="text-4xl font-bold text-white mb-4">Operator Station</h1>
            <p className="text-slate-400">Select your work station to view assigned jobs</p>
          </div>

          <div className="grid grid-cols-2 md:grid-cols-3 gap-6">
            {stages.map((stage) => (
              <button
                key={stage.value}
                onClick={() => setSelectedStage(stage.value)}
                className={`${stage.color} p-8 rounded-2xl text-white hover:scale-105 transition-transform shadow-lg`}
              >
                <Box className="w-12 h-12 mx-auto mb-4" />
                <p className="text-xl font-bold">{stage.label}</p>
                <p className="text-sm opacity-75">Station</p>
              </button>
            ))}
          </div>
        </div>
      </div>
    );
  }

  const currentStageData = stages.find(s => s.value === selectedStage);

  return (
    <div className="min-h-screen py-20 bg-slate-900" data-testid="operator-dashboard">
      <div className="max-w-6xl mx-auto px-4">
        {/* Header */}
        <div className="flex items-center justify-between mb-8">
          <div>
            <button
              onClick={() => setSelectedStage('')}
              className="text-slate-400 hover:text-white text-sm mb-2"
            >
              ← Change Station
            </button>
            <h1 className="text-3xl font-bold text-white flex items-center gap-3">
              <div className={`w-10 h-10 ${currentStageData.color} rounded-lg flex items-center justify-center`}>
                <Box className="w-5 h-5 text-white" />
              </div>
              {currentStageData.label} Station
            </h1>
          </div>
          <div className="text-right">
            <p className="text-slate-400 text-sm">Jobs in Queue</p>
            <p className="text-4xl font-bold text-white">{orders.length}</p>
          </div>
        </div>

        {loading ? (
          <div className="flex items-center justify-center py-20">
            <Loader className="w-8 h-8 animate-spin text-white" />
          </div>
        ) : orders.length === 0 ? (
          <div className="text-center py-20">
            <CheckCircle className="w-20 h-20 text-green-500 mx-auto mb-4" />
            <p className="text-2xl font-bold text-white mb-2">All Clear!</p>
            <p className="text-slate-400">No pending jobs at this station</p>
          </div>
        ) : (
          <div className="space-y-4">
            {orders.map((order, index) => (
              <Card key={order.id} className={`bg-slate-800 border-slate-700 ${index === 0 ? 'ring-2 ring-green-500' : ''}`}>
                <CardContent className="p-6">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-6">
                      {/* Priority indicator for first item */}
                      {index === 0 && (
                        <div className="flex flex-col items-center">
                          <span className="bg-green-500 text-white text-xs font-bold px-3 py-1 rounded-full">
                            NEXT
                          </span>
                        </div>
                      )}
                      
                      <div className={`w-16 h-16 ${currentStageData.color} rounded-xl flex items-center justify-center`}>
                        <QrCode className="w-8 h-8 text-white" />
                      </div>
                      
                      <div>
                        <h3 className="text-xl font-bold text-white">{order.job_card_number}</h3>
                        <p className="text-slate-400">{order.glass_type}</p>
                        <div className="flex gap-4 mt-2">
                          <span className="text-sm text-slate-300">
                            <strong>{order.width}" × {order.height}"</strong>
                          </span>
                          <span className="text-sm text-slate-300">
                            Thickness: <strong>{order.thickness}mm</strong>
                          </span>
                          <span className="text-sm text-slate-300">
                            Qty: <strong>{order.quantity} pcs</strong>
                          </span>
                        </div>
                      </div>
                    </div>

                    <div className="flex items-center gap-4">
                      {order.priority > 1 && (
                        <span className={`px-3 py-1 rounded text-sm font-bold ${
                          order.priority === 3 ? 'bg-red-500 text-white' : 'bg-orange-500 text-white'
                        }`}>
                          {order.priority === 3 ? 'URGENT' : 'HIGH'}
                        </span>
                      )}

                      <Button
                        variant="outline"
                        onClick={() => {
                          setSelectedOrder(order);
                          setShowBreakageModal(true);
                        }}
                        className="border-red-500 text-red-500 hover:bg-red-500 hover:text-white"
                      >
                        <AlertTriangle className="w-4 h-4 mr-2" />
                        Breakage
                      </Button>

                      <Button
                        onClick={() => handleCompleteStage(order)}
                        className={`${currentStageData.color} hover:opacity-90 px-8`}
                        size="lg"
                      >
                        Complete
                        <ArrowRight className="w-5 h-5 ml-2" />
                      </Button>
                    </div>
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>
        )}

        {/* Breakage Modal */}
        {showBreakageModal && selectedOrder && (
          <div className="fixed inset-0 bg-black/80 flex items-center justify-center z-50 p-4">
            <Card className="max-w-md w-full bg-slate-800 border-slate-700">
              <CardContent className="p-8">
                <div className="flex items-center gap-3 mb-6">
                  <AlertTriangle className="w-8 h-8 text-red-500" />
                  <h2 className="text-2xl font-bold text-white">Report Breakage</h2>
                </div>
                
                <div className="bg-slate-700 rounded-lg p-4 mb-6">
                  <p className="text-sm text-slate-400">Job Card</p>
                  <p className="text-lg font-bold text-white">{selectedOrder.job_card_number}</p>
                  <p className="text-sm text-slate-400 mt-2">Stage: {selectedStage}</p>
                </div>

                <form onSubmit={handleReportBreakage} className="space-y-4">
                  <div>
                    <label className="block text-sm font-medium text-slate-300 mb-2">Quantity Broken *</label>
                    <input
                      type="number"
                      value={breakageData.quantity_broken}
                      onChange={(e) => setBreakageData({...breakageData, quantity_broken: parseInt(e.target.value) || 1})}
                      className="w-full h-12 rounded-lg bg-slate-700 border-slate-600 text-white px-4"
                      min="1"
                      max={selectedOrder.quantity}
                      required
                    />
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-slate-300 mb-2">Reason *</label>
                    <select
                      value={breakageData.reason}
                      onChange={(e) => setBreakageData({...breakageData, reason: e.target.value})}
                      className="w-full h-12 rounded-lg bg-slate-700 border-slate-600 text-white px-4"
                      required
                    >
                      <option value="">Select reason</option>
                      <option value="Machine malfunction">Machine malfunction</option>
                      <option value="Operator error">Operator error</option>
                      <option value="Material defect">Material defect</option>
                      <option value="Handling damage">Handling damage</option>
                      <option value="Temperature issue">Temperature issue</option>
                      <option value="Other">Other</option>
                    </select>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-slate-300 mb-2">Cost per Unit (₹)</label>
                    <input
                      type="number"
                      value={breakageData.cost_per_unit}
                      onChange={(e) => setBreakageData({...breakageData, cost_per_unit: parseFloat(e.target.value) || 0})}
                      className="w-full h-12 rounded-lg bg-slate-700 border-slate-600 text-white px-4"
                      min="0"
                    />
                  </div>

                  <div className="bg-red-900/30 rounded-lg p-4">
                    <p className="text-sm text-red-300">Estimated Loss</p>
                    <p className="text-2xl font-bold text-red-400">
                      ₹{(breakageData.quantity_broken * breakageData.cost_per_unit).toLocaleString()}
                    </p>
                  </div>

                  <div className="flex gap-3 pt-4">
                    <Button type="submit" className="flex-1 bg-red-600 hover:bg-red-700">
                      Report Breakage
                    </Button>
                    <Button 
                      type="button"
                      variant="outline"
                      onClick={() => {
                        setShowBreakageModal(false);
                        setSelectedOrder(null);
                      }}
                      className="flex-1 border-slate-600 text-slate-300"
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

export default OperatorDashboard;
