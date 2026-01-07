import React, { useState, useEffect } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { 
  Hammer, Plus, Minus, Trash2, AlertTriangle, Calculator,
  ArrowLeft, ArrowRight, CheckCircle, FileText, Loader2,
  CreditCard, Banknote, Truck, MapPin
} from 'lucide-react';
import { toast } from 'sonner';
import { useAuth } from '../contexts/AuthContext';
import axios from 'axios';
import erpApi from '../utils/erpApi';

const API_BASE = process.env.REACT_APP_BACKEND_URL;

const JobWorkPage = () => {
  const { user } = useAuth();
  const navigate = useNavigate();
  const [step, setStep] = useState(1); // 1: Items, 2: Details+Disclaimer, 3: Payment, 4: Success
  const [loading, setLoading] = useState(false);
  const [labourRates, setLabourRates] = useState({});
  const [calculatedCost, setCalculatedCost] = useState(null);
  const [disclaimerAccepted, setDisclaimerAccepted] = useState(false);
  const [createdOrder, setCreatedOrder] = useState(null);
  const [paymentMethod, setPaymentMethod] = useState(null); // null = not selected yet
  
  // Transport state
  const [transportRequired, setTransportRequired] = useState(false);
  const [transportLocation, setTransportLocation] = useState({
    address: '',
    landmark: '',
    lat: null,
    lng: null
  });
  const [transportCost, setTransportCost] = useState(null);
  const [calculatingTransport, setCalculatingTransport] = useState(false);
  const [gettingLocation, setGettingLocation] = useState(false);
  
  const [items, setItems] = useState([{
    thickness_mm: 6,
    width_inch: 24,
    height_inch: 36,
    quantity: 1,
    notes: ''
  }]);
  
  const [customerDetails, setCustomerDetails] = useState({
    customer_name: user?.name || '',
    company_name: '',
    phone: user?.phone || '',
    email: user?.email || '',
    delivery_address: '',
    notes: ''
  });

  useEffect(() => {
    fetchLabourRates();
  }, []);

  const getAuthHeaders = () => {
    const token = localStorage.getItem('token');
    return { Authorization: `Bearer ${token}` };
  };

  const fetchLabourRates = async () => {
    try {
      const res = await axios.get(`${API_BASE}/api/erp/job-work/labour-rates`);
      setLabourRates(res.data.labour_rates || {});
    } catch (error) {
      console.error('Failed to fetch labour rates');
    }
  };

  const thicknessOptions = [4, 5, 6, 8, 10, 12, 15, 19];

  // Transport functions
  const getTotalSqft = () => {
    return items.reduce((acc, item) => {
      const sqft = (item.width_inch * item.height_inch) / 144;
      return acc + (sqft * item.quantity);
    }, 0);
  };

  const getCurrentLocation = () => {
    if (!navigator.geolocation) {
      toast.error('Geolocation not supported');
      return;
    }
    
    setGettingLocation(true);
    navigator.geolocation.getCurrentPosition(
      (position) => {
        setTransportLocation(prev => ({
          ...prev,
          lat: position.coords.latitude,
          lng: position.coords.longitude
        }));
        setGettingLocation(false);
        toast.success('Location captured!');
        calculateTransportCost({
          lat: position.coords.latitude,
          lng: position.coords.longitude
        });
      },
      (error) => {
        setGettingLocation(false);
        toast.error('Failed to get location: ' + error.message);
      },
      { enableHighAccuracy: true, timeout: 10000 }
    );
  };

  const calculateTransportCost = async (locationOverride = null) => {
    const loc = locationOverride || transportLocation;
    
    if (!loc.address && !loc.lat) {
      toast.error('Please provide delivery address or use current location');
      return;
    }
    
    setCalculatingTransport(true);
    try {
      const response = await erpApi.transport.calculateCost({
        delivery_location: {
          address: loc.address,
          lat: loc.lat,
          lng: loc.lng,
          landmark: loc.landmark
        },
        total_sqft: getTotalSqft(),
        include_gst: true
      });
      
      setTransportCost(response.data);
      toast.success(`Transport cost: ₹${response.data.total_transport_cost.toLocaleString()}`);
    } catch (error) {
      console.error('Transport calculation failed:', error);
      toast.error(error.response?.data?.detail || 'Failed to calculate transport cost');
      setTransportCost(null);
    } finally {
      setCalculatingTransport(false);
    }
  };

  const addItem = () => {
    setItems([...items, {
      thickness_mm: 6,
      width_inch: 24,
      height_inch: 36,
      quantity: 1,
      notes: ''
    }]);
  };

  const removeItem = (index) => {
    if (items.length > 1) {
      setItems(items.filter((_, i) => i !== index));
    }
  };

  const updateItem = (index, field, value) => {
    const newItems = [...items];
    newItems[index][field] = value;
    setItems(newItems);
  };

  const calculateCost = async () => {
    setLoading(true);
    try {
      const res = await axios.post(
        `${API_BASE}/api/erp/job-work/calculate`,
        items,
        { headers: { ...getAuthHeaders(), 'Content-Type': 'application/json' } }
      );
      setCalculatedCost(res.data);
      setStep(2);
    } catch (error) {
      toast.error('Failed to calculate cost');
    } finally {
      setLoading(false);
    }
  };

  const submitOrder = async () => {
    if (!disclaimerAccepted) {
      toast.error('Please accept the disclaimer to proceed');
      return;
    }
    
    if (!customerDetails.customer_name || !customerDetails.phone || !customerDetails.delivery_address) {
      toast.error('Please fill all required fields');
      return;
    }

    // Validate transport if required
    if (transportRequired && !transportCost) {
      toast.error('Please calculate transport cost first');
      return;
    }

    setLoading(true);
    try {
      const res = await axios.post(
        `${API_BASE}/api/erp/job-work/orders`,
        {
          ...customerDetails,
          items,
          disclaimer_accepted: true,
          // Transport data
          transport_required: transportRequired,
          transport_cost: transportCost?.total_transport_cost || 0,
          transport_distance: transportCost?.distance_km || 0,
          transport_location: transportRequired ? {
            address: transportLocation.address,
            landmark: transportLocation.landmark,
            lat: transportLocation.lat,
            lng: transportLocation.lng
          } : null
        },
        { headers: { ...getAuthHeaders(), 'Content-Type': 'application/json' } }
      );
      
      setCreatedOrder(res.data.order);
      toast.success('Job work order created! Please complete payment.');
      setStep(3); // Move to payment step
    } catch (error) {
      toast.error(error.response?.data?.detail || 'Failed to create order');
    } finally {
      setLoading(false);
    }
  };

  const handlePayment = async () => {
    if (!createdOrder || !paymentMethod) return;
    
    if (paymentMethod === 'cash') {
      // Set cash preference
      setLoading(true);
      try {
        await axios.post(
          `${API_BASE}/api/erp/job-work/orders/${createdOrder.id}/set-cash-preference`,
          {},
          { headers: getAuthHeaders() }
        );
        toast.success('Cash payment preference saved. Admin notified. Please pay at our office.');
        setStep(4);
      } catch (error) {
        toast.error('Failed to set payment preference');
      } finally {
        setLoading(false);
      }
    } else {
      // Online payment via Razorpay
      setLoading(true);
      try {
        const res = await axios.post(
          `${API_BASE}/api/erp/job-work/orders/${createdOrder.id}/initiate-payment`,
          {},
          { headers: getAuthHeaders() }
        );
        
        const options = {
          key: process.env.REACT_APP_RAZORPAY_KEY_ID,
          amount: res.data.amount,
          currency: 'INR',
          order_id: res.data.razorpay_order_id,
          name: 'Lucumaa Glass',
          description: `Job Work - ${res.data.job_work_number} (${res.data.advance_percent}% Advance)`,
          handler: async (response) => {
            try {
              await axios.post(
                `${API_BASE}/api/erp/job-work/orders/${createdOrder.id}/verify-payment?razorpay_payment_id=${response.razorpay_payment_id}&razorpay_signature=${response.razorpay_signature}`,
                {},
                { headers: getAuthHeaders() }
              );
              toast.success('Payment successful!');
              setStep(4);
            } catch (err) {
              toast.error('Payment verification failed');
            }
          },
          modal: {
            ondismiss: () => setLoading(false)
          },
          prefill: {
            name: customerDetails.customer_name,
            email: customerDetails.email,
            contact: customerDetails.phone
          },
          theme: { color: '#ea580c' }
        };
        
        const razorpay = new window.Razorpay(options);
        razorpay.open();
      } catch (error) {
        toast.error('Failed to initiate payment');
        setLoading(false);
      }
    }
  };

  return (
    <div className="min-h-screen bg-slate-50 py-8" data-testid="job-work-page">
      <div className="max-w-4xl mx-auto px-4">
        {/* Header */}
        <div className="flex items-center gap-4 mb-8">
          <Link to="/portal">
            <Button variant="outline" size="sm">
              <ArrowLeft className="w-4 h-4 mr-2" />
              Back
            </Button>
          </Link>
          <div>
            <h1 className="text-2xl font-bold text-slate-900 flex items-center gap-2">
              <Hammer className="w-7 h-7 text-orange-600" />
              Job Work Order
            </h1>
            <p className="text-slate-600">Bring your own glass for toughening</p>
          </div>
        </div>

        {/* Progress Steps */}
        <div className="flex items-center justify-center mb-8">
          {[1, 2, 3, 4].map((s) => (
            <React.Fragment key={s}>
              <div className={`w-10 h-10 rounded-full flex items-center justify-center font-bold ${
                step >= s ? 'bg-orange-600 text-white' : 'bg-slate-200 text-slate-500'
              }`}>
                {step > s ? <CheckCircle className="w-5 h-5" /> : s}
              </div>
              {s < 4 && (
                <div className={`w-16 h-1 ${step > s ? 'bg-orange-600' : 'bg-slate-200'}`} />
              )}
            </React.Fragment>
          ))}
        </div>
        <div className="flex justify-center gap-8 mb-8 text-xs text-slate-500">
          <span className={step >= 1 ? 'text-orange-600 font-medium' : ''}>Items</span>
          <span className={step >= 2 ? 'text-orange-600 font-medium' : ''}>Details</span>
          <span className={step >= 3 ? 'text-orange-600 font-medium' : ''}>Payment</span>
          <span className={step >= 4 ? 'text-orange-600 font-medium' : ''}>Done</span>
        </div>

        {/* Step 1: Add Items */}
        {step === 1 && (
          <div className="space-y-6">
            {/* Labour Rates Info */}
            <Card className="bg-orange-50 border-orange-200">
              <CardContent className="p-4">
                <h3 className="font-medium text-orange-900 mb-2">Labour Rates (per sq.ft)</h3>
                <div className="flex flex-wrap gap-2">
                  {Object.entries(labourRates).map(([mm, rate]) => (
                    <span key={mm} className="bg-white px-3 py-1 rounded-full text-sm">
                      {mm}mm: ₹{rate}
                    </span>
                  ))}
                </div>
              </CardContent>
            </Card>

            {/* Items */}
            {items.map((item, index) => (
              <Card key={index}>
                <CardHeader className="pb-2">
                  <div className="flex items-center justify-between">
                    <CardTitle className="text-base">Glass Item #{index + 1}</CardTitle>
                    {items.length > 1 && (
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => removeItem(index)}
                        className="text-red-500 hover:text-red-700"
                      >
                        <Trash2 className="w-4 h-4" />
                      </Button>
                    )}
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  {/* Thickness */}
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-2">Glass Thickness (mm)</label>
                    <div className="flex flex-wrap gap-2">
                      {thicknessOptions.map((mm) => (
                        <button
                          key={mm}
                          onClick={() => updateItem(index, 'thickness_mm', mm)}
                          className={`px-4 py-2 rounded-lg border-2 font-medium transition-all ${
                            item.thickness_mm === mm
                              ? 'border-orange-500 bg-orange-50 text-orange-700'
                              : 'border-slate-200 hover:border-slate-300'
                          }`}
                        >
                          {mm}mm
                          <span className="text-xs block text-slate-500">₹{labourRates[mm] || 15}/sqft</span>
                        </button>
                      ))}
                    </div>
                  </div>

                  {/* Dimensions */}
                  <div className="grid grid-cols-3 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-1">Width (inch)</label>
                      <input
                        type="number"
                        value={item.width_inch}
                        onChange={(e) => updateItem(index, 'width_inch', parseFloat(e.target.value) || 0)}
                        className="w-full h-11 rounded-lg border border-slate-300 px-3"
                        min="1"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-1">Height (inch)</label>
                      <input
                        type="number"
                        value={item.height_inch}
                        onChange={(e) => updateItem(index, 'height_inch', parseFloat(e.target.value) || 0)}
                        className="w-full h-11 rounded-lg border border-slate-300 px-3"
                        min="1"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-1">Quantity</label>
                      <div className="flex items-center gap-2">
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={() => updateItem(index, 'quantity', Math.max(1, item.quantity - 1))}
                        >
                          <Minus className="w-4 h-4" />
                        </Button>
                        <span className="w-12 text-center font-bold">{item.quantity}</span>
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={() => updateItem(index, 'quantity', item.quantity + 1)}
                        >
                          <Plus className="w-4 h-4" />
                        </Button>
                      </div>
                    </div>
                  </div>

                  {/* Notes */}
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-1">Notes (optional)</label>
                    <input
                      type="text"
                      value={item.notes}
                      onChange={(e) => updateItem(index, 'notes', e.target.value)}
                      className="w-full h-11 rounded-lg border border-slate-300 px-3"
                      placeholder="Any special instructions for this item"
                    />
                  </div>
                </CardContent>
              </Card>
            ))}

            <Button variant="outline" onClick={addItem} className="w-full gap-2">
              <Plus className="w-4 h-4" />
              Add Another Glass Item
            </Button>

            <Button 
              onClick={calculateCost} 
              className="w-full bg-orange-600 hover:bg-orange-700 gap-2"
              disabled={loading}
            >
              {loading ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <Calculator className="w-4 h-4" />
              )}
              Calculate Cost & Continue
            </Button>
          </div>
        )}

        {/* Step 2: Review & Details */}
        {step === 2 && calculatedCost && (
          <div className="space-y-6">
            {/* Cost Summary */}
            <Card>
              <CardHeader>
                <CardTitle>Cost Summary</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-3">
                  {calculatedCost.items.map((item, index) => (
                    <div key={index} className="flex justify-between items-center p-3 bg-slate-50 rounded-lg">
                      <div>
                        <p className="font-medium">Item #{index + 1}: {item.thickness_mm}mm Glass</p>
                        <p className="text-sm text-slate-500">
                          {item.width_inch}&quot; × {item.height_inch}&quot; × {item.quantity} pcs = {item.total_sqft} sq.ft
                        </p>
                      </div>
                      <p className="font-bold">₹{item.labour_cost.toLocaleString()}</p>
                    </div>
                  ))}
                  
                  <div className="border-t pt-3 space-y-2">
                    <div className="flex justify-between">
                      <span>Total Pieces</span>
                      <span className="font-medium">{calculatedCost.summary.total_pieces}</span>
                    </div>
                    <div className="flex justify-between">
                      <span>Total Area</span>
                      <span className="font-medium">{calculatedCost.summary.total_sqft} sq.ft</span>
                    </div>
                    <div className="flex justify-between">
                      <span>Labour Charges</span>
                      <span className="font-medium">₹{calculatedCost.summary.labour_charges.toLocaleString()}</span>
                    </div>
                    <div className="flex justify-between text-blue-600">
                      <span>GST ({calculatedCost.summary.gst_rate}%)</span>
                      <span className="font-medium">₹{calculatedCost.summary.gst_amount.toLocaleString()}</span>
                    </div>
                    {transportRequired && transportCost && (
                      <div className="flex justify-between text-blue-600">
                        <span>Transport</span>
                        <span className="font-medium">₹{transportCost.total_transport_cost?.toLocaleString()}</span>
                      </div>
                    )}
                    <div className="flex justify-between text-lg font-bold border-t pt-2">
                      <span>Grand Total</span>
                      <span className="text-orange-600">
                        ₹{(calculatedCost.summary.grand_total + (transportRequired && transportCost ? transportCost.total_transport_cost : 0)).toLocaleString()}
                      </span>
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Customer Details */}
            <Card>
              <CardHeader>
                <CardTitle>Customer Details</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid md:grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-1">Name *</label>
                    <input
                      type="text"
                      value={customerDetails.customer_name}
                      onChange={(e) => setCustomerDetails({ ...customerDetails, customer_name: e.target.value })}
                      className="w-full h-11 rounded-lg border border-slate-300 px-3"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-1">Company</label>
                    <input
                      type="text"
                      value={customerDetails.company_name}
                      onChange={(e) => setCustomerDetails({ ...customerDetails, company_name: e.target.value })}
                      className="w-full h-11 rounded-lg border border-slate-300 px-3"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-1">Phone *</label>
                    <input
                      type="tel"
                      value={customerDetails.phone}
                      onChange={(e) => setCustomerDetails({ ...customerDetails, phone: e.target.value })}
                      className="w-full h-11 rounded-lg border border-slate-300 px-3"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-1">Email</label>
                    <input
                      type="email"
                      value={customerDetails.email}
                      onChange={(e) => setCustomerDetails({ ...customerDetails, email: e.target.value })}
                      className="w-full h-11 rounded-lg border border-slate-300 px-3"
                    />
                  </div>
                </div>
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-1">Delivery Address *</label>
                  <textarea
                    value={customerDetails.delivery_address}
                    onChange={(e) => setCustomerDetails({ ...customerDetails, delivery_address: e.target.value })}
                    className="w-full rounded-lg border border-slate-300 px-3 py-2 h-20"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-1">Additional Notes</label>
                  <textarea
                    value={customerDetails.notes}
                    onChange={(e) => setCustomerDetails({ ...customerDetails, notes: e.target.value })}
                    className="w-full rounded-lg border border-slate-300 px-3 py-2 h-16"
                  />
                </div>
              </CardContent>
            </Card>

            {/* Transport Option */}
            <Card className="border-blue-200">
              <CardHeader className="pb-3">
                <CardTitle className="text-base flex items-center gap-2">
                  <Truck className="w-5 h-5 text-blue-600" />
                  Transport Service
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <label className="flex items-center gap-3 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={transportRequired}
                    onChange={(e) => {
                      setTransportRequired(e.target.checked);
                      if (!e.target.checked) {
                        setTransportCost(null);
                      }
                    }}
                    className="w-5 h-5 rounded border-blue-300 text-blue-600 focus:ring-blue-500"
                  />
                  <span className="text-slate-700">
                    I need transport for delivery of toughened glass
                  </span>
                </label>
                
                {transportRequired && (
                  <div className="bg-blue-50 rounded-lg p-4 space-y-4">
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-1">Delivery Location</label>
                      <div className="flex gap-2">
                        <input
                          type="text"
                          value={transportLocation.address}
                          onChange={(e) => setTransportLocation(prev => ({ ...prev, address: e.target.value }))}
                          placeholder="Enter delivery address or use location"
                          className="flex-1 h-10 rounded-lg border border-slate-300 px-3"
                        />
                        <Button 
                          type="button"
                          variant="outline"
                          onClick={getCurrentLocation}
                          disabled={gettingLocation}
                          className="gap-2"
                        >
                          <MapPin className="w-4 h-4" />
                          {gettingLocation ? '...' : 'Location'}
                        </Button>
                      </div>
                      {transportLocation.lat && (
                        <p className="text-xs text-green-600 mt-1">
                          📍 GPS: {transportLocation.lat.toFixed(4)}, {transportLocation.lng.toFixed(4)}
                        </p>
                      )}
                    </div>
                    
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-1">Landmark (Optional)</label>
                      <input
                        type="text"
                        value={transportLocation.landmark}
                        onChange={(e) => setTransportLocation(prev => ({ ...prev, landmark: e.target.value }))}
                        placeholder="Near landmark for easy finding"
                        className="w-full h-10 rounded-lg border border-slate-300 px-3"
                      />
                    </div>
                    
                    <Button 
                      type="button"
                      onClick={() => calculateTransportCost()}
                      disabled={calculatingTransport || (!transportLocation.address && !transportLocation.lat)}
                      className="w-full bg-blue-600 hover:bg-blue-700"
                    >
                      {calculatingTransport ? (
                        <Loader2 className="w-4 h-4 animate-spin mr-2" />
                      ) : (
                        <Calculator className="w-4 h-4 mr-2" />
                      )}
                      Calculate Transport Cost
                    </Button>
                    
                    {transportCost && (
                      <div className="bg-white rounded-lg p-3 border border-blue-200">
                        <div className="flex justify-between text-sm">
                          <span>Distance</span>
                          <span className="font-medium">{transportCost.distance_km?.toFixed(1)} km</span>
                        </div>
                        <div className="flex justify-between text-sm">
                          <span>Transport Charge</span>
                          <span className="font-medium">₹{transportCost.base_charge?.toLocaleString()}</span>
                        </div>
                        <div className="flex justify-between text-sm">
                          <span>GST (18%)</span>
                          <span className="font-medium">₹{transportCost.gst_amount?.toLocaleString()}</span>
                        </div>
                        <div className="flex justify-between text-sm font-bold border-t pt-2 mt-2 text-blue-600">
                          <span>Total Transport</span>
                          <span>₹{transportCost.total_transport_cost?.toLocaleString()}</span>
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </CardContent>
            </Card>

            {/* Disclaimer */}
            <Card className="border-red-200 bg-red-50">
              <CardHeader>
                <CardTitle className="text-red-900 flex items-center gap-2">
                  <AlertTriangle className="w-5 h-5" />
                  Important Disclaimer
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="text-sm text-red-800 space-y-2">
                  <p className="font-bold">⚠️ PLEASE READ CAREFULLY:</p>
                  <ul className="list-disc pl-5 space-y-1">
                    <li>Glass toughening involves heating to 620°C and rapid cooling</li>
                    <li>There is inherent risk of breakage due to NiS inclusions, scratches, or internal stresses</li>
                    <li><strong>Company is NOT responsible for any glass breakage during toughening</strong></li>
                    <li><strong>NO compensation or refund will be provided for broken glass</strong></li>
                    <li>Customer bears full responsibility for glass quality</li>
                  </ul>
                </div>
                
                <label className="flex items-start gap-3 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={disclaimerAccepted}
                    onChange={(e) => setDisclaimerAccepted(e.target.checked)}
                    className="w-5 h-5 mt-0.5 rounded border-red-300 text-red-600 focus:ring-red-500"
                  />
                  <span className="text-sm text-red-900">
                    I have read and accept the above disclaimer. I understand that the company is not responsible 
                    for any glass breakage during the toughening process and no compensation will be provided.
                  </span>
                </label>
              </CardContent>
            </Card>

            <div className="flex gap-4">
              <Button variant="outline" onClick={() => setStep(1)} className="flex-1">
                <ArrowLeft className="w-4 h-4 mr-2" />
                Back
              </Button>
              <Button 
                onClick={submitOrder} 
                className="flex-1 bg-orange-600 hover:bg-orange-700"
                disabled={loading || !disclaimerAccepted}
              >
                {loading ? (
                  <Loader2 className="w-4 h-4 animate-spin mr-2" />
                ) : (
                  <FileText className="w-4 h-4 mr-2" />
                )}
                Create Job Work Order
              </Button>
            </div>
          </div>
        )}

        {/* Step 3: Payment */}
        {step === 3 && createdOrder && (
          <div className="space-y-6">
            <Card>
              <CardHeader>
                <CardTitle className="text-center">Complete Payment</CardTitle>
              </CardHeader>
              <CardContent className="space-y-6">
                {/* Order Summary */}
                <div className="bg-orange-50 rounded-lg p-4 text-center">
                  <p className="text-sm text-orange-600 mb-1">Job Work Order</p>
                  <p className="text-2xl font-bold text-orange-700">{createdOrder.job_work_number}</p>
                  <p className="text-3xl font-bold text-slate-900 mt-2">
                    ₹{createdOrder.summary?.grand_total?.toLocaleString()}
                  </p>
                  <p className="text-sm text-slate-500 mt-1">
                    {createdOrder.summary?.total_pieces} pcs • {createdOrder.summary?.total_sqft} sq.ft
                  </p>
                  {/* Show advance requirement */}
                  <div className="mt-3 p-2 bg-white rounded border border-orange-200">
                    <p className="text-xs text-orange-700">
                      {createdOrder.advance_percent === 100 
                        ? '100% Advance Required (Single Item Order)'
                        : `${createdOrder.advance_percent}% Advance Required (₹${createdOrder.advance_required?.toLocaleString()})`
                      }
                    </p>
                  </div>
                </div>

                {/* Step 1: Select Payment Method */}
                <div className="space-y-3">
                  <p className="font-medium text-slate-900 text-center">Step 1: Select Payment Method</p>
                  
                  <button
                    onClick={() => setPaymentMethod('online')}
                    className={`w-full p-4 rounded-lg border-2 flex items-center gap-3 transition-all ${
                      paymentMethod === 'online' 
                        ? 'border-orange-500 bg-orange-50' 
                        : 'border-slate-200 hover:border-slate-300'
                    }`}
                    data-testid="select-online-payment"
                  >
                    <CreditCard className={`w-6 h-6 ${paymentMethod === 'online' ? 'text-orange-600' : 'text-slate-400'}`} />
                    <div className="text-left">
                      <p className="font-medium">Online Payment</p>
                      <p className="text-sm text-slate-500">Pay via UPI, Cards, Net Banking</p>
                    </div>
                    {paymentMethod === 'online' && (
                      <CheckCircle className="w-5 h-5 text-orange-600 ml-auto" />
                    )}
                  </button>

                  <button
                    onClick={() => setPaymentMethod('cash')}
                    className={`w-full p-4 rounded-lg border-2 flex items-center gap-3 transition-all ${
                      paymentMethod === 'cash' 
                        ? 'border-green-500 bg-green-50' 
                        : 'border-slate-200 hover:border-slate-300'
                    }`}
                    data-testid="select-cash-payment"
                  >
                    <Banknote className={`w-6 h-6 ${paymentMethod === 'cash' ? 'text-green-600' : 'text-slate-400'}`} />
                    <div className="text-left">
                      <p className="font-medium">Cash Payment</p>
                      <p className="text-sm text-slate-500">Pay at our office when bringing glass</p>
                    </div>
                    {paymentMethod === 'cash' && (
                      <CheckCircle className="w-5 h-5 text-green-600 ml-auto" />
                    )}
                  </button>
                </div>

                {paymentMethod === 'cash' && (
                  <div className="bg-amber-50 rounded-lg p-4 text-sm text-amber-800">
                    <AlertTriangle className="w-4 h-4 inline mr-2" />
                    <strong>Note:</strong> Admin will be notified. Please pay ₹{createdOrder.advance_required?.toLocaleString()} at our office before processing begins.
                  </div>
                )}

                {/* Step 2: Pay Button - Only shows after method selected */}
                {paymentMethod && (
                  <div className="space-y-2">
                    <p className="font-medium text-slate-900 text-center">Step 2: Confirm Payment</p>
                    <Button 
                      onClick={handlePayment}
                      className={`w-full h-12 text-lg ${
                        paymentMethod === 'cash' 
                          ? 'bg-green-600 hover:bg-green-700' 
                          : 'bg-orange-600 hover:bg-orange-700'
                      }`}
                      disabled={loading}
                      data-testid="pay-now-button"
                    >
                      {loading ? (
                        <Loader2 className="w-5 h-5 animate-spin" />
                      ) : paymentMethod === 'cash' ? (
                        <>
                          <Banknote className="w-5 h-5 mr-2" />
                          Confirm Cash Payment
                        </>
                      ) : (
                        <>
                          <CreditCard className="w-5 h-5 mr-2" />
                          Pay ₹{createdOrder.advance_required?.toLocaleString()} Now
                        </>
                      )}
                    </Button>
                  </div>
                )}

                {!paymentMethod && (
                  <p className="text-center text-slate-500 text-sm">
                    👆 Please select a payment method to continue
                  </p>
                )}
              </CardContent>
            </Card>
          </div>
        )}

        {/* Step 4: Success */}
        {step === 4 && (
          <Card className="text-center py-12">
            <CardContent>
              <div className="w-20 h-20 bg-green-100 rounded-full flex items-center justify-center mx-auto mb-6">
                <CheckCircle className="w-10 h-10 text-green-600" />
              </div>
              <h2 className="text-2xl font-bold text-slate-900 mb-2">
                {paymentMethod === 'cash' ? 'Order Confirmed!' : 'Payment Successful!'}
              </h2>
              <p className="text-slate-600 mb-2">
                Job Work Order: <span className="font-bold text-orange-600">{createdOrder?.job_work_number}</span>
              </p>
              {createdOrder?.advance_percent < 100 && paymentMethod === 'online' && (
                <p className="text-amber-600 mb-2">
                  Advance Paid: ₹{createdOrder?.advance_required?.toLocaleString()} ({createdOrder?.advance_percent}%)
                  <br/>
                  <span className="text-sm">Remaining ₹{(createdOrder?.summary?.grand_total - createdOrder?.advance_required)?.toLocaleString()} due after delivery</span>
                </p>
              )}
              <p className="text-slate-600 mb-6">
                {paymentMethod === 'cash' 
                  ? 'Please bring your glass and payment to our factory.'
                  : 'Your payment has been received. Please bring your glass to our factory.'
                }
              </p>
              <div className="bg-orange-50 rounded-lg p-4 mb-6 text-sm text-orange-800">
                <p className="font-medium mb-2">Next Steps:</p>
                <ol className="list-decimal pl-5 space-y-1 text-left">
                  <li>Bring your glass material to our factory</li>
                  {paymentMethod === 'cash' && <li>Pay ₹{createdOrder?.advance_required?.toLocaleString()} at our office</li>}
                  <li>We&apos;ll toughen your glass (2-3 days)</li>
                  <li>Collect your toughened glass when ready</li>
                  {createdOrder?.advance_percent < 100 && <li>Pay remaining amount on delivery</li>}
                </ol>
              </div>
              <div className="flex gap-4 justify-center">
                <Link to="/portal">
                  <Button variant="outline">Go to Portal</Button>
                </Link>
                <Button 
                  onClick={() => { 
                    setStep(1); 
                    setCalculatedCost(null); 
                    setDisclaimerAccepted(false); 
                    setCreatedOrder(null);
                    setPaymentMethod(null);
                  }} 
                  className="bg-orange-600 hover:bg-orange-700"
                >
                  Create Another Order
                </Button>
              </div>
            </CardContent>
          </Card>
        )}
      </div>
    </div>
  );
};

export default JobWorkPage;
