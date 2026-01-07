import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import { 
  Plus, Minus, Trash2, Calculator, CreditCard, CheckCircle, 
  Upload, ShoppingCart, Package, ArrowRight, ArrowLeft, Palette,
  Truck, MapPin, Navigation, Loader2, Gift, Receipt, Building2, Search, User
} from 'lucide-react';
import { Button } from '../components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card';
import { toast } from 'sonner';
import api from '../utils/api';
import { erpApi } from '../utils/erpApi';
import CustomerSearch from '../components/CustomerSearch';

const API_URL = process.env.REACT_APP_BACKEND_URL;

const CustomizeBook = () => {
  const { user } = useAuth();
  const navigate = useNavigate();
  const [step, setStep] = useState(1);
  const [products, setProducts] = useState([]);
  const [loading, setLoading] = useState(false);
  
  // Product config from admin settings
  const [productConfig, setProductConfig] = useState({
    colors: [],
    thickness_options: [],
    glass_types: []
  });
  
  // Cart items - multiple glass pieces
  const [cartItems, setCartItems] = useState([{
    id: Date.now(),
    product_id: '',
    product_name: '',
    thickness: '',
    color: '',
    color_name: '',
    width: '',
    height: '',
    quantity: 1,
    unit_price: 0,
    total_price: 0,
    calculated: false
  }]);
  
  // Order details
  const [orderDetails, setOrderDetails] = useState({
    customer_name: '',
    company_name: '',
    delivery_address: '',
    notes: '',
    advance_percent: 100,
    is_credit_order: false
  });
  
  // Customer Master integration
  const [selectedCustomer, setSelectedCustomer] = useState(null);
  const [customerProfileId, setCustomerProfileId] = useState(null);
  
  const [file, setFile] = useState(null);
  const [advanceSettings, setAdvanceSettings] = useState(null);
  const [allowedPercentages, setAllowedPercentages] = useState([100]);
  const [creditAvailable, setCreditAvailable] = useState(false);

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

  // Rewards state
  const [rewardsBalance, setRewardsBalance] = useState({ credit_balance: 0, points_balance: 0, total_available: 0 });
  const [redeemAmount, setRedeemAmount] = useState(0);
  const [applyingRewards, setApplyingRewards] = useState(false);

  // GST state
  const [gstStates, setGstStates] = useState([]);
  const [deliveryState, setDeliveryState] = useState('27'); // Default Maharashtra
  const [gstInfo, setGstInfo] = useState(null);
  const [customerGSTIN, setCustomerGSTIN] = useState('');
  const [calculatingGST, setCalculatingGST] = useState(false);
  const [companyGSTInfo, setCompanyGSTInfo] = useState(null);

  useEffect(() => {
    fetchProducts();
    fetchProductConfig();
    fetchRewardsBalance();
    fetchGSTData();
    // Pre-fill from user data
    if (user) {
      setOrderDetails(prev => ({ 
        ...prev, 
        customer_name: user.name || '',
        delivery_address: user.address || ''
      }));
    }
    
    // Check for repeat order data
    const repeatData = sessionStorage.getItem('repeatOrder');
    if (repeatData) {
      const parsed = JSON.parse(repeatData);
      setCartItems([{
        id: Date.now(),
        product_id: parsed.product_id || '',
        product_name: '',
        thickness: parsed.thickness?.toString() || '',
        color: parsed.color || '',
        color_name: parsed.color_name || '',
        width: parsed.width?.toString() || '',
        height: parsed.height?.toString() || '',
        quantity: parsed.quantity || 1,
        unit_price: 0,
        total_price: 0,
        calculated: false
      }]);
      sessionStorage.removeItem('repeatOrder');
      toast.info('Previous order specifications loaded');
    }
  }, [user]);

  const fetchProducts = async () => {
    try {
      const response = await api.products.getAll();
      setProducts(response.data);
    } catch (error) {
      console.error('Failed to fetch products:', error);
      toast.error('Failed to load products');
    }
  };

  const fetchProductConfig = async () => {
    try {
      const response = await erpApi.config.getAll();
      setProductConfig({
        colors: response.data.colors || [],
        thickness_options: response.data.thickness_options || [],
        glass_types: response.data.glass_types || []
      });
    } catch (error) {
      console.error('Failed to fetch product config:', error);
      // Set default colors if API fails
      setProductConfig({
        colors: [
          { id: 'clear', name: 'Clear', hex_code: '#E8E8E8', price_multiplier: 1.0 },
          { id: 'grey', name: 'Grey', hex_code: '#808080', price_multiplier: 1.1 },
          { id: 'bronze', name: 'Bronze', hex_code: '#CD7F32', price_multiplier: 1.15 },
          { id: 'green', name: 'Green', hex_code: '#228B22', price_multiplier: 1.1 },
          { id: 'blue', name: 'Blue', hex_code: '#4169E1', price_multiplier: 1.15 },
        ],
        thickness_options: [],
        glass_types: []
      });
    }
  };

  const fetchRewardsBalance = async () => {
    try {
      const response = await erpApi.rewards.getMyBalance();
      setRewardsBalance(response.data);
    } catch (error) {
      console.error('Failed to fetch rewards balance:', error);
      setRewardsBalance({ credit_balance: 0, points_balance: 0, total_available: 0 });
    }
  };

  const fetchGSTData = async () => {
    try {
      const [statesRes, companyRes] = await Promise.all([
        erpApi.gst.getStates(),
        erpApi.gst.getCompanyInfo()
      ]);
      setGstStates(statesRes.data.states || []);
      setCompanyGSTInfo(companyRes.data);
    } catch (error) {
      console.error('Failed to fetch GST data:', error);
    }
  };

  const calculateGST = async (baseAmount, stateCode) => {
    if (!baseAmount || !stateCode) return null;
    
    setCalculatingGST(true);
    try {
      const response = await erpApi.gst.calculateGST({
        amount: baseAmount,
        delivery_state_code: stateCode,
        hsn_code: '7007' // Default: Safety glass
      });
      setGstInfo(response.data);
      return response.data;
    } catch (error) {
      console.error('GST calculation failed:', error);
      toast.error('GST calculation failed');
      return null;
    } finally {
      setCalculatingGST(false);
    }
  };

  // Recalculate GST when delivery state changes or items change
  useEffect(() => {
    if (step === 2 && deliveryState && cartItems.some(i => i.calculated)) {
      const baseAmount = getSubtotalBeforeRewards();
      if (baseAmount > 0) {
        calculateGST(baseAmount, deliveryState);
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [deliveryState, step]);

  // Handle customer selection from CustomerSearch
  const handleCustomerSelect = (customer) => {
    setSelectedCustomer(customer);
    setCustomerProfileId(customer.id);
    
    // Auto-populate order details from customer profile
    setOrderDetails(prev => ({
      ...prev,
      customer_name: customer.display_name || customer.company_name || customer.individual_name || prev.customer_name,
      company_name: customer.company_name || '',
      is_credit_order: customer.credit_type === 'credit_allowed'
    }));
    
    // Auto-populate GSTIN
    if (customer.gstin) {
      setCustomerGSTIN(customer.gstin);
    }
    
    // Auto-populate billing address as delivery address
    if (customer.billing_address) {
      const addr = customer.billing_address;
      const fullAddress = [
        addr.address_line1,
        addr.address_line2,
        addr.city,
        `${addr.state} - ${addr.pin_code}`
      ].filter(Boolean).join(', ');
      
      setOrderDetails(prev => ({
        ...prev,
        delivery_address: fullAddress
      }));
    }
    
    // Auto-set delivery state from GSTIN
    if (customer.gstin && customer.gstin.length >= 2) {
      const stateCode = customer.gstin.substring(0, 2);
      setDeliveryState(stateCode);
    }
    
    // Enable credit order option if customer has credit
    if (customer.credit_type === 'credit_allowed') {
      setCreditAvailable(true);
      toast.success(`Credit customer selected! Limit: ₹${customer.credit_limit?.toLocaleString()}, ${customer.credit_days} days`);
    }
  };

  const handleCustomerClear = () => {
    setSelectedCustomer(null);
    setCustomerProfileId(null);
    setOrderDetails(prev => ({
      ...prev,
      is_credit_order: false
    }));
    // Don't clear all fields - user might want to enter manually
  };

  const addNewItem = () => {
    setCartItems([...cartItems, {
      id: Date.now(),
      product_id: '',
      product_name: '',
      thickness: '',
      color: '',
      color_name: '',
      width: '',
      height: '',
      quantity: 1,
      unit_price: 0,
      total_price: 0,
      calculated: false
    }]);
  };

  const removeItem = (itemId) => {
    if (cartItems.length === 1) {
      toast.error('At least one item is required');
      return;
    }
    setCartItems(cartItems.filter(item => item.id !== itemId));
  };

  const updateItem = (itemId, field, value) => {
    setCartItems(cartItems.map(item => {
      if (item.id === itemId) {
        const updated = { ...item, [field]: value, calculated: false };
        if (field === 'product_id') {
          const product = products.find(p => p.id === value);
          updated.product_name = product?.name || '';
          updated.thickness = ''; // Reset thickness when product changes
          updated.color = ''; // Reset color when product changes
          updated.color_name = '';
        }
        if (field === 'color') {
          const selectedColor = productConfig.colors.find(c => c.id === value);
          updated.color_name = selectedColor?.name || '';
        }
        return updated;
      }
      return item;
    }));
  };

  const calculateItemPrice = async (itemId) => {
    const item = cartItems.find(i => i.id === itemId);
    if (!item.product_id || !item.thickness || !item.width || !item.height) {
      toast.error('Please fill all required fields');
      return;
    }

    setLoading(true);
    try {
      const response = await api.pricing.calculate({
        product_id: item.product_id,
        thickness: parseFloat(item.thickness),
        width: parseFloat(item.width),
        height: parseFloat(item.height),
        quantity: parseInt(item.quantity)
      });
      
      setCartItems(cartItems.map(i => {
        if (i.id === itemId) {
          return {
            ...i,
            unit_price: response.data.unit_price,
            total_price: response.data.total,
            calculated: true
          };
        }
        return i;
      }));
      
      toast.success('Price calculated!');
    } catch (error) {
      console.error('Price calculation failed:', error);
      toast.error('Failed to calculate price');
    } finally {
      setLoading(false);
    }
  };

  const calculateAllPrices = async () => {
    setLoading(true);
    let allCalculated = true;
    
    for (const item of cartItems) {
      if (!item.product_id || !item.thickness || !item.width || !item.height) {
        toast.error(`Please fill all fields for ${item.product_name || 'item'}`);
        allCalculated = false;
        break;
      }
      
      try {
        const response = await api.pricing.calculate({
          product_id: item.product_id,
          thickness: parseFloat(item.thickness),
          width: parseFloat(item.width),
          height: parseFloat(item.height),
          quantity: parseInt(item.quantity)
        });
        
        setCartItems(prev => prev.map(i => {
          if (i.id === item.id) {
            return {
              ...i,
              unit_price: response.data.unit_price,
              total_price: response.data.total,
              calculated: true
            };
          }
          return i;
        }));
      } catch (error) {
        toast.error(`Failed to calculate price for ${item.product_name}`);
        allCalculated = false;
        break;
      }
    }
    
    setLoading(false);
    
    if (allCalculated) {
      // Fetch advance options
      const total = cartItems.reduce((sum, item) => sum + (item.total_price || 0), 0);
      await fetchAdvanceOptions(total);
      setStep(2);
      toast.success('All prices calculated!');
    }
  };

  const fetchAdvanceOptions = async (totalAmount) => {
    try {
      const token = localStorage.getItem('token');
      const response = await fetch(
        `${API_URL}/api/settings/advance/validate-order?amount=${totalAmount}`,
        { headers: { 'Authorization': `Bearer ${token}` } }
      );
      const data = await response.json();
      setAdvanceSettings(data);
      setAllowedPercentages(data.allowed_percentages || [100]);
      setCreditAvailable(data.credit_available || false);
    } catch (error) {
      setAllowedPercentages([25, 50, 75, 100]);
    }
  };

  const getCartTotal = () => {
    const itemsTotal = cartItems.reduce((sum, item) => sum + (item.total_price || 0), 0);
    const transport = transportRequired && transportCost ? transportCost.total_transport_cost : 0;
    const subtotal = itemsTotal + transport;
    const afterRewards = Math.max(0, subtotal - redeemAmount);
    // Add GST if calculated
    const gstAmount = gstInfo?.breakdown?.total_gst || 0;
    return afterRewards + gstAmount;
  };

  const getSubtotalBeforeGST = () => {
    const itemsTotal = cartItems.reduce((sum, item) => sum + (item.total_price || 0), 0);
    const transport = transportRequired && transportCost ? transportCost.total_transport_cost : 0;
    const subtotal = itemsTotal + transport;
    return Math.max(0, subtotal - redeemAmount);
  };

  const getSubtotalBeforeRewards = () => {
    const itemsTotal = cartItems.reduce((sum, item) => sum + (item.total_price || 0), 0);
    const transport = transportRequired && transportCost ? transportCost.total_transport_cost : 0;
    return itemsTotal + transport;
  };

  const getItemsTotal = () => {
    return cartItems.reduce((sum, item) => sum + (item.total_price || 0), 0);
  };

  const getTotalSqft = () => {
    return cartItems.reduce((sum, item) => {
      const sqft = (parseFloat(item.width || 0) * parseFloat(item.height || 0) / 144) * parseInt(item.quantity || 1);
      return sum + sqft;
    }, 0);
  };

  const getTotalQuantity = () => {
    return cartItems.reduce((sum, item) => sum + parseInt(item.quantity || 0), 0);
  };

  // Transport functions
  const getCurrentLocation = () => {
    if (!navigator.geolocation) {
      toast.error('Geolocation not supported by your browser');
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
        // Auto-calculate transport cost
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

  const handleCreateOrder = async () => {
    if (!user) {
      toast.error('Please login to place an order');
      navigate('/login');
      return;
    }

    if (!orderDetails.delivery_address) {
      toast.error('Please enter delivery address');
      return;
    }

    if (!orderDetails.customer_name) {
      toast.error('Please enter customer name');
      return;
    }

    setLoading(true);
    try {
      // Create order with calculated total from cart
      const firstItem = cartItems[0];
      const subtotal = getSubtotalBeforeRewards();
      const cartTotal = getCartTotal(); // After redeem
      
      // Debug log for payment troubleshooting
      console.log('Order Creation Debug:', {
        subtotal,
        redeemAmount,
        cartTotal,
        gstInfo,
        advancePercent: orderDetails.advance_percent,
        expectedAdvance: Math.round(cartTotal * orderDetails.advance_percent / 100)
      });
      
      // Build detailed order notes with all items including color
      const itemDetails = cartItems.map((item, i) => 
        `Item ${i+1}: ${item.product_name} - ${item.thickness}mm - ${item.color_name || 'No Color'} - ${item.width}x${item.height} - Qty: ${item.quantity} - ₹${item.total_price}`
      ).join('\n');
      
      // Add rewards info to notes if applied
      const rewardsNote = redeemAmount > 0 ? `\n\nRewards Applied: ₹${redeemAmount} (Original: ₹${subtotal})` : '';
      
      // Build GST note
      const deliveryStateName = gstStates.find(s => s.code === deliveryState)?.name || '';
      const gstNote = gstInfo ? `\n\nGST Details:\nDelivery State: ${deliveryStateName} (${deliveryState})\nGST Type: ${gstInfo.gst_type === 'intra_state' ? 'CGST+SGST' : 'IGST'}\n${
        gstInfo.gst_type === 'intra_state' 
          ? `CGST: ₹${gstInfo.breakdown?.cgst_amount}, SGST: ₹${gstInfo.breakdown?.sgst_amount}` 
          : `IGST: ₹${gstInfo.breakdown?.igst_amount}`
      }\nTotal GST: ₹${gstInfo.breakdown?.total_gst}\nHSN Code: 7007${customerGSTIN ? `\nCustomer GSTIN: ${customerGSTIN}` : ''}` : '';
      
      const response = await api.orders.create({
        product_id: firstItem.product_id,
        thickness: parseFloat(firstItem.thickness),
        color: firstItem.color || '',
        width: parseFloat(firstItem.width),
        height: parseFloat(firstItem.height),
        quantity: getTotalQuantity(),
        customer_name: orderDetails.customer_name,
        company_name: orderDetails.company_name,
        delivery_address: orderDetails.delivery_address,
        notes: orderDetails.notes + (cartItems.length >= 1 ? `\n\nOrder Details:\n${itemDetails}` : '') + rewardsNote + gstNote,
        advance_percent: parseInt(orderDetails.advance_percent),
        is_credit_order: orderDetails.is_credit_order,
        override_total: cartTotal, // Send calculated cart total with GST
        rewards_applied: redeemAmount, // Send rewards amount applied
        // GST Info for order
        delivery_state_code: deliveryState,
        customer_gstin: customerGSTIN || '',
        gst_info: gstInfo ? {
          gst_type: gstInfo.gst_type,
          hsn_code: '7007',
          cgst_rate: gstInfo.breakdown?.cgst_rate || 0,
          cgst_amount: gstInfo.breakdown?.cgst_amount || 0,
          sgst_rate: gstInfo.breakdown?.sgst_rate || 0,
          sgst_amount: gstInfo.breakdown?.sgst_amount || 0,
          igst_rate: gstInfo.breakdown?.igst_rate || 0,
          igst_amount: gstInfo.breakdown?.igst_amount || 0,
          total_gst: gstInfo.breakdown?.total_gst || 0
        } : null,
        // Customer Master Integration
        customer_profile_id: customerProfileId || null
      });

      const { order_id, order_number, razorpay_order_id, advance_amount, total_amount, advance_percent, remaining_amount, is_credit_order } = response.data;
      
      // Redeem rewards if applied
      if (redeemAmount > 0) {
        try {
          await erpApi.rewards.redeemCredit({ amount: redeemAmount, order_id: order_id });
          console.log('Rewards redeemed successfully');
        } catch (error) {
          console.error('Failed to redeem rewards:', error);
          // Don't block order - rewards will be handled manually
        }
      }
      
      // Debug log response from backend
      console.log('Order Response Debug:', {
        order_id,
        total_amount,
        advance_percent,
        advance_amount,
        remaining_amount,
        razorpay_order_id
      });

      if (file) {
        await api.orders.uploadFile(order_id, file);
      }

      // Credit Order - No payment needed
      if (is_credit_order) {
        toast.success(`Credit order #${order_number} created! Payment of ₹${total_amount?.toLocaleString()} due after delivery.`);
        navigate('/portal');
        return;
      }

      // Razorpay payment - use advance_amount from backend (already calculated correctly)
      const razorpayAmount = Math.round(advance_amount * 100); // Convert to paise
      console.log('Razorpay Debug:', { advance_amount, razorpayAmount });
      
      const options = {
        key: process.env.REACT_APP_RAZORPAY_KEY_ID || 'rzp_test_key',
        amount: razorpayAmount,
        currency: 'INR',
        order_id: razorpay_order_id,
        name: 'Lucumaa Glass',
        description: advance_percent === 100 
          ? `Order #${order_number} - Full Payment` 
          : `Order #${order_number} - ${advance_percent}% Advance`,
        handler: async (response) => {
          try {
            await api.orders.verifyPayment(order_id, {
              razorpay_payment_id: response.razorpay_payment_id,
              razorpay_signature: response.razorpay_signature
            });
            
            if (advance_percent === 100) {
              toast.success('Payment successful! Order confirmed.');
            } else {
              toast.success(`Advance of ₹${advance_amount.toLocaleString()} paid! Remaining ₹${remaining_amount.toLocaleString()} due before dispatch.`);
            }
            navigate('/portal');
          } catch (error) {
            toast.error('Payment verification failed');
          }
        },
        prefill: {
          name: orderDetails.customer_name || user.name,
          email: user.email,
          contact: user.phone
        },
        theme: { color: '#0d9488' }
      };

      const razorpay = new window.Razorpay(options);
      razorpay.open();
    } catch (error) {
      console.error('Order creation failed:', error);
      toast.error('Failed to create order');
    } finally {
      setLoading(false);
    }
  };

  const getSelectedProduct = (productId) => products.find(p => p.id === productId);

  return (
    <div className="min-h-screen py-16 md:py-20 bg-slate-50" data-testid="customize-book-page">
      <div className="max-w-5xl mx-auto px-4 sm:px-6 lg:px-8">
        {/* Header */}
        <div className="text-center mb-8 md:mb-12">
          <h1 className="text-2xl md:text-4xl font-bold text-slate-900 mb-3 md:mb-4">
            Customize & Book Your Glass
          </h1>
          <p className="text-base md:text-lg text-slate-600">
            Add multiple glass pieces with different sizes and specifications
          </p>
        </div>

        {/* Progress Steps */}
        <div className="mb-8 flex items-center justify-center gap-2 md:gap-4">
          <div className={`flex items-center gap-1 md:gap-2 ${step >= 1 ? 'text-teal-700' : 'text-slate-400'}`}>
            <div className={`w-8 h-8 md:w-10 md:h-10 rounded-full flex items-center justify-center text-sm md:text-base ${step >= 1 ? 'bg-teal-600 text-white' : 'bg-slate-200'}`}>
              1
            </div>
            <span className="font-medium text-sm md:text-base hidden sm:inline">Configure</span>
          </div>
          <div className="w-8 md:w-12 h-1 bg-slate-200"></div>
          <div className={`flex items-center gap-1 md:gap-2 ${step >= 2 ? 'text-teal-700' : 'text-slate-400'}`}>
            <div className={`w-8 h-8 md:w-10 md:h-10 rounded-full flex items-center justify-center text-sm md:text-base ${step >= 2 ? 'bg-teal-600 text-white' : 'bg-slate-200'}`}>
              2
            </div>
            <span className="font-medium text-sm md:text-base hidden sm:inline">Details</span>
          </div>
          <div className="w-8 md:w-12 h-1 bg-slate-200"></div>
          <div className={`flex items-center gap-1 md:gap-2 ${step >= 3 ? 'text-teal-700' : 'text-slate-400'}`}>
            <div className={`w-8 h-8 md:w-10 md:h-10 rounded-full flex items-center justify-center text-sm md:text-base ${step >= 3 ? 'bg-teal-600 text-white' : 'bg-slate-200'}`}>
              3
            </div>
            <span className="font-medium text-sm md:text-base hidden sm:inline">Payment</span>
          </div>
        </div>

        {/* Step 1: Configure Glass Items */}
        {step === 1 && (
          <div className="space-y-6">
            {cartItems.map((item, index) => (
              <Card key={item.id} className="relative">
                <CardHeader className="pb-2">
                  <div className="flex items-center justify-between">
                    <CardTitle className="text-base md:text-lg flex items-center gap-2">
                      <Package className="w-5 h-5 text-teal-600" />
                      Glass Item #{index + 1}
                    </CardTitle>
                    {cartItems.length > 1 && (
                      <Button
                        onClick={() => removeItem(item.id)}
                        variant="ghost"
                        size="sm"
                        className="text-red-500 hover:text-red-700 hover:bg-red-50"
                      >
                        <Trash2 className="w-4 h-4" />
                      </Button>
                    )}
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  {/* Glass Type */}
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-2">
                      Glass Type *
                    </label>
                    <select
                      value={item.product_id}
                      onChange={(e) => updateItem(item.id, 'product_id', e.target.value)}
                      className="w-full h-11 md:h-12 rounded-lg border border-slate-300 px-3 md:px-4 text-sm md:text-base"
                      data-testid={`select-product-${index}`}
                    >
                      <option value="">Choose glass type</option>
                      {products.map(product => (
                        <option key={product.id} value={product.id}>{product.name}</option>
                      ))}
                    </select>
                  </div>

                  {/* Thickness */}
                  {item.product_id && getSelectedProduct(item.product_id)?.thickness_options && (
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">
                        Thickness (mm) *
                      </label>
                      <div className="flex flex-wrap gap-2">
                        {getSelectedProduct(item.product_id).thickness_options.map(thickness => (
                          <button
                            key={thickness}
                            onClick={() => updateItem(item.id, 'thickness', thickness.toString())}
                            className={`px-3 md:px-4 py-2 rounded-lg border-2 font-medium text-sm transition-all ${
                              item.thickness === thickness.toString()
                                ? 'border-teal-600 bg-teal-50 text-teal-700'
                                : 'border-slate-200 hover:border-slate-300'
                            }`}
                          >
                            {thickness}mm
                          </button>
                        ))}
                      </div>
                    </div>
                  )}

                  {/* Color Selection */}
                  {item.product_id && productConfig.colors.length > 0 && (
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">
                        <Palette className="w-4 h-4 inline mr-1" />
                        Color *
                      </label>
                      <div className="flex flex-wrap gap-2">
                        {productConfig.colors.filter(c => c.active !== false).map(color => (
                          <button
                            key={color.id}
                            onClick={() => updateItem(item.id, 'color', color.id)}
                            className={`flex items-center gap-2 px-3 md:px-4 py-2 rounded-lg border-2 font-medium text-sm transition-all ${
                              item.color === color.id
                                ? 'border-teal-600 bg-teal-50 text-teal-700'
                                : 'border-slate-200 hover:border-slate-300'
                            }`}
                            data-testid={`select-color-${index}-${color.id}`}
                          >
                            <span 
                              className="w-5 h-5 rounded-full border border-slate-300 shadow-sm"
                              style={{ backgroundColor: color.hex_code || '#ccc' }}
                            />
                            {color.name}
                            {color.price_multiplier > 1 && (
                              <span className="text-xs text-amber-600">+{Math.round((color.price_multiplier - 1) * 100)}%</span>
                            )}
                          </button>
                        ))}
                      </div>
                    </div>
                  )}

                  {/* Dimensions & Quantity */}
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-3 md:gap-4">
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-1">Width (in) *</label>
                      <input
                        type="number"
                        value={item.width}
                        onChange={(e) => updateItem(item.id, 'width', e.target.value)}
                        className="w-full h-11 rounded-lg border border-slate-300 px-3 text-sm"
                        placeholder="24"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-1">Height (in) *</label>
                      <input
                        type="number"
                        value={item.height}
                        onChange={(e) => updateItem(item.id, 'height', e.target.value)}
                        className="w-full h-11 rounded-lg border border-slate-300 px-3 text-sm"
                        placeholder="36"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-1">Quantity *</label>
                      <div className="flex items-center h-11 border border-slate-300 rounded-lg">
                        <button
                          onClick={() => updateItem(item.id, 'quantity', Math.max(1, parseInt(item.quantity) - 1))}
                          className="px-3 h-full hover:bg-slate-100 rounded-l-lg"
                        >
                          <Minus className="w-4 h-4" />
                        </button>
                        <input
                          type="number"
                          value={item.quantity}
                          onChange={(e) => updateItem(item.id, 'quantity', Math.max(1, parseInt(e.target.value) || 1))}
                          className="w-full h-full text-center border-0 text-sm"
                          min="1"
                        />
                        <button
                          onClick={() => updateItem(item.id, 'quantity', parseInt(item.quantity) + 1)}
                          className="px-3 h-full hover:bg-slate-100 rounded-r-lg"
                        >
                          <Plus className="w-4 h-4" />
                        </button>
                      </div>
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-1">Price</label>
                      <div className="h-11 rounded-lg bg-slate-100 px-3 flex items-center justify-center font-semibold text-teal-700">
                        {item.calculated ? `₹${item.total_price.toLocaleString()}` : '—'}
                      </div>
                    </div>
                  </div>

                  {/* Calculate Single Item */}
                  {!item.calculated && item.product_id && item.thickness && item.width && item.height && (
                    <Button
                      onClick={() => calculateItemPrice(item.id)}
                      variant="outline"
                      size="sm"
                      disabled={loading}
                      className="text-teal-600 border-teal-600"
                    >
                      <Calculator className="w-4 h-4 mr-2" />
                      Calculate Price
                    </Button>
                  )}
                  
                  {item.calculated && (
                    <p className="text-sm text-green-600 flex items-center gap-1">
                      <CheckCircle className="w-4 h-4" />
                      Price calculated: ₹{item.total_price.toLocaleString()}
                    </p>
                  )}
                </CardContent>
              </Card>
            ))}

            {/* Add More Items */}
            <Button
              onClick={addNewItem}
              variant="outline"
              className="w-full py-6 border-dashed border-2 text-slate-600 hover:text-teal-600 hover:border-teal-600"
              data-testid="add-item-btn"
            >
              <Plus className="w-5 h-5 mr-2" />
              Add Another Glass Item
            </Button>

            {/* Transport Option */}
            <Card className="border-amber-200 bg-amber-50/50">
              <CardHeader className="pb-3">
                <CardTitle className="text-base flex items-center gap-2">
                  <Truck className="w-5 h-5 text-amber-600" />
                  Transport Service
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                {/* Transport Required Checkbox */}
                <label className="flex items-center gap-3 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={transportRequired}
                    onChange={(e) => {
                      setTransportRequired(e.target.checked);
                      if (!e.target.checked) {
                        setTransportCost(null);
                        setTransportLocation({ address: '', landmark: '', lat: null, lng: null });
                      }
                    }}
                    className="w-5 h-5 rounded border-amber-400 text-amber-600 focus:ring-amber-500"
                    data-testid="transport-checkbox"
                  />
                  <span className="text-slate-700 font-medium">
                    I need transport/delivery service from factory
                  </span>
                </label>

                {/* Location Input - Show when transport required */}
                {transportRequired && (
                  <div className="space-y-4 pt-2 border-t border-amber-200">
                    <p className="text-sm text-slate-600">
                      Provide your delivery location for transport cost calculation
                    </p>
                    
                    {/* Location Options */}
                    <div className="flex flex-wrap gap-2">
                      <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        onClick={getCurrentLocation}
                        disabled={gettingLocation}
                        className="gap-2"
                        data-testid="get-current-location-btn"
                      >
                        {gettingLocation ? (
                          <Loader2 className="w-4 h-4 animate-spin" />
                        ) : (
                          <Navigation className="w-4 h-4" />
                        )}
                        {gettingLocation ? 'Getting Location...' : 'Use Current Location'}
                      </Button>
                      
                      {transportLocation.lat && (
                        <span className="text-xs text-green-600 flex items-center gap-1">
                          <CheckCircle className="w-4 h-4" />
                          GPS Location Captured
                        </span>
                      )}
                    </div>

                    {/* Manual Address */}
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-1">
                        <MapPin className="w-4 h-4 inline mr-1" />
                        Delivery Address
                      </label>
                      <textarea
                        value={transportLocation.address}
                        onChange={(e) => setTransportLocation(prev => ({ ...prev, address: e.target.value }))}
                        placeholder="Full delivery address (Street, Area, City, State, Pincode)"
                        className="w-full rounded-lg border border-slate-300 px-3 py-2 h-20 text-sm"
                        data-testid="transport-address-input"
                      />
                    </div>

                    {/* Landmark */}
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-1">
                        Nearby Landmark (Optional)
                      </label>
                      <input
                        type="text"
                        value={transportLocation.landmark}
                        onChange={(e) => setTransportLocation(prev => ({ ...prev, landmark: e.target.value }))}
                        placeholder="Any nearby landmark for easy location"
                        className="w-full h-10 rounded-lg border border-slate-300 px-3 text-sm"
                      />
                    </div>

                    {/* Calculate Transport Button */}
                    <Button
                      type="button"
                      onClick={() => calculateTransportCost()}
                      disabled={calculatingTransport || (!transportLocation.address && !transportLocation.lat)}
                      className="bg-amber-600 hover:bg-amber-700 gap-2"
                      data-testid="calculate-transport-btn"
                    >
                      {calculatingTransport ? (
                        <>
                          <Loader2 className="w-4 h-4 animate-spin" />
                          Calculating...
                        </>
                      ) : (
                        <>
                          <Calculator className="w-4 h-4" />
                          Calculate Transport Cost
                        </>
                      )}
                    </Button>

                    {/* Transport Cost Display */}
                    {transportCost && (
                      <div className="bg-white rounded-lg p-4 border border-amber-200">
                        <div className="flex justify-between items-start mb-3">
                          <div>
                            <p className="font-medium text-slate-900">Transport Cost Breakdown</p>
                            <p className="text-sm text-slate-500">
                              Distance: {transportCost.distance_km} km from factory
                            </p>
                          </div>
                          <p className="text-xl font-bold text-amber-700">
                            ₹{transportCost.total_transport_cost.toLocaleString()}
                          </p>
                        </div>
                        <div className="text-xs text-slate-500 space-y-1">
                          <div className="flex justify-between">
                            <span>Base charge ({transportCost.breakdown.base_km_included} km included)</span>
                            <span>₹{transportCost.breakdown.base_charge}</span>
                          </div>
                          {transportCost.breakdown.extra_km > 0 && (
                            <div className="flex justify-between">
                              <span>Extra {transportCost.breakdown.extra_km} km @ ₹{transportCost.breakdown.per_km_rate}/km</span>
                              <span>₹{(transportCost.breakdown.distance_charge - transportCost.breakdown.base_charge).toFixed(0)}</span>
                            </div>
                          )}
                          {transportCost.breakdown.load_charge > 0 && (
                            <div className="flex justify-between">
                              <span>Load charge ({transportCost.breakdown.load_sqft.toFixed(1)} sq.ft)</span>
                              <span>₹{transportCost.breakdown.load_charge}</span>
                            </div>
                          )}
                          {transportCost.breakdown.gst_amount > 0 && (
                            <div className="flex justify-between">
                              <span>GST ({transportCost.breakdown.gst_percent}%)</span>
                              <span>₹{transportCost.breakdown.gst_amount}</span>
                            </div>
                          )}
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </CardContent>
            </Card>

            {/* Summary & Next */}
            <Card className="bg-teal-50 border-teal-200">
              <CardContent className="p-4 md:p-6">
                <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
                  <div>
                    <p className="text-sm text-teal-700">Cart Summary</p>
                    <p className="text-2xl font-bold text-teal-800">
                      {cartItems.length} item(s) • {getTotalQuantity()} pcs
                      {transportRequired && transportCost && (
                        <span className="text-base font-normal text-amber-700 ml-2">
                          + Transport ₹{transportCost.total_transport_cost.toLocaleString()}
                        </span>
                      )}
                    </p>
                  </div>
                  <Button
                    onClick={calculateAllPrices}
                    disabled={loading || cartItems.some(i => !i.product_id || !i.thickness || !i.width || !i.height) || (transportRequired && !transportCost)}
                    className="bg-teal-600 hover:bg-teal-700 gap-2 w-full md:w-auto"
                    data-testid="calculate-all-btn"
                  >
                    {loading ? 'Calculating...' : (
                      <>
                        <Calculator className="w-4 h-4" />
                        Calculate All & Continue
                      </>
                    )}
                  </Button>
                </div>
              </CardContent>
            </Card>
          </div>
        )}

        {/* Step 2: Order Details */}
        {step === 2 && (
          <div className="space-y-6">
            {/* Order Summary */}
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <ShoppingCart className="w-5 h-5 text-teal-600" />
                  Order Summary
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-3">
                  {cartItems.map((item, index) => (
                    <div key={item.id} className="flex justify-between items-center py-2 border-b last:border-0">
                      <div>
                        <p className="font-medium text-slate-900">{item.product_name}</p>
                        <p className="text-sm text-slate-500">
                          {item.thickness}mm • {item.color_name && `${item.color_name} • `}{item.width}&quot; x {item.height}&quot; • Qty: {item.quantity}
                        </p>
                      </div>
                      <p className="font-semibold text-teal-700">₹{item.total_price.toLocaleString()}</p>
                    </div>
                  ))}
                  
                  {/* Transport Cost in Summary */}
                  {transportRequired && transportCost && (
                    <div className="flex justify-between items-center py-2 border-b bg-amber-50 px-2 rounded">
                      <div>
                        <p className="font-medium text-amber-800 flex items-center gap-2">
                          <Truck className="w-4 h-4" />
                          Transport Service
                        </p>
                        <p className="text-sm text-amber-600">
                          {transportCost.distance_km} km • {getTotalSqft().toFixed(1)} sq.ft load
                        </p>
                      </div>
                      <p className="font-semibold text-amber-700">₹{transportCost.total_transport_cost.toLocaleString()}</p>
                    </div>
                  )}
                </div>
                
                {/* Subtotals */}
                <div className="mt-4 pt-4 border-t space-y-2">
                  <div className="flex justify-between items-center text-sm">
                    <span className="text-slate-600">Glass Items Subtotal</span>
                    <span className="text-slate-900">₹{getItemsTotal().toLocaleString()}</span>
                  </div>
                  {transportRequired && transportCost && (
                    <div className="flex justify-between items-center text-sm">
                      <span className="text-amber-600">Transport Charges</span>
                      <span className="text-amber-700">₹{transportCost.total_transport_cost.toLocaleString()}</span>
                    </div>
                  )}
                  {redeemAmount > 0 && (
                    <div className="flex justify-between items-center text-sm">
                      <span className="text-purple-600">Rewards Redeemed</span>
                      <span className="text-purple-700">-₹{redeemAmount.toLocaleString()}</span>
                    </div>
                  )}
                  
                  {/* GST Breakdown */}
                  {gstInfo && (
                    <div className="bg-blue-50 rounded-lg p-3 mt-3">
                      <p className="text-xs font-medium text-blue-900 mb-2 flex items-center gap-1">
                        <Receipt className="w-3 h-3" />
                        GST Breakdown ({gstInfo.gst_type === 'intra_state' ? 'Intra-State' : 'Inter-State'})
                      </p>
                      <div className="space-y-1 text-sm">
                        <div className="flex justify-between">
                          <span className="text-blue-700">Taxable Amount</span>
                          <span className="text-blue-800">₹{getSubtotalBeforeGST().toLocaleString()}</span>
                        </div>
                        {gstInfo.gst_type === 'intra_state' ? (
                          <>
                            <div className="flex justify-between text-xs">
                              <span className="text-blue-600">CGST @ {gstInfo.breakdown?.cgst_rate}%</span>
                              <span className="text-blue-700">₹{gstInfo.breakdown?.cgst_amount?.toLocaleString()}</span>
                            </div>
                            <div className="flex justify-between text-xs">
                              <span className="text-blue-600">SGST @ {gstInfo.breakdown?.sgst_rate}%</span>
                              <span className="text-blue-700">₹{gstInfo.breakdown?.sgst_amount?.toLocaleString()}</span>
                            </div>
                          </>
                        ) : (
                          <div className="flex justify-between text-xs">
                            <span className="text-blue-600">IGST @ {gstInfo.breakdown?.igst_rate}%</span>
                            <span className="text-blue-700">₹{gstInfo.breakdown?.igst_amount?.toLocaleString()}</span>
                          </div>
                        )}
                        <div className="flex justify-between pt-1 border-t border-blue-200">
                          <span className="text-blue-800 font-medium">Total GST</span>
                          <span className="text-blue-900 font-semibold">₹{gstInfo.breakdown?.total_gst?.toLocaleString()}</span>
                        </div>
                      </div>
                    </div>
                  )}
                  
                  {calculatingGST && (
                    <div className="flex items-center gap-2 text-sm text-blue-600">
                      <Loader2 className="w-4 h-4 animate-spin" />
                      Calculating GST...
                    </div>
                  )}
                  
                  <div className="flex justify-between items-center pt-3 border-t">
                    <span className="text-lg font-bold text-slate-900">Grand Total (Incl. GST)</span>
                    <span className="text-2xl font-bold text-teal-700">₹{getCartTotal().toLocaleString()}</span>
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Rewards Redemption */}
            {rewardsBalance.total_available > 0 && (
              <Card className="border-purple-200 bg-purple-50/50">
                <CardHeader className="pb-3">
                  <CardTitle className="text-base flex items-center gap-2">
                    <Gift className="w-5 h-5 text-purple-600" />
                    Apply Rewards
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    <div className="flex items-center justify-between">
                      <div>
                        <p className="font-medium text-slate-900">Available Balance</p>
                        <p className="text-sm text-slate-600">
                          Credit: ₹{rewardsBalance.credit_balance?.toLocaleString()} • 
                          Points: {rewardsBalance.points_balance} (₹{rewardsBalance.points_value?.toLocaleString()})
                        </p>
                      </div>
                      <div className="text-2xl font-bold text-purple-600">
                        ₹{rewardsBalance.total_available?.toLocaleString()}
                      </div>
                    </div>
                    
                    <div className="flex items-center gap-3">
                      <div className="flex-1">
                        <label className="block text-sm font-medium text-slate-700 mb-1">
                          Amount to Redeem
                        </label>
                        <div className="flex items-center gap-2">
                          <span className="text-slate-500">₹</span>
                          <input
                            type="number"
                            value={redeemAmount || ''}
                            onChange={(e) => {
                              const value = Math.min(
                                parseFloat(e.target.value) || 0,
                                rewardsBalance.total_available,
                                getSubtotalBeforeRewards()
                              );
                              setRedeemAmount(Math.max(0, value));
                            }}
                            max={Math.min(rewardsBalance.total_available, getSubtotalBeforeRewards())}
                            className="w-full h-10 rounded-lg border border-slate-300 px-3"
                            placeholder="0"
                            data-testid="redeem-amount-input"
                          />
                        </div>
                      </div>
                      <div className="flex flex-col gap-1 pt-5">
                        <Button
                          type="button"
                          variant="outline"
                          size="sm"
                          onClick={() => setRedeemAmount(Math.min(rewardsBalance.total_available, getSubtotalBeforeRewards()))}
                          className="text-xs"
                        >
                          Use All
                        </Button>
                        {redeemAmount > 0 && (
                          <Button
                            type="button"
                            variant="ghost"
                            size="sm"
                            onClick={() => setRedeemAmount(0)}
                            className="text-xs text-red-600"
                          >
                            Clear
                          </Button>
                        )}
                      </div>
                    </div>
                    
                    {redeemAmount > 0 && (
                      <div className="bg-purple-100 rounded-lg p-3 text-sm text-purple-800">
                        <p className="font-medium">You&apos;ll save ₹{redeemAmount.toLocaleString()} on this order!</p>
                        <p className="text-purple-600">Remaining rewards: ₹{(rewardsBalance.total_available - redeemAmount).toLocaleString()}</p>
                      </div>
                    )}
                  </div>
                </CardContent>
              </Card>
            )}

            {/* Customer Details */}
            <Card>
              <CardHeader>
                <CardTitle>Delivery Details</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                {/* Customer Search - Auto-populate from Customer Master */}
                <div className="p-4 bg-gradient-to-r from-blue-50 to-purple-50 rounded-lg border border-blue-200">
                  <label className="block text-sm font-medium text-blue-800 mb-2 flex items-center gap-2">
                    <Search className="w-4 h-4" />
                    Search Existing Customer (Optional)
                  </label>
                  <CustomerSearch
                    selectedCustomer={selectedCustomer}
                    onSelect={handleCustomerSelect}
                    onClear={handleCustomerClear}
                    placeholder="Search by name, mobile, GSTIN..."
                    showCreditInfo={true}
                  />
                  {!selectedCustomer && (
                    <p className="text-xs text-blue-600 mt-2">
                      Select a customer to auto-fill billing details, GSTIN, and credit terms
                    </p>
                  )}
                  {selectedCustomer?.credit_type === 'credit_allowed' && (
                    <div className="mt-2 p-2 bg-green-100 rounded text-sm text-green-800">
                      <CreditCard className="w-4 h-4 inline mr-1" />
                      Credit Customer: ₹{selectedCustomer.credit_limit?.toLocaleString()} limit, {selectedCustomer.credit_days} days
                    </div>
                  )}
                </div>

                <div className="grid md:grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-1">Customer Name *</label>
                    <input
                      type="text"
                      value={orderDetails.customer_name}
                      onChange={(e) => setOrderDetails({ ...orderDetails, customer_name: e.target.value })}
                      className="w-full h-11 rounded-lg border border-slate-300 px-3"
                      placeholder="Your name"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-1">Company Name</label>
                    <input
                      type="text"
                      value={orderDetails.company_name}
                      onChange={(e) => setOrderDetails({ ...orderDetails, company_name: e.target.value })}
                      className="w-full h-11 rounded-lg border border-slate-300 px-3"
                      placeholder="Company (optional)"
                    />
                  </div>
                </div>
                
                {/* Delivery State & GSTIN - GST Section */}
                <div className="grid md:grid-cols-2 gap-4 p-4 bg-blue-50 rounded-lg border border-blue-200">
                  <div>
                    <label className="block text-sm font-medium text-blue-800 mb-1">
                      <MapPin className="w-4 h-4 inline mr-1" />
                      Delivery State *
                    </label>
                    <select
                      value={deliveryState}
                      onChange={(e) => {
                        setDeliveryState(e.target.value);
                        // GST will auto-recalculate via useEffect
                      }}
                      className="w-full h-11 rounded-lg border border-blue-300 px-3 bg-white"
                      data-testid="delivery-state-select"
                    >
                      {gstStates.map((state) => (
                        <option key={state.code} value={state.code}>
                          {state.name} ({state.code})
                        </option>
                      ))}
                    </select>
                    <p className="text-xs text-blue-600 mt-1">
                      {gstInfo?.gst_type === 'intra_state' 
                        ? `Same state as factory - CGST + SGST applies`
                        : `Different state - IGST applies`
                      }
                    </p>
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-blue-800 mb-1">
                      <Building2 className="w-4 h-4 inline mr-1" />
                      GSTIN (Optional)
                    </label>
                    <input
                      type="text"
                      value={customerGSTIN}
                      onChange={(e) => setCustomerGSTIN(e.target.value.toUpperCase())}
                      className="w-full h-11 rounded-lg border border-blue-300 px-3 font-mono"
                      placeholder="22AAAAA0000A1Z5"
                      maxLength={15}
                      data-testid="customer-gstin-input"
                    />
                    <p className="text-xs text-blue-600 mt-1">
                      For GST invoice, enter your 15-digit GSTIN
                    </p>
                  </div>
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-1">Delivery Address *</label>
                  <textarea
                    value={orderDetails.delivery_address}
                    onChange={(e) => setOrderDetails({ ...orderDetails, delivery_address: e.target.value })}
                    className="w-full rounded-lg border border-slate-300 px-3 py-2 h-24"
                    placeholder="Full delivery address"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-1">Special Notes</label>
                  <textarea
                    value={orderDetails.notes}
                    onChange={(e) => setOrderDetails({ ...orderDetails, notes: e.target.value })}
                    className="w-full rounded-lg border border-slate-300 px-3 py-2 h-20"
                    placeholder="Any special instructions or requirements"
                  />
                </div>
                
                {/* File Upload */}
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-1">
                    Upload Drawing/Reference (Optional)
                  </label>
                  <div className="border-2 border-dashed border-slate-300 rounded-lg p-4 text-center">
                    <input
                      type="file"
                      onChange={(e) => setFile(e.target.files?.[0] || null)}
                      className="hidden"
                      id="file-upload"
                      accept=".pdf,.png,.jpg,.jpeg"
                    />
                    <label htmlFor="file-upload" className="cursor-pointer">
                      <Upload className="w-8 h-8 text-slate-400 mx-auto mb-2" />
                      <p className="text-sm text-slate-600">
                        {file ? file.name : 'Click to upload (PDF, PNG, JPG)'}
                      </p>
                    </label>
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Payment Options */}
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <CreditCard className="w-5 h-5 text-teal-600" />
                  Payment Options
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-2">Select Advance Percentage</label>
                  <div className="flex flex-wrap gap-2">
                    {allowedPercentages.map(percent => (
                      <button
                        key={percent}
                        onClick={() => setOrderDetails({ ...orderDetails, advance_percent: percent, is_credit_order: false })}
                        className={`px-4 py-2 rounded-lg border-2 font-medium transition-all ${
                          orderDetails.advance_percent === percent && !orderDetails.is_credit_order
                            ? 'border-teal-600 bg-teal-50 text-teal-700'
                            : 'border-slate-200 hover:border-slate-300'
                        }`}
                      >
                        {percent}%
                      </button>
                    ))}
                    {creditAvailable && (
                      <button
                        onClick={() => setOrderDetails({ ...orderDetails, is_credit_order: true, advance_percent: 0 })}
                        className={`px-4 py-2 rounded-lg border-2 font-medium transition-all ${
                          orderDetails.is_credit_order
                            ? 'border-purple-600 bg-purple-50 text-purple-700'
                            : 'border-slate-200 hover:border-slate-300'
                        }`}
                      >
                        Credit (Pay Later)
                      </button>
                    )}
                  </div>
                </div>

                {/* Payment Summary */}
                <div className="bg-slate-50 rounded-lg p-4">
                  <div className="space-y-2">
                    <div className="flex justify-between text-sm">
                      <span>Total Amount</span>
                      <span>₹{getCartTotal().toLocaleString()}</span>
                    </div>
                    {!orderDetails.is_credit_order && (
                      <>
                        <div className="flex justify-between text-sm text-teal-700">
                          <span>Pay Now ({orderDetails.advance_percent}%)</span>
                          <span className="font-semibold">₹{Math.round(getCartTotal() * orderDetails.advance_percent / 100).toLocaleString()}</span>
                        </div>
                        {orderDetails.advance_percent < 100 && (
                          <div className="flex justify-between text-sm text-amber-600">
                            <span>Due Before Dispatch</span>
                            <span>₹{Math.round(getCartTotal() * (100 - orderDetails.advance_percent) / 100).toLocaleString()}</span>
                          </div>
                        )}
                      </>
                    )}
                    {orderDetails.is_credit_order && (
                      <div className="flex justify-between text-sm text-purple-700">
                        <span>Pay After Delivery</span>
                        <span className="font-semibold">₹{getCartTotal().toLocaleString()}</span>
                      </div>
                    )}
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Actions */}
            <div className="flex flex-col md:flex-row gap-4">
              <Button
                onClick={() => setStep(1)}
                variant="outline"
                className="gap-2 md:w-auto"
              >
                <ArrowLeft className="w-4 h-4" />
                Back to Cart
              </Button>
              <Button
                onClick={handleCreateOrder}
                disabled={loading || !orderDetails.customer_name || !orderDetails.delivery_address}
                className="bg-teal-600 hover:bg-teal-700 gap-2 flex-1 md:flex-none"
                data-testid="place-order-btn"
              >
                {loading ? 'Processing...' : (
                  <>
                    {orderDetails.is_credit_order ? 'Create Credit Order' : 'Proceed to Payment'}
                    <ArrowRight className="w-4 h-4" />
                  </>
                )}
              </Button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default CustomizeBook;
