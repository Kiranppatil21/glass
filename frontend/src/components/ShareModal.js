import React, { useState } from 'react';
import { 
  X, Download, Mail, MessageCircle, FileText, Receipt, Files,
  CheckCircle, Loader2, Copy
} from 'lucide-react';
import { Button } from './ui/button';
import { toast } from 'sonner';
import axios from 'axios';

const API_BASE = process.env.REACT_APP_BACKEND_URL;

const ShareModal = ({ isOpen, onClose, order, isJobWork = false }) => {
  const [sharing, setSharing] = useState(null);
  const [email, setEmail] = useState('');
  const [showEmailInput, setShowEmailInput] = useState(false);

  if (!isOpen || !order) return null;

  const getAuthHeaders = () => {
    const token = localStorage.getItem('token');
    return { Authorization: `Bearer ${token}` };
  };

  const getToken = () => localStorage.getItem('token');

  const orderNumber = isJobWork ? order.job_work_number : order.order_number;
  const customerPhone = isJobWork ? order.phone : (order.customer_phone || order.phone);

  // Log share action
  const logShareAction = async (docType, channel) => {
    try {
      await axios.post(
        `${API_BASE}/api/erp/pdf/share-log?order_id=${order.id}&document_type=${docType}&share_channel=${channel}`,
        {},
        { headers: getAuthHeaders() }
      );
    } catch (error) {
      console.error('Failed to log share action:', error);
    }
  };

  // Download handlers
  const handleDownload = async (type) => {
    setSharing(type);
    try {
      let url = '';
      switch(type) {
        case 'invoice':
          url = isJobWork 
            ? `${API_BASE}/api/erp/pdf/job-work-invoice/${order.id}?token=${getToken()}`
            : `${API_BASE}/api/erp/pdf/invoice/${order.id}?token=${getToken()}`;
          break;
        case 'receipt':
          url = `${API_BASE}/api/erp/pdf/payment-receipt/${order.id}?token=${getToken()}`;
          break;
        case 'merged':
          url = `${API_BASE}/api/erp/pdf/merged/${order.id}?token=${getToken()}`;
          break;
        default:
          return;
      }
      
      window.open(url, '_blank');
      await logShareAction(type, 'download');
      toast.success(`${type === 'merged' ? 'Invoice + Receipt' : type.charAt(0).toUpperCase() + type.slice(1)} downloaded!`);
    } catch (error) {
      toast.error('Download failed');
    } finally {
      setSharing(null);
    }
  };

  // WhatsApp share
  const handleWhatsAppShare = async (type) => {
    setSharing(`whatsapp-${type}`);
    try {
      let url = '';
      let message = '';
      
      switch(type) {
        case 'invoice':
          url = isJobWork 
            ? `${API_BASE}/api/erp/pdf/job-work-invoice/${order.id}?token=${getToken()}`
            : `${API_BASE}/api/erp/pdf/invoice/${order.id}?token=${getToken()}`;
          message = `📄 *Invoice - ${orderNumber}*\n\nDear Customer,\n\nPlease find your invoice attached.\n\n🔗 Download: ${url}\n\n_Lucumaa Glass - Premium Glass Manufacturing_`;
          break;
        case 'receipt':
          url = `${API_BASE}/api/erp/pdf/payment-receipt/${order.id}?token=${getToken()}`;
          message = `🧾 *Payment Receipt - ${orderNumber}*\n\nDear Customer,\n\nThank you for your payment. Please find your receipt attached.\n\n🔗 Download: ${url}\n\n_Lucumaa Glass - Premium Glass Manufacturing_`;
          break;
        case 'merged':
          url = `${API_BASE}/api/erp/pdf/merged/${order.id}?token=${getToken()}`;
          message = `📑 *Invoice & Receipt - ${orderNumber}*\n\nDear Customer,\n\nPlease find your Invoice and Payment Receipt attached.\n\n🔗 Download: ${url}\n\n_Lucumaa Glass - Premium Glass Manufacturing_`;
          break;
        default:
          return;
      }
      
      // Format phone for WhatsApp
      let phone = customerPhone || '';
      phone = phone.replace(/[^0-9]/g, '');
      if (phone.length === 10) phone = '91' + phone;
      
      const whatsappUrl = `https://wa.me/${phone}?text=${encodeURIComponent(message)}`;
      window.open(whatsappUrl, '_blank');
      
      await logShareAction(type, 'whatsapp');
      toast.success('Opening WhatsApp...');
    } catch (error) {
      toast.error('WhatsApp share failed');
    } finally {
      setSharing(null);
    }
  };

  // Email share
  const handleEmailShare = async (type) => {
    if (!email) {
      setShowEmailInput(true);
      return;
    }
    
    setSharing(`email-${type}`);
    try {
      // Send via backend SMTP with PDF attachment
      const response = await axios.post(
        `${API_BASE}/api/erp/pdf/send-email`,
        {
          recipient_email: email,
          document_type: type,
          order_id: order.id,
          custom_message: ""
        },
        { headers: getAuthHeaders() }
      );
      
      await logShareAction(type, 'email_smtp');
      toast.success(`Email sent to ${email}!`);
      setShowEmailInput(false);
      setEmail('');
    } catch (error) {
      console.error('Email send error:', error);
      // Fallback to mailto if SMTP fails
      let url = '';
      let subject = '';
      let body = '';
      
      switch(type) {
        case 'invoice':
          url = isJobWork 
            ? `${API_BASE}/api/erp/pdf/job-work-invoice/${order.id}?token=${getToken()}`
            : `${API_BASE}/api/erp/pdf/invoice/${order.id}?token=${getToken()}`;
          subject = `Invoice - ${orderNumber} | Lucumaa Glass`;
          body = `Dear Customer,\n\nPlease find your invoice for Order ${orderNumber}.\n\nDownload Invoice: ${url}\n\nThank you for your business!\n\nBest Regards,\nLucumaa Glass\nPremium Glass Manufacturing`;
          break;
        case 'receipt':
          url = `${API_BASE}/api/erp/pdf/payment-receipt/${order.id}?token=${getToken()}`;
          subject = `Payment Receipt - ${orderNumber} | Lucumaa Glass`;
          body = `Dear Customer,\n\nThank you for your payment. Please find your payment receipt for Order ${orderNumber}.\n\nDownload Receipt: ${url}\n\nBest Regards,\nLucumaa Glass\nPremium Glass Manufacturing`;
          break;
        case 'merged':
          url = `${API_BASE}/api/erp/pdf/merged/${order.id}?token=${getToken()}`;
          subject = `Invoice & Receipt - ${orderNumber} | Lucumaa Glass`;
          body = `Dear Customer,\n\nPlease find your Invoice and Payment Receipt for Order ${orderNumber}.\n\nDownload: ${url}\n\nBest Regards,\nLucumaa Glass\nPremium Glass Manufacturing`;
          break;
        default:
          return;
      }
      
      const mailtoUrl = `mailto:${email}?subject=${encodeURIComponent(subject)}&body=${encodeURIComponent(body)}`;
      window.location.href = mailtoUrl;
      
      await logShareAction(type, 'email');
      toast.info('Opening email client (SMTP unavailable)');
      setShowEmailInput(false);
    } finally {
      setSharing(null);
    }
  };

  // Copy link
  const handleCopyLink = async (type) => {
    let url = '';
    switch(type) {
      case 'invoice':
        url = isJobWork 
          ? `${API_BASE}/api/erp/pdf/job-work-invoice/${order.id}?token=${getToken()}`
          : `${API_BASE}/api/erp/pdf/invoice/${order.id}?token=${getToken()}`;
        break;
      case 'receipt':
        url = `${API_BASE}/api/erp/pdf/payment-receipt/${order.id}?token=${getToken()}`;
        break;
      case 'merged':
        url = `${API_BASE}/api/erp/pdf/merged/${order.id}?token=${getToken()}`;
        break;
      default:
        return;
    }
    
    await navigator.clipboard.writeText(url);
    toast.success('Link copied to clipboard!');
    await logShareAction(type, 'copy_link');
  };

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-md animate-in fade-in zoom-in duration-200">
        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b">
          <div>
            <h3 className="font-bold text-lg text-slate-900">Share Documents</h3>
            <p className="text-sm text-slate-500">Order: {orderNumber}</p>
          </div>
          <Button variant="ghost" size="sm" onClick={onClose} className="h-8 w-8 p-0">
            <X className="w-5 h-5" />
          </Button>
        </div>

        {/* Email Input (if shown) */}
        {showEmailInput && (
          <div className="p-4 bg-blue-50 border-b">
            <label className="text-sm font-medium text-slate-700 mb-2 block">Enter Email Address</label>
            <div className="flex gap-2">
              <input
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                placeholder="customer@email.com"
                className="flex-1 h-10 rounded-lg border border-slate-300 px-3"
              />
              <Button onClick={() => setShowEmailInput(false)} variant="ghost">Cancel</Button>
            </div>
          </div>
        )}

        {/* Document Options */}
        <div className="p-4 space-y-4">
          {/* Invoice */}
          <div className="bg-slate-50 rounded-lg p-4">
            <div className="flex items-center gap-3 mb-3">
              <div className="w-10 h-10 rounded-full bg-red-100 flex items-center justify-center">
                <FileText className="w-5 h-5 text-red-600" />
              </div>
              <div>
                <p className="font-medium text-slate-900">Invoice</p>
                <p className="text-xs text-slate-500">GST-compliant tax invoice</p>
              </div>
            </div>
            <div className="flex gap-2">
              <Button 
                size="sm" 
                variant="outline" 
                className="flex-1 gap-1"
                onClick={() => handleDownload('invoice')}
                disabled={sharing === 'invoice'}
              >
                {sharing === 'invoice' ? <Loader2 className="w-3 h-3 animate-spin" /> : <Download className="w-3 h-3" />}
                Download
              </Button>
              <Button 
                size="sm" 
                className="flex-1 gap-1 bg-green-600 hover:bg-green-700"
                onClick={() => handleWhatsAppShare('invoice')}
                disabled={sharing === 'whatsapp-invoice'}
              >
                <MessageCircle className="w-3 h-3" />
                WhatsApp
              </Button>
              <Button 
                size="sm" 
                variant="outline" 
                className="flex-1 gap-1"
                onClick={() => handleEmailShare('invoice')}
                disabled={sharing === 'email-invoice'}
              >
                <Mail className="w-3 h-3" />
                Email
              </Button>
            </div>
          </div>

          {/* Receipt */}
          <div className="bg-slate-50 rounded-lg p-4">
            <div className="flex items-center gap-3 mb-3">
              <div className="w-10 h-10 rounded-full bg-green-100 flex items-center justify-center">
                <Receipt className="w-5 h-5 text-green-600" />
              </div>
              <div>
                <p className="font-medium text-slate-900">Payment Receipt</p>
                <p className="text-xs text-slate-500">Online + Cash payments</p>
              </div>
            </div>
            <div className="flex gap-2">
              <Button 
                size="sm" 
                variant="outline" 
                className="flex-1 gap-1"
                onClick={() => handleDownload('receipt')}
                disabled={sharing === 'receipt'}
              >
                {sharing === 'receipt' ? <Loader2 className="w-3 h-3 animate-spin" /> : <Download className="w-3 h-3" />}
                Download
              </Button>
              <Button 
                size="sm" 
                className="flex-1 gap-1 bg-green-600 hover:bg-green-700"
                onClick={() => handleWhatsAppShare('receipt')}
                disabled={sharing === 'whatsapp-receipt'}
              >
                <MessageCircle className="w-3 h-3" />
                WhatsApp
              </Button>
              <Button 
                size="sm" 
                variant="outline" 
                className="flex-1 gap-1"
                onClick={() => handleEmailShare('receipt')}
                disabled={sharing === 'email-receipt'}
              >
                <Mail className="w-3 h-3" />
                Email
              </Button>
            </div>
          </div>

          {/* Merged PDF */}
          <div className="bg-gradient-to-r from-red-50 to-green-50 rounded-lg p-4 border-2 border-dashed border-slate-300">
            <div className="flex items-center gap-3 mb-3">
              <div className="w-10 h-10 rounded-full bg-purple-100 flex items-center justify-center">
                <Files className="w-5 h-5 text-purple-600" />
              </div>
              <div>
                <p className="font-medium text-slate-900">Invoice + Receipt</p>
                <p className="text-xs text-slate-500">Merged PDF (2 pages)</p>
              </div>
            </div>
            <div className="flex gap-2">
              <Button 
                size="sm" 
                variant="outline" 
                className="flex-1 gap-1"
                onClick={() => handleDownload('merged')}
                disabled={sharing === 'merged'}
              >
                {sharing === 'merged' ? <Loader2 className="w-3 h-3 animate-spin" /> : <Download className="w-3 h-3" />}
                Download
              </Button>
              <Button 
                size="sm" 
                className="flex-1 gap-1 bg-green-600 hover:bg-green-700"
                onClick={() => handleWhatsAppShare('merged')}
                disabled={sharing === 'whatsapp-merged'}
              >
                <MessageCircle className="w-3 h-3" />
                WhatsApp
              </Button>
              <Button 
                size="sm" 
                variant="outline" 
                className="flex-1 gap-1"
                onClick={() => handleEmailShare('merged')}
                disabled={sharing === 'email-merged'}
              >
                <Mail className="w-3 h-3" />
                Email
              </Button>
            </div>
          </div>

          {/* Delivery Slip - Only for Job Work */}
          {isJobWork && (
            <div className={`rounded-lg p-4 border ${order.payment_status === 'completed' ? 'bg-emerald-50 border-emerald-200' : 'bg-amber-50 border-amber-200'}`}>
              <div className="flex items-center gap-3 mb-3">
                <div className={`w-10 h-10 rounded-full flex items-center justify-center ${order.payment_status === 'completed' ? 'bg-emerald-100' : 'bg-amber-100'}`}>
                  <FileText className={`w-5 h-5 ${order.payment_status === 'completed' ? 'text-emerald-600' : 'text-amber-600'}`} />
                </div>
                <div>
                  <p className="font-medium text-slate-900">Delivery Slip / Gate Pass</p>
                  <p className="text-xs text-slate-500">
                    {order.payment_status === 'completed' 
                      ? 'For pickup at factory' 
                      : '⚠️ Requires full payment'
                    }
                  </p>
                </div>
              </div>
              {order.payment_status === 'completed' ? (
                <div className="flex gap-2">
                  <Button 
                    size="sm" 
                    variant="outline" 
                    className="flex-1 gap-1 border-emerald-300 text-emerald-700 hover:bg-emerald-100"
                    onClick={() => {
                      const token = localStorage.getItem('token');
                      window.open(`${API_BASE}/api/erp/pdf/job-work-slip/${order.id}?token=${token}`, '_blank');
                    }}
                  >
                    <Download className="w-3 h-3" />
                    Download Slip
                  </Button>
                  <Button 
                    size="sm" 
                    className="flex-1 gap-1 bg-emerald-600 hover:bg-emerald-700"
                    onClick={() => {
                      const token = localStorage.getItem('token');
                      const url = `${API_BASE}/api/erp/pdf/job-work-slip/${order.id}?token=${token}`;
                      const message = `📋 *Delivery Slip - ${orderNumber}*\n\nDear Customer,\n\nYour toughened glass is ready for pickup. Please carry this slip.\n\n🔗 Download: ${url}\n\n_Lucumaa Glass_`;
                      let phone = customerPhone || '';
                      phone = phone.replace(/[^0-9]/g, '');
                      if (phone.length === 10) phone = '91' + phone;
                      window.open(`https://wa.me/${phone}?text=${encodeURIComponent(message)}`, '_blank');
                    }}
                  >
                    <MessageCircle className="w-3 h-3" />
                    WhatsApp
                  </Button>
                </div>
              ) : (
                <div className="bg-amber-100 rounded-lg p-3 text-center">
                  <p className="text-sm text-amber-800 font-medium">
                    🔒 Delivery slip available only after full payment is settled
                  </p>
                  <p className="text-xs text-amber-600 mt-1">
                    Current status: {order.payment_status || 'Pending'}
                  </p>
                </div>
              )}
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="p-4 bg-slate-50 rounded-b-xl border-t text-center">
          <p className="text-xs text-slate-500">
            System Generated • No Signature Required
          </p>
        </div>
      </div>
    </div>
  );
};

export default ShareModal;
