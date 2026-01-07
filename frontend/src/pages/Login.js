import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import { motion, AnimatePresence } from 'framer-motion';
import { Button } from '../components/ui/button';
import { Card, CardContent } from '../components/ui/card';
import { toast } from 'sonner';
import { Mail, Phone, ArrowLeft, Lock, KeyRound, CheckCircle } from 'lucide-react';
import axios from 'axios';

const API_BASE = process.env.REACT_APP_BACKEND_URL;
const COMPANY_LOGO = "https://customer-assets.emergentagent.com/job_0aec802e-e67b-4582-8fac-1517907b7262/artifacts/752tez4i_Logo%20Cucumaa%20Glass.png";

const Login = () => {
  const [mode, setMode] = useState('login'); // login, register, forgot, otp, reset, success
  const [formData, setFormData] = useState({
    email: '',
    password: '',
    name: '',
    phone: '',
    role: 'customer'
  });
  const [resetData, setResetData] = useState({
    method: 'email', // email or mobile
    identifier: '',
    otp: '',
    resetToken: '',
    newPassword: '',
    confirmPassword: ''
  });
  const [loading, setLoading] = useState(false);
  const [otpSent, setOtpSent] = useState(false);
  const { login, register } = useAuth();
  const navigate = useNavigate();

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);

    try {
      if (mode === 'login') {
        const userData = await login(formData.email, formData.password);
        toast.success('Login successful!');
        // Redirect based on role
        const userRole = userData?.role || 'customer';
        if (['admin', 'super_admin', 'owner', 'manager', 'finance', 'hr', 'production_manager', 'operator'].includes(userRole)) {
          navigate('/erp');
        } else {
          navigate('/portal'); // Customer goes to portal
        }
      } else if (mode === 'register') {
        await register(formData);
        toast.success('Registration successful!');
        navigate('/portal'); // New customers go to portal
      }
    } catch (error) {
      console.error('Auth error:', error);
      toast.error(error.response?.data?.detail || 'Authentication failed');
    } finally {
      setLoading(false);
    }
  };

  const handleSendOTP = async () => {
    if (!resetData.identifier) {
      toast.error(`Please enter your ${resetData.method === 'email' ? 'email' : 'mobile number'}`);
      return;
    }

    setLoading(true);
    try {
      await axios.post(`${API_BASE}/api/auth/send-otp`, {
        method: resetData.method,
        identifier: resetData.identifier,
        purpose: 'reset'
      });
      setOtpSent(true);
      setMode('otp');
      toast.success(`OTP sent to your ${resetData.method === 'email' ? 'email' : 'mobile'}!`);
    } catch (error) {
      toast.error(error.response?.data?.detail || 'Failed to send OTP');
    } finally {
      setLoading(false);
    }
  };

  const handleVerifyOTP = async () => {
    if (!resetData.otp || resetData.otp.length !== 6) {
      toast.error('Please enter a valid 6-digit OTP');
      return;
    }

    setLoading(true);
    try {
      const res = await axios.post(`${API_BASE}/api/auth/verify-otp`, {
        identifier: resetData.identifier,
        otp: resetData.otp,
        purpose: 'reset'
      });
      setResetData({ ...resetData, resetToken: res.data.reset_token });
      setMode('reset');
      toast.success('OTP verified! Set your new password.');
    } catch (error) {
      toast.error(error.response?.data?.detail || 'Invalid or expired OTP');
    } finally {
      setLoading(false);
    }
  };

  const handleResetPassword = async () => {
    if (resetData.newPassword.length < 6) {
      toast.error('Password must be at least 6 characters');
      return;
    }
    if (resetData.newPassword !== resetData.confirmPassword) {
      toast.error('Passwords do not match');
      return;
    }

    setLoading(true);
    try {
      await axios.post(`${API_BASE}/api/auth/reset-password`, {
        reset_token: resetData.resetToken,
        new_password: resetData.newPassword
      });
      setMode('success');
      toast.success('Password reset successful!');
    } catch (error) {
      toast.error(error.response?.data?.detail || 'Failed to reset password');
    } finally {
      setLoading(false);
    }
  };

  const resetForm = () => {
    setMode('login');
    setResetData({
      method: 'email',
      identifier: '',
      otp: '',
      resetToken: '',
      newPassword: '',
      confirmPassword: ''
    });
    setOtpSent(false);
  };

  const renderLoginForm = () => (
    <form onSubmit={handleSubmit} className="space-y-6">
      <div>
        <label className="block text-sm font-medium text-slate-700 mb-2">Email</label>
        <input
          type="email"
          value={formData.email}
          onChange={(e) => setFormData({ ...formData, email: e.target.value })}
          className="w-full h-12 rounded-lg border border-slate-300 px-4 focus:ring-2 focus:ring-teal-500 focus:border-transparent"
          required
          data-testid="input-email"
        />
      </div>

      <div>
        <label className="block text-sm font-medium text-slate-700 mb-2">Password</label>
        <input
          type="password"
          value={formData.password}
          onChange={(e) => setFormData({ ...formData, password: e.target.value })}
          className="w-full h-12 rounded-lg border border-slate-300 px-4 focus:ring-2 focus:ring-teal-500 focus:border-transparent"
          required
          data-testid="input-password"
        />
      </div>

      <div className="flex justify-end">
        <button
          type="button"
          onClick={() => setMode('forgot')}
          className="text-sm text-teal-600 hover:text-teal-700 font-medium"
          data-testid="forgot-password-link"
        >
          Forgot Password?
        </button>
      </div>

      <Button
        type="submit"
        className="w-full bg-teal-600 hover:bg-teal-700 h-12"
        disabled={loading}
        data-testid="submit-btn"
      >
        {loading ? 'Please wait...' : 'Login'}
      </Button>
    </form>
  );

  const renderRegisterForm = () => (
    <form onSubmit={handleSubmit} className="space-y-5">
      <div>
        <label className="block text-sm font-medium text-slate-700 mb-2">Full Name</label>
        <input
          type="text"
          value={formData.name}
          onChange={(e) => setFormData({ ...formData, name: e.target.value })}
          className="w-full h-12 rounded-lg border border-slate-300 px-4 focus:ring-2 focus:ring-teal-500 focus:border-transparent"
          required
          data-testid="input-name"
        />
      </div>
      <div>
        <label className="block text-sm font-medium text-slate-700 mb-2">Phone Number</label>
        <input
          type="tel"
          value={formData.phone}
          onChange={(e) => setFormData({ ...formData, phone: e.target.value })}
          className="w-full h-12 rounded-lg border border-slate-300 px-4 focus:ring-2 focus:ring-teal-500 focus:border-transparent"
          required
          data-testid="input-phone"
        />
      </div>
      <div>
        <label className="block text-sm font-medium text-slate-700 mb-2">Account Type</label>
        <select
          value={formData.role}
          onChange={(e) => setFormData({ ...formData, role: e.target.value })}
          className="w-full h-12 rounded-lg border border-slate-300 px-4 focus:ring-2 focus:ring-teal-500 focus:border-transparent"
          data-testid="select-role"
        >
          <option value="customer">Customer</option>
          <option value="dealer">Dealer / Architect</option>
        </select>
      </div>
      <div>
        <label className="block text-sm font-medium text-slate-700 mb-2">Email</label>
        <input
          type="email"
          value={formData.email}
          onChange={(e) => setFormData({ ...formData, email: e.target.value })}
          className="w-full h-12 rounded-lg border border-slate-300 px-4 focus:ring-2 focus:ring-teal-500 focus:border-transparent"
          required
          data-testid="input-email"
        />
      </div>
      <div>
        <label className="block text-sm font-medium text-slate-700 mb-2">Password</label>
        <input
          type="password"
          value={formData.password}
          onChange={(e) => setFormData({ ...formData, password: e.target.value })}
          className="w-full h-12 rounded-lg border border-slate-300 px-4 focus:ring-2 focus:ring-teal-500 focus:border-transparent"
          required
          minLength={6}
          data-testid="input-password"
        />
      </div>

      <Button
        type="submit"
        className="w-full bg-teal-600 hover:bg-teal-700 h-12"
        disabled={loading}
        data-testid="submit-btn"
      >
        {loading ? 'Please wait...' : 'Create Account'}
      </Button>
    </form>
  );

  const renderForgotPassword = () => (
    <div className="space-y-6">
      <button
        onClick={resetForm}
        className="flex items-center gap-2 text-slate-600 hover:text-slate-900 text-sm font-medium"
        data-testid="back-to-login"
      >
        <ArrowLeft className="w-4 h-4" /> Back to Login
      </button>

      <div className="text-center">
        <div className="w-16 h-16 bg-teal-100 rounded-full flex items-center justify-center mx-auto mb-4">
          <KeyRound className="w-8 h-8 text-teal-600" />
        </div>
        <h2 className="text-xl font-bold text-slate-900 mb-2">Reset Password</h2>
        <p className="text-slate-600 text-sm">Choose how you want to receive the OTP</p>
      </div>

      {/* Method Selection */}
      <div className="grid grid-cols-2 gap-3">
        <button
          onClick={() => setResetData({ ...resetData, method: 'email', identifier: '' })}
          className={`p-4 rounded-xl border-2 transition-all ${
            resetData.method === 'email'
              ? 'border-teal-500 bg-teal-50'
              : 'border-slate-200 hover:border-slate-300'
          }`}
          data-testid="method-email"
        >
          <Mail className={`w-6 h-6 mx-auto mb-2 ${resetData.method === 'email' ? 'text-teal-600' : 'text-slate-400'}`} />
          <span className={`text-sm font-medium ${resetData.method === 'email' ? 'text-teal-700' : 'text-slate-600'}`}>
            Email OTP
          </span>
        </button>
        <button
          onClick={() => setResetData({ ...resetData, method: 'mobile', identifier: '' })}
          className={`p-4 rounded-xl border-2 transition-all ${
            resetData.method === 'mobile'
              ? 'border-teal-500 bg-teal-50'
              : 'border-slate-200 hover:border-slate-300'
          }`}
          data-testid="method-mobile"
        >
          <Phone className={`w-6 h-6 mx-auto mb-2 ${resetData.method === 'mobile' ? 'text-teal-600' : 'text-slate-400'}`} />
          <span className={`text-sm font-medium ${resetData.method === 'mobile' ? 'text-teal-700' : 'text-slate-600'}`}>
            Mobile OTP
          </span>
        </button>
      </div>

      {/* Identifier Input */}
      <div>
        <label className="block text-sm font-medium text-slate-700 mb-2">
          {resetData.method === 'email' ? 'Email Address' : 'Mobile Number'}
        </label>
        <div className="relative">
          {resetData.method === 'mobile' && (
            <span className="absolute left-4 top-1/2 -translate-y-1/2 text-slate-500">+91</span>
          )}
          <input
            type={resetData.method === 'email' ? 'email' : 'tel'}
            value={resetData.identifier}
            onChange={(e) => setResetData({ ...resetData, identifier: e.target.value })}
            placeholder={resetData.method === 'email' ? 'your@email.com' : '9876543210'}
            className={`w-full h-12 rounded-lg border border-slate-300 focus:ring-2 focus:ring-teal-500 focus:border-transparent ${
              resetData.method === 'mobile' ? 'pl-12' : 'px-4'
            }`}
            data-testid="identifier-input"
          />
        </div>
      </div>

      <Button
        onClick={handleSendOTP}
        className="w-full bg-teal-600 hover:bg-teal-700 h-12"
        disabled={loading || !resetData.identifier}
        data-testid="send-otp-btn"
      >
        {loading ? 'Sending...' : 'Send OTP'}
      </Button>
    </div>
  );

  const renderOTPVerification = () => (
    <div className="space-y-6">
      <button
        onClick={() => setMode('forgot')}
        className="flex items-center gap-2 text-slate-600 hover:text-slate-900 text-sm font-medium"
      >
        <ArrowLeft className="w-4 h-4" /> Change Method
      </button>

      <div className="text-center">
        <div className="w-16 h-16 bg-teal-100 rounded-full flex items-center justify-center mx-auto mb-4">
          <Mail className="w-8 h-8 text-teal-600" />
        </div>
        <h2 className="text-xl font-bold text-slate-900 mb-2">Enter OTP</h2>
        <p className="text-slate-600 text-sm">
          We sent a 6-digit code to{' '}
          <span className="font-medium text-slate-900">
            {resetData.method === 'email' ? resetData.identifier : `+91 ${resetData.identifier}`}
          </span>
        </p>
      </div>

      {/* OTP Input */}
      <div>
        <label className="block text-sm font-medium text-slate-700 mb-2 text-center">Enter 6-digit OTP</label>
        <input
          type="text"
          value={resetData.otp}
          onChange={(e) => setResetData({ ...resetData, otp: e.target.value.replace(/\D/g, '').slice(0, 6) })}
          placeholder="000000"
          className="w-full h-14 text-center text-2xl tracking-[0.5em] font-mono rounded-lg border border-slate-300 focus:ring-2 focus:ring-teal-500 focus:border-transparent"
          maxLength={6}
          data-testid="otp-input"
        />
      </div>

      <Button
        onClick={handleVerifyOTP}
        className="w-full bg-teal-600 hover:bg-teal-700 h-12"
        disabled={loading || resetData.otp.length !== 6}
        data-testid="verify-otp-btn"
      >
        {loading ? 'Verifying...' : 'Verify OTP'}
      </Button>

      <p className="text-center text-sm text-slate-500">
        Didn't receive the code?{' '}
        <button
          onClick={handleSendOTP}
          className="text-teal-600 hover:text-teal-700 font-medium"
          disabled={loading}
        >
          Resend OTP
        </button>
      </p>
    </div>
  );

  const renderNewPassword = () => (
    <div className="space-y-6">
      <div className="text-center">
        <div className="w-16 h-16 bg-teal-100 rounded-full flex items-center justify-center mx-auto mb-4">
          <Lock className="w-8 h-8 text-teal-600" />
        </div>
        <h2 className="text-xl font-bold text-slate-900 mb-2">Set New Password</h2>
        <p className="text-slate-600 text-sm">Create a strong password for your account</p>
      </div>

      <div>
        <label className="block text-sm font-medium text-slate-700 mb-2">New Password</label>
        <input
          type="password"
          value={resetData.newPassword}
          onChange={(e) => setResetData({ ...resetData, newPassword: e.target.value })}
          placeholder="Minimum 6 characters"
          className="w-full h-12 rounded-lg border border-slate-300 px-4 focus:ring-2 focus:ring-teal-500 focus:border-transparent"
          minLength={6}
          data-testid="new-password-input"
        />
      </div>

      <div>
        <label className="block text-sm font-medium text-slate-700 mb-2">Confirm Password</label>
        <input
          type="password"
          value={resetData.confirmPassword}
          onChange={(e) => setResetData({ ...resetData, confirmPassword: e.target.value })}
          placeholder="Re-enter password"
          className="w-full h-12 rounded-lg border border-slate-300 px-4 focus:ring-2 focus:ring-teal-500 focus:border-transparent"
          data-testid="confirm-password-input"
        />
      </div>

      <Button
        onClick={handleResetPassword}
        className="w-full bg-teal-600 hover:bg-teal-700 h-12"
        disabled={loading || !resetData.newPassword || !resetData.confirmPassword}
        data-testid="reset-password-btn"
      >
        {loading ? 'Resetting...' : 'Reset Password'}
      </Button>
    </div>
  );

  const renderSuccess = () => (
    <div className="text-center py-8 space-y-6">
      <div className="w-20 h-20 bg-green-100 rounded-full flex items-center justify-center mx-auto">
        <CheckCircle className="w-10 h-10 text-green-600" />
      </div>
      <div>
        <h2 className="text-2xl font-bold text-slate-900 mb-2">Password Reset!</h2>
        <p className="text-slate-600">Your password has been successfully changed.</p>
      </div>
      <Button
        onClick={resetForm}
        className="bg-teal-600 hover:bg-teal-700 h-12 px-8"
        data-testid="back-to-login-btn"
      >
        Back to Login
      </Button>
    </div>
  );

  return (
    <div className="min-h-screen py-20 bg-gradient-to-br from-slate-50 to-teal-50" data-testid="login-page">
      <div className="max-w-md mx-auto px-4">
        {/* Logo */}
        <div className="text-center mb-8">
          <img 
            src={COMPANY_LOGO} 
            alt="Lucumaa Glass" 
            className="h-20 w-auto mx-auto mb-4"
          />
          {(mode === 'login' || mode === 'register') && (
            <>
              <h1 className="text-3xl font-bold text-slate-900 mb-2" data-testid="login-title">
                {mode === 'login' ? 'Welcome Back' : 'Create Account'}
              </h1>
              <p className="text-slate-600">
                {mode === 'login' ? 'Login to access your dashboard' : 'Register to start ordering'}
              </p>
            </>
          )}
        </div>

        <Card className="shadow-xl border-0">
          <CardContent className="p-8">
            <AnimatePresence mode="wait">
              <motion.div
                key={mode}
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -10 }}
                transition={{ duration: 0.2 }}
              >
                {mode === 'login' && renderLoginForm()}
                {mode === 'register' && renderRegisterForm()}
                {mode === 'forgot' && renderForgotPassword()}
                {mode === 'otp' && renderOTPVerification()}
                {mode === 'reset' && renderNewPassword()}
                {mode === 'success' && renderSuccess()}
              </motion.div>
            </AnimatePresence>

            {(mode === 'login' || mode === 'register') && (
              <div className="mt-6 text-center">
                <button
                  onClick={() => setMode(mode === 'login' ? 'register' : 'login')}
                  className="text-teal-600 hover:text-teal-700 font-medium"
                  data-testid="toggle-mode"
                >
                  {mode === 'login' ? "Don't have an account? Register" : 'Already have an account? Login'}
                </button>
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
};

export default Login;
