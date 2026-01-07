import React, { useState, useEffect, useRef } from 'react';
import { Search, User, Building2, X, Loader2, Phone, MapPin, CreditCard } from 'lucide-react';
import { erpApi } from '../utils/erpApi';

/**
 * CustomerSearch - A reusable dropdown component to search and select customers from Customer Master
 * 
 * Props:
 * - onSelect: (customer) => void - Called when a customer is selected
 * - onClear: () => void - Called when selection is cleared
 * - selectedCustomer: object - Currently selected customer
 * - placeholder: string - Placeholder text
 * - disabled: boolean - Disable the input
 * - showCreditInfo: boolean - Show credit limit/days info
 * - className: string - Additional CSS classes
 */
const CustomerSearch = ({ 
  onSelect, 
  onClear, 
  selectedCustomer = null,
  placeholder = "Search customer by name, mobile, GSTIN...",
  disabled = false,
  showCreditInfo = true,
  className = ""
}) => {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [showDropdown, setShowDropdown] = useState(false);
  const wrapperRef = useRef(null);
  const debounceRef = useRef(null);

  // Close dropdown when clicking outside
  useEffect(() => {
    const handleClickOutside = (event) => {
      if (wrapperRef.current && !wrapperRef.current.contains(event.target)) {
        setShowDropdown(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  // Debounced search
  useEffect(() => {
    if (debounceRef.current) {
      clearTimeout(debounceRef.current);
    }

    if (query.length >= 2) {
      setLoading(true);
      debounceRef.current = setTimeout(async () => {
        try {
          const response = await erpApi.customerMaster.searchForInvoice(query);
          setResults(response.data || []);
          setShowDropdown(true);
        } catch (error) {
          console.error('Customer search failed:', error);
          setResults([]);
        } finally {
          setLoading(false);
        }
      }, 300);
    } else {
      setResults([]);
      setShowDropdown(false);
    }

    return () => {
      if (debounceRef.current) {
        clearTimeout(debounceRef.current);
      }
    };
  }, [query]);

  const handleSelect = (customer) => {
    setShowDropdown(false);
    setQuery('');
    onSelect(customer);
  };

  const handleClear = () => {
    setQuery('');
    setResults([]);
    if (onClear) onClear();
  };

  // If customer is selected, show the selection
  if (selectedCustomer) {
    return (
      <div 
        className={`relative ${className}`}
        data-testid="customer-search-selected"
      >
        <div className="border rounded-lg p-3 bg-blue-50 border-blue-200">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <div className={`p-2 rounded-full ${
                selectedCustomer.invoice_type === 'B2B' 
                  ? 'bg-purple-100 text-purple-600' 
                  : 'bg-green-100 text-green-600'
              }`}>
                {selectedCustomer.invoice_type === 'B2B' 
                  ? <Building2 className="w-5 h-5" /> 
                  : <User className="w-5 h-5" />
                }
              </div>
              <div>
                <p className="font-medium text-gray-900">
                  {selectedCustomer.display_name || selectedCustomer.company_name || selectedCustomer.individual_name}
                </p>
                <div className="flex items-center gap-3 text-xs text-gray-500">
                  <span className="flex items-center gap-1">
                    <Phone className="w-3 h-3" />
                    {selectedCustomer.mobile}
                  </span>
                  {selectedCustomer.gstin && (
                    <span className="font-mono">{selectedCustomer.gstin}</span>
                  )}
                </div>
              </div>
            </div>
            <div className="flex items-center gap-2">
              {showCreditInfo && selectedCustomer.credit_type === 'credit_allowed' && (
                <span className="px-2 py-1 bg-green-100 text-green-700 text-xs rounded-full flex items-center gap-1">
                  <CreditCard className="w-3 h-3" />
                  ₹{selectedCustomer.credit_limit?.toLocaleString()} / {selectedCustomer.credit_days}d
                </span>
              )}
              <span className={`px-2 py-1 text-xs rounded-full ${
                selectedCustomer.invoice_type === 'B2B' 
                  ? 'bg-purple-100 text-purple-700' 
                  : 'bg-green-100 text-green-700'
              }`}>
                {selectedCustomer.invoice_type}
              </span>
              {!disabled && (
                <button 
                  onClick={handleClear}
                  className="p-1 hover:bg-blue-100 rounded text-blue-600"
                  data-testid="clear-customer-btn"
                >
                  <X className="w-4 h-4" />
                </button>
              )}
            </div>
          </div>
          
          {/* Show billing address preview */}
          {selectedCustomer.billing_address && (
            <div className="mt-2 pt-2 border-t border-blue-200 text-xs text-gray-600 flex items-start gap-1">
              <MapPin className="w-3 h-3 mt-0.5 flex-shrink-0" />
              <span>
                {selectedCustomer.billing_address.address_line1}, 
                {selectedCustomer.billing_address.city}, 
                {selectedCustomer.billing_address.state} - 
                {selectedCustomer.billing_address.pin_code}
              </span>
            </div>
          )}
        </div>
      </div>
    );
  }

  // Search input mode
  return (
    <div 
      ref={wrapperRef} 
      className={`relative ${className}`}
      data-testid="customer-search"
    >
      <div className="relative">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onFocus={() => query.length >= 2 && setShowDropdown(true)}
          placeholder={placeholder}
          disabled={disabled}
          className="w-full pl-10 pr-10 py-2.5 border rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 disabled:bg-gray-100"
          data-testid="customer-search-input"
        />
        {loading && (
          <Loader2 className="absolute right-3 top-1/2 -translate-y-1/2 w-4 h-4 text-blue-500 animate-spin" />
        )}
      </div>

      {/* Dropdown Results */}
      {showDropdown && results.length > 0 && (
        <div 
          className="absolute z-50 w-full mt-1 bg-white border rounded-lg shadow-lg max-h-80 overflow-y-auto"
          data-testid="customer-search-dropdown"
        >
          {results.map((customer) => (
            <button
              key={customer.id}
              onClick={() => handleSelect(customer)}
              className="w-full px-4 py-3 text-left hover:bg-gray-50 border-b last:border-b-0 flex items-center gap-3"
              data-testid={`customer-option-${customer.id}`}
            >
              <div className={`p-2 rounded-full ${
                customer.invoice_type === 'B2B' 
                  ? 'bg-purple-100 text-purple-600' 
                  : 'bg-green-100 text-green-600'
              }`}>
                {customer.invoice_type === 'B2B' 
                  ? <Building2 className="w-4 h-4" /> 
                  : <User className="w-4 h-4" />
                }
              </div>
              <div className="flex-1 min-w-0">
                <p className="font-medium text-gray-900 truncate">
                  {customer.display_name || customer.company_name || customer.individual_name}
                </p>
                <div className="flex items-center gap-2 text-xs text-gray-500">
                  <span>{customer.mobile}</span>
                  {customer.gstin && (
                    <>
                      <span>•</span>
                      <span className="font-mono">{customer.gstin}</span>
                    </>
                  )}
                </div>
              </div>
              <div className="flex flex-col items-end gap-1">
                <span className={`px-2 py-0.5 text-xs rounded-full ${
                  customer.invoice_type === 'B2B' 
                    ? 'bg-purple-100 text-purple-700' 
                    : 'bg-green-100 text-green-700'
                }`}>
                  {customer.invoice_type}
                </span>
                {showCreditInfo && customer.credit_type === 'credit_allowed' && (
                  <span className="text-xs text-green-600">
                    Credit: ₹{customer.credit_limit?.toLocaleString()}
                  </span>
                )}
              </div>
            </button>
          ))}
        </div>
      )}

      {/* No results */}
      {showDropdown && query.length >= 2 && !loading && results.length === 0 && (
        <div className="absolute z-50 w-full mt-1 bg-white border rounded-lg shadow-lg p-4 text-center text-gray-500">
          <p>No customers found matching "{query}"</p>
          <p className="text-xs mt-1">Try searching by name, mobile, or GSTIN</p>
        </div>
      )}
    </div>
  );
};

export default CustomerSearch;
