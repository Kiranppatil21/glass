import React, { useState, useEffect, useRef } from 'react';
import { Card, CardContent } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { 
  MapPin, Clock, Car, Bike, Play, Square, Users, Route,
  Phone, FileText, TrendingUp, AlertTriangle, Navigation,
  CheckCircle, XCircle, Calendar, Fuel, Building, Eye,
  Award, Target, Zap, Bell, StopCircle, Timer, BarChart3,
  Download, RefreshCw, ChevronRight, Map as MapIcon
} from 'lucide-react';
import { toast } from 'sonner';
import axios from 'axios';
import { 
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  PieChart, Pie, Cell, LineChart, Line, Legend, RadarChart, Radar, PolarGrid,
  PolarAngleAxis, PolarRadiusAxis
} from 'recharts';

const API_BASE = process.env.REACT_APP_BACKEND_URL;

// Leaflet CSS import
const leafletCSS = `
  .leaflet-container { height: 100%; width: 100%; border-radius: 12px; }
  .leaflet-control-attribution { display: none; }
`;

const SFADashboard = () => {
  const [activeTab, setActiveTab] = useState('my-day');
  const [myDay, setMyDay] = useState(null);
  const [teamDashboard, setTeamDashboard] = useState(null);
  const [alerts, setAlerts] = useState([]);
  const [performance, setPerformance] = useState(null);
  const [misReport, setMisReport] = useState(null);
  const [loading, setLoading] = useState(true);
  const [showStartDayModal, setShowStartDayModal] = useState(false);
  const [showVisitModal, setShowVisitModal] = useState(false);
  const [showMapModal, setShowMapModal] = useState(false);
  const [selectedEmployee, setSelectedEmployee] = useState(null);
  const [currentLocation, setCurrentLocation] = useState(null);
  const [vehicleType, setVehicleType] = useState('bike');
  const [visitData, setVisitData] = useState({ customer_name: '', purpose: '', notes: '' });
  const [mapData, setMapData] = useState(null);
  const mapRef = useRef(null);
  const [L, setL] = useState(null);

  const getAuthHeaders = () => {
    const token = localStorage.getItem('token');
    return { Authorization: `Bearer ${token}` };
  };

  useEffect(() => {
    // Dynamically import Leaflet
    import('leaflet').then((leaflet) => {
      setL(leaflet.default);
    });
    
    getCurrentLocation();
    fetchMyDay();
    fetchAlerts();
  }, []);

  useEffect(() => {
    if (activeTab === 'team') fetchTeamDashboard();
    if (activeTab === 'performance') fetchPerformance();
    if (activeTab === 'reports') fetchMISReport();
    if (activeTab === 'alerts') fetchAlerts();
  }, [activeTab]);

  const getCurrentLocation = () => {
    if (navigator.geolocation) {
      navigator.geolocation.getCurrentPosition(
        (position) => {
          setCurrentLocation({
            latitude: position.coords.latitude,
            longitude: position.coords.longitude
          });
        },
        (error) => {
          console.error('Location error:', error);
        }
      );
    }
  };

  const fetchMyDay = async () => {
    try {
      const res = await axios.get(`${API_BASE}/api/erp/sfa/my-day`, { headers: getAuthHeaders() });
      setMyDay(res.data);
    } catch (error) {
      console.error('Failed to fetch day:', error);
    } finally {
      setLoading(false);
    }
  };

  const fetchTeamDashboard = async () => {
    try {
      const res = await axios.get(`${API_BASE}/api/erp/sfa/team-dashboard`, { headers: getAuthHeaders() });
      setTeamDashboard(res.data);
    } catch (error) {
      toast.error('Failed to load team dashboard');
    }
  };

  const fetchAlerts = async () => {
    try {
      const res = await axios.get(`${API_BASE}/api/erp/sfa/alerts`, { headers: getAuthHeaders() });
      setAlerts(res.data.alerts || []);
    } catch (error) {
      // Alerts endpoint may not exist yet, silently fail
      setAlerts([]);
    }
  };

  const fetchPerformance = async () => {
    try {
      const res = await axios.get(`${API_BASE}/api/erp/sfa/performance-scorecard`, { headers: getAuthHeaders() });
      setPerformance(res.data);
    } catch (error) {
      // Try monthly summary as fallback
      try {
        const res = await axios.get(`${API_BASE}/api/erp/sfa/reports/monthly-summary`, { headers: getAuthHeaders() });
        setPerformance({ report: res.data.report });
      } catch (e) {
        console.error('Failed to load performance');
      }
    }
  };

  const fetchMISReport = async () => {
    try {
      const res = await axios.get(`${API_BASE}/api/erp/sfa/reports/daily-summary`, { headers: getAuthHeaders() });
      setMisReport(res.data);
    } catch (error) {
      toast.error('Failed to load MIS report');
    }
  };

  const fetchEmployeeTimeline = async (userId) => {
    try {
      const res = await axios.get(`${API_BASE}/api/erp/sfa/employee-timeline/${userId}`, { headers: getAuthHeaders() });
      setMapData(res.data);
      setShowMapModal(true);
    } catch (error) {
      toast.error('Failed to load timeline');
    }
  };

  const handleStartDay = async () => {
    if (!currentLocation) {
      toast.error('Location not available. Please enable GPS.');
      return;
    }
    try {
      await axios.post(`${API_BASE}/api/erp/sfa/day-start`, {
        latitude: currentLocation.latitude,
        longitude: currentLocation.longitude,
        vehicle_type: vehicleType
      }, { headers: getAuthHeaders() });
      toast.success('Day started successfully!');
      setShowStartDayModal(false);
      fetchMyDay();
    } catch (error) {
      toast.error(error.response?.data?.detail || 'Failed to start day');
    }
  };

  const handleEndDay = async () => {
    if (!currentLocation) {
      toast.error('Location not available');
      return;
    }
    if (!window.confirm('Are you sure you want to end your day?')) return;
    try {
      const res = await axios.post(`${API_BASE}/api/erp/sfa/day-end`, {
        latitude: currentLocation.latitude,
        longitude: currentLocation.longitude
      }, { headers: getAuthHeaders() });
      toast.success(`Day ended! Distance: ${res.data.summary.total_distance_km} KM, Fuel: ₹${res.data.summary.fuel_allowance}`);
      fetchMyDay();
    } catch (error) {
      toast.error(error.response?.data?.detail || 'Failed to end day');
    }
  };

  const handleStartVisit = async () => {
    if (!currentLocation || !visitData.customer_name) {
      toast.error('Please fill customer name and enable location');
      return;
    }
    try {
      await axios.post(`${API_BASE}/api/erp/sfa/visit-start`, {
        ...visitData,
        latitude: currentLocation.latitude,
        longitude: currentLocation.longitude
      }, { headers: getAuthHeaders() });
      toast.success('Visit started!');
      setShowVisitModal(false);
      setVisitData({ customer_name: '', purpose: '', notes: '' });
      fetchMyDay();
    } catch (error) {
      toast.error(error.response?.data?.detail || 'Failed to start visit');
    }
  };

  const handleEndVisit = async (visitId, outcome) => {
    try {
      await axios.post(`${API_BASE}/api/erp/sfa/visit-end/${visitId}`, {
        outcome,
        notes: ''
      }, { headers: getAuthHeaders() });
      toast.success('Visit completed!');
      fetchMyDay();
    } catch (error) {
      toast.error('Failed to end visit');
    }
  };

  const tabs = [
    { key: 'my-day', label: 'My Day', icon: Calendar },
    { key: 'team', label: 'Team View', icon: Users },
    { key: 'performance', label: 'Performance', icon: Award },
    { key: 'reports', label: 'MIS Reports', icon: BarChart3 },
    { key: 'alerts', label: 'Alerts', icon: Bell, badge: alerts.length },
  ];

  const fuelRates = { bike: 3.50, car: 8.00, company_vehicle: 0 };
  const CHART_COLORS = ['#0d9488', '#f59e0b', '#ef4444', '#8b5cf6', '#3b82f6', '#10b981'];

  // Map Component
  const RouteMap = ({ routeData }) => {
    const mapContainerRef = useRef(null);
    
    useEffect(() => {
      if (!L || !mapContainerRef.current || !routeData?.route_coordinates?.length) return;
      
      // Clear previous map
      if (mapRef.current) {
        mapRef.current.remove();
      }
      
      const coordinates = routeData.route_coordinates;
      const center = coordinates[0] ? [coordinates[0].lat, coordinates[0].lng] : [28.6139, 77.2090];
      
      const map = L.map(mapContainerRef.current).setView(center, 13);
      mapRef.current = map;
      
      L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        maxZoom: 19,
      }).addTo(map);
      
      // Create route points array
      const routePoints = coordinates.map(c => [c.lat, c.lng]);
      
      // Add route polyline if more than 1 point
      if (routePoints.length > 1) {
        L.polyline(routePoints, { color: '#0d9488', weight: 4, opacity: 0.8 }).addTo(map);
      }
      
      // Add markers
      if (coordinates.length > 0) {
        // Start marker (green)
        const startIcon = L.divIcon({
          className: 'custom-marker',
          html: '<div style="background:#10b981;width:24px;height:24px;border-radius:50%;border:3px solid white;box-shadow:0 2px 6px rgba(0,0,0,0.3);"></div>',
          iconSize: [24, 24],
          iconAnchor: [12, 12]
        });
        L.marker([coordinates[0].lat, coordinates[0].lng], { icon: startIcon })
          .bindPopup('Start: ' + (routeData.start_location?.address || 'Day Start'))
          .addTo(map);
        
        // End marker (red) if day ended
        if (coordinates.length > 1) {
          const last = coordinates[coordinates.length - 1];
          const endIcon = L.divIcon({
            className: 'custom-marker',
            html: '<div style="background:#ef4444;width:24px;height:24px;border-radius:50%;border:3px solid white;box-shadow:0 2px 6px rgba(0,0,0,0.3);"></div>',
            iconSize: [24, 24],
            iconAnchor: [12, 12]
          });
          L.marker([last.lat, last.lng], { icon: endIcon })
            .bindPopup('End: ' + (routeData.end_location?.address || 'Current Location'))
            .addTo(map);
        }
        
        // Visit markers (blue)
        (routeData.visits || []).forEach((visit, idx) => {
          if (visit.start_location) {
            const visitIcon = L.divIcon({
              className: 'custom-marker',
              html: `<div style="background:#3b82f6;width:20px;height:20px;border-radius:50%;border:2px solid white;display:flex;align-items:center;justify-content:center;color:white;font-size:10px;font-weight:bold;">${idx + 1}</div>`,
              iconSize: [20, 20],
              iconAnchor: [10, 10]
            });
            L.marker([visit.start_location.latitude, visit.start_location.longitude], { icon: visitIcon })
              .bindPopup(`Visit ${idx + 1}: ${visit.customer_name}<br/>Purpose: ${visit.purpose}<br/>Outcome: ${visit.outcome || 'In Progress'}`)
              .addTo(map);
          }
        });
        
        // Fit bounds to show all markers
        if (routePoints.length > 0) {
          const bounds = L.latLngBounds(routePoints);
          map.fitBounds(bounds, { padding: [50, 50] });
        }
      }
      
      return () => {
        if (mapRef.current) {
          mapRef.current.remove();
          mapRef.current = null;
        }
      };
    }, [L, routeData]);
    
    return <div ref={mapContainerRef} style={{ height: '400px', width: '100%' }} />;
  };

  if (loading) {
    return <div className="flex items-center justify-center h-64"><RefreshCw className="w-8 h-8 animate-spin text-teal-600" /></div>;
  }

  return (
    <div className="space-y-6" data-testid="sfa-dashboard">
      <style>{leafletCSS}</style>
      
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-900 flex items-center gap-3">
            <Navigation className="w-8 h-8 text-teal-600" />
            Field Sales Automation
          </h1>
          <p className="text-slate-600 mt-1">Attendance, Movement, Communication & Expense Intelligence</p>
        </div>
        <div className="flex gap-2">
          {myDay?.status !== 'active' ? (
            <Button onClick={() => setShowStartDayModal(true)} className="bg-green-600 hover:bg-green-700">
              <Play className="w-4 h-4 mr-2" /> Start Day
            </Button>
          ) : (
            <>
              <Button onClick={() => setShowVisitModal(true)} className="bg-teal-600 hover:bg-teal-700">
                <Building className="w-4 h-4 mr-2" /> New Visit
              </Button>
              <Button onClick={() => { setMapData(myDay); setShowMapModal(true); }} variant="outline">
                <MapIcon className="w-4 h-4 mr-2" /> View Route
              </Button>
              <Button onClick={handleEndDay} variant="outline" className="border-red-300 text-red-600">
                <Square className="w-4 h-4 mr-2" /> End Day
              </Button>
            </>
          )}
        </div>
      </div>

      {/* Tabs */}
      <div className="flex gap-2 border-b border-slate-200 pb-2 overflow-x-auto">
        {tabs.map((tab) => (
          <button
            key={tab.key}
            onClick={() => setActiveTab(tab.key)}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-all whitespace-nowrap ${
              activeTab === tab.key ? 'bg-teal-600 text-white' : 'text-slate-600 hover:bg-slate-100'
            }`}
          >
            <tab.icon className="w-4 h-4" />
            {tab.label}
            {tab.badge > 0 && (
              <span className="bg-red-500 text-white text-xs px-2 py-0.5 rounded-full">{tab.badge}</span>
            )}
          </button>
        ))}
      </div>

      {/* My Day Tab */}
      {activeTab === 'my-day' && myDay && (
        <div className="space-y-6">
          {/* Status Cards */}
          <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
            <Card className={myDay.is_active ? 'bg-green-50 border-green-200' : 'bg-slate-50'}>
              <CardContent className="p-4 text-center">
                <Clock className="w-6 h-6 mx-auto mb-2 text-green-600" />
                <p className="text-xs text-slate-600">Status</p>
                <p className="text-lg font-bold text-slate-900 capitalize">{myDay.status || 'Not Started'}</p>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="p-4 text-center">
                <Route className="w-6 h-6 mx-auto mb-2 text-blue-600" />
                <p className="text-xs text-slate-600">Distance</p>
                <p className="text-xl font-bold text-slate-900">{myDay.total_distance_km || 0} KM</p>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="p-4 text-center">
                <Building className="w-6 h-6 mx-auto mb-2 text-purple-600" />
                <p className="text-xs text-slate-600">Visits</p>
                <p className="text-xl font-bold text-slate-900">{myDay.visits_count || 0}</p>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="p-4 text-center">
                <Timer className="w-6 h-6 mx-auto mb-2 text-orange-600" />
                <p className="text-xs text-slate-600">Working Hours</p>
                <p className="text-xl font-bold text-slate-900">{myDay.working_hours || 0}h</p>
              </CardContent>
            </Card>
            <Card className="bg-teal-50 border-teal-200">
              <CardContent className="p-4 text-center">
                <Fuel className="w-6 h-6 mx-auto mb-2 text-teal-600" />
                <p className="text-xs text-slate-600">Fuel Allowance</p>
                <p className="text-xl font-bold text-teal-700">₹{myDay.fuel_allowance || 0}</p>
              </CardContent>
            </Card>
          </div>

          {/* Map + Timeline Grid */}
          {myDay.status !== 'not_started' && (
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              {/* Route Map */}
              <Card>
                <CardContent className="p-4">
                  <h3 className="font-bold text-slate-900 mb-3 flex items-center gap-2">
                    <MapIcon className="w-5 h-5 text-teal-600" /> Today's Route
                  </h3>
                  {L && myDay.route_coordinates?.length > 0 ? (
                    <RouteMap routeData={myDay} />
                  ) : (
                    <div className="h-[400px] bg-slate-100 rounded-xl flex items-center justify-center">
                      <p className="text-slate-500">Route data loading...</p>
                    </div>
                  )}
                </CardContent>
              </Card>

              {/* Timeline */}
              <Card>
                <CardContent className="p-4">
                  <h3 className="font-bold text-slate-900 mb-3">Day Timeline</h3>
                  <div className="space-y-4 max-h-[400px] overflow-y-auto pr-2">
                    {myDay.day_start && (
                      <div className="flex items-start gap-3">
                        <div className="w-10 h-10 bg-green-100 rounded-full flex items-center justify-center flex-shrink-0">
                          <Play className="w-5 h-5 text-green-600" />
                        </div>
                        <div>
                          <p className="font-medium text-slate-900">Day Started</p>
                          <p className="text-sm text-slate-500">{new Date(myDay.day_start).toLocaleTimeString()}</p>
                          <p className="text-xs text-slate-400">{myDay.start_location?.address || 'Location recorded'}</p>
                        </div>
                      </div>
                    )}
                    {(myDay.visits || []).map((visit, idx) => (
                      <div key={idx} className="flex items-start gap-3">
                        <div className={`w-10 h-10 rounded-full flex items-center justify-center flex-shrink-0 ${
                          visit.status === 'completed' ? 'bg-blue-100' : 'bg-yellow-100'
                        }`}>
                          <Building className={`w-5 h-5 ${visit.status === 'completed' ? 'text-blue-600' : 'text-yellow-600'}`} />
                        </div>
                        <div className="flex-1">
                          <p className="font-medium text-slate-900">{visit.customer_name}</p>
                          <p className="text-sm text-slate-500">{visit.purpose}</p>
                          {visit.duration_minutes && <p className="text-xs text-slate-400">Duration: {visit.duration_minutes} mins</p>}
                          {visit.status === 'in_progress' ? (
                            <div className="flex gap-2 mt-2">
                              <Button size="sm" onClick={() => handleEndVisit(visit.id, 'successful')} className="bg-green-600 h-8 text-xs">
                                <CheckCircle className="w-3 h-3 mr-1" /> Success
                              </Button>
                              <Button size="sm" variant="outline" onClick={() => handleEndVisit(visit.id, 'follow_up')} className="h-8 text-xs">
                                Follow Up
                              </Button>
                              <Button size="sm" variant="outline" onClick={() => handleEndVisit(visit.id, 'not_interested')} className="h-8 text-xs text-red-600">
                                <XCircle className="w-3 h-3 mr-1" /> No Interest
                              </Button>
                            </div>
                          ) : (
                            <span className={`text-xs px-2 py-1 rounded inline-block mt-1 ${
                              visit.outcome === 'successful' ? 'bg-green-100 text-green-700' : 
                              visit.outcome === 'follow_up' ? 'bg-yellow-100 text-yellow-700' :
                              'bg-slate-100 text-slate-600'
                            }`}>{visit.outcome}</span>
                          )}
                        </div>
                      </div>
                    ))}
                    {myDay.day_end && (
                      <div className="flex items-start gap-3">
                        <div className="w-10 h-10 bg-red-100 rounded-full flex items-center justify-center flex-shrink-0">
                          <Square className="w-5 h-5 text-red-600" />
                        </div>
                        <div>
                          <p className="font-medium text-slate-900">Day Ended</p>
                          <p className="text-sm text-slate-500">{new Date(myDay.day_end).toLocaleTimeString()}</p>
                          <p className="text-xs text-slate-400">Working Hours: {myDay.working_hours}h | Distance: {myDay.total_distance_km} KM</p>
                        </div>
                      </div>
                    )}
                  </div>
                </CardContent>
              </Card>
            </div>
          )}

          {/* Vehicle & Summary */}
          {myDay.status !== 'not_started' && (
            <Card>
              <CardContent className="p-4">
                <h3 className="font-bold text-slate-900 mb-3">Day Summary</h3>
                <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
                  <div className="p-3 bg-slate-50 rounded-lg">
                    <span className="text-sm text-slate-600">Vehicle</span>
                    <p className="font-medium flex items-center gap-2 mt-1">
                      {myDay.vehicle_type === 'bike' ? <Bike className="w-4 h-4" /> : <Car className="w-4 h-4" />}
                      {myDay.vehicle_type}
                    </p>
                  </div>
                  <div className="p-3 bg-slate-50 rounded-lg">
                    <span className="text-sm text-slate-600">Fuel Rate</span>
                    <p className="font-medium mt-1">₹{fuelRates[myDay.vehicle_type] || 3.5}/KM</p>
                  </div>
                  <div className="p-3 bg-slate-50 rounded-lg">
                    <span className="text-sm text-slate-600">Total Distance</span>
                    <p className="font-medium mt-1">{myDay.total_distance_km || 0} KM</p>
                  </div>
                  <div className="p-3 bg-slate-50 rounded-lg">
                    <span className="text-sm text-slate-600">Total Visits</span>
                    <p className="font-medium mt-1">{myDay.visits_count || 0} visits</p>
                  </div>
                  <div className="p-3 bg-teal-50 rounded-lg">
                    <span className="text-sm text-teal-700 font-medium">Fuel Allowance</span>
                    <p className="font-bold text-teal-700 mt-1">₹{myDay.fuel_allowance || 0}</p>
                  </div>
                </div>
              </CardContent>
            </Card>
          )}
        </div>
      )}

      {/* Team Tab */}
      {activeTab === 'team' && teamDashboard && (
        <div className="space-y-6">
          <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
            <Card className="bg-green-50">
              <CardContent className="p-4 text-center">
                <p className="text-2xl font-bold text-green-600">{teamDashboard.summary?.active || 0}</p>
                <p className="text-sm text-slate-600">Active Now</p>
              </CardContent>
            </Card>
            <Card className="bg-blue-50">
              <CardContent className="p-4 text-center">
                <p className="text-2xl font-bold text-blue-600">{teamDashboard.summary?.completed || 0}</p>
                <p className="text-sm text-slate-600">Completed</p>
              </CardContent>
            </Card>
            <Card className="bg-red-50">
              <CardContent className="p-4 text-center">
                <p className="text-2xl font-bold text-red-600">{teamDashboard.summary?.absent || 0}</p>
                <p className="text-sm text-slate-600">Absent</p>
              </CardContent>
            </Card>
            <Card className="bg-purple-50">
              <CardContent className="p-4 text-center">
                <p className="text-2xl font-bold text-purple-600">{teamDashboard.summary?.total_visits || 0}</p>
                <p className="text-sm text-slate-600">Total Visits</p>
              </CardContent>
            </Card>
            <Card className="bg-teal-50">
              <CardContent className="p-4 text-center">
                <p className="text-2xl font-bold text-teal-600">{teamDashboard.summary?.total_distance_km || 0}</p>
                <p className="text-sm text-slate-600">Total KM</p>
              </CardContent>
            </Card>
          </div>

          <Card>
            <CardContent className="p-0">
              <table className="w-full">
                <thead className="bg-slate-50 border-b">
                  <tr>
                    <th className="text-left p-4 text-sm font-medium text-slate-600">Employee</th>
                    <th className="text-left p-4 text-sm font-medium text-slate-600">Status</th>
                    <th className="text-left p-4 text-sm font-medium text-slate-600">Start Time</th>
                    <th className="text-left p-4 text-sm font-medium text-slate-600">Visits</th>
                    <th className="text-left p-4 text-sm font-medium text-slate-600">Distance</th>
                    <th className="text-left p-4 text-sm font-medium text-slate-600">Tracking</th>
                    <th className="text-left p-4 text-sm font-medium text-slate-600">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {(teamDashboard.team || []).map((member, idx) => (
                    <tr key={idx} className="border-b hover:bg-slate-50">
                      <td className="p-4">
                        <p className="font-medium text-slate-900">{member.employee?.name}</p>
                        <p className="text-xs text-slate-500">{member.employee?.phone}</p>
                      </td>
                      <td className="p-4">
                        <span className={`px-2 py-1 rounded text-xs font-medium ${
                          member.status === 'active' ? 'bg-green-100 text-green-700' :
                          member.status === 'completed' ? 'bg-blue-100 text-blue-700' :
                          'bg-red-100 text-red-700'
                        }`}>{member.status}</span>
                      </td>
                      <td className="p-4 text-sm text-slate-600">
                        {member.day_start ? new Date(member.day_start).toLocaleTimeString() : '-'}
                      </td>
                      <td className="p-4 font-medium">{member.visits_count}</td>
                      <td className="p-4">{member.distance_km} KM</td>
                      <td className="p-4">
                        {member.is_tracking ? (
                          <span className="text-green-600 flex items-center gap-1"><MapPin className="w-4 h-4 animate-pulse" /> Live</span>
                        ) : (
                          <span className="text-slate-400">Offline</span>
                        )}
                      </td>
                      <td className="p-4">
                        <Button 
                          size="sm" 
                          variant="outline" 
                          onClick={() => fetchEmployeeTimeline(member.employee?.id)}
                          className="h-8"
                        >
                          <Eye className="w-4 h-4 mr-1" /> View
                        </Button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Performance Tab */}
      {activeTab === 'performance' && (
        <div className="space-y-6">
          {/* Summary Cards */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <Card className="bg-teal-50 border-teal-200">
              <CardContent className="p-4 text-center">
                <p className="text-2xl font-bold text-teal-700">{performance?.summary?.average_score || 0}</p>
                <p className="text-sm text-teal-600">Avg Score</p>
              </CardContent>
            </Card>
            <Card className="bg-green-50 border-green-200">
              <CardContent className="p-4 text-center">
                <p className="text-2xl font-bold text-green-700">{performance?.summary?.top_performers || 0}</p>
                <p className="text-sm text-green-600">Top Performers (A/A+)</p>
              </CardContent>
            </Card>
            <Card className="bg-yellow-50 border-yellow-200">
              <CardContent className="p-4 text-center">
                <p className="text-2xl font-bold text-yellow-700">{performance?.summary?.needs_improvement || 0}</p>
                <p className="text-sm text-yellow-600">Needs Improvement</p>
              </CardContent>
            </Card>
            <Card className="bg-blue-50 border-blue-200">
              <CardContent className="p-4 text-center">
                <p className="text-2xl font-bold text-blue-700">{performance?.summary?.total_employees || 0}</p>
                <p className="text-sm text-blue-600">Total Employees</p>
              </CardContent>
            </Card>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Performance Leaderboard with Grades */}
            <Card>
              <CardContent className="p-6">
                <h3 className="font-bold text-slate-900 mb-4 flex items-center gap-2">
                  <Award className="w-5 h-5 text-yellow-500" /> Performance Scorecard
                </h3>
                <div className="space-y-3">
                  {(performance?.report || []).slice(0, 10).map((emp, idx) => (
                    <div key={idx} className="flex items-center justify-between p-3 bg-slate-50 rounded-lg hover:bg-slate-100 transition-colors">
                      <div className="flex items-center gap-3">
                        <span className={`w-8 h-8 flex items-center justify-center rounded-full font-bold ${
                          idx === 0 ? 'bg-yellow-100 text-yellow-700' :
                          idx === 1 ? 'bg-slate-200 text-slate-700' :
                          idx === 2 ? 'bg-orange-100 text-orange-700' :
                          'bg-slate-100 text-slate-600'
                        }`}>{idx + 1}</span>
                        <div>
                          <p className="font-medium text-slate-900">{emp.employee_name}</p>
                          <p className="text-xs text-slate-500">
                            {emp.metrics?.days_worked || emp.days_worked || 0} days | {emp.metrics?.total_visits || emp.total_visits || 0} visits | {emp.metrics?.total_distance_km || emp.total_distance_km || 0} KM
                          </p>
                        </div>
                      </div>
                      <div className="text-right flex items-center gap-3">
                        <div>
                          <p className="text-lg font-bold text-teal-600">{emp.scores?.overall || 0}%</p>
                          <p className="text-xs text-slate-400">Score</p>
                        </div>
                        <span className={`w-10 h-10 flex items-center justify-center rounded-lg font-bold text-lg ${
                          emp.grade === 'A+' ? 'bg-green-500 text-white' :
                          emp.grade === 'A' ? 'bg-green-400 text-white' :
                          emp.grade === 'B+' ? 'bg-blue-400 text-white' :
                          emp.grade === 'B' ? 'bg-blue-300 text-white' :
                          emp.grade === 'C' ? 'bg-yellow-400 text-white' :
                          emp.grade === 'D' ? 'bg-orange-400 text-white' :
                          'bg-red-400 text-white'
                        }`}>{emp.grade || '-'}</span>
                      </div>
                    </div>
                  ))}
                  {(!performance?.report || performance.report.length === 0) && (
                    <p className="text-center text-slate-500 py-4">No performance data available</p>
                  )}
                </div>
              </CardContent>
            </Card>

            {/* Performance Chart */}
            <Card>
              <CardContent className="p-6">
                <h3 className="font-bold text-slate-900 mb-4">Score Distribution</h3>
                <ResponsiveContainer width="100%" height={300}>
                  <BarChart data={(performance?.report || []).slice(0, 8).map(e => ({
                    name: e.employee_name?.split(' ')[0] || 'Unknown',
                    attendance: e.scores?.attendance || 0,
                    distance: e.scores?.distance || 0,
                    visits: e.scores?.visits || 0,
                    conversion: e.scores?.conversion || 0
                  }))}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="name" tick={{ fontSize: 11 }} />
                    <YAxis domain={[0, 100]} />
                    <Tooltip />
                    <Legend />
                    <Bar dataKey="attendance" fill="#10b981" name="Attendance" />
                    <Bar dataKey="distance" fill="#3b82f6" name="Distance" />
                    <Bar dataKey="visits" fill="#8b5cf6" name="Visits" />
                    <Bar dataKey="conversion" fill="#f59e0b" name="Conversion" />
                  </BarChart>
                </ResponsiveContainer>
              </CardContent>
            </Card>
          </div>

          {/* Detailed Scores Table */}
          <Card>
            <CardContent className="p-6">
              <h3 className="font-bold text-slate-900 mb-4">Detailed Score Breakdown</h3>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="bg-slate-50 border-b">
                    <tr>
                      <th className="text-left p-3 font-medium text-slate-600">Employee</th>
                      <th className="text-center p-3 font-medium text-slate-600">Attendance</th>
                      <th className="text-center p-3 font-medium text-slate-600">Distance</th>
                      <th className="text-center p-3 font-medium text-slate-600">Visits</th>
                      <th className="text-center p-3 font-medium text-slate-600">Conversion</th>
                      <th className="text-center p-3 font-medium text-slate-600">Time Util.</th>
                      <th className="text-center p-3 font-medium text-slate-600">Overall</th>
                      <th className="text-center p-3 font-medium text-slate-600">Grade</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(performance?.report || []).map((emp, idx) => (
                      <tr key={idx} className="border-b hover:bg-slate-50">
                        <td className="p-3 font-medium">{emp.employee_name}</td>
                        <td className="p-3 text-center">
                          <span className={`px-2 py-1 rounded ${emp.scores?.attendance >= 80 ? 'bg-green-100 text-green-700' : emp.scores?.attendance >= 60 ? 'bg-yellow-100 text-yellow-700' : 'bg-red-100 text-red-700'}`}>
                            {emp.scores?.attendance || 0}%
                          </span>
                        </td>
                        <td className="p-3 text-center">
                          <span className={`px-2 py-1 rounded ${emp.scores?.distance >= 80 ? 'bg-green-100 text-green-700' : emp.scores?.distance >= 60 ? 'bg-yellow-100 text-yellow-700' : 'bg-red-100 text-red-700'}`}>
                            {emp.scores?.distance || 0}%
                          </span>
                        </td>
                        <td className="p-3 text-center">
                          <span className={`px-2 py-1 rounded ${emp.scores?.visits >= 80 ? 'bg-green-100 text-green-700' : emp.scores?.visits >= 60 ? 'bg-yellow-100 text-yellow-700' : 'bg-red-100 text-red-700'}`}>
                            {emp.scores?.visits || 0}%
                          </span>
                        </td>
                        <td className="p-3 text-center">
                          <span className={`px-2 py-1 rounded ${emp.scores?.conversion >= 50 ? 'bg-green-100 text-green-700' : emp.scores?.conversion >= 30 ? 'bg-yellow-100 text-yellow-700' : 'bg-red-100 text-red-700'}`}>
                            {emp.scores?.conversion || 0}%
                          </span>
                        </td>
                        <td className="p-3 text-center">
                          <span className={`px-2 py-1 rounded ${emp.scores?.time_utilization >= 50 ? 'bg-green-100 text-green-700' : emp.scores?.time_utilization >= 30 ? 'bg-yellow-100 text-yellow-700' : 'bg-red-100 text-red-700'}`}>
                            {emp.scores?.time_utilization || 0}%
                          </span>
                        </td>
                        <td className="p-3 text-center font-bold text-teal-600">{emp.scores?.overall || 0}%</td>
                        <td className="p-3 text-center">
                          <span className={`px-3 py-1 rounded font-bold ${
                            emp.grade === 'A+' || emp.grade === 'A' ? 'bg-green-500 text-white' :
                            emp.grade === 'B+' || emp.grade === 'B' ? 'bg-blue-400 text-white' :
                            emp.grade === 'C' ? 'bg-yellow-400 text-white' :
                            'bg-red-400 text-white'
                          }`}>{emp.grade || '-'}</span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </CardContent>
          </Card>

          {/* KPI Cards */}
          <Card>
            <CardContent className="p-6">
              <h3 className="font-bold text-slate-900 mb-4">Team KPIs This Month</h3>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <div className="p-4 bg-gradient-to-br from-teal-500 to-teal-600 rounded-xl text-white">
                  <Target className="w-8 h-8 mb-2 opacity-80" />
                  <p className="text-3xl font-bold">{(performance?.report || []).reduce((a, b) => a + (b.metrics?.total_visits || b.total_visits || 0), 0)}</p>
                  <p className="text-teal-100">Total Visits</p>
                </div>
                <div className="p-4 bg-gradient-to-br from-blue-500 to-blue-600 rounded-xl text-white">
                  <Route className="w-8 h-8 mb-2 opacity-80" />
                  <p className="text-3xl font-bold">{(performance?.report || []).reduce((a, b) => a + (b.metrics?.total_distance_km || b.total_distance_km || 0), 0).toFixed(0)}</p>
                  <p className="text-blue-100">Total KM</p>
                </div>
                <div className="p-4 bg-gradient-to-br from-purple-500 to-purple-600 rounded-xl text-white">
                  <Fuel className="w-8 h-8 mb-2 opacity-80" />
                  <p className="text-3xl font-bold">₹{(performance?.report || []).reduce((a, b) => a + (b.metrics?.total_fuel_allowance || b.total_fuel_allowance || 0), 0).toFixed(0)}</p>
                  <p className="text-purple-100">Fuel Allowance</p>
                </div>
                <div className="p-4 bg-gradient-to-br from-orange-500 to-orange-600 rounded-xl text-white">
                  <Users className="w-8 h-8 mb-2 opacity-80" />
                  <p className="text-3xl font-bold">{(performance?.report || []).length}</p>
                  <p className="text-orange-100">Active Reps</p>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Reports Tab */}
      {activeTab === 'reports' && misReport && (
        <div className="space-y-6">
          <Card>
            <CardContent className="p-6">
              <div className="flex justify-between items-center mb-4">
                <h3 className="font-bold text-slate-900">Daily MIS Report - {misReport.date}</h3>
                <Button variant="outline" size="sm">
                  <Download className="w-4 h-4 mr-2" /> Export
                </Button>
              </div>
              <div className="grid grid-cols-4 gap-4 mb-6">
                <div className="p-3 bg-slate-50 rounded-lg text-center">
                  <p className="text-2xl font-bold text-slate-900">{misReport.summary?.total_employees || 0}</p>
                  <p className="text-sm text-slate-500">Employees</p>
                </div>
                <div className="p-3 bg-slate-50 rounded-lg text-center">
                  <p className="text-2xl font-bold text-slate-900">{misReport.summary?.total_distance_km || 0}</p>
                  <p className="text-sm text-slate-500">Total KM</p>
                </div>
                <div className="p-3 bg-slate-50 rounded-lg text-center">
                  <p className="text-2xl font-bold text-slate-900">{misReport.summary?.total_visits || 0}</p>
                  <p className="text-sm text-slate-500">Total Visits</p>
                </div>
                <div className="p-3 bg-teal-50 rounded-lg text-center">
                  <p className="text-2xl font-bold text-teal-700">₹{misReport.summary?.total_fuel_allowance || 0}</p>
                  <p className="text-sm text-teal-600">Fuel Allowance</p>
                </div>
              </div>
              
              <table className="w-full">
                <thead className="bg-slate-50 border-b">
                  <tr>
                    <th className="text-left p-3 text-sm font-medium text-slate-600">Employee</th>
                    <th className="text-left p-3 text-sm font-medium text-slate-600">Vehicle</th>
                    <th className="text-left p-3 text-sm font-medium text-slate-600">Start</th>
                    <th className="text-left p-3 text-sm font-medium text-slate-600">End</th>
                    <th className="text-left p-3 text-sm font-medium text-slate-600">Hours</th>
                    <th className="text-left p-3 text-sm font-medium text-slate-600">KM</th>
                    <th className="text-left p-3 text-sm font-medium text-slate-600">Visits</th>
                    <th className="text-left p-3 text-sm font-medium text-slate-600">Fuel ₹</th>
                    <th className="text-left p-3 text-sm font-medium text-slate-600">Status</th>
                  </tr>
                </thead>
                <tbody>
                  {(misReport.report || []).map((row, idx) => (
                    <tr key={idx} className="border-b hover:bg-slate-50">
                      <td className="p-3 font-medium">{row.employee_name}</td>
                      <td className="p-3 capitalize">{row.vehicle_type}</td>
                      <td className="p-3 text-sm">{row.day_start ? new Date(row.day_start).toLocaleTimeString() : '-'}</td>
                      <td className="p-3 text-sm">{row.day_end ? new Date(row.day_end).toLocaleTimeString() : '-'}</td>
                      <td className="p-3">{row.working_hours || 0}h</td>
                      <td className="p-3 font-medium">{row.distance_km}</td>
                      <td className="p-3">{row.visits_total}</td>
                      <td className="p-3 font-medium text-teal-600">₹{row.fuel_allowance}</td>
                      <td className="p-3">
                        <span className={`px-2 py-1 rounded text-xs ${
                          row.status === 'completed' ? 'bg-green-100 text-green-700' :
                          row.status === 'active' ? 'bg-blue-100 text-blue-700' :
                          'bg-slate-100 text-slate-600'
                        }`}>{row.status}</span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Alerts Tab */}
      {activeTab === 'alerts' && (
        <div className="space-y-4">
          {/* Alert Summary Cards */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <Card className="bg-red-50 border-red-200">
              <CardContent className="p-4 text-center">
                <MapPin className="w-6 h-6 mx-auto mb-2 text-red-500" />
                <p className="text-2xl font-bold text-red-700">{alerts.filter(a => a.type === 'location_off').length}</p>
                <p className="text-xs text-red-600">Location OFF</p>
              </CardContent>
            </Card>
            <Card className="bg-yellow-50 border-yellow-200">
              <CardContent className="p-4 text-center">
                <StopCircle className="w-6 h-6 mx-auto mb-2 text-yellow-500" />
                <p className="text-2xl font-bold text-yellow-700">{alerts.filter(a => a.type === 'long_stop').length}</p>
                <p className="text-xs text-yellow-600">Long Stops</p>
              </CardContent>
            </Card>
            <Card className="bg-orange-50 border-orange-200">
              <CardContent className="p-4 text-center">
                <Building className="w-6 h-6 mx-auto mb-2 text-orange-500" />
                <p className="text-2xl font-bold text-orange-700">{alerts.filter(a => a.type === 'zero_visits').length}</p>
                <p className="text-xs text-orange-600">Zero Visits</p>
              </CardContent>
            </Card>
            <Card className="bg-blue-50 border-blue-200">
              <CardContent className="p-4 text-center">
                <Zap className="w-6 h-6 mx-auto mb-2 text-blue-500" />
                <p className="text-2xl font-bold text-blue-700">{alerts.filter(a => a.type === 'low_battery').length}</p>
                <p className="text-xs text-blue-600">Low Battery</p>
              </CardContent>
            </Card>
          </div>

          {alerts.length === 0 ? (
            <Card>
              <CardContent className="p-8 text-center">
                <CheckCircle className="w-12 h-12 mx-auto mb-4 text-green-500" />
                <h3 className="font-bold text-slate-900 mb-2">All Clear!</h3>
                <p className="text-slate-500">No alerts or exceptions at this time. All field employees are on track.</p>
              </CardContent>
            </Card>
          ) : (
            <div className="space-y-3">
              <h3 className="font-bold text-slate-900 flex items-center gap-2">
                <AlertTriangle className="w-5 h-5 text-red-500" />
                Active Alerts ({alerts.length})
              </h3>
              {alerts.map((alert, idx) => (
                <Card key={idx} className={`border-l-4 ${
                  alert.severity === 'high' ? 'border-l-red-500 bg-red-50' :
                  alert.severity === 'medium' ? 'border-l-yellow-500 bg-yellow-50' :
                  'border-l-blue-500 bg-blue-50'
                }`}>
                  <CardContent className="p-4">
                    <div className="flex items-start justify-between">
                      <div className="flex items-start gap-3">
                        {alert.type === 'location_off' ? (
                          <MapPin className="w-5 h-5 text-red-500 animate-pulse" />
                        ) : alert.type === 'long_stop' ? (
                          <StopCircle className="w-5 h-5 text-yellow-500" />
                        ) : alert.type === 'zero_visits' ? (
                          <Building className="w-5 h-5 text-orange-500" />
                        ) : (
                          <Zap className="w-5 h-5 text-blue-500" />
                        )}
                        <div>
                          <p className="font-medium text-slate-900">{alert.title}</p>
                          <p className="text-sm text-slate-600">{alert.message}</p>
                          <div className="flex items-center gap-3 mt-2">
                            <span className="text-xs text-slate-400">
                              {alert.timestamp ? new Date(alert.timestamp).toLocaleTimeString() : ''}
                            </span>
                            <span className={`px-2 py-0.5 rounded text-xs ${
                              alert.type === 'location_off' ? 'bg-red-100 text-red-700' :
                              alert.type === 'long_stop' ? 'bg-yellow-100 text-yellow-700' :
                              alert.type === 'zero_visits' ? 'bg-orange-100 text-orange-700' :
                              'bg-blue-100 text-blue-700'
                            }`}>{alert.type?.replace('_', ' ')}</span>
                          </div>
                        </div>
                      </div>
                      <div className="flex flex-col items-end gap-2">
                        <span className={`px-2 py-1 rounded text-xs font-medium uppercase ${
                          alert.severity === 'high' ? 'bg-red-200 text-red-700' :
                          alert.severity === 'medium' ? 'bg-yellow-200 text-yellow-700' :
                          'bg-blue-200 text-blue-700'
                        }`}>{alert.severity}</span>
                        {alert.user_id && (
                          <Button 
                            size="sm" 
                            variant="outline" 
                            className="h-7 text-xs"
                            onClick={() => fetchEmployeeTimeline(alert.user_id)}
                          >
                            <Eye className="w-3 h-3 mr-1" /> View
                          </Button>
                        )}
                      </div>
                    </div>
                  </CardContent>
                </Card>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Start Day Modal */}
      {showStartDayModal && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <Card className="w-full max-w-md mx-4">
            <CardContent className="p-6">
              <h3 className="text-lg font-bold text-slate-900 mb-4">Start Your Day</h3>
              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-2">Select Vehicle</label>
                  <div className="grid grid-cols-3 gap-3">
                    {['bike', 'car', 'company_vehicle'].map((type) => (
                      <button
                        key={type}
                        onClick={() => setVehicleType(type)}
                        className={`p-4 rounded-lg border-2 transition-all ${
                          vehicleType === type ? 'border-teal-500 bg-teal-50' : 'border-slate-200'
                        }`}
                      >
                        {type === 'bike' ? <Bike className="w-6 h-6 mx-auto mb-1" /> : <Car className="w-6 h-6 mx-auto mb-1" />}
                        <p className="text-xs capitalize">{type.replace('_', ' ')}</p>
                        <p className="text-xs text-slate-500">₹{fuelRates[type]}/KM</p>
                      </button>
                    ))}
                  </div>
                </div>
                <div className="p-3 bg-slate-50 rounded-lg">
                  <p className="text-sm text-slate-600">
                    <MapPin className="w-4 h-4 inline mr-1" />
                    {currentLocation ? `Location: ${currentLocation.latitude.toFixed(4)}, ${currentLocation.longitude.toFixed(4)}` : 'Getting location...'}
                  </p>
                </div>
                <div className="flex gap-3">
                  <Button onClick={() => setShowStartDayModal(false)} variant="outline" className="flex-1">Cancel</Button>
                  <Button onClick={handleStartDay} className="flex-1 bg-green-600 hover:bg-green-700" disabled={!currentLocation}>
                    <Play className="w-4 h-4 mr-2" /> Start Day
                  </Button>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Visit Modal */}
      {showVisitModal && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <Card className="w-full max-w-md mx-4">
            <CardContent className="p-6">
              <h3 className="text-lg font-bold text-slate-900 mb-4">Start New Visit</h3>
              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-1">Customer Name *</label>
                  <input
                    type="text"
                    value={visitData.customer_name}
                    onChange={(e) => setVisitData({ ...visitData, customer_name: e.target.value })}
                    className="w-full px-3 py-2 border border-slate-300 rounded-lg"
                    placeholder="Enter customer/shop name"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-1">Purpose</label>
                  <select
                    value={visitData.purpose}
                    onChange={(e) => setVisitData({ ...visitData, purpose: e.target.value })}
                    className="w-full px-3 py-2 border border-slate-300 rounded-lg"
                  >
                    <option value="">Select purpose</option>
                    <option value="Product Demo">Product Demo</option>
                    <option value="Sales Follow-up">Sales Follow-up</option>
                    <option value="Payment Collection">Payment Collection</option>
                    <option value="Complaint Resolution">Complaint Resolution</option>
                    <option value="New Lead">New Lead</option>
                  </select>
                </div>
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-1">Notes</label>
                  <textarea
                    value={visitData.notes}
                    onChange={(e) => setVisitData({ ...visitData, notes: e.target.value })}
                    className="w-full px-3 py-2 border border-slate-300 rounded-lg"
                    rows={2}
                  />
                </div>
                <div className="flex gap-3">
                  <Button onClick={() => setShowVisitModal(false)} variant="outline" className="flex-1">Cancel</Button>
                  <Button onClick={handleStartVisit} className="flex-1 bg-teal-600 hover:bg-teal-700">
                    <Building className="w-4 h-4 mr-2" /> Start Visit
                  </Button>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Map Modal */}
      {showMapModal && mapData && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <Card className="w-full max-w-4xl mx-4 max-h-[90vh] overflow-hidden">
            <CardContent className="p-6">
              <div className="flex justify-between items-center mb-4">
                <h3 className="text-lg font-bold text-slate-900">
                  Route Map - {mapData.user_name || 'Today'}
                </h3>
                <Button onClick={() => setShowMapModal(false)} variant="outline" size="sm">Close</Button>
              </div>
              <div className="grid grid-cols-3 gap-3 mb-4">
                <div className="p-2 bg-slate-50 rounded text-center">
                  <p className="text-lg font-bold">{mapData.total_distance_km || mapData.summary?.distance_km || 0} KM</p>
                  <p className="text-xs text-slate-500">Distance</p>
                </div>
                <div className="p-2 bg-slate-50 rounded text-center">
                  <p className="text-lg font-bold">{mapData.visits_count || mapData.visits?.length || 0}</p>
                  <p className="text-xs text-slate-500">Visits</p>
                </div>
                <div className="p-2 bg-teal-50 rounded text-center">
                  <p className="text-lg font-bold text-teal-700">₹{mapData.fuel_allowance || mapData.summary?.fuel_allowance || 0}</p>
                  <p className="text-xs text-slate-500">Fuel</p>
                </div>
              </div>
              {L ? (
                <RouteMap routeData={mapData} />
              ) : (
                <div className="h-[400px] bg-slate-100 rounded-xl flex items-center justify-center">
                  <p className="text-slate-500">Loading map...</p>
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      )}
    </div>
  );
};

export default SFADashboard;
