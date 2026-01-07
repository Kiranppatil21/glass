import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { 
  TrendingUp, TrendingDown, Minus, BarChart3, Package, 
  Calendar, RefreshCw, Brain, Lightbulb, AlertTriangle,
  ChevronRight, Sparkles
} from 'lucide-react';
import erpApi from '../../utils/erpApi';
import { toast } from 'sonner';
import { AreaChart, Area, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, PieChart, Pie, Cell } from 'recharts';

const ForecastDashboard = () => {
  const [loading, setLoading] = useState(true);
  const [stats, setStats] = useState(null);
  const [forecast, setForecast] = useState(null);
  const [forecastDays, setForecastDays] = useState(90);
  const [generatingForecast, setGeneratingForecast] = useState(false);

  useEffect(() => {
    fetchData();
  }, []);

  const fetchData = async () => {
    try {
      setLoading(true);
      const [statsRes] = await Promise.all([
        erpApi.forecast.getOrderStats()
      ]);
      setStats(statsRes.data);
    } catch (error) {
      console.error('Failed to fetch stats:', error);
      toast.error('Failed to load statistics');
    } finally {
      setLoading(false);
    }
  };

  const generateForecast = async () => {
    try {
      setGeneratingForecast(true);
      toast.info('Generating AI forecast... This may take a moment.');
      const response = await erpApi.forecast.getDemandForecast(forecastDays);
      setForecast(response.data);
      toast.success('AI forecast generated successfully!');
    } catch (error) {
      console.error('Failed to generate forecast:', error);
      toast.error('Failed to generate forecast');
    } finally {
      setGeneratingForecast(false);
    }
  };

  const COLORS = ['#0d9488', '#0ea5e9', '#8b5cf6', '#f59e0b', '#ef4444'];

  const getTrendIcon = (trend) => {
    if (trend === 'growing') return <TrendingUp className="w-5 h-5 text-emerald-500" />;
    if (trend === 'declining') return <TrendingDown className="w-5 h-5 text-red-500" />;
    return <Minus className="w-5 h-5 text-slate-500" />;
  };

  const getTrendColor = (trend) => {
    if (trend === 'growing') return 'text-emerald-600 bg-emerald-50';
    if (trend === 'declining') return 'text-red-600 bg-red-50';
    return 'text-slate-600 bg-slate-50';
  };

  if (loading) {
    return (
      <div className="p-6 flex items-center justify-center h-96" data-testid="forecast-loading">
        <div className="text-lg text-slate-600">Loading statistics...</div>
      </div>
    );
  }

  return (
    <div className="p-6 space-y-6" data-testid="forecast-dashboard">
      {/* Header */}
      <div className="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold text-slate-900 flex items-center gap-3">
            <Brain className="w-8 h-8 text-teal-600" />
            AI Demand Forecasting
          </h1>
          <p className="text-slate-600 mt-1">
            Analyze order patterns and predict future demand using AI
          </p>
        </div>
        <div className="flex items-center gap-3">
          <select
            value={forecastDays}
            onChange={(e) => setForecastDays(Number(e.target.value))}
            className="px-3 py-2 border rounded-lg text-sm"
            data-testid="forecast-days-select"
          >
            <option value={30}>Last 30 days</option>
            <option value={60}>Last 60 days</option>
            <option value={90}>Last 90 days</option>
            <option value={180}>Last 180 days</option>
            <option value={365}>Last 365 days</option>
          </select>
          <Button 
            onClick={generateForecast}
            disabled={generatingForecast}
            className="bg-teal-600 hover:bg-teal-700"
            data-testid="generate-forecast-btn"
          >
            {generatingForecast ? (
              <>
                <RefreshCw className="w-4 h-4 mr-2 animate-spin" />
                Analyzing...
              </>
            ) : (
              <>
                <Sparkles className="w-4 h-4 mr-2" />
                Generate AI Forecast
              </>
            )}
          </Button>
        </div>
      </div>

      {/* Stats Cards */}
      {stats && (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          <Card className="border-l-4 border-l-teal-500">
            <CardContent className="p-4">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-slate-500">Today's Orders</p>
                  <p className="text-2xl font-bold text-slate-900">{stats.orders?.today || 0}</p>
                </div>
                <Calendar className="w-10 h-10 text-teal-500 opacity-50" />
              </div>
            </CardContent>
          </Card>
          
          <Card className="border-l-4 border-l-blue-500">
            <CardContent className="p-4">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-slate-500">This Week</p>
                  <p className="text-2xl font-bold text-slate-900">{stats.orders?.this_week || 0}</p>
                </div>
                <BarChart3 className="w-10 h-10 text-blue-500 opacity-50" />
              </div>
            </CardContent>
          </Card>
          
          <Card className="border-l-4 border-l-purple-500">
            <CardContent className="p-4">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-slate-500">This Month</p>
                  <p className="text-2xl font-bold text-slate-900">{stats.orders?.this_month || 0}</p>
                </div>
                <Package className="w-10 h-10 text-purple-500 opacity-50" />
              </div>
            </CardContent>
          </Card>
          
          <Card className="border-l-4 border-l-amber-500">
            <CardContent className="p-4">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-slate-500">Monthly Revenue</p>
                  <p className="text-2xl font-bold text-slate-900">
                    ₹{(stats.revenue?.this_month || 0).toLocaleString()}
                  </p>
                </div>
                <TrendingUp className="w-10 h-10 text-amber-500 opacity-50" />
              </div>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Top Products */}
      {stats?.top_products && stats.top_products.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Package className="w-5 h-5 text-teal-600" />
              Top Products This Month
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid lg:grid-cols-2 gap-6">
              <div className="space-y-3">
                {stats.top_products.map((product, index) => (
                  <div key={index} className="flex items-center justify-between p-3 bg-slate-50 rounded-lg">
                    <div className="flex items-center gap-3">
                      <span className="w-8 h-8 rounded-full bg-teal-100 text-teal-700 flex items-center justify-center font-bold text-sm">
                        {index + 1}
                      </span>
                      <span className="font-medium text-slate-900">{product.name}</span>
                    </div>
                    <div className="text-right">
                      <p className="font-bold text-teal-600">₹{product.revenue?.toLocaleString()}</p>
                      <p className="text-xs text-slate-500">{product.count} orders</p>
                    </div>
                  </div>
                ))}
              </div>
              <div className="h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie
                      data={stats.top_products}
                      dataKey="count"
                      nameKey="name"
                      cx="50%"
                      cy="50%"
                      outerRadius={80}
                      label={({ name, count }) => `${name}: ${count}`}
                    >
                      {stats.top_products.map((_, index) => (
                        <Cell key={index} fill={COLORS[index % COLORS.length]} />
                      ))}
                    </Pie>
                    <Tooltip />
                  </PieChart>
                </ResponsiveContainer>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* AI Forecast Results */}
      {forecast && (
        <div className="space-y-6">
          {/* Forecast Summary */}
          <Card className="border-2 border-teal-200 bg-gradient-to-r from-teal-50 to-white">
            <CardHeader>
              <CardTitle className="flex items-center gap-2 text-teal-700">
                <Brain className="w-6 h-6" />
                AI Analysis Summary
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid lg:grid-cols-3 gap-6">
                {/* Trend */}
                <div className="text-center p-4 bg-white rounded-xl shadow-sm">
                  <div className={`inline-flex items-center gap-2 px-4 py-2 rounded-full ${getTrendColor(forecast.trend)}`}>
                    {getTrendIcon(forecast.trend)}
                    <span className="font-semibold capitalize">{forecast.trend}</span>
                  </div>
                  <p className="text-3xl font-bold mt-3 text-slate-900">
                    {forecast.trend_percentage > 0 ? '+' : ''}{forecast.trend_percentage?.toFixed(1)}%
                  </p>
                  <p className="text-sm text-slate-500 mt-1">Trend Change</p>
                </div>
                
                {/* Next Month Prediction */}
                <div className="text-center p-4 bg-white rounded-xl shadow-sm">
                  <p className="text-sm text-slate-500 mb-2">Predicted Orders</p>
                  <p className="text-4xl font-bold text-teal-600">
                    {forecast.next_month_prediction?.estimated_orders || 0}
                  </p>
                  <p className="text-sm text-slate-500 mt-2">Next Month</p>
                </div>
                
                {/* Predicted Revenue */}
                <div className="text-center p-4 bg-white rounded-xl shadow-sm">
                  <p className="text-sm text-slate-500 mb-2">Predicted Revenue</p>
                  <p className="text-4xl font-bold text-emerald-600">
                    ₹{(forecast.next_month_prediction?.estimated_revenue || 0).toLocaleString()}
                  </p>
                  <p className="text-sm text-slate-500 mt-2">Next Month</p>
                </div>
              </div>
              
              {/* Summary Text */}
              <div className="mt-6 p-4 bg-slate-50 rounded-lg">
                <p className="text-slate-700 leading-relaxed">{forecast.summary}</p>
              </div>
            </CardContent>
          </Card>

          {/* Peak Periods & High Demand Products */}
          <div className="grid lg:grid-cols-2 gap-6">
            {/* Peak Periods */}
            {forecast.peak_periods && forecast.peak_periods.length > 0 && (
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Calendar className="w-5 h-5 text-blue-600" />
                    Peak Periods
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-2">
                    {forecast.peak_periods.map((period, index) => (
                      <div key={index} className="flex items-center gap-2 p-2 bg-blue-50 rounded-lg">
                        <ChevronRight className="w-4 h-4 text-blue-600" />
                        <span className="text-slate-700">{period}</span>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>
            )}

            {/* High Demand Products */}
            {forecast.next_month_prediction?.high_demand_products && (
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <AlertTriangle className="w-5 h-5 text-amber-600" />
                    High Demand Expected
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-2">
                    {forecast.next_month_prediction.high_demand_products.map((product, index) => (
                      <div key={index} className="flex items-center gap-2 p-2 bg-amber-50 rounded-lg">
                        <Package className="w-4 h-4 text-amber-600" />
                        <span className="text-slate-700 font-medium">{product}</span>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>
            )}
          </div>

          {/* Recommendations */}
          {forecast.recommendations && forecast.recommendations.length > 0 && (
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Lightbulb className="w-5 h-5 text-amber-500" />
                  AI Recommendations
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid md:grid-cols-2 gap-4">
                  {forecast.recommendations.map((rec, index) => (
                    <div key={index} className="flex gap-3 p-4 bg-gradient-to-r from-amber-50 to-orange-50 rounded-xl border border-amber-100">
                      <div className="w-8 h-8 rounded-full bg-amber-100 text-amber-700 flex items-center justify-center font-bold text-sm flex-shrink-0">
                        {index + 1}
                      </div>
                      <p className="text-slate-700">{rec}</p>
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>
          )}

          {/* Insights */}
          {forecast.insights && forecast.insights.length > 0 && (
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Sparkles className="w-5 h-5 text-purple-500" />
                  Additional Insights
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-3">
                  {forecast.insights.map((insight, index) => (
                    <div key={index} className="flex gap-3 p-3 bg-purple-50 rounded-lg border-l-4 border-purple-400">
                      <p className="text-slate-700">{insight}</p>
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>
          )}

          {/* Data Period Info */}
          <div className="text-center text-sm text-slate-500">
            <p>
              Analysis based on <strong>{forecast.data_period?.order_count || 0}</strong> orders 
              from <strong>{forecast.data_period?.start}</strong> to <strong>{forecast.data_period?.end}</strong>
            </p>
            <p className="mt-1">
              Generated at: {new Date(forecast.generated_at).toLocaleString()}
            </p>
          </div>
        </div>
      )}

      {/* No Forecast Yet */}
      {!forecast && (
        <Card className="border-dashed border-2 border-slate-200">
          <CardContent className="py-16 text-center">
            <Brain className="w-16 h-16 text-slate-300 mx-auto mb-4" />
            <h3 className="text-xl font-semibold text-slate-700 mb-2">
              No Forecast Generated Yet
            </h3>
            <p className="text-slate-500 mb-6 max-w-md mx-auto">
              Click "Generate AI Forecast" to analyze your order history and get demand predictions powered by AI.
            </p>
            <Button 
              onClick={generateForecast}
              disabled={generatingForecast}
              className="bg-teal-600 hover:bg-teal-700"
            >
              {generatingForecast ? (
                <>
                  <RefreshCw className="w-4 h-4 mr-2 animate-spin" />
                  Analyzing...
                </>
              ) : (
                <>
                  <Sparkles className="w-4 h-4 mr-2" />
                  Generate AI Forecast
                </>
              )}
            </Button>
          </CardContent>
        </Card>
      )}
    </div>
  );
};

export default ForecastDashboard;
