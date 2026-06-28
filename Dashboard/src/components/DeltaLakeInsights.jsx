import React, { useState, useEffect } from 'react';
import { Database, TrendingUp, CloudRain, Route, Activity, ChevronUp, ChevronDown } from 'lucide-react';

const DeltaLakeInsights = () => {
  const [activeTab, setActiveTab] = useState('airline');
  const [data, setData] = useState({
    airline_daily_performance: [],
    route_daily_statistics: [],
    hourly_delay_trends: [],
    weather_impact_analysis: []
  });
  const [loading, setLoading] = useState(true);
  const [isExpanded, setIsExpanded] = useState(true);

  const fetchGoldTables = async () => {
    setLoading(true);
    try {
      const endpoints = [
        'airline_daily_performance',
        'route_daily_statistics',
        'hourly_delay_trends',
        'weather_impact_analysis'
      ];
      
      const results = await Promise.all(
        endpoints.map(ep => fetch(`http://localhost:8000/api/gold/${ep}`).then(r => r.json()))
      );
      
      setData({
        airline_daily_performance: results[0] || [],
        route_daily_statistics: results[1] || [],
        hourly_delay_trends: results[2] || [],
        weather_impact_analysis: results[3] || []
      });
    } catch (error) {
      console.error("Failed to fetch Delta Lake Gold tables:", error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchGoldTables();
    // Refresh data every 60 seconds (since batch runs every 5 minutes)
    const interval = setInterval(fetchGoldTables, 60000);
    return () => clearInterval(interval);
  }, []);

  const renderAirlinePerformance = () => (
    <div className="table-container fade-in">
      <table className="data-table">
        <thead>
          <tr>
            <th>Date</th>
            <th>Airline</th>
            <th>Total Flights</th>
            <th>On Time %</th>
            <th>Avg Delay (min)</th>
            <th>Affected Pax</th>
          </tr>
        </thead>
        <tbody>
          {data.airline_daily_performance.length > 0 ? data.airline_daily_performance.map((row, idx) => (
            <tr key={idx}>
              <td>{row.report_date}</td>
              <td style={{fontWeight: 'bold', color: 'var(--color-blue)'}}>{row.airline_icao}</td>
              <td>{row.total_flights}</td>
              <td><span className={`badge ${row.on_time_percentage > 80 ? 'green' : 'amber'}`}>{row.on_time_percentage}%</span></td>
              <td>{row.avg_delay_minutes}</td>
              <td>{row.total_affected_passengers}</td>
            </tr>
          )) : <tr><td colSpan="6" style={{textAlign: 'center', padding: '2rem'}}>Belum ada data agregasi batch (Delta Lake).</td></tr>}
        </tbody>
      </table>
    </div>
  );

  const renderRouteStats = () => (
    <div className="table-container fade-in">
      <table className="data-table">
        <thead>
          <tr>
            <th>Date</th>
            <th>Route</th>
            <th>Total Flights</th>
            <th>Avg Delay (min)</th>
            <th>Critical Delays</th>
            <th>Avg Weather Score</th>
          </tr>
        </thead>
        <tbody>
          {data.route_daily_statistics.length > 0 ? data.route_daily_statistics.map((row, idx) => (
            <tr key={idx}>
              <td>{row.report_date}</td>
              <td style={{fontWeight: 'bold', color: 'var(--color-purple)'}}>{row.origin} → {row.destination}</td>
              <td>{row.total_flights}</td>
              <td>{row.avg_delay_minutes}</td>
              <td>{row.critical_delay_count}</td>
              <td>{row.avg_weather_score}</td>
            </tr>
          )) : <tr><td colSpan="6" style={{textAlign: 'center', padding: '2rem'}}>Belum ada data agregasi batch (Delta Lake).</td></tr>}
        </tbody>
      </table>
    </div>
  );

  const renderHourlyTrends = () => (
    <div className="table-container fade-in">
      <table className="data-table">
        <thead>
          <tr>
            <th>Date</th>
            <th>Hour (UTC)</th>
            <th>Total Flights</th>
            <th>Delayed %</th>
            <th>Avg Delay (min)</th>
            <th>Traffic Density</th>
          </tr>
        </thead>
        <tbody>
          {data.hourly_delay_trends.length > 0 ? data.hourly_delay_trends.map((row, idx) => (
            <tr key={idx}>
              <td>{row.report_date}</td>
              <td><span className="badge amber" style={{background: 'rgba(59, 130, 246, 0.1)', color: 'var(--color-blue)', border: '1px solid rgba(59, 130, 246, 0.3)'}}>{row.hour_utc}:00</span></td>
              <td>{row.total_flights}</td>
              <td><span className={`badge ${row.delayed_percentage > 50 ? 'red' : 'amber'}`}>{row.delayed_percentage}%</span></td>
              <td>{row.avg_delay_minutes}</td>
              <td>{row.avg_traffic_density}</td>
            </tr>
          )) : <tr><td colSpan="6" style={{textAlign: 'center', padding: '2rem'}}>Belum ada data agregasi batch (Delta Lake).</td></tr>}
        </tbody>
      </table>
    </div>
  );

  const renderWeatherImpact = () => (
    <div className="table-container fade-in">
      <table className="data-table">
        <thead>
          <tr>
            <th>Date</th>
            <th>Weather Bucket</th>
            <th>Total Flights</th>
            <th>Critical %</th>
            <th>Avg Delay (min)</th>
            <th>Avg FDI</th>
          </tr>
        </thead>
        <tbody>
          {data.weather_impact_analysis.length > 0 ? data.weather_impact_analysis.map((row, idx) => (
            <tr key={idx}>
              <td>{row.report_date}</td>
              <td>
                <span className={`badge ${row.weather_bucket === 'CLEAR' ? 'green' : row.weather_bucket === 'MODERATE' ? 'amber' : 'red'}`}>
                  {row.weather_bucket}
                </span>
              </td>
              <td>{row.total_flights}</td>
              <td>{row.critical_percentage}%</td>
              <td>{row.avg_delay_minutes}</td>
              <td>{row.avg_fdi}</td>
            </tr>
          )) : <tr><td colSpan="6" style={{textAlign: 'center', padding: '2rem'}}>Belum ada data agregasi batch (Delta Lake).</td></tr>}
        </tbody>
      </table>
    </div>
  );

  return (
    <div className="glass-panel" style={{ marginTop: '1rem', display: 'flex', flexDirection: 'column', transition: 'all 0.3s' }}>
      <div 
        className="panel-header" 
        style={{ cursor: 'pointer', borderBottom: isExpanded ? '1px solid var(--glass-border)' : 'none', paddingBottom: isExpanded ? '1rem' : '0', marginBottom: isExpanded ? '1rem' : '0', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}
        onClick={() => setIsExpanded(!isExpanded)}
      >
        <h2 style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', margin: 0, fontSize: '1.25rem', color: 'var(--text-primary)' }}>
          <Database size={20} color="var(--color-blue)" />
          Delta Lake Analytics (Batch Processing)
          {loading && <span style={{fontSize: '0.8rem', color: 'var(--text-secondary)', marginLeft: '1rem', fontWeight: 'normal'}}>Memuat...</span>}
        </h2>
        
        <button 
          className="btn-demo" 
          style={{ padding: '4px 10px', fontSize: '0.75rem', background: 'transparent', borderColor: 'var(--panel-border)' }} 
          onClick={(e) => { e.stopPropagation(); setIsExpanded(!isExpanded); }}
        >
          {isExpanded ? <><ChevronDown size={14}/> HIDE</> : <><ChevronUp size={14}/> SHOW</>}
        </button>
      </div>
      
      {isExpanded && (
        <>
          <div style={{ display: 'flex', gap: '0.5rem', marginBottom: '1rem', flexWrap: 'wrap' }}>
            <button 
              className={`btn-demo ${activeTab === 'airline' ? 'active-tab' : ''}`}
              style={{ background: activeTab === 'airline' ? 'var(--color-blue)' : 'var(--glass-bg)' }}
              onClick={() => setActiveTab('airline')}
            >
              <Activity size={16} /> Performa Maskapai
            </button>
            <button 
              className={`btn-demo ${activeTab === 'route' ? 'active-tab' : ''}`}
              style={{ background: activeTab === 'route' ? 'var(--color-purple)' : 'var(--glass-bg)' }}
              onClick={() => setActiveTab('route')}
            >
              <Route size={16} /> Statistik Rute
            </button>
            <button 
              className={`btn-demo ${activeTab === 'hourly' ? 'active-tab' : ''}`}
              style={{ background: activeTab === 'hourly' ? 'var(--color-orange)' : 'var(--glass-bg)' }}
              onClick={() => setActiveTab('hourly')}
            >
              <TrendingUp size={16} /> Tren Delay per Jam
            </button>
            <button 
              className={`btn-demo ${activeTab === 'weather' ? 'active-tab' : ''}`}
              style={{ background: activeTab === 'weather' ? 'var(--color-red)' : 'var(--glass-bg)' }}
              onClick={() => setActiveTab('weather')}
            >
              <CloudRain size={16} /> Analisis Dampak Cuaca
            </button>
          </div>

          <div style={{ flex: 1, minHeight: '300px', overflowY: 'auto' }}>
            {activeTab === 'airline' && renderAirlinePerformance()}
            {activeTab === 'route' && renderRouteStats()}
            {activeTab === 'hourly' && renderHourlyTrends()}
            {activeTab === 'weather' && renderWeatherImpact()}
          </div>
        </>
      )}
    </div>
  );
};

export default DeltaLakeInsights;
