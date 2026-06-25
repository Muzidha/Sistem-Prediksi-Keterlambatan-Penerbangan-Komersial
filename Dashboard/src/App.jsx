import React, { useState, useEffect } from 'react';
import { CloudLightning, Moon, Sun } from 'lucide-react';
import KPIHeader from './components/KPIHeader';
import LiveMap from './components/LiveMap';
import HighRiskTable from './components/HighRiskTable';
import AirlineLeaderboard from './components/AirlineLeaderboard';

function App() {
  const [data, setData] = useState({
    stats: null,
    impact: null,
    airlines: [],
    flights: [],
    alerts: [],
    total_flights: 0,
    total_alerts: 0
  });

  const [connected, setConnected] = useState(false);
  const [demoMode, setDemoMode] = useState(false);
  const demoModeRef = React.useRef(false);
  const [theme, setTheme] = useState('dark');

  useEffect(() => {
    document.body.className = theme === 'light' ? 'light-mode' : '';
  }, [theme]);

  useEffect(() => {
    let ws;
    
    const connectWebSocket = () => {
      ws = new WebSocket('ws://localhost:8000/ws/flights');

      ws.onopen = () => {
        console.log('WebSocket Connected');
        setConnected(true);
      };

      ws.onmessage = (event) => {
        try {
          const payload = JSON.parse(event.data);
          if (demoModeRef.current && payload.flights && payload.flights.length > 0) {
              const mockAlerts = payload.flights.slice(0, 8).map((f, i) => ({
                ...f,
                delay_category: 'CRITICAL DELAY',
                fdi: (75 + i * 3).toFixed(1),
                res: (80 + i * 2).toFixed(1),
                res_category: 'CRITICAL',
                predicted_delay_minutes: (120 + i * 15).toFixed(1),
                fdi_category: 'CRITICAL',
                estimated_compensation_eur: (300000 + i * 45000),
                affected_passengers: (180 + i * 45)
              }));
              
              payload.alerts = mockAlerts;
              
              if (!payload.impact) payload.impact = {};
              payload.impact.critical_delay_flights = mockAlerts.length;
              payload.impact.total_estimated_compensation_eur = 3500000;
              payload.impact.total_affected_passengers = 1450;
              payload.impact.avg_fdi = 45.5; 
              
              if (!payload.stats) payload.stats = {};
              payload.stats.critical_flights = mockAlerts.length;
              payload.stats.total_estimated_compensation_eur = 3500000;
              payload.stats.total_affected_passengers = 1450;
              payload.stats.avg_fdi = 45.5;
          }

          setData(payload);
        } catch (error) {
          console.error("Failed to parse websocket message", error);
        }
      };

      ws.onclose = () => {
        console.log('WebSocket Disconnected. Reconnecting in 3s...');
        setConnected(false);
        setTimeout(connectWebSocket, 3000);
      };
      
      ws.onerror = (error) => {
        console.error('WebSocket Error:', error);
      };
    };

    connectWebSocket();

    return () => {
      if (ws) ws.close();
    };
  }, []);

  const triggerBadWeatherDemo = async () => {
    demoModeRef.current = !demoModeRef.current;
    setDemoMode(demoModeRef.current);
    
    if (demoModeRef.current) {
      alert("Demo Cuaca Buruk DIAKTIFKAN!\nData delay dan kerugian maskapai dimanipulasi sementara.");
    } else {
      alert("Demo Cuaca Buruk DIMATIKAN.\nKembali ke data real-time normal.");
    }
  };

  return (
    <div className="dashboard-wrapper">
      <div className="map-background">
        <LiveMap theme={theme} flights={demoMode ? [...data.flights, ...(data.alerts || [])] : data.flights} />
      </div>

      <div className="overlay-ui">
        <div className="top-bar">
          <h1>
            ✈️ GLOBESYNC REAL-TIME OPS
            <span className={`status-indicator ${connected ? 'pulsing' : 'offline'}`} title={connected ? "Connected to live feed" : "Disconnected"}></span>
          </h1>
          <div style={{ display: 'flex', gap: '1rem' }}>
            <button 
              className="btn-demo" 
              style={{ background: 'rgba(59, 130, 246, 0.1)', color: 'var(--color-blue)', borderColor: 'rgba(59, 130, 246, 0.5)' }} 
              onClick={() => setTheme(t => t === 'dark' ? 'light' : 'dark')}
            >
              {theme === 'dark' ? <Sun size={16} /> : <Moon size={16} />}
              {theme === 'dark' ? 'LIGHT MODE' : 'DARK MODE'}
            </button>
            <button className="btn-demo" onClick={triggerBadWeatherDemo}>
              <CloudLightning size={16} />
              INJECT WEATHER ANOMALY
            </button>
          </div>
        </div>

        <div className="main-layout" style={{ flex: 1, overflow: 'hidden' }}>
          <div className="left-column">
            <KPIHeader impact={data.impact} stats={data.stats} />
          </div>

          <div className="right-column">
            <AirlineLeaderboard airlines={data.airlines} />
          </div>
        </div>

        {/* Bottom Full Width Table */}
        <div style={{ pointerEvents: 'auto', marginTop: '1rem', width: '100%' }}>
          <HighRiskTable alerts={data.alerts} />
        </div>
      </div>
    </div>
  );
}

export default App;
