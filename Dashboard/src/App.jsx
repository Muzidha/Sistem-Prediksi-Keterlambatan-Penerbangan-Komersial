import React, { useState, useEffect } from 'react';
import { CloudLightning } from 'lucide-react';
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
          setData(prev => {
            // If demo mode is active, we might inject mock weather data
            // but for now, we just pass the real data.
            return {
              ...prev,
              ...payload
            };
          });
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
    setDemoMode(true);
    // For demo purposes, we mutate the state locally to show immediate effect
    // Ideally, this hits an endpoint: await fetch('http://localhost:8000/api/demo/inject-weather', { method: 'POST' })
    alert("Injeksi cuaca buruk sedang disimulasikan! (Bisa disambungkan ke endpoint backend nantinya)");
    
    // Simulate frontend reaction
    setData(prev => {
      const mockAlerts = [...prev.flights].slice(0, 5).map(f => ({
        ...f,
        delay_category: 'CRITICAL DELAY',
        fdi: (Math.random() * 50 + 50).toFixed(1),
        res: (Math.random() * 50 + 50).toFixed(1),
        res_category: 'CRITICAL',
        predicted_delay_minutes: (Math.random() * 120 + 60).toFixed(1)
      }));
      
      return {
        ...prev,
        alerts: mockAlerts,
        impact: {
          ...prev.impact,
          critical_delay_flights: (prev.impact?.critical_delay_flights || 0) + 5,
          total_estimated_compensation_eur: (prev.impact?.total_estimated_compensation_eur || 0) + 2500000
        }
      };
    });
  };

  return (
    <div className="dashboard-container">
      <div className="top-bar glass-panel">
        <h1>
          ✈️ Real-time Flight Delay Analytics
          <span className="status-indicator" style={{ background: connected ? 'var(--color-green)' : 'var(--color-red)' }}></span>
        </h1>
        <button className="btn-demo" onClick={triggerBadWeatherDemo}>
          <CloudLightning size={20} />
          Inject Bad Weather (Demo)
        </button>
      </div>

      <KPIHeader impact={data.impact} stats={data.stats} />

      <div className="main-grid">
        <div style={{ display: 'flex', flexDirection: 'column', gap: '1.5rem' }}>
          <LiveMap flights={demoMode && data.alerts.length > 0 ? [...data.flights, ...data.alerts] : data.flights} />
          <HighRiskTable alerts={data.alerts} />
        </div>
        
        <div>
          <AirlineLeaderboard airlines={data.airlines} />
        </div>
      </div>
    </div>
  );
}

export default App;
