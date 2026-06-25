import React, { useState } from 'react';
import { ChevronUp, ChevronDown } from 'lucide-react';
import { getAirlineName } from './AirlineLeaderboard';

const HighRiskTable = ({ alerts }) => {
  const [isExpanded, setIsExpanded] = useState(true);
  const [searchIcao, setSearchIcao] = useState('');
  // Sort alerts by FDI descending
  const sortedAlerts = [...(alerts || [])].sort((a, b) => {
    return parseFloat(b.fdi || 0) - parseFloat(a.fdi || 0);
  });

  const getFdiClass = (fdi) => {
    if (fdi >= 30) return 'text-red-500 font-bold';
    if (fdi >= 15) return 'text-amber-500';
    return '';
  };

  const getResStatus = (res_category) => {
    if (res_category === 'CRITICAL' || res_category === 'HIGH') {
      return <span style={{color: 'var(--color-red)'}}>Menyebabkan jadwal berantakan (Merembet)</span>;
    }
    if (res_category === 'MODERATE') {
      return <span style={{color: 'var(--color-amber)'}}>Beresiko kecil merembet</span>;
    }
    return <span style={{color: 'var(--text-secondary)'}}>Aman</span>;
  };

  return (
    <div className="glass-panel" style={{ padding: '1rem', transition: 'all 0.3s' }}>
      <div 
        className="panel-header" 
        style={{ cursor: 'pointer', display: 'flex', justifyContent: 'space-between', alignItems: 'center', margin: 0, paddingBottom: isExpanded ? '1rem' : '0' }}
        onClick={() => setIsExpanded(!isExpanded)}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
          <span className="status-indicator"></span>
          High-Risk Flights & Ripple Effect
        </div>
        
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }} onClick={(e) => e.stopPropagation()}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <input
              type="text"
              placeholder="Cari ICAO..."
              value={searchIcao}
              onChange={(e) => setSearchIcao(e.target.value.toUpperCase())}
              maxLength={3}
              style={{
                padding: '4px 8px',
                borderRadius: '4px',
                border: '1px solid rgba(255,255,255,0.2)',
                background: 'rgba(0,0,0,0.2)',
                color: 'var(--text-primary)',
                width: '100px',
                fontSize: '0.8rem'
              }}
            />
            {searchIcao.length > 0 && (
              <span style={{ fontSize: '0.8rem', color: 'var(--color-blue)', fontWeight: 600 }}>
                {getAirlineName(searchIcao)}
              </span>
            )}
          </div>
          <button className="btn-demo" style={{ padding: '4px 10px', fontSize: '0.75rem', background: 'transparent', borderColor: 'var(--panel-border)' }} onClick={() => setIsExpanded(!isExpanded)}>
            {isExpanded ? <><ChevronDown size={14}/> HIDE</> : <><ChevronUp size={14}/> SHOW</>}
          </button>
        </div>
      </div>
      
      {isExpanded && (
        <div className="table-container" style={{ maxHeight: '30vh', overflowY: 'auto' }}>
          <table className="data-table">
          <thead>
            <tr>
              <th>Callsign</th>
              <th>Airline</th>
              <th>Route</th>
              <th>Pred. Delay</th>
              <th>Est. Comp.</th>
              <th>Pelanggan Terdampak</th>
              <th>FDI</th>
              <th>Ripple Effect Score</th>
            </tr>
          </thead>
          <tbody>
            {sortedAlerts.length === 0 ? (
              <tr>
                <td colSpan="8" style={{ textAlign: 'center', color: 'var(--color-green)', padding: '2rem' }}>
                  Tidak ada penerbangan berisiko tinggi saat ini.
                </td>
              </tr>
            ) : (
              sortedAlerts.slice(0, 10).map((flight) => (
                <tr key={flight.flight_id}>
                  <td style={{ fontWeight: 600 }}>{flight.callsign}</td>
                  <td>{flight.airline}</td>
                  <td>{flight.origin} → {flight.destination}</td>
                  <td>
                    {parseFloat(flight.predicted_delay_minutes).toFixed(1)} m
                  </td>
                  <td>
                    <span style={{ fontWeight: 600, color: 'var(--color-blue)' }}>
                      €{Number(flight.estimated_compensation_eur || 0).toLocaleString()}
                    </span>
                  </td>
                  <td style={{ color: 'var(--color-red)', fontWeight: '600' }}>
                    {flight.affected_passengers ? Number(flight.affected_passengers).toLocaleString() : 0}
                  </td>
                  <td>
                    <span className={`badge ${parseFloat(flight.fdi) >= 30 ? 'red' : 'amber'}`}>
                      {parseFloat(flight.fdi).toFixed(1)}
                    </span>
                  </td>
                  <td>
                    <div style={{display:'flex', flexDirection:'column', gap:'4px'}}>
                      <strong style={{color: flight.res_category === 'CRITICAL' ? 'var(--color-red)' : 'inherit'}}>
                        {parseFloat(flight.res).toFixed(1)} ({flight.res_category})
                      </strong>
                      <span style={{fontSize: '0.8rem'}}>
                        {getResStatus(flight.res_category)}
                      </span>
                    </div>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
      )}
    </div>
  );
};

export default HighRiskTable;
