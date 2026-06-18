import React from 'react';

const HighRiskTable = ({ alerts }) => {
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
    <div className="glass-panel">
      <div className="panel-header">
        <span className="status-indicator"></span>
        High-Risk Flights & Ripple Effect
      </div>
      
      <div className="table-container">
        <table className="data-table">
          <thead>
            <tr>
              <th>Callsign</th>
              <th>Airline</th>
              <th>Route</th>
              <th>Pred. Delay</th>
              <th>FDI</th>
              <th>Ripple Effect Score</th>
            </tr>
          </thead>
          <tbody>
            {sortedAlerts.length === 0 ? (
              <tr>
                <td colSpan="6" style={{ textAlign: 'center', color: 'var(--color-green)', padding: '2rem' }}>
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
    </div>
  );
};

export default HighRiskTable;
