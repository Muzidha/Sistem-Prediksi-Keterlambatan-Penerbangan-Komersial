import React from 'react';
import { AlertTriangle, DollarSign, Users, Activity } from 'lucide-react';

const KPIHeader = ({ impact, stats }) => {
  // Use mock values if data not yet available
  const criticalFlights = impact?.critical_delay_flights || stats?.critical_flights || 0;
  const compensation = impact?.total_estimated_compensation_eur || stats?.total_estimated_compensation_eur || 0;
  const passengers = impact?.total_affected_passengers || stats?.total_affected_passengers || 0;
  const avgFdi = impact?.avg_fdi || stats?.avg_fdi || 0;

  const formatEuro = (amount) => {
    if (amount >= 1000000) return `€${(amount / 1000000).toFixed(2)}M`;
    if (amount >= 1000) return `€${(amount / 1000).toFixed(1)}k`;
    return `€${amount}`;
  };

  const formatNumber = (num) => new Intl.NumberFormat('en-US').format(num);

  return (
    <div className="kpi-grid">
      <div className="glass-panel kpi-card">
        <div className="kpi-icon critical">
          <AlertTriangle size={28} />
        </div>
        <div className="kpi-content">
          <h3>CRITICAL DELAYS</h3>
          <div className="kpi-value" style={{ color: criticalFlights > 0 ? 'var(--color-red)' : 'var(--text-primary)' }}>
            {criticalFlights}
          </div>
        </div>
      </div>

      <div className="glass-panel kpi-card">
        <div className="kpi-icon money">
          <DollarSign size={28} />
        </div>
        <div className="kpi-content">
          <h3>EST. COMPENSATION</h3>
          <div className="kpi-value" style={{ color: compensation > 0 ? 'var(--color-red)' : 'var(--text-primary)' }}>
            {formatEuro(compensation)}
          </div>
        </div>
      </div>

      <div className="glass-panel kpi-card">
        <div className="kpi-icon people">
          <Users size={28} />
        </div>
        <div className="kpi-content">
          <h3>AFFECTED PASSENGERS</h3>
          <div className="kpi-value">
            {formatNumber(passengers)}
          </div>
        </div>
      </div>

      <div className="glass-panel kpi-card">
        <div className="kpi-icon chart">
          <Activity size={28} />
        </div>
        <div className="kpi-content">
          <h3>AVG. DELAY INDEX (FDI)</h3>
          <div className="kpi-value" style={{ color: avgFdi > 20 ? 'var(--color-amber)' : 'var(--color-green)' }}>
            {Number(avgFdi).toFixed(1)}
          </div>
        </div>
      </div>
    </div>
  );
};

export default KPIHeader;
