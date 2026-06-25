import React from 'react';

export const airlineNames = {
  // ── Maskapai Indonesia (yang sudah ada) ──────────────────────────
  'GIA': 'Garuda Indonesia',
  'LNI': 'Lion Air',
  'CTV': 'Citilink',
  'BTK': 'Batik Air',
  'SJW': 'Super Air Jet',       // note: ICAO resminya SJV (lihat catatan)
  'SJV': 'Super Air Jet',
  'AWQ': 'Indonesia AirAsia',
  'SJY': 'Sriwijaya Air',
  'NAM': 'NAM Air',             // note: ICAO resminya LKN (lihat catatan)
  'TGA': 'Trigana Air',         // note: ICAO resminya TGN (lihat catatan)
  'TNU': 'TransNusa',
  'MWG': 'AirBorneo',
  'OEY': 'Rimbun Air',

  // ── Maskapai Indonesia (BARU ditambahkan) ────────────────────────
  'WON': 'Wings Air',
  'SQS': 'Susi Air',
  'PAS': 'Pelita Air',
  'LKN': 'NAM Air',             // ICAO resmi NAM Air
  'TGN': 'Trigana Air',         // ICAO resmi Trigana Air
  'FHS': 'FlyJaya',
  'JLB': 'Jhonlin Air Transport',
  'MYU': 'My Indo Airlines',    // kargo
  'CAD': 'Cardig Air',          // kargo
  'TMG': 'Tri-MG Intra Asia Airlines', // kargo
  'AFE': 'Airfast Indonesia',
  'IDA': 'Indonesia Air Transport',
  'TVV': 'Travira Air',
  'ESD': 'Eastindo Air',
  'XAR': 'Xpress Air',          // sudah tidak aktif

  // ── Maskapai Internasional (yang sudah ada) ──────────────────────
  'AXM': 'AirAsia',             // Malaysia
  'SIA': 'Singapore Airlines',
  'MAS': 'Malaysia Airlines',
  'THA': 'Thai Airways',
  'JST': 'Jetstar',
  'QFA': 'Qantas',
  'UAE': 'Emirates',
  'QTR': 'Qatar Airways',
  'SVA': 'Saudia',
  'ANA': 'All Nippon Airways',
  'JAL': 'Japan Airlines',
  'CPA': 'Cathay Pacific',
  'CAL': 'China Airlines',
  'EVA': 'EVA Air',
  'ETH': 'Ethiopian Airlines',
  'DKH': 'Juneyao Airlines',
  'AIH': 'Air Incheon',
  'FFM': 'Firefly',             // Malaysia
  'FIN': 'Finnair',
  'BAW': 'British Airways',
  'AFR': 'Air France',
  'DLH': 'Lufthansa',
  'KLM': 'KLM Royal Dutch Airlines',
  'DAL': 'Delta Air Lines',
  'AAL': 'American Airlines',
  'UAL': 'United Airlines',
  'THY': 'Turkish Airlines',
  'VJC': 'VietJet Air',

  // ── Maskapai Internasional (BARU ditambahkan) ────────────────────
  'SLK': 'SilkAir',             // Singapore, sudah merger ke SIA
  'TGW': 'Scoot',               // Singapore, LCC
  'AXB': 'AirAsia X',          // Malaysia, long-haul LCC
  'AWX': 'Indonesia AirAsia X', // Indonesia, long-haul LCC
  'MXD': 'Malindo Air',         // Malaysia (sekarang Batik Air Malaysia)
  'OAL': 'Olympic Air',
  'KAL': 'Korean Air',
  'AAR': 'Asiana Airlines',
  'CES': 'China Eastern Airlines',
  'CSN': 'China Southern Airlines',
  'CCA': 'Air China',
  'HVN': 'Vietnam Airlines',
  'BKP': 'Bangkok Airways',
  'TFW': 'IndiGo',
  'PAL': 'Philippine Airlines',
  'RPA': 'Royal Brunei Airlines',
  'GFA': 'Gulf Air',
  'ETD': 'Etihad Airways',
  'FDB': 'flydubai',
  'ELY': 'El Al Israel Airlines',
  'MHD': 'Mahan Air',
  'AMQ': 'Aeroméxico',
  'BOE': 'WestJet',
  'SWR': 'Swiss International Air Lines',
  'AUA': 'Austrian Airlines',
  'BEL': 'Brussels Airlines',
  'IBE': 'Iberia',
  'TAP': 'TAP Air Portugal',
  'AZA': 'ITA Airways',
  'VLG': 'Vueling',
  'NLY': 'Edelweiss Air',
  'CXI': 'Cebu Pacific',
  'TBA': 'Tibet Airlines',
  'CSZ': 'Shenzhen Airlines',
  'CDG': 'Shandong Airlines',
  'CHH': 'Hainan Airlines',
  'XAM': 'Xiamen Airlines',
  'CFI': 'Sichuan Airlines',
  'UIA': 'Ukraine International Airlines',
  'AFL': 'Aeroflot',
  'RMF' : 'Royal Malaysian Air Force',
  'VOZ' : 'Virgin Australia',
  'BVT' : 'Berjaya Air',
  'OMA' : 'Oman Air',
  'KXP' : 'Kargo Express',
  'BBL' : 'BBN Airlines Indonesia'
};

export const getAirlineName = (icao) => airlineNames[icao] || 'Unknown Airline';

const AirlineLeaderboard = ({ airlines }) => {
  // Sort airlines by api_score descending
  const sortedAirlines = [...(airlines || [])].sort((a, b) => {
    return parseFloat(b.api_score || 0) - parseFloat(a.api_score || 0);
  });

  const getBadgeClass = (category) => {
    switch (category) {
      case 'EXCELLENT': return 'badge green';
      case 'GOOD': return 'badge green';
      case 'FAIR': return 'badge amber';
      case 'POOR': return 'badge red';
      default: return 'badge';
    }
  };

  return (
    <div className="glass-panel">
      <div className="panel-header">
        Airline Performance Index
      </div>

      <div className="table-container">
        <table className="data-table">
          <thead>
            <tr>
              <th>Rank</th>
              <th>Airline</th>
              <th>Total Flights</th>
              <th>On-Time %</th>
              <th>API Score</th>
              <th>Rating</th>
            </tr>
          </thead>
          <tbody>
            {sortedAirlines.length === 0 ? (
              <tr>
                <td colSpan="6" style={{ textAlign: 'center', padding: '2rem' }}>
                  Belum ada data maskapai.
                </td>
              </tr>
            ) : (
              sortedAirlines.slice(0, 10).map((airline, index) => (
                <tr key={airline.airline_icao}>
                  <td style={{ fontWeight: 'bold', color: index < 3 ? 'var(--color-amber)' : 'inherit' }}>
                    #{index + 1}
                  </td>
                  <td>
                    <div style={{ display: 'flex', flexDirection: 'column' }}>
                      <span style={{ fontWeight: 600, color: 'var(--color-blue)', fontSize: '0.9rem' }}>
                        {getAirlineName(airline.airline_icao)}
                      </span>
                      <span style={{ fontSize: '0.75rem', color: 'var(--text-secondary)' }}>
                        ICAO: {airline.airline_icao}
                      </span>
                    </div>
                  </td>
                  <td>{airline.total_flights}</td>
                  <td>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>{parseFloat(airline.on_time_rate).toFixed(1)}%</span>
                      <div style={{ flex: 1, height: '6px', background: 'rgba(255,255,255,0.1)', borderRadius: '3px' }}>
                        <div style={{
                          height: '100%',
                          borderRadius: '3px',
                          width: `${airline.on_time_rate}%`,
                          background: airline.on_time_rate > 80 ? 'var(--color-green)' : airline.on_time_rate > 60 ? 'var(--color-amber)' : 'var(--color-red)'
                        }}></div>
                      </div>
                    </div>
                  </td>
                  <td style={{ fontWeight: 'bold', fontSize: '1.1rem' }}>
                    {parseFloat(airline.api_score).toFixed(1)}
                  </td>
                  <td>
                    <span className={getBadgeClass(airline.api_category)}>
                      {airline.api_category}
                    </span>
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

export default AirlineLeaderboard;
