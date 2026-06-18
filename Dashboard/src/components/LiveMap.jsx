import React, { useEffect } from 'react';
import { MapContainer, TileLayer, Marker, Popup, useMap, ZoomControl } from 'react-leaflet';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';

const getPlaneIcon = (color, heading) => {
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="24" height="24" style="filter: drop-shadow(0 0 4px ${color}); transform: rotate(${heading}deg); transition: transform 1s;" fill="${color}">
    <path d="M21,16V14L13,9V3.5A1.5,1.5 0 0,0 11.5,2A1.5,1.5 0 0,0 10,3.5V9L2,14V16L10,13.5V19L8,20.5V22L11.5,21L15,22V20.5L13,19V13.5L21,16Z" />
  </svg>`;
  return L.divIcon({
    className: 'plane-icon',
    html: svg,
    iconSize: [24, 24],
    iconAnchor: [12, 12],
    popupAnchor: [0, -12]
  });
};

const getAirportIcon = () => {
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 34" width="24" height="34">
    <ellipse cx="12" cy="32" rx="6" ry="2" fill="rgba(0,0,0,0.3)" />
    <path d="M12 2C6.48 2 2 6.48 2 12c0 7.5 10 20 10 20s10-12.5 10-20c0-5.52-4.48-10-10-10z" fill="#2563eb" stroke="white" stroke-width="1.5"/>
    <path d="M9 10h6v2H9v-2zm1 2h4v5h-4v-5zm1-3h2v1h-2V9zm-3 8h8v1H8v-1z" fill="white"/>
  </svg>`;
  return L.divIcon({
    className: 'airport-icon',
    html: svg,
    iconSize: [24, 34],
    iconAnchor: [12, 34],
    popupAnchor: [0, -30]
  });
};

const AIRPORTS = [
  { id: 'PKY', icao: 'WAGG', name: 'Palangkaraya Tjilik Riwut Airport', country: '🇮🇩', lat: -2.2246, lon: 113.9436 },
  { id: 'CGK', icao: 'WIII', name: 'Soekarno-Hatta Int. Airport', country: '🇮🇩', lat: -6.1255, lon: 106.6558 },
  { id: 'DPS', icao: 'WADD', name: 'Ngurah Rai Int. Airport', country: '🇮🇩', lat: -8.7481, lon: 115.1672 },
  { id: 'SUB', icao: 'WARR', name: 'Juanda Int. Airport', country: '🇮🇩', lat: -7.3798, lon: 112.7836 },
  { id: 'KNO', icao: 'WIMM', name: 'Kualanamu Int. Airport', country: '🇮🇩', lat: 3.6300, lon: 98.8797 },
  { id: 'UPG', icao: 'WAAA', name: 'Sultan Hasanuddin Int. Airport', country: '🇮🇩', lat: -5.0616, lon: 119.5540 },
  { id: 'BPN', icao: 'WALL', name: 'Sepinggan Int. Airport', country: '🇮🇩', lat: -1.2682, lon: 116.8943 },
  { id: 'YIA', icao: 'WAHI', name: 'Yogyakarta Int. Airport', country: '🇮🇩', lat: -7.9009, lon: 110.0594 },
  { id: 'PLM', icao: 'WIPP', name: 'Sultan Mahmud Badaruddin II', country: '🇮🇩', lat: -2.8981, lon: 104.7001 },
  { id: 'SRG', icao: 'WAHS', name: 'Jenderal Ahmad Yani', country: '🇮🇩', lat: -6.9723, lon: 110.3756 },
  { id: 'PDG', icao: 'WIEE', name: 'Minangkabau Int. Airport', country: '🇮🇩', lat: -0.7863, lon: 100.2809 },
  { id: 'BDO', icao: 'WICC', name: 'Husein Sastranegara', country: '🇮🇩', lat: -6.9009, lon: 107.5756 },
  { id: 'BDJ', icao: 'WAOO', name: 'Syamsudin Noor Airport', country: '🇮🇩', lat: -3.4406, lon: 114.7621 },
  { id: 'PNK', icao: 'WIOO', name: 'Supadio Int. Airport', country: '🇮🇩', lat: -0.1491, lon: 109.4042 },
  { id: 'LOP', icao: 'WADL', name: 'Lombok Int. Airport', country: '🇮🇩', lat: -8.7578, lon: 116.2750 },
  { id: 'MDC', icao: 'WAMM', name: 'Sam Ratulangi Int. Airport', country: '🇮🇩', lat: 1.5492, lon: 124.9261 },
  { id: 'SIN', icao: 'WSSS', name: 'Singapore Changi Airport', country: '🇸🇬', lat: 1.3644, lon: 103.9915 },
  { id: 'KUL', icao: 'WMKK', name: 'Kuala Lumpur Int. Airport', country: '🇲🇾', lat: 2.7456, lon: 101.7099 },
  { id: 'LHR', icao: 'EGLL', name: 'London Heathrow Airport', country: '🇬🇧', lat: 51.4700, lon: -0.4543 },
];

const AircraftImage = ({ registration }) => {
  const [imgSrc, setImgSrc] = React.useState(null);
  
  React.useEffect(() => {
    if (!registration) return;
    fetch(`https://api.planespotters.net/pub/photos/reg/${registration}`)
      .then(res => res.json())
      .then(data => {
        if (data && data.photos && data.photos.length > 0) {
          setImgSrc(data.photos[0].thumbnail_large.src);
        }
      })
      .catch(() => {});
  }, [registration]);

  // Generic commercial plane fallback
  const defaultImg = "https://images.unsplash.com/photo-1436491865332-7a61a109cc05?auto=format&fit=crop&w=400&h=200&q=80";

  return (
    <div className="fr24-image">
      <img 
        src={imgSrc || defaultImg} 
        alt={`Aircraft ${registration}`} 
        onError={(e) => { e.target.src = defaultImg; }}
      />
    </div>
  );
};

// Fix Leaflet container size issues
const ResizeMap = () => {
  const map = useMap();
  useEffect(() => {
    setTimeout(() => {
      map.invalidateSize();
    }, 500);
  }, [map]);
  return null;
};

const LiveMap = ({ flights, theme }) => {
  // Default center around Indonesia/Malaysia
  const defaultCenter = [0.789, 113.921];
  const zoom = 5;
  const prevPosRef = React.useRef({});

  const getColor = (category) => {
    if (category === 'CRITICAL DELAY') return '#ef4444'; // Red
    if (category === 'MEDIUM DELAY') return '#f59e0b'; // Amber
    return '#fbbf24'; // Yellow (matching FR24 default)
  };

  return (
    <MapContainer center={defaultCenter} zoom={zoom} style={{ height: '100vh', width: '100vw' }} zoomControl={false}>
      <ZoomControl position="bottomright" />
      <ResizeMap />
          <TileLayer
            attribution='&copy; Esri &mdash; Esri, DeLorme, NAVTEQ, TomTom, Intermap, iPC, USGS, FAO, NPS, NRCAN, GeoBase, Kadaster NL, Ordnance Survey, Esri Japan, METI, Esri China (Hong Kong), and the GIS User Community'
            url={theme === 'light' 
              ? "https://server.arcgisonline.com/ArcGIS/rest/services/World_Topo_Map/MapServer/tile/{z}/{y}/{x}"
              : "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"}
          />
          
          {/* Airport Markers */}
          {AIRPORTS.map((airport) => (
            <Marker
              key={airport.id}
              position={[airport.lat, airport.lon]}
              icon={getAirportIcon()}
            >
              <Popup className="airport-popup">
                <div style={{ textAlign: 'left' }}>
                  <h4 style={{ margin: '0 0 4px 0', fontSize: '0.95rem', fontWeight: '500' }}>
                    {airport.name}
                  </h4>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '6px', fontSize: '0.95rem' }}>
                    <span>{airport.country}</span>
                    <strong style={{ fontWeight: 600 }}>{airport.id} / {airport.icao}</strong>
                  </div>
                </div>
              </Popup>
            </Marker>
          ))}
          
          {flights && flights.map((flight) => {
            if (!flight.lat || !flight.lon) return null;
            
            const color = getColor(flight.delay_category);
            const currentPos = [parseFloat(flight.lat), parseFloat(flight.lon)];
            let heading = 0;
            
            if (prevPosRef.current[flight.flight_id]) {
              const prev = prevPosRef.current[flight.flight_id];
              const dy = currentPos[0] - prev.pos[0];
              const dx = currentPos[1] - prev.pos[1];
              
              if (Math.abs(dx) > 0.0001 || Math.abs(dy) > 0.0001) {
                heading = Math.atan2(dx, dy) * (180 / Math.PI);
              } else {
                heading = prev.heading;
              }
            } else {
              // Initial pseudo-random heading based on ID so they don't all face North initially
              const hash = flight.flight_id.split('').reduce((a, b) => { a = ((a << 5) - a) + b.charCodeAt(0); return a & a }, 0);
              heading = Math.abs(hash) % 360;
            }
            
            // Save current position for next render
            prevPosRef.current[flight.flight_id] = { pos: currentPos, heading };
            
            return (
              <Marker
                key={flight.flight_id}
                position={currentPos}
                icon={getPlaneIcon(color, heading)}
              >
                <Popup className="fr24-popup">
                  <div className="fr24-card">
                    <div className="fr24-header">
                      <div className="fr24-callsign-row">
                        <span className="fr24-callsign">{flight.callsign}</span>
                        <span className="fr24-aircraft">{flight.aircraft_model || 'B738'}</span>
                      </div>
                      <div className="fr24-airline">{flight.airline}</div>
                    </div>
                    
                    <AircraftImage registration={flight.registration} />
                    
                    <div className="fr24-route">
                      <div className="fr24-airport">
                        <h2>{flight.origin || 'N/A'}</h2>
                      </div>
                      <div className="fr24-plane-icon">✈</div>
                      <div className="fr24-airport">
                        <h2>{flight.destination || 'N/A'}</h2>
                      </div>
                    </div>
                    
                    <div className="fr24-details">
                      <div>
                        <strong>Status</strong>
                        <span style={{ color: flight.delay_category === 'CRITICAL DELAY' ? '#dc2626' : flight.delay_category === 'MEDIUM DELAY' ? '#d97706' : '#059669', fontWeight: 600 }}>
                          {flight.delay_category}
                        </span>
                      </div>
                      <div>
                        <strong>Delay</strong>
                        <span>{parseFloat(flight.predicted_delay_minutes || 0).toFixed(1)} mins</span>
                      </div>
                      <div>
                        <strong>FDI Score</strong>
                        <span>{parseFloat(flight.fdi || 0).toFixed(1)}</span>
                      </div>
                      <div>
                        <strong>Altitude / Speed</strong>
                        <span>{flight.altitude_ft} ft / {flight.speed_kn} kts</span>
                      </div>
                    </div>
                  </div>
                </Popup>
              </Marker>
            );
          })}
        </MapContainer>
  );
};

export default LiveMap;
