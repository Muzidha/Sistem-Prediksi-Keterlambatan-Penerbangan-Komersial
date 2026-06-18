import React, { useEffect } from 'react';
import { MapContainer, TileLayer, Marker, Popup, useMap } from 'react-leaflet';
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
    <div className="glass-panel" style={{ padding: '0.5rem', height: '100%' }}>
      <div className="map-container">
        <MapContainer center={defaultCenter} zoom={zoom} style={{ height: '100%', width: '100%' }}>
          <ResizeMap />
          <TileLayer
            attribution='&copy; Esri &mdash; Esri, DeLorme, NAVTEQ, TomTom, Intermap, iPC, USGS, FAO, NPS, NRCAN, GeoBase, Kadaster NL, Ordnance Survey, Esri Japan, METI, Esri China (Hong Kong), and the GIS User Community'
            url={theme === 'light' 
              ? "https://server.arcgisonline.com/ArcGIS/rest/services/World_Topo_Map/MapServer/tile/{z}/{y}/{x}"
              : "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"}
          />
          
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
      </div>
    </div>
  );
};

export default LiveMap;
