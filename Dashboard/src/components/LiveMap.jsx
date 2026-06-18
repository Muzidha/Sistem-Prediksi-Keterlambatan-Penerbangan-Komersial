import React, { useEffect } from 'react';
import { MapContainer, TileLayer, CircleMarker, Popup, useMap } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';

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

const LiveMap = ({ flights }) => {
  // Default center around Indonesia/Malaysia
  const defaultCenter = [0.789, 113.921];
  const zoom = 5;

  const getColor = (category) => {
    if (category === 'CRITICAL DELAY') return '#ef4444'; // Red
    if (category === 'MEDIUM DELAY') return '#f59e0b'; // Amber
    return '#10b981'; // Green
  };

  return (
    <div className="glass-panel" style={{ padding: '0.5rem', height: '100%' }}>
      <div className="map-container">
        <MapContainer center={defaultCenter} zoom={zoom} style={{ height: '100%', width: '100%' }}>
          <ResizeMap />
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
            url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
          />
          
          {flights && flights.map((flight) => {
            if (!flight.lat || !flight.lon) return null;
            
            const color = getColor(flight.delay_category);
            
            return (
              <CircleMarker
                key={flight.flight_id}
                center={[parseFloat(flight.lat), parseFloat(flight.lon)]}
                radius={6}
                pathOptions={{ 
                  fillColor: color, 
                  color: color, 
                  weight: 1, 
                  fillOpacity: 0.8 
                }}
              >
                <Popup className="custom-popup">
                  <div>
                    <h3 style={{ margin: '0 0 8px 0', borderBottom: '1px solid #334155', paddingBottom: '4px' }}>
                      {flight.callsign} ({flight.airline})
                    </h3>
                    <p><strong>Route:</strong> {flight.origin} → {flight.destination}</p>
                    <p>
                      <strong>Status:</strong>{' '}
                      <span className={`status-dot ${flight.delay_category === 'CRITICAL DELAY' ? 'red' : flight.delay_category === 'MEDIUM DELAY' ? 'amber' : 'green'}`}></span>
                      {flight.delay_category}
                    </p>
                    {flight.predicted_delay_minutes && parseFloat(flight.predicted_delay_minutes) > 0 && (
                      <p><strong>Delay:</strong> {parseFloat(flight.predicted_delay_minutes).toFixed(1)} mins</p>
                    )}
                    <p><strong>FDI:</strong> {flight.fdi}</p>
                    <p><strong>Altitude:</strong> {flight.altitude_ft} ft</p>
                    <p><strong>Speed:</strong> {flight.speed_kn} kts</p>
                  </div>
                </Popup>
              </CircleMarker>
            );
          })}
        </MapContainer>
      </div>
    </div>
  );
};

export default LiveMap;
