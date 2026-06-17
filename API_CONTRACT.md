# API Contract — Flight Delay Prediction Backend

Dokumen ini menjelaskan kontrak API untuk dashboard frontend (Anggota 5). FastAPI juga menyediakan dokumentasi interaktif otomatis di:

```
http://localhost:8000/docs
http://localhost:8000/redoc
```

---

## Base URL

```
http://localhost:8000
```

## General

- **Protocol**: HTTP/1.1 (dapat diupgrade ke HTTPS di production)
- **Content-Type**: `application/json`
- **CORS**: Enabled (`*`) untuk development
- **Authentication**: None (public API)

## Standard Response Format

Semua response JSON mengikuti format dasar sesuai endpoint.

### Error Response

```json
{
  "error": "Flight not found",
  "flight_id": "unknown-id"
}
```

---

## Endpoints

### 1. Root / Health Check

```
GET /
```

**Description**: Cek status service.

**Response 200**:
```json
{
  "service": "Flight Delay Prediction Backend",
  "status": "running",
  "timestamp": "2026-06-17T12:56:28Z"
}
```

---

### 2. List All Flights

```
GET /api/flights
```

**Description**: Mengambil semua penerbangan aktif dari Redis. Mendukung filter opsional.

**Query Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `airline` | string | No | Filter by airline ICAO code, e.g. `AXM` |
| `delay_category` | string | No | Filter by delay category: `ON TIME`, `MEDIUM DELAY`, `CRITICAL DELAY` |
| `fdi_category` | string | No | Filter by FDI category: `LOW`, `MODERATE`, `HIGH`, `CRITICAL` |
| `res_category` | string | No | Filter by RES category: `LOW`, `MODERATE`, `HIGH`, `CRITICAL` |
| `limit` | integer | No | Maximum number of results (1-1000) |

**Example Requests**:

```bash
# Filter by airline
curl "http://localhost:8000/api/flights?airline=AXM"

# Filter critical delays only
curl "http://localhost:8000/api/flights?delay_category=CRITICAL DELAY"

# Filter high risk flights with pagination
curl "http://localhost:8000/api/flights?fdi_category=HIGH&limit=20"

# Combined filters
curl "http://localhost:8000/api/flights?airline=AXM&delay_category=MEDIUM DELAY&limit=10"
```

**Response 200**:
```json
{
  "count": 230,
  "filters": {
    "airline": null,
    "delay_category": null,
    "fdi_category": null,
    "res_category": null,
    "limit": null
  },
  "flights": [
    {
      "flight_id": "403acf39",
      "callsign": "AXM118",
      "airline": "AXM",
      "aircraft": "A21N",
      "registration": "9M-VAA",
      "origin": "KUL",
      "destination": "CAN",
      "lat": "3.78",
      "lon": "103.96",
      "altitude_ft": "31000",
      "speed_kn": "436",
      "precip_mm": "0",
      "wind_knots": "9.9",
      "visibility_m": "14040.0",
      "weather_score": "0.057",
      "traffic_density": "7",
      "distance_km": "2826.2",
      "route_deviation": "0.99",
      "predicted_delay_minutes": "13.6",
      "delay_category": "ON TIME",
      "capacity": "220",
      "affected_passengers": "187",
      "fdi": "15.2",
      "fdi_category": "LOW",
      "estimated_compensation_eur": "0",
      "res": "11.0",
      "res_category": "LOW",
      "risk_score": "0.00",
      "hour_utc": "12",
      "flight_phase": "1",
      "updated_at": "2026-06-17T12:56:35.979925Z"
    }
  ]
}
```

**Field Types**:

| Field | Type | Description |
|-------|------|-------------|
| `count` | integer | Jumlah penerbangan |
| `flights` | array[object] | Daftar penerbangan |
| `flight_id` | string | ID unik penerbangan |
| `callsign` | string | Callsign penerbangan |
| `airline` | string | Kode ICAO maskapai |
| `aircraft` | string | Tipe pesawat |
| `registration` | string | Registrasi pesawat |
| `origin` | string | Bandara asal (ICAO/IATA) |
| `destination` | string | Bandara tujuan (ICAO/IATA) |
| `lat` | string | Latitude |
| `lon` | string | Longitude |
| `altitude_ft` | string | Ketinggian kaki |
| `speed_kn` | string | Kecepatan knot |
| `precip_mm` | string | Curah hujan (mm) |
| `wind_knots` | string | Kecepatan angin (knot) |
| `visibility_m` | string | Jarak pandang (meter) |
| `weather_score` | string | Skor cuaca 0-1 |
| `traffic_density` | string | Kepadatan traffic 1-10 |
| `distance_km` | string | Estimasi jarak (km) |
| `route_deviation` | string | Deviasi rute |
| `predicted_delay_minutes` | string | Prediksi delay menit |
| `delay_category` | string | `ON TIME` / `MEDIUM DELAY` / `CRITICAL DELAY` |
| `capacity` | string | Kapasitas kursi |
| `affected_passengers` | string | Estimasi penumpang terdampak |
| `fdi` | string | Flight Delay Index 0-100 |
| `fdi_category` | string | `LOW` / `MODERATE` / `HIGH` / `CRITICAL` |
| `estimated_compensation_eur` | string | Estimasi kompensasi EUR |
| `res` | string | Ripple Effect Score 0-100 |
| `res_category` | string | `LOW` / `MODERATE` / `HIGH` / `CRITICAL` |
| `risk_score` | string | Skor risiko manual fallback |
| `hour_utc` | string | Jam UTC |
| `flight_phase` | string | Fase penerbangan |
| `updated_at` | string | ISO 8601 timestamp |

---

### 3. Flight Detail

```
GET /api/flights/{flight_id}
```

**Description**: Mengambil detail satu penerbangan.

**Path Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `flight_id` | string | Yes | ID penerbangan |

**Response 200**: Object flight (sama seperti item di `/api/flights`).

**Response 404**:
```json
{
  "error": "Flight not found",
  "flight_id": "unknown-id"
}
```

---

### 4. Flight Impact Metrics

```
GET /api/flights/{flight_id}/impact
```

**Description**: Mengambil metrik dampak lengkap untuk satu penerbangan.

**Response 200**:
```json
{
  "flight_id": "403acf39",
  "callsign": "AXM118",
  "airline": "AXM",
  "aircraft": "A21N",
  "registration": "9M-VAA",
  "origin": "KUL",
  "destination": "CAN",
  "predicted_delay_minutes": "13.6",
  "delay_category": "ON TIME",
  "capacity": "220",
  "affected_passengers": "187",
  "fdi": "15.2",
  "fdi_category": "LOW",
  "estimated_compensation_eur": "0",
  "res": 11.0,
  "res_category": "LOW",
  "next_flight_id": null,
  "next_flight_gap_min": null,
  "downstream_flight_count": 0
}
```

---

### 5. Ripple Effect Score Detail

```
GET /api/flights/{flight_id}/ripple
```

**Description**: Mengambil detail Ripple Effect Score satu penerbangan.

**Response 200**:
```json
{
  "flight_id": "403acf39",
  "registration": "9M-VAA",
  "ripple_effect": {
    "res": 11.0,
    "res_category": "LOW",
    "next_flight_id": "5039a2b1",
    "next_flight_gap_min": 95,
    "downstream_flight_count": 3
  }
}
```

**Field Types**:

| Field | Type | Description |
|-------|------|-------------|
| `res` | float | Ripple Effect Score 0-100 |
| `res_category` | string | Kategori RES |
| `next_flight_id` | string \| null | Flight ID penerbangan berikutnya |
| `next_flight_gap_min` | integer \| null | Selisih waktu ke penerbangan berikutnya (menit) |
| `downstream_flight_count` | integer | Jumlah penerbangan lanjutan dalam 24 jam |

---

### 6. Aggregate Statistics

```
GET /api/stats
```

**Description**: Mengambil ringkasan statistik dari Redis (di-update oleh Spark setiap batch).

**Response 200**:
```json
{
  "batch_id": "8",
  "total_flights": "73",
  "delayed_flights": "44",
  "critical_flights": "0",
  "high_fdi_flights": "0",
  "avg_delay_minutes": "17.4",
  "avg_fdi": "17.4",
  "total_affected_passengers": "13230",
  "total_estimated_compensation_eur": "0",
  "updated_at": "2026-06-17T12:58:16.548280Z"
}
```

---

### 7. Alerts

```
GET /api/alerts
```

**Description**: Mengambil daftar penerbangan dengan delay CRITICAL.

**Query Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `limit` | integer | No | Maximum number of alerts (1-1000) |

**Example Requests**:

```bash
# Get latest 10 alerts
curl "http://localhost:8000/api/alerts?limit=10"
```

**Response 200**:
```json
{
  "count": 0,
  "limit": null,
  "alerts": []
}
```

Alert item memiliki struktur sama dengan object flight.

---

### 8. Aggregate Impact

```
GET /api/impact
```

**Description**: Mengambil agregat impact seluruh penerbangan aktif.

**Response 200**:
```json
{
  "count": 230,
  "impact": {
    "total_flights": 230,
    "high_fdi_flights": 0,
    "critical_delay_flights": 0,
    "high_res_flights": 0,
    "total_affected_passengers": 42732,
    "total_estimated_compensation_eur": 0,
    "avg_fdi": 17.5,
    "avg_res": 18.9
  }
}
```

---

### 9. Airline Performance Ranking

```
GET /api/airlines
```

**Description**: Mengambil ranking performa semua maskapai, diurutkan dari API score tertinggi.

**Query Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `min_api_score` | float | No | Minimum API score filter (0-100) |
| `category` | string | No | Filter by category: `EXCELLENT`, `GOOD`, `FAIR`, `POOR` |
| `limit` | integer | No | Maximum number of results (1-500) |

**Example Requests**:

```bash
# Top 5 airlines
curl "http://localhost:8000/api/airlines?limit=5"

# Only excellent airlines
curl "http://localhost:8000/api/airlines?category=EXCELLENT"

# Airlines with API score >= 70
curl "http://localhost:8000/api/airlines?min_api_score=70"

# Combined filters
curl "http://localhost:8000/api/airlines?category=GOOD&limit=10"
```

**Response 200**:
```json
{
  "count": 51,
  "filters": {
    "min_api_score": null,
    "category": null,
    "limit": null
  },
  "airlines": [
    {
      "airline_icao": "AXM",
      "total_flights": 29,
      "on_time_rate": 48.3,
      "critical_delay_rate": 0.0,
      "high_fdi_rate": 0.0,
      "high_res_rate": 0.0,
      "avg_delay_minutes": 16.1,
      "avg_fdi": 17.1,
      "avg_res": 0.0,
      "total_affected_passengers": 4471,
      "total_estimated_compensation_eur": 0,
      "api_score": 74.2,
      "api_category": "GOOD"
    }
  ]
}
```

**Field Types**:

| Field | Type | Description |
|-------|------|-------------|
| `airline_icao` | string | Kode ICAO maskapai |
| `total_flights` | integer | Jumlah penerbangan aktif |
| `on_time_rate` | float | Persentase on-time (%) |
| `critical_delay_rate` | float | Persentase critical delay (%) |
| `high_fdi_rate` | float | Persentase FDI HIGH/CRITICAL (%) |
| `high_res_rate` | float | Persentase RES HIGH/CRITICAL (%) |
| `avg_delay_minutes` | float | Rata-rata delay menit |
| `avg_fdi` | float | Rata-rata FDI |
| `avg_res` | float | Rata-rata RES |
| `total_affected_passengers` | integer | Total penumpang terdampak |
| `total_estimated_compensation_eur` | integer | Total estimasi kompensasi EUR |
| `api_score` | float | Airline Performance Index 0-100 |
| `api_category` | string | `EXCELLENT` / `GOOD` / `FAIR` / `POOR` |

---

### 10. Airline Detail

```
GET /api/airlines/{icao}
```

**Description**: Mengambil detail performa satu maskapai.

**Path Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `icao` | string | Yes | Kode ICAO maskapai |

**Response 200**: Object airline performance (sama seperti item di `/api/airlines`).

**Response 404**:
```json
{
  "error": "Airline not found",
  "airline_icao": "UNKNOWN"
}
```

---

## WebSocket

### Endpoint

```
WS /ws/flights
```

**Description**: Push data real-time setiap 10 detik.

**Message Format** (JSON):
```json
{
  "timestamp": "2026-06-17T12:58:28Z",
  "stats": { /* sama seperti GET /api/stats */ },
  "impact": { /* sama seperti GET /api/impact */ },
  "airlines": [ /* top 10 maskapai */ ],
  "flights": [ /* array semua penerbangan */ ],
  "alerts": [ /* array penerbangan CRITICAL */ ],
  "total_flights": 230,
  "total_alerts": 0
}
```

**Frontend Usage Example (JavaScript)**:
```javascript
const ws = new WebSocket('ws://localhost:8000/ws/flights');

ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log('Flights:', data.total_flights);
  console.log('Impact:', data.impact);
  console.log('Top Airlines:', data.airlines);
  console.log('Alerts:', data.total_alerts);
};

ws.onerror = (error) => {
  console.error('WebSocket error:', error);
};

ws.onclose = () => {
  console.log('WebSocket disconnected');
};
```

---

## Data Refresh Strategy

| Endpoint / Channel | Refresh Rate | Rekomendasi Frontend |
|--------------------|--------------|----------------------|
| `WS /ws/flights` | Setiap 10 detik | Gunakan sebagai sumber utama real-time |
| `GET /api/stats` | On-demand / polling 10s | Backup jika WS gagal |
| `GET /api/impact` | On-demand | Untuk halaman dashboard overview |
| `GET /api/airlines` | On-demand / polling 30s | Untuk halaman ranking maskapai |
| `GET /api/flights/{id}/impact` | On-demand | Untuk modal detail penerbangan |

---

## Notes for Frontend

1. **Numeric fields are strings in flight objects** — Konversi ke number saat perlu (contoh: `parseFloat(flight.lat)`).

2. **`res` and `res_category` in `/api/flights`** — Saat masih awal running, banyak penerbangan memiliki `downstream_flight_count: 0` sehingga RES rendah. Ini normal karena data rotasi baru terkumpul.

3. **Alerts mungkin kosong** — Jika tidak ada penerbangan dengan `CRITICAL DELAY`, array alerts akan kosong.

4. **CORS enabled** — Frontend dapat berjalan di port manapun selama development.
