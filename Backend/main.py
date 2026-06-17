"""
=============================================================
  Backend API & WebSocket — Anggota 4
  - Delta Lake sink ditangani oleh Spark processor
  - Service ini mengelola:
    * Redis alert flags untuk penerbangan CRITICAL DELAY
    * FastAPI REST endpoints
    * WebSocket push update ke dashboard (Anggota 5)
=============================================================
"""

import os
import asyncio
import json
from contextlib import asynccontextmanager
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone

import redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()

# ─── Konfigurasi ───────────────────────────────────────────
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

FLIGHT_KEY_PREFIX = "flight:"
ALERT_KEY_PREFIX = "alert:"
ROTATION_KEY_PREFIX = "rotation:"
STATS_KEY = "stats:latest"
ALERT_TTL_SECONDS = 3600          # Alert disimpan 1 jam
ALERT_SCAN_INTERVAL_SECONDS = 10  # Scan critical delay tiap 10 detik
WS_PUSH_INTERVAL_SECONDS = 10     # Push WebSocket tiap 10 detik

# Bandara hub besar (dampak koneksi tinggi)
HUB_AIRPORTS = {
    # Asia Tenggara
    "KUL", "SIN", "CGK", "BKK", "MNL", "HAN", "SGN", "RGN", "PNH",
    # Asia Timur
    "HKG", "PVG", "PEK", "ICN", "NRT", "HND", "TPE",
    # Timur Tengah
    "DXB", "DOH", "AUH", "IST",
    # Eropa
    "LHR", "CDG", "FRA", "AMS", "MAD", "FCO", "MUC",
    # Amerika
    "JFK", "LAX", "ORD", "ATL", "DFW", "SFO", "SEA",
}

# ─── Redis Client ──────────────────────────────────────────
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    decode_responses=True,
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_all_flights() -> List[Dict[str, Any]]:
    """Ambil semua penerbangan aktif dari Redis."""
    flights = []
    try:
        # SCAN lebih aman daripada KEYS untuk jumlah key besar
        for key in redis_client.scan_iter(match=f"{FLIGHT_KEY_PREFIX}*", count=100):
            data = redis_client.hgetall(key)
            if data:
                flights.append(data)
    except Exception as e:
        print(f"[REDIS ERROR] get_all_flights: {e}")
    return flights


def get_flight_by_id(flight_id: str) -> Optional[Dict[str, Any]]:
    """Ambil detail satu penerbangan berdasarkan flight_id."""
    # Coba beberapa kemungkinan key
    key = f"{FLIGHT_KEY_PREFIX}{flight_id}"
    data = redis_client.hgetall(key)
    return data if data else None


def get_stats() -> Dict[str, Any]:
    """Ambil ringkasan statistik dari Redis."""
    data = redis_client.hgetall(STATS_KEY)
    if not data:
        return {
            "total_flights": "0",
            "delayed_flights": "0",
            "critical_flights": "0",
            "avg_delay_minutes": "0.0",
            "updated_at": utc_now_iso(),
        }
    return data


def get_alerts() -> List[Dict[str, Any]]:
    """Ambil daftar alert penerbangan CRITICAL DELAY."""
    alerts = []
    try:
        for key in redis_client.scan_iter(match=f"{ALERT_KEY_PREFIX}*", count=100):
            data = redis_client.hgetall(key)
            if data:
                # Tambahkan TTL remaining untuk info frontend
                ttl = redis_client.ttl(key)
                data["ttl_seconds"] = str(ttl) if ttl > 0 else "0"
                alerts.append(data)
    except Exception as e:
        print(f"[REDIS ERROR] get_alerts: {e}")
    return alerts


def extract_impact_metrics(flight: Dict[str, Any]) -> Dict[str, Any]:
    """Ekstrak metrik dampak dari data Redis untuk response API."""
    ripple = compute_ripple_effect_score(flight)
    return {
        "flight_id": flight.get("flight_id", ""),
        "callsign": flight.get("callsign", ""),
        "airline": flight.get("airline", ""),
        "aircraft": flight.get("aircraft", ""),
        "registration": flight.get("registration", ""),
        "origin": flight.get("origin", ""),
        "destination": flight.get("destination", ""),
        "predicted_delay_minutes": flight.get("predicted_delay_minutes", "0"),
        "delay_category": flight.get("delay_category", "UNKNOWN"),
        "capacity": flight.get("capacity", "0"),
        "affected_passengers": flight.get("affected_passengers", "0"),
        "fdi": flight.get("fdi", "0"),
        "fdi_category": flight.get("fdi_category", "UNKNOWN"),
        "estimated_compensation_eur": flight.get("estimated_compensation_eur", "0"),
        "res": ripple["res"],
        "res_category": ripple["res_category"],
        "next_flight_id": ripple["next_flight_id"],
        "next_flight_gap_min": ripple["next_flight_gap_min"],
        "downstream_flight_count": ripple["downstream_flight_count"],
    }


def get_rotation(registration: str) -> List[Dict[str, Any]]:
    """Ambil daftar penerbangan dengan registrasi pesawat yang sama dari Redis."""
    if not registration:
        return []
    try:
        rotation_key = f"{ROTATION_KEY_PREFIX}{registration}"
        # Ambil semua flight_id beserta timestamp (score)
        items = redis_client.zrange(rotation_key, 0, -1, withscores=True)
        flights = []
        for flight_id, ts in items:
            data = get_flight_by_id(flight_id)
            if data:
                data["_timestamp"] = ts
                flights.append(data)
        return flights
    except Exception as e:
        print(f"[REDIS ERROR] get_rotation: {e}")
        return []


def compute_ripple_effect_score(flight: Dict[str, Any]) -> Dict[str, Any]:
    """
    Hitung Ripple Effect Score (RES) — estimasi seberapa besar delay ini
    merembet ke penerbangan lanjutan / penumpang connecting.

    Output:
      res              : skor 0-100
      res_category     : LOW / MODERATE / HIGH / CRITICAL
      next_flight_id   : flight_id penerbangan berikutnya (jika ada)
      next_flight_gap_min : selisih waktu ke penerbangan berikutnya (menit)
      downstream_flight_count : jumlah penerbangan lanjutan dalam 24 jam
    """

    def to_float(v, default=0.0):
        try:
            return float(v)
        except (TypeError, ValueError):
            return default

    def to_int(v, default=0):
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return default

    delay_minutes = to_float(flight.get("predicted_delay_minutes", 0))
    distance_km = to_float(flight.get("distance_km", 0))
    hour_utc = to_int(flight.get("hour_utc", 0))
    destination = flight.get("destination", "")
    fdi = to_float(flight.get("fdi", 0))
    registration = flight.get("registration", "")
    current_ts = to_int(flight.get("timestamp", 0))

    rotation = get_rotation(registration)
    # Hanya penerbangan lanjutan (timestamp lebih besar dari saat ini)
    downstream = [r for r in rotation if to_int(r.get("timestamp", 0)) > current_ts]
    downstream_count = len(downstream)

    # ── Faktor 1: Rotation tightness ────────────────────────
    next_flight_id = None
    next_gap_min = None
    rotation_score = 0.0
    if downstream:
        # Urutkan berdasarkan timestamp
        downstream_sorted = sorted(
            downstream,
            key=lambda x: to_int(x.get("timestamp", 0))
        )
        next_flight = downstream_sorted[0]
        next_ts = to_int(next_flight.get("timestamp", 0))
        next_gap_min = max(0, int((next_ts - current_ts) / 60))
        next_flight_id = next_flight.get("flight_id")

        if next_gap_min < 60:
            rotation_score = 100.0
        elif next_gap_min < 120:
            rotation_score = 75.0
        elif next_gap_min < 180:
            rotation_score = 50.0
        elif next_gap_min < 300:
            rotation_score = 25.0
        else:
            rotation_score = 10.0

    # ── Faktor 2: Downstream flight count ───────────────────
    if downstream_count >= 5:
        downstream_score = 100.0
    elif downstream_count >= 3:
        downstream_score = 66.0
    elif downstream_count >= 1:
        downstream_score = 33.0
    else:
        downstream_score = 0.0

    # ── Faktor 3: Hub destination ───────────────────────────
    hub_score = 100.0 if destination in HUB_AIRPORTS else 30.0

    # ── Faktor 4: Delay magnitude ───────────────────────────
    delay_score = min(delay_minutes / 120.0, 1.0) * 100.0

    # ── Faktor 5: Time recovery (kapan delay terjadi) ───────
    # Malam hari & peak hour lebih sulit pulih
    if 0 <= hour_utc < 6:
        recovery_score = 100.0
    elif 6 <= hour_utc < 10 or 17 <= hour_utc < 21:
        recovery_score = 75.0
    elif 21 <= hour_utc <= 23:
        recovery_score = 50.0
    else:
        recovery_score = 25.0

    # ── Bobot RES ───────────────────────────────────────────
    res = (
        rotation_score * 0.30 +
        downstream_score * 0.20 +
        hub_score * 0.20 +
        delay_score * 0.15 +
        recovery_score * 0.10 +
        (fdi / 100.0 * 100.0) * 0.05
    )
    res = round(min(100.0, max(0.0, res)), 1)

    if res <= 25:
        res_category = "LOW"
    elif res <= 50:
        res_category = "MODERATE"
    elif res <= 75:
        res_category = "HIGH"
    else:
        res_category = "CRITICAL"

    return {
        "res": res,
        "res_category": res_category,
        "next_flight_id": next_flight_id,
        "next_flight_gap_min": next_gap_min,
        "downstream_flight_count": downstream_count,
    }


def compute_aggregate_impact(flights: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Hitung agregat impact dari semua penerbangan aktif."""
    def to_float(v, default=0.0):
        try:
            return float(v)
        except (TypeError, ValueError):
            return default

    def to_int(v, default=0):
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return default

    total_pax = sum(to_int(f.get("affected_passengers", 0)) for f in flights)
    total_comp = sum(to_int(f.get("estimated_compensation_eur", 0)) for f in flights)
    high_fdi = sum(1 for f in flights if f.get("fdi_category") in ("HIGH", "CRITICAL"))
    critical = sum(1 for f in flights if f.get("delay_category") == "CRITICAL DELAY")
    avg_fdi = (sum(to_float(f.get("fdi", 0)) for f in flights) / max(len(flights), 1))

    # Hitung RES untuk setiap flight lalu agregat
    res_values = []
    high_res_count = 0
    for f in flights:
        ripple = compute_ripple_effect_score(f)
        res_values.append(ripple["res"])
        if ripple["res_category"] in ("HIGH", "CRITICAL"):
            high_res_count += 1
    avg_res = sum(res_values) / max(len(res_values), 1) if res_values else 0.0

    return {
        "total_flights": len(flights),
        "high_fdi_flights": high_fdi,
        "critical_delay_flights": critical,
        "high_res_flights": high_res_count,
        "total_affected_passengers": total_pax,
        "total_estimated_compensation_eur": total_comp,
        "avg_fdi": round(avg_fdi, 1),
        "avg_res": round(avg_res, 1),
    }


def compute_airline_performance(flights: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Hitung performa per maskapai dari penerbangan aktif di Redis.
    Output: dict { airline_icao: metrics }
    """
    def to_float(v, default=0.0):
        try:
            return float(v)
        except (TypeError, ValueError):
            return default

    def to_int(v, default=0):
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return default

    airlines = {}
    for f in flights:
        icao = f.get("airline", "")
        if not icao:
            continue
        if icao not in airlines:
            airlines[icao] = []
        airlines[icao].append(f)

    performance = {}
    for icao, fs in airlines.items():
        total = len(fs)
        on_time = sum(1 for f in fs if f.get("delay_category") == "ON TIME")
        critical = sum(1 for f in fs if f.get("delay_category") == "CRITICAL DELAY")
        high_fdi = sum(1 for f in fs if f.get("fdi_category") in ("HIGH", "CRITICAL"))
        high_res = sum(1 for f in fs if f.get("res_category") in ("HIGH", "CRITICAL"))

        avg_delay = sum(to_float(f.get("predicted_delay_minutes", 0)) for f in fs) / max(total, 1)
        avg_fdi = sum(to_float(f.get("fdi", 0)) for f in fs) / max(total, 1)
        avg_res = sum(to_float(f.get("res", 0)) for f in fs) / max(total, 1)
        total_pax = sum(to_int(f.get("affected_passengers", 0)) for f in fs)
        total_comp = sum(to_int(f.get("estimated_compensation_eur", 0)) for f in fs)

        on_time_rate = on_time / max(total, 1)
        critical_rate = critical / max(total, 1)
        high_fdi_rate = high_fdi / max(total, 1)
        high_res_rate = high_res / max(total, 1)

        # ── Airline Performance Index (API) skor 0-100 ────────
        # 40% on-time rate, 25% delay control, 20% critical control,
        # 10% FDI control, 5% RES control
        normalized_delay = min(avg_delay / 120.0, 1.0)
        normalized_fdi = min(avg_fdi / 100.0, 1.0)
        normalized_res = min(avg_res / 100.0, 1.0)

        api_score = (
            on_time_rate * 40.0 +
            (1.0 - normalized_delay) * 25.0 +
            (1.0 - critical_rate) * 20.0 +
            (1.0 - normalized_fdi) * 10.0 +
            (1.0 - normalized_res) * 5.0
        )
        api_score = round(min(100.0, max(0.0, api_score)), 1)

        if api_score >= 80:
            api_category = "EXCELLENT"
        elif api_score >= 60:
            api_category = "GOOD"
        elif api_score >= 40:
            api_category = "FAIR"
        else:
            api_category = "POOR"

        performance[icao] = {
            "airline_icao": icao,
            "total_flights": total,
            "on_time_rate": round(on_time_rate * 100, 1),
            "critical_delay_rate": round(critical_rate * 100, 1),
            "high_fdi_rate": round(high_fdi_rate * 100, 1),
            "high_res_rate": round(high_res_rate * 100, 1),
            "avg_delay_minutes": round(avg_delay, 1),
            "avg_fdi": round(avg_fdi, 1),
            "avg_res": round(avg_res, 1),
            "total_affected_passengers": total_pax,
            "total_estimated_compensation_eur": total_comp,
            "api_score": api_score,
            "api_category": api_category,
        }

    return performance


async def scan_critical_alerts():
    """
    Background task: scan penerbangan CRITICAL DELAY di Redis,
    simpan ke key alert:<flight_id> dengan TTL 1 jam.
    """
    while True:
        try:
            critical_count = 0
            pipe = redis_client.pipeline()

            for key in redis_client.scan_iter(match=f"{FLIGHT_KEY_PREFIX}*", count=100):
                data = redis_client.hgetall(key)
                if not data:
                    continue
                if data.get("delay_category") == "CRITICAL DELAY":
                    flight_id = data.get("flight_id", "")
                    if not flight_id:
                        continue
                    alert_key = f"{ALERT_KEY_PREFIX}{flight_id}"
                    # Simpan data penerbangan + flag alert
                    alert_data = dict(data)
                    alert_data["alert_created_at"] = utc_now_iso()
                    pipe.hset(alert_key, mapping=alert_data)
                    pipe.expire(alert_key, ALERT_TTL_SECONDS)
                    critical_count += 1

            pipe.execute()
            print(f"[ALERT SCAN] {critical_count} critical alerts updated at {utc_now_iso()}")

        except Exception as e:
            print(f"[ALERT SCAN ERROR] {e}")

        await asyncio.sleep(ALERT_SCAN_INTERVAL_SECONDS)


async def build_dashboard_payload() -> Dict[str, Any]:
    """Gabungkan data untuk REST/WS response (efisien, scan Redis sekali)."""
    flights = get_all_flights()
    alerts = get_alerts()
    impact = compute_aggregate_impact(flights)
    airlines_perf = compute_airline_performance(flights)
    # Sort airlines by API score descending, take top 10
    top_airlines = sorted(
        airlines_perf.values(),
        key=lambda x: x["api_score"],
        reverse=True
    )[:10]
    return {
        "timestamp": utc_now_iso(),
        "stats": get_stats(),
        "impact": impact,
        "airlines": top_airlines,
        "flights": flights,
        "alerts": alerts,
        "total_flights": len(flights),
        "total_alerts": len(alerts),
    }


# ─── Lifespan: background task ─────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[BACKEND] Starting alert scanner background task...")
    task = asyncio.create_task(scan_critical_alerts())
    yield
    task.cancel()
    print("[BACKEND] Alert scanner stopped.")


app = FastAPI(
    title="Flight Delay Prediction API",
    description="Backend & WebSocket untuk dashboard prediksi keterlambatan penerbangan",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS agar dashboard frontend (Anggota 5) bisa akses
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── REST Endpoints ────────────────────────────────────────
@app.get("/")
async def root():
    return {
        "service": "Flight Delay Prediction Backend",
        "status": "running",
        "timestamp": utc_now_iso(),
    }


@app.get("/api/flights")
async def api_flights(
    airline: Optional[str] = Query(None, description="Filter by airline ICAO code, e.g. AXM"),
    delay_category: Optional[str] = Query(None, description="Filter by delay category: ON TIME, MEDIUM DELAY, CRITICAL DELAY"),
    fdi_category: Optional[str] = Query(None, description="Filter by FDI category: LOW, MODERATE, HIGH, CRITICAL"),
    res_category: Optional[str] = Query(None, description="Filter by RES category: LOW, MODERATE, HIGH, CRITICAL"),
    limit: Optional[int] = Query(None, ge=1, le=1000, description="Limit number of results"),
):
    """Semua penerbangan aktif dari Redis dengan filter opsional."""
    flights = get_all_flights()

    if airline:
        flights = [f for f in flights if f.get("airline", "").upper() == airline.upper()]
    if delay_category:
        flights = [f for f in flights if f.get("delay_category", "") == delay_category]
    if fdi_category:
        flights = [f for f in flights if f.get("fdi_category", "") == fdi_category]
    if res_category:
        flights = [f for f in flights if f.get("res_category", "") == res_category]
    if limit:
        flights = flights[:limit]

    return {
        "count": len(flights),
        "filters": {
            "airline": airline,
            "delay_category": delay_category,
            "fdi_category": fdi_category,
            "res_category": res_category,
            "limit": limit,
        },
        "flights": flights,
    }


@app.get("/api/flights/{flight_id}")
async def api_flight_detail(flight_id: str):
    """Detail satu penerbangan."""
    flight = get_flight_by_id(flight_id)
    if not flight:
        return {"error": "Flight not found", "flight_id": flight_id}
    return flight


@app.get("/api/flights/{flight_id}/impact")
async def api_flight_impact(flight_id: str):
    """Metrik dampak (FDI, RES, affected passengers, compensation) satu penerbangan."""
    flight = get_flight_by_id(flight_id)
    if not flight:
        return {"error": "Flight not found", "flight_id": flight_id}
    return extract_impact_metrics(flight)


@app.get("/api/flights/{flight_id}/ripple")
async def api_flight_ripple(flight_id: str):
    """Detail Ripple Effect Score satu penerbangan."""
    flight = get_flight_by_id(flight_id)
    if not flight:
        return {"error": "Flight not found", "flight_id": flight_id}
    return {
        "flight_id": flight_id,
        "registration": flight.get("registration", ""),
        "ripple_effect": compute_ripple_effect_score(flight),
    }


@app.get("/api/impact")
async def api_impact():
    """Agregat impact seluruh penerbangan aktif."""
    flights = get_all_flights()
    return {
        "count": len(flights),
        "impact": compute_aggregate_impact(flights),
    }


@app.get("/api/airlines")
async def api_airlines(
    min_api_score: Optional[float] = Query(None, ge=0, le=100, description="Minimum API score filter"),
    category: Optional[str] = Query(None, description="Filter by category: EXCELLENT, GOOD, FAIR, POOR"),
    limit: Optional[int] = Query(None, ge=1, le=500, description="Limit number of results"),
):
    """Daftar performa semua maskapai (Airline Performance Index) dengan filter opsional."""
    flights = get_all_flights()
    perf = compute_airline_performance(flights)
    ranked = sorted(
        perf.values(),
        key=lambda x: x["api_score"],
        reverse=True
    )

    if min_api_score is not None:
        ranked = [a for a in ranked if a["api_score"] >= min_api_score]
    if category:
        ranked = [a for a in ranked if a["api_category"] == category.upper()]
    if limit:
        ranked = ranked[:limit]

    return {
        "count": len(ranked),
        "filters": {
            "min_api_score": min_api_score,
            "category": category,
            "limit": limit,
        },
        "airlines": ranked,
    }


@app.get("/api/airlines/{icao}")
async def api_airline_detail(icao: str):
    """Detail performa satu maskapai."""
    flights = get_all_flights()
    perf = compute_airline_performance(flights)
    if icao not in perf:
        return {"error": "Airline not found", "airline_icao": icao}
    return perf[icao]


@app.get("/api/stats")
async def api_stats():
    """Ringkasan statistik dari Redis."""
    return get_stats()


@app.get("/api/alerts")
async def api_alerts(
    limit: Optional[int] = Query(None, ge=1, le=1000, description="Limit number of alerts"),
):
    """Daftar penerbangan dengan CRITICAL DELAY."""
    alerts = get_alerts()
    if limit:
        alerts = alerts[:limit]
    return {
        "count": len(alerts),
        "limit": limit,
        "alerts": alerts,
    }


# ─── WebSocket Endpoint ────────────────────────────────────
@app.websocket("/ws/flights")
async def websocket_flights(websocket: WebSocket):
    """
    WebSocket: push data penerbangan, stats, dan alerts
    setiap 10 detik ke dashboard frontend.
    """
    await websocket.accept()
    print(f"[WS] Client connected: {websocket.client}")
    try:
        while True:
            payload = await build_dashboard_payload()
            await websocket.send_json(payload)
            await asyncio.sleep(WS_PUSH_INTERVAL_SECONDS)
    except WebSocketDisconnect:
        print(f"[WS] Client disconnected: {websocket.client}")
    except Exception as e:
        print(f"[WS ERROR] {e}")
    finally:
        try:
            await websocket.close()
        except Exception:
            pass
