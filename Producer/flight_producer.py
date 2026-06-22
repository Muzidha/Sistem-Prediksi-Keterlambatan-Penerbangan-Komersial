import json
import time
import os
import sys
import requests
import concurrent.futures
from confluent_kafka import Producer, KafkaException
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "commercial-flight-stream")
POLL_INTERVAL   = int(os.getenv("POLL_INTERVAL_SECONDS", 30))
BOUNDS          = "6,-11,95,141"
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "7fe2acea0cac01fef802df31f7bb48a8")

weather_cache = {}

def log(msg):
    print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] {msg}", flush=True)

def delivery_report(err, msg):
    if err:
        log(f"DELIVERY ERROR: {err}")

def fetch_flights():
    url = "https://data-cloud.flightradar24.com/zones/fcgi/feed.js"
    params = {
        "faa":1,"satellite":1,"mlat":1,"flarm":1,"adsb":1,
        "gnd":0,"air":1,"vehicles":0,"estimated":1,
        "maxage":14400,"gliders":0,"stats":1,
        "bounds": BOUNDS,
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
        "Referer": "https://www.flightradar24.com/",
    }
    resp = requests.get(url, params=params, headers=headers, timeout=20)
    log(f"FR24 status: {resp.status_code} | size: {len(resp.content)} bytes")
    resp.raise_for_status()
    return resp.json()

def fetch_weather(lat, lon):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat, "longitude": lon,
        "current": "precipitation,windspeed_10m,visibility,weathercode",
        "windspeed_unit": "kn",
    }
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    c = resp.json().get("current", {})
    return {
        "precipitation_mm": c.get("precipitation", 0),
        "wind_knots":       c.get("windspeed_10m", 0),
        "visibility_m":     c.get("visibility", 10000),
        "weather_code":     c.get("weathercode", 0),
    }

def fetch_weather_openweather(lat, lon):
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "lat": lat, "lon": lon,
        "appid": OPENWEATHER_API_KEY,
        "units": "metric"
    }
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    
    wind_ms = data.get("wind", {}).get("speed", 0)
    wind_knots = wind_ms * 1.94384
    
    rain_1h = data.get("rain", {}).get("1h", 0)
    snow_1h = data.get("snow", {}).get("1h", 0)
    precip_mm = rain_1h + snow_1h
    
    visibility_m = data.get("visibility", 10000)
    
    weather_list = data.get("weather", [])
    weather_code = weather_list[0].get("id", 0) if weather_list else 0
    
    return {
        "precipitation_mm": precip_mm,
        "wind_knots":       round(wind_knots, 2),
        "visibility_m":     visibility_m,
        "weather_code":     weather_code,
    }

def get_weather_cached(lat, lon):
    # Round coordinates to 1 decimal place (~11 km) to reuse weather data
    key = (round(lat, 1), round(lon, 1))
    now = time.time()
    
    # Cache hit: return if less than 30 minutes (1800s) old
    if key in weather_cache and (now - weather_cache[key][0]) < 1800:
        return weather_cache[key][1]
    
    # Cache miss: fetch from API
    try:
        w = fetch_weather(lat, lon)
        weather_cache[key] = (now, w)
        return w
    except Exception as e:
        log(f"Open-Meteo fetch failed for {key}: {e}. Trying OpenWeatherMap...")
        try:
            w = fetch_weather_openweather(lat, lon)
            weather_cache[key] = (now, w)
            return w
        except Exception as e2:
            log(f"OpenWeatherMap fetch failed for {key}: {e2}.")
            return {}

def parse_flight(flight_id, raw):
    try:
        return {
            "flight_id":      flight_id,
            "callsign":       raw[16] or raw[13] or "",
            "airline_icao":   raw[18] if len(raw) > 18 else "",
            "aircraft_model": raw[8]  or "",
            "registration":   raw[9]  or "",
            "origin":         raw[11] or "",
            "destination":    raw[12] or "",
            "latitude":       float(raw[1]),
            "longitude":      float(raw[2]),
            "altitude_ft":    int(raw[4]),
            "speed_kn":       int(raw[5]),
            "heading_deg":    int(raw[3]),
            "on_ground":      bool(raw[14]),
            "timestamp":      raw[10],
            "ingested_at":    datetime.utcnow().isoformat() + "Z",
        }
    except Exception:
        return None

def create_producer():
    log(f"Konek ke Kafka: {KAFKA_BOOTSTRAP}")
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "socket.timeout.ms": 10000,
        "message.timeout.ms": 30000,
        "retries": 5,
    }
    p = Producer(conf)
    log("Producer siap!")
    return p

def process_flight(flight_id, raw):
    if not isinstance(raw, list) or len(raw) < 17:
        return None
    flight = parse_flight(flight_id, raw)
    if not flight or flight["on_ground"]:
        return None
    
    flight["weather"] = get_weather_cached(flight["latitude"], flight["longitude"])
    return flight

def main():
    sys.stdout.reconfigure(line_buffering=True)
    producer = create_producer()
    log(f"Topic: {KAFKA_TOPIC} | Interval: {POLL_INTERVAL}s")

    while True:
        try:
            log("Mengambil data penerbangan...")
            raw_data = fetch_flights()
            log(f"Total key dari FR24: {len(raw_data)}")

            flights_sent = 0
            
            # Using ThreadPoolExecutor to speed up weather fetching (max_workers=5 to avoid 429 rate limit)
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = []
                for flight_id, raw in raw_data.items():
                    if flight_id in ["full_count", "version"]:
                        continue
                    futures.append(executor.submit(process_flight, flight_id, raw))
                
                for future in concurrent.futures.as_completed(futures):
                    flight = future.result()
                    if not flight:
                        continue
                    
                    try:
                        producer.produce(
                            KAFKA_TOPIC,
                            key=flight["flight_id"].encode("utf-8"),
                            value=json.dumps(flight).encode("utf-8"),
                            callback=delivery_report,
                        )
                        flights_sent += 1

                        if flights_sent % 100 == 0:
                            producer.poll(0)
                    except Exception as e:
                        log(f"Produce error: {e}")

            producer.flush(timeout=30)
            log(f"Terkirim: {flights_sent} penerbangan aktif ke Kafka")

        except Exception as e:
            log(f"ERROR: {e}")

        log(f"Tunggu {POLL_INTERVAL} detik...")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()