"""
=============================================================
  Spark Structured Streaming + ML Inference
  Anggota 2+3: Spark Processing & Prediksi Delay (Regresi)
=============================================================
  Alur kerja:
    Kafka (commercial-flight-stream)
      → Spark readStream
      → Parsing JSON + Data Cleaning
      → Feature Engineering (incl. weather_score, traffic_density,
        distance_km, route_deviation, StringIndexed aircraft/airline)
      → ML Inference — RandomForestRegressor → delay_minutes
      → Sink ke Redis (untuk visualisasi Anggota 4)
      → Sink ke Kafka topic baru (flight-predictions)
=============================================================
"""

import os
import json
import redis
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, IntegerType,
    BooleanType, LongType, DoubleType
)
from pyspark.ml import PipelineModel

# ─── Konfigurasi ───────────────────────────────────────────
KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_INPUT = os.getenv("KAFKA_TOPIC", "commercial-flight-stream")
KAFKA_TOPIC_OUT   = "flight-predictions"
REDIS_HOST        = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT        = int(os.getenv("REDIS_PORT", 6379))
MODEL_PATH        = os.getenv("MODEL_PATH", "./model_keterlambatan")
CHECKPOINT_DIR    = "./checkpoint_spark"

# ─── Aircraft capacity lookup (untuk estimasi penumpang terdampak) ─
AIRCRAFT_CAPACITY = {
    "A20N": 180, "A21N": 220, "A319": 150, "A320": 180, "A321": 220,
    "A332": 250, "A333": 300, "A343": 300, "A359": 325,
    "B738": 189, "B739": 215, "B73H": 189, "B744": 416, "B748": 467,
    "B752": 200, "B763": 290, "B772": 400, "B773": 450, "B77W": 396,
    "B788": 250, "B789": 290, "B78X": 330,
    "CRJ9":  90, "E190": 100, "E195": 120,
}
DEFAULT_CAPACITY = 180
DEFAULT_LOAD_FACTOR = 0.85

# ─── Schema JSON dari Kafka ─────────────────────────────────
# Sesuai dengan format yang dikirim Producer (Anggota 1)
weather_schema = StructType([
    StructField("precipitation_mm", FloatType(), True),
    StructField("wind_knots",       FloatType(), True),
    StructField("visibility_m",     FloatType(), True),
    StructField("weather_code",     IntegerType(), True),
])

flight_schema = StructType([
    StructField("flight_id",      StringType(),  True),
    StructField("callsign",       StringType(),  True),
    StructField("airline_icao",   StringType(),  True),
    StructField("aircraft_model", StringType(),  True),
    StructField("registration",   StringType(),  True),
    StructField("origin",         StringType(),  True),
    StructField("destination",    StringType(),  True),
    StructField("latitude",       DoubleType(),  True),
    StructField("longitude",      DoubleType(),  True),
    StructField("altitude_ft",    IntegerType(), True),
    StructField("speed_kn",       IntegerType(), True),
    StructField("heading_deg",    IntegerType(), True),
    StructField("on_ground",      BooleanType(), True),
    StructField("timestamp",      LongType(),    True),
    StructField("ingested_at",    StringType(),  True),
    StructField("weather",        weather_schema, True),
])


def create_spark_session():
    """Membuat SparkSession dengan package Kafka & Delta Lake."""
    print("[SPARK] Menginisialisasi SparkSession...")
    spark = (
        SparkSession.builder
        .appName("FlightDelayPrediction")
        # Package kafka connector & delta lake untuk Spark
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0"
        )
        # Konfigurasi catalog Delta Lake
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        # Supaya log tidak terlalu ramai
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("[SPARK] SparkSession siap!")
    return spark


def read_kafka_stream(spark):
    """Membaca stream dari Kafka topic."""
    print(f"[KAFKA] Konek ke {KAFKA_BOOTSTRAP}, topic: {KAFKA_TOPIC_INPUT}")
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC_INPUT)
        # Mulai dari pesan terbaru (gunakan "earliest" untuk replay semua)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_json(raw_df):
    """
    Step 1 (Bronze): Parse JSON dari kolom 'value' Kafka.
    Hanya parsing + flatten weather struct, TANPA cleaning.
    Output ini langsung masuk ke Bronze layer.
    """
    parsed = (
        raw_df
        .select(
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(F.col("value").cast("string"), flight_schema).alias("d")
        )
        .select("kafka_timestamp", "d.*")
    )

    # Flatten nested weather struct
    flattened = (
        parsed
        .withColumn("precipitation_mm", F.col("weather.precipitation_mm"))
        .withColumn("wind_knots",        F.col("weather.wind_knots"))
        .withColumn("visibility_m",      F.col("weather.visibility_m"))
        .withColumn("weather_code",      F.col("weather.weather_code"))
        .drop("weather")
    )
    return flattened


def clean_data(parsed_df):
    """
    Step 2 (Silver): Data Cleaning — buang baris dengan field
    kritis kosong, filter nilai tidak masuk akal, isi null.
    Input: output dari parse_json (Bronze data).
    """
    cleaned = (
        parsed_df
        # ── Data Cleaning ──────────────────────────────────
        # Buang baris yang field pentingnya null
        .filter(F.col("flight_id").isNotNull())
        .filter(F.col("latitude").isNotNull())
        .filter(F.col("longitude").isNotNull())
        # Pesawat tidak boleh di darat
        .filter(F.col("on_ground") == False)
        # Altitude masuk akal: 1.000 - 60.000 kaki
        .filter((F.col("altitude_ft") >= 1000) & (F.col("altitude_ft") <= 60000))
        # Speed masuk akal: 50 - 700 knot
        .filter((F.col("speed_kn") >= 50) & (F.col("speed_kn") <= 700))
        # Heading valid
        .filter((F.col("heading_deg") >= 0) & (F.col("heading_deg") <= 360))
        # Isi nilai weather yang null dengan default
        .fillna({
            "precipitation_mm": 0.0,
            "wind_knots":        0.0,
            "visibility_m":      10000.0,
            "weather_code":      0,
        })
        # Isi string kosong
        .fillna({
            "airline_icao":   "UNKNOWN",
            "aircraft_model": "UNKNOWN",
            "origin":         "UNKNOWN",
            "destination":    "UNKNOWN",
        })
    )
    return cleaned


def feature_engineering(df):
    """
    Feature Engineering untuk model ML (Regresi delay_minutes).
    Membuat fitur-fitur baru dari data mentah, termasuk fitur
    wajib Anggota 3: weather_score, traffic_density, distance_km,
    route_deviation.
    """
    featured = (
        df
        # ── Fitur Cuaca ─────────────────────────────────────
        # Apakah ada hujan? (precipitation > 0.5 mm)
        .withColumn("is_raining",
            (F.col("precipitation_mm") > 0.5).cast(IntegerType()))

        # Apakah angin kencang? (> 25 knot = kondisi berbahaya)
        .withColumn("is_high_wind",
            (F.col("wind_knots") > 25).cast(IntegerType()))

        # Visibilitas rendah? (< 3000 m = kabut/hujan lebat)
        .withColumn("is_low_visibility",
            (F.col("visibility_m") < 3000).cast(IntegerType()))

        # Kategori cuaca buruk berdasarkan WMO weather code
        # Code 60-69 = hujan, 70-79 = salju, 80-99 = badai
        .withColumn("is_bad_weather",
            ((F.col("weather_code") >= 60) & (F.col("weather_code") <= 99))
            .cast(IntegerType()))

        # ── weather_score (Anggota 3) ────────────────────────
        # Skor komposit cuaca 0–1 (0 = cerah, 1 = badai)
        # Gabungan normalisasi precipitation, wind, visibility, weather_code
        .withColumn("weather_score",
            F.least(
                F.lit(1.0),
                (
                    F.least(F.col("precipitation_mm") / 50.0, F.lit(1.0)) * 0.3 +
                    F.least(F.col("wind_knots") / 60.0, F.lit(1.0)) * 0.25 +
                    F.greatest(F.lit(0.0),
                        (F.lit(1.0) - F.col("visibility_m") / 15000.0)) * 0.25 +
                    F.when(F.col("weather_code") >= 80, 1.0)
                     .when(F.col("weather_code") >= 60, 0.6)
                     .when(F.col("weather_code") >= 40, 0.3)
                     .otherwise(0.0) * 0.2
                )
            ).cast(FloatType()))

        # ── Fitur Penerbangan ────────────────────────────────
        # Fase penerbangan berdasarkan altitude
        .withColumn("flight_phase",
            F.when(F.col("altitude_ft") < 15000, 0)   # Climbing/Descending rendah
             .when(F.col("altitude_ft") < 35000, 1)   # Cruising
             .otherwise(2))                            # High altitude

        # Apakah kecepatan rendah (mungkin mendekat bandara / delay)?
        .withColumn("is_slow",
            (F.col("speed_kn") < 200).cast(IntegerType()))

        # Rasio kecepatan terhadap altitude (proxy efisiensi penerbangan)
        .withColumn("speed_altitude_ratio",
            F.when(F.col("altitude_ft") > 0,
                   F.col("speed_kn") / F.col("altitude_ft") * 1000)
             .otherwise(0.0))

        # ── distance_km (Anggota 3) ──────────────────────────
        # Estimasi jarak penerbangan dari speed dan altitude
        # Proxy: pesawat cruising (alt tinggi, speed tinggi) = rute panjang
        # Rumus aproksimasi sederhana berbasis speed_kn dan altitude_ft
        .withColumn("distance_km",
            F.when(F.col("altitude_ft") > 30000,
                   F.col("speed_kn") * 1.852 * 3.5)     # rute panjang (~3.5 jam)
             .when(F.col("altitude_ft") > 20000,
                   F.col("speed_kn") * 1.852 * 2.0)     # rute sedang (~2 jam)
             .otherwise(
                   F.col("speed_kn") * 1.852 * 1.0)     # rute pendek (~1 jam)
            .cast(FloatType()))

        # ── route_deviation (Anggota 3) ──────────────────────
        # Estimasi deviasi rute dari kondisi cuaca dan angin
        # Cuaca buruk + angin kencang → deviasi lebih besar
        .withColumn("route_deviation",
            (
                F.when(F.col("wind_knots") > 30,
                       F.col("wind_knots") * 0.3)
                 .otherwise(F.col("wind_knots") * 0.1) +
                F.when(F.col("weather_code") >= 60, F.lit(5.0))
                 .otherwise(F.lit(0.0))
            ).cast(FloatType()))

        # ── Fitur Waktu ──────────────────────────────────────
        # Jam UTC dari timestamp (jam peak = 06-09, 17-20)
        .withColumn("hour_utc",
            F.hour(F.from_unixtime(F.col("timestamp"))))
        .withColumn("is_peak_hour",
            (((F.col("hour_utc") >= 6) & (F.col("hour_utc") <= 9)) |
             ((F.col("hour_utc") >= 17) & (F.col("hour_utc") <= 20)))
            .cast(IntegerType()))

        # Hari dalam seminggu (1=Minggu, 7=Sabtu di Spark)
        .withColumn("day_of_week",
            F.dayofweek(F.from_unixtime(F.col("timestamp"))))

        # Weekend?
        .withColumn("is_weekend",
            ((F.col("day_of_week") == 1) | (F.col("day_of_week") == 7))
            .cast(IntegerType()))

        # ── traffic_density (Anggota 3) ──────────────────────
        # Estimasi kepadatan traffic berdasarkan jam dan hari
        # Peak hours + weekday = density tinggi
        .withColumn("traffic_density",
            (F.when((F.col("hour_utc") >= 6) & (F.col("hour_utc") <= 9), 8)
              .when((F.col("hour_utc") >= 17) & (F.col("hour_utc") <= 20), 9)
              .when((F.col("hour_utc") >= 10) & (F.col("hour_utc") <= 16), 6)
              .otherwise(3)
             +
             F.when((F.col("day_of_week") >= 2) & (F.col("day_of_week") <= 6),
                    F.lit(1))   # weekday bonus
              .otherwise(F.lit(0))
            ).cast(IntegerType()))

        # ── Skor Risiko Manual (rule-based fallback) ─────────
        # Dipakai jika model ML belum dilatih → estimasi delay menit
        .withColumn("risk_score_manual",
            (F.col("is_raining")        * 2.0 +
             F.col("is_high_wind")      * 2.5 +
             F.col("is_low_visibility") * 3.0 +
             F.col("is_bad_weather")    * 2.0 +
             F.col("is_peak_hour")      * 1.5 +
             F.col("is_weekend")        * 0.5 +
             F.col("is_slow")           * 1.0))
    )
    return featured


# ─── UDF untuk estimasi kapasitas penumpang ─────────────────
@udf(IntegerType())
def get_capacity(aircraft_model):
    """Lookup kapasitas kursi berdasarkan tipe pesawat."""
    return AIRCRAFT_CAPACITY.get(aircraft_model, DEFAULT_CAPACITY)


def compute_impact_metrics(df):
    """
    Hitung metrik dampak tambahan (Anggota 4):
      - capacity: kapasitas kursi pesawat
      - affected_passengers: estimasi penumpang terdampak
      - fdi: Flight Delay Index (0-100)
      - fdi_category: LOW / MODERATE / HIGH / CRITICAL
      - estimated_compensation_eur: estimasi biaya kompensasi (EUR)
    """
    impacted = (
        df
        # Kapasitas & penumpang terdampak
        .withColumn("capacity", get_capacity(F.col("aircraft_model")))
        .withColumn("affected_passengers",
            F.round(F.col("capacity") * DEFAULT_LOAD_FACTOR).cast(IntegerType()))

        # Normalisasi delay untuk FDI (cap 180 menit = 1.0)
        .withColumn("normalized_delay",
            F.least(F.col("predicted_delay_minutes") / 180.0, F.lit(1.0))
             .cast(FloatType()))

        # Flight Delay Index (0-100) — komposit multi-faktor
        .withColumn("fdi",
            (
                F.col("normalized_delay") * 40.0 +
                F.col("weather_score") * 25.0 +
                (F.col("traffic_density") / 10.0) * 15.0 +
                F.least(F.col("route_deviation") / 50.0, F.lit(1.0)) * 10.0 +
                F.col("is_peak_hour") * 10.0
            ).cast(FloatType()))

        # Kategori FDI
        .withColumn("fdi_category",
            F.when(F.col("fdi") <= 25,  "LOW")
             .when(F.col("fdi") <= 50,  "MODERATE")
             .when(F.col("fdi") <= 75,  "HIGH")
             .otherwise("CRITICAL"))

        # Estimasi biaya kompensasi (model EU261 sederhana)
        .withColumn("estimated_compensation_eur",
            F.when(F.col("predicted_delay_minutes") < 180, F.lit(0))
             .when(F.col("distance_km") < 1500, F.col("affected_passengers") * 250)
             .when(F.col("distance_km") < 3500, F.col("affected_passengers") * 400)
             .otherwise(F.col("affected_passengers") * 600)
             .cast(IntegerType()))
    )
    return impacted


def predict_delay(df, model=None):
    """
    Prediksi keterlambatan dalam MENIT (regresi).
    - Jika model ML tersedia: gunakan PipelineModel (RandomForestRegressor)
      → kolom 'prediction' langsung berisi predicted_delay_minutes
    - Jika tidak: gunakan risk_score_manual * 6 sebagai estimasi menit

    Kategori delay:
      < 15 menit  → "ON TIME"
      15–60 menit → "MEDIUM DELAY"
      > 60 menit  → "CRITICAL DELAY"
    """
    if model is not None:
        # Gunakan model MLlib regresi — prediction = delay_minutes
        predictions = model.transform(df)
        result = predictions.withColumn(
            "predicted_delay_minutes",
            F.round(F.col("prediction"), 1).cast(FloatType())
        )
    else:
        # Fallback: rule-based dari risk_score_manual
        # risk_score_manual (0–12.5) × 6 ≈ estimasi menit delay
        result = df.withColumn(
            "predicted_delay_minutes",
            F.round(F.col("risk_score_manual") * 6.0, 1).cast(FloatType())
        )

    # Tambah label kategori delay berdasarkan menit
    result = result.withColumn("delay_category",
        F.when(F.col("predicted_delay_minutes") < 15,  "ON TIME")
         .when(F.col("predicted_delay_minutes") <= 60, "MEDIUM DELAY")
         .otherwise("CRITICAL DELAY"))

    return result


def write_to_redis(batch_df, batch_id):
    """
    Menulis hasil prediksi regresi ke Redis.
    Format key: flight:<flight_id>
    Expire: 300 detik (5 menit) — data streaming terus diperbarui

    Field output:
      predicted_delay_minutes  — float, prediksi menit keterlambatan
      delay_category           — ON TIME / MEDIUM DELAY / CRITICAL DELAY
    """
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT,
                        decode_responses=True)

        rows = batch_df.select(
            "flight_id", "callsign", "airline_icao", "aircraft_model",
            "registration", "origin", "destination",
            "latitude", "longitude", "altitude_ft", "speed_kn",
            "precipitation_mm", "wind_knots", "visibility_m",
            "weather_score", "traffic_density",
            "distance_km", "route_deviation",
            "predicted_delay_minutes", "delay_category",
            "capacity", "affected_passengers",
            "fdi", "fdi_category", "estimated_compensation_eur",
            "timestamp", "risk_score_manual", "hour_utc", "flight_phase"
        ).collect()

        pipe = r.pipeline()
        for row in rows:
            key  = f"flight:{row['flight_id']}"
            data = {
                "flight_id":               row["flight_id"]        or "",
                "callsign":                row["callsign"]         or "",
                "airline":                 row["airline_icao"]     or "",
                "aircraft":                row["aircraft_model"]   or "",
                "registration":            row["registration"]     or "",
                "origin":                  row["origin"]           or "",
                "destination":             row["destination"]      or "",
                "lat":                     str(row["latitude"]     or 0),
                "lon":                     str(row["longitude"]    or 0),
                "altitude_ft":             str(row["altitude_ft"]  or 0),
                "speed_kn":                str(row["speed_kn"]     or 0),
                "precip_mm":               str(row["precipitation_mm"] or 0),
                "wind_knots":              str(row["wind_knots"]   or 0),
                "visibility_m":            str(row["visibility_m"] or 0),
                "weather_score":           f"{row['weather_score']:.3f}",
                "traffic_density":         str(row["traffic_density"] or 0),
                "distance_km":             f"{row['distance_km']:.1f}",
                "route_deviation":         f"{row['route_deviation']:.2f}",
                "predicted_delay_minutes": f"{row['predicted_delay_minutes']:.1f}",
                "delay_category":          row["delay_category"]  or "UNKNOWN",
                "capacity":                str(row["capacity"]              or 0),
                "affected_passengers":     str(row["affected_passengers"]   or 0),
                "fdi":                     f"{row['fdi']:.1f}",
                "fdi_category":            row["fdi_category"]  or "UNKNOWN",
                "estimated_compensation_eur": str(row["estimated_compensation_eur"] or 0),
                "risk_score":              f"{row['risk_score_manual']:.2f}",
                "hour_utc":                str(row["hour_utc"]     or 0),
                "flight_phase":            str(row["flight_phase"] or 0),
                "updated_at":              str(
                    __import__("datetime").datetime.utcnow().isoformat()
                ) + "Z",
            }
            pipe.hset(key, mapping=data)
            pipe.expire(key, 300)   # data expired setelah 5 menit

            # ── Track rotasi pesawat untuk Ripple Effect Score ──
            # Simpan urutan penerbangan per registrasi dalam sorted set
            reg = row["registration"]
            ts = row["timestamp"]
            if reg and ts:
                rotation_key = f"rotation:{reg}"
                pipe.zadd(rotation_key, {row["flight_id"]: int(ts)})
                pipe.expire(rotation_key, 86400)  # keep 24h

        # Ringkasan statistik batch
        delayed_count = sum(1 for r in rows
                            if r["delay_category"] in
                            ("MEDIUM DELAY", "CRITICAL DELAY"))
        critical_count = sum(1 for r in rows
                             if r["delay_category"] == "CRITICAL DELAY")
        high_fdi_count = sum(1 for r in rows
                             if r["fdi_category"] in ("HIGH", "CRITICAL"))
        avg_delay = (sum(r["predicted_delay_minutes"] or 0 for r in rows)
                     / max(len(rows), 1))
        avg_fdi = (sum(r["fdi"] or 0 for r in rows)
                   / max(len(rows), 1))
        total_passengers = sum(r["affected_passengers"] or 0 for r in rows)
        total_compensation = sum(r["estimated_compensation_eur"] or 0 for r in rows)

        stats = {
            "batch_id":                    str(batch_id),
            "total_flights":               str(len(rows)),
            "delayed_flights":             str(delayed_count),
            "critical_flights":            str(critical_count),
            "high_fdi_flights":            str(high_fdi_count),
            "avg_delay_minutes":           f"{avg_delay:.1f}",
            "avg_fdi":                     f"{avg_fdi:.1f}",
            "total_affected_passengers":   str(total_passengers),
            "total_estimated_compensation_eur": str(total_compensation),
            "updated_at":                  str(
                __import__("datetime").datetime.utcnow().isoformat()
            ) + "Z",
        }
        pipe.hset("stats:latest", mapping=stats)
        pipe.execute()

        print(f"[REDIS] Batch {batch_id}: {len(rows)} penerbangan | "
              f"Delayed: {delayed_count} | Critical: {critical_count} | "
              f"High FDI: {high_fdi_count} | Avg delay: {avg_delay:.1f} min | "
              f"Avg FDI: {avg_fdi:.1f} | Pax: {total_passengers} | "
              f"Compensation: EUR {total_compensation}")

    except Exception as e:
        print(f"[REDIS ERROR] Batch {batch_id}: {e}")


def write_to_kafka_out(df):
    """
    Menulis hasil prediksi regresi ke Kafka topic baru: flight-predictions
    Agar bisa dikonsumsi oleh anggota tim lain (Anggota 4 visualisasi).
    """
    output_cols = [
        "flight_id", "callsign", "airline_icao", "aircraft_model",
        "registration", "origin", "destination",
        "latitude", "longitude", "altitude_ft", "speed_kn",
        "predicted_delay_minutes", "delay_category",
        "capacity", "affected_passengers",
        "fdi", "fdi_category", "estimated_compensation_eur",
        "weather_score", "traffic_density",
        "distance_km", "route_deviation",
        "precipitation_mm", "wind_knots", "weather_code",
        "timestamp", "hour_utc", "flight_phase", "ingested_at"
    ]

    # Buat struct dan convert ke JSON string untuk dikirim ke Kafka
    kafka_out = (
        df.select(output_cols)
        .withColumn("value",
            F.to_json(F.struct(*output_cols)))
        .withColumn("key",
            F.col("flight_id"))
        .select("key", "value")
    )
    return kafka_out


def write_to_bronze(df):
    """
    🥉 Bronze Layer — Raw parsed data dari Kafka.
    Tidak ada cleaning/enrichment. Berfungsi sebagai arsip
    untuk replay jika ada bug di Silver.
    Partisi: ingestion_date
    """
    bronze_cols = [
        "flight_id", "callsign", "airline_icao", "aircraft_model",
        "registration", "origin", "destination",
        "latitude", "longitude", "altitude_ft", "speed_kn",
        "heading_deg", "on_ground", "timestamp",
        "precipitation_mm", "wind_knots", "visibility_m", "weather_code",
        "ingested_at"
    ]
    return (
        df.select(bronze_cols)
        .withColumn("ingestion_date",
            F.to_date(F.from_unixtime(F.col("timestamp"))))
    )


def write_to_silver(df):
    """
    🥈 Silver Layer — Cleaned + enriched + ML predictions.
    Single source of truth untuk analisis dan Gold aggregation.
    Partisi: processing_date
    """
    silver_cols = [
        "flight_id", "callsign", "airline_icao", "aircraft_model",
        "registration", "origin", "destination",
        "latitude", "longitude", "altitude_ft", "speed_kn",
        "predicted_delay_minutes", "delay_category",
        "capacity", "affected_passengers",
        "fdi", "fdi_category", "estimated_compensation_eur",
        "weather_score", "traffic_density",
        "distance_km", "route_deviation",
        "precipitation_mm", "wind_knots", "visibility_m", "weather_code",
        "timestamp", "hour_utc", "flight_phase", "ingested_at"
    ]
    return (
        df.select(silver_cols)
        .withColumn("processing_date", F.current_date())
    )


def main():
    print("=" * 60)
    print("  SPARK FLIGHT DELAY PREDICTION — Starting...")
    print("  Medallion Architecture: Bronze → Silver → Gold")
    print("=" * 60)

    spark = create_spark_session()

    # ── Load model ML jika ada ──────────────────────────────
    model = None
    if os.path.exists(MODEL_PATH):
        try:
            model = PipelineModel.load(MODEL_PATH)
            print(f"[ML] Model berhasil dimuat dari: {MODEL_PATH}")
        except Exception as e:
            print(f"[ML] Gagal load model: {e} → pakai rule-based fallback")
    else:
        print(f"[ML] Model tidak ditemukan di '{MODEL_PATH}'.")
        print("[ML] Gunakan rule-based scoring. Jalankan train_model.py terlebih dahulu.")

    # ── Baca dari Kafka ─────────────────────────────────────
    raw_df = read_kafka_stream(spark)

    # ── Parse JSON (input untuk Bronze & Silver) ────────────
    parsed_df = parse_json(raw_df)

    # ── Pipeline Silver: Clean → Feature Eng → ML ───────────
    cleaned_df  = clean_data(parsed_df)
    featured_df = feature_engineering(cleaned_df)
    delay_df    = predict_delay(featured_df, model)
    result_df   = compute_impact_metrics(delay_df)

    # ── Sink 1: Redis (untuk visualisasi real-time) ─────────
    query_redis = (
        result_df.writeStream
        .outputMode("append")
        .foreachBatch(write_to_redis)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/redis")
        .trigger(processingTime="10 seconds")
        .start()
    )
    print("[STREAM] Sink 1 — Redis (real-time) dimulai.")

    # ── Sink 2: Kafka topic baru flight-predictions ─────────
    kafka_out_df = write_to_kafka_out(result_df)
    query_kafka = (
        kafka_out_df.writeStream
        .outputMode("append")
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", KAFKA_TOPIC_OUT)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/kafka_out")
        .trigger(processingTime="10 seconds")
        .start()
    )
    print(f"[STREAM] Sink 2 — Kafka output → topic: {KAFKA_TOPIC_OUT}")

    # ── Sink 3: Delta Lake BRONZE (raw parsed data) ─────────
    BRONZE_PATH = "/app/delta_lake/bronze/raw_flights"
    bronze_df = write_to_bronze(parsed_df)
    query_bronze = (
        bronze_df.writeStream
        .outputMode("append")
        .format("delta")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/delta_bronze")
        .partitionBy("ingestion_date")
        .trigger(processingTime="10 seconds")
        .start(BRONZE_PATH)
    )
    print(f"[STREAM] Sink 3 — 🥉 Delta BRONZE → {BRONZE_PATH}")

    # ── Sink 4: Delta Lake SILVER (enriched + ML) ───────────
    SILVER_PATH = "/app/delta_lake/silver/enriched_flights"
    silver_df = write_to_silver(result_df)
    query_silver = (
        silver_df.writeStream
        .outputMode("append")
        .format("delta")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/delta_silver")
        .partitionBy("processing_date")
        .trigger(processingTime="10 seconds")
        .start(SILVER_PATH)
    )
    print(f"[STREAM] Sink 4 — 🥈 Delta SILVER → {SILVER_PATH}")

    # ── Sink 5: Console (debug — lihat di terminal) ─────────
    query_console = (
        result_df.select(
            "flight_id", "callsign", "airline_icao",
            "origin", "destination",
            "altitude_ft", "speed_kn",
            "predicted_delay_minutes", "delay_category",
            "fdi", "fdi_category", "affected_passengers",
            "estimated_compensation_eur"
        )
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .option("numRows", 10)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/console")
        .trigger(processingTime="10 seconds")
        .start()
    )
    print("[STREAM] Sink 5 — Console (debug) dimulai.")

    print("\n[INFO] Semua stream berjalan (5 sinks).")
    print("[INFO] Medallion: Bronze ✅ | Silver ✅ | Gold → jalankan delta_aggregator.py")
    print("[INFO] Tekan Ctrl+C untuk berhenti.\n")

    # Tunggu sampai semua query selesai / ada error
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n[INFO] Dihentikan oleh user.")
    finally:
        query_redis.stop()
        query_kafka.stop()
        query_bronze.stop()
        query_silver.stop()
        query_console.stop()
        spark.stop()
        print("[INFO] SparkSession ditutup.")


if __name__ == "__main__":
    main()
