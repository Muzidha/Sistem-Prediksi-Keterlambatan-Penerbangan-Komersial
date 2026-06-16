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
    """Membuat SparkSession dengan package Kafka."""
    print("[SPARK] Menginisialisasi SparkSession...")
    spark = (
        SparkSession.builder
        .appName("FlightDelayPrediction")
        # Package kafka connector untuk Spark — sesuaikan versi Scala/Spark kamu
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
        )
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


def parse_and_clean(raw_df):
    """
    Step 1: Parse JSON dari kolom 'value' Kafka.
    Step 2: Data Cleaning — buang baris dengan field kritis kosong,
            filter nilai tidak masuk akal.
    """
    # Parse JSON
    parsed = (
        raw_df
        .select(
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(F.col("value").cast("string"), flight_schema).alias("d")
        )
        .select("kafka_timestamp", "d.*")
    )

    # Flatten nested weather struct
    cleaned = (
        parsed
        .withColumn("precipitation_mm", F.col("weather.precipitation_mm"))
        .withColumn("wind_knots",        F.col("weather.wind_knots"))
        .withColumn("visibility_m",      F.col("weather.visibility_m"))
        .withColumn("weather_code",      F.col("weather.weather_code"))
        .drop("weather")
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
            "origin", "destination",
            "latitude", "longitude", "altitude_ft", "speed_kn",
            "precipitation_mm", "wind_knots", "visibility_m",
            "weather_score", "traffic_density",
            "distance_km", "route_deviation",
            "predicted_delay_minutes", "delay_category",
            "risk_score_manual", "hour_utc", "flight_phase"
        ).collect()

        pipe = r.pipeline()
        for row in rows:
            key  = f"flight:{row['flight_id']}"
            data = {
                "flight_id":               row["flight_id"]        or "",
                "callsign":                row["callsign"]         or "",
                "airline":                 row["airline_icao"]     or "",
                "aircraft":                row["aircraft_model"]   or "",
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
                "risk_score":              f"{row['risk_score_manual']:.2f}",
                "hour_utc":                str(row["hour_utc"]     or 0),
                "flight_phase":            str(row["flight_phase"] or 0),
                "updated_at":              str(
                    __import__("datetime").datetime.utcnow().isoformat()
                ) + "Z",
            }
            pipe.hset(key, mapping=data)
            pipe.expire(key, 300)   # data expired setelah 5 menit

        # Ringkasan statistik batch
        delayed_count = sum(1 for r in rows
                            if r["delay_category"] in
                            ("MEDIUM DELAY", "CRITICAL DELAY"))
        critical_count = sum(1 for r in rows
                             if r["delay_category"] == "CRITICAL DELAY")
        avg_delay = (sum(r["predicted_delay_minutes"] or 0 for r in rows)
                     / max(len(rows), 1))

        stats = {
            "batch_id":          str(batch_id),
            "total_flights":     str(len(rows)),
            "delayed_flights":   str(delayed_count),
            "critical_flights":  str(critical_count),
            "avg_delay_minutes": f"{avg_delay:.1f}",
            "updated_at":        str(
                __import__("datetime").datetime.utcnow().isoformat()
            ) + "Z",
        }
        pipe.hset("stats:latest", mapping=stats)
        pipe.execute()

        print(f"[REDIS] Batch {batch_id}: {len(rows)} penerbangan | "
              f"Delayed: {delayed_count} | Critical: {critical_count} | "
              f"Avg delay: {avg_delay:.1f} min")

    except Exception as e:
        print(f"[REDIS ERROR] Batch {batch_id}: {e}")


def write_to_kafka_out(df):
    """
    Menulis hasil prediksi regresi ke Kafka topic baru: flight-predictions
    Agar bisa dikonsumsi oleh anggota tim lain (Anggota 4 visualisasi).
    """
    output_cols = [
        "flight_id", "callsign", "airline_icao", "aircraft_model",
        "origin", "destination",
        "latitude", "longitude", "altitude_ft", "speed_kn",
        "predicted_delay_minutes", "delay_category",
        "weather_score", "traffic_density",
        "distance_km", "route_deviation",
        "precipitation_mm", "wind_knots", "weather_code",
        "hour_utc", "flight_phase", "ingested_at"
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


def main():
    print("=" * 60)
    print("  SPARK FLIGHT DELAY PREDICTION — Starting...")
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

    # ── Pipeline transformasi ───────────────────────────────
    cleaned_df  = parse_and_clean(raw_df)
    featured_df = feature_engineering(cleaned_df)
    result_df   = predict_delay(featured_df, model)

    # ── Sink 1: Redis (untuk visualisasi real-time) ─────────
    query_redis = (
        result_df.writeStream
        .outputMode("append")
        .foreachBatch(write_to_redis)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/redis")
        .trigger(processingTime="10 seconds")   # proses tiap 10 detik
        .start()
    )
    print("[STREAM] Redis sink dimulai.")

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
    print(f"[STREAM] Kafka output sink dimulai → topic: {KAFKA_TOPIC_OUT}")

    # ── Sink 3: Console (debug — lihat di terminal) ─────────
    query_console = (
        result_df.select(
            "flight_id", "callsign", "airline_icao",
            "origin", "destination",
            "altitude_ft", "speed_kn",
            "predicted_delay_minutes", "delay_category"
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
    print("[STREAM] Console sink dimulai (debug).")

    print("\n[INFO] Semua stream berjalan. Tekan Ctrl+C untuk berhenti.\n")

    # Tunggu sampai semua query selesai / ada error
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n[INFO] Dihentikan oleh user.")
    finally:
        query_redis.stop()
        query_kafka.stop()
        query_console.stop()
        spark.stop()
        print("[INFO] SparkSession ditutup.")


if __name__ == "__main__":
    main()
