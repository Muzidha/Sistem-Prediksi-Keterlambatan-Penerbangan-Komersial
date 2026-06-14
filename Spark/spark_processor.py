"""
=============================================================
  BAGIAN 2 — Spark Structured Streaming + ML Inference
  Anggota 2: Spark Processing & Prediksi Keterlambatan
=============================================================
  Alur kerja:
    Kafka (commercial-flight-stream)
      → Spark readStream
      → Parsing JSON + Data Cleaning
      → Feature Engineering
      → ML Inference (model dari train_model.py)
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
    Feature Engineering untuk model ML.
    Membuat fitur-fitur baru dari data mentah.
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

        # ── Fitur Penerbangan ────────────────────────────────
        # Fase penerbangan berdasarkan altitude
        # Climbing: < 15000 ft, Cruising: 15000-35000 ft, Descending: 35000+ ft
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

        # ── Fitur Waktu ──────────────────────────────────────
        # Jam UTC dari timestamp (jam peak = 06-09, 17-20)
        .withColumn("hour_utc",
            F.hour(F.from_unixtime(F.col("timestamp"))))
        .withColumn("is_peak_hour",
            (((F.col("hour_utc") >= 6) & (F.col("hour_utc") <= 9)) |
             ((F.col("hour_utc") >= 17) & (F.col("hour_utc") <= 20)))
            .cast(IntegerType()))

        # Hari dalam seminggu (0=Senin, 6=Minggu)
        .withColumn("day_of_week",
            F.dayofweek(F.from_unixtime(F.col("timestamp"))))

        # Weekend?
        .withColumn("is_weekend",
            ((F.col("day_of_week") == 1) | (F.col("day_of_week") == 7))
            .cast(IntegerType()))

        # ── Skor Risiko Manual (rule-based fallback) ─────────
        # Dipakai jika model ML belum dilatih
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
    Prediksi keterlambatan.
    - Jika model ML tersedia: gunakan PipelineModel dari MLlib.
    - Jika tidak: gunakan risk_score_manual sebagai fallback.
    """
    if model is not None:
        # Gunakan model MLlib yang sudah dilatih
        predictions = model.transform(df)
        result = (
            predictions
            .withColumn("delay_probability",
                # Ambil probabilitas kelas 1 (terlambat) dari vektor
                F.udf(lambda v: float(v[1]) if v is not None else 0.5,
                      FloatType())(F.col("probability")))
            .withColumn("predicted_delay",
                F.col("prediction").cast(IntegerType()))
        )
    else:
        # Fallback: rule-based dari risk_score_manual
        # Score 0-10, normalize ke 0-1 sebagai probability
        result = (
            df
            .withColumn("delay_probability",
                F.least(F.col("risk_score_manual") / 10.0,
                        F.lit(0.99)))
            .withColumn("predicted_delay",
                (F.col("delay_probability") >= 0.5).cast(IntegerType()))
        )

    # Tambah label kategori risiko
    result = result.withColumn("delay_category",
        F.when(F.col("delay_probability") < 0.3,  "LOW")
         .when(F.col("delay_probability") < 0.6,  "MEDIUM")
         .when(F.col("delay_probability") < 0.8,  "HIGH")
         .otherwise("CRITICAL"))

    return result


def write_to_redis(batch_df, batch_id):
    """
    Menulis hasil prediksi ke Redis.
    Format key: flight:<flight_id>
    Expire: 300 detik (5 menit) — data streaming terus diperbarui
    """
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT,
                        decode_responses=True)

        rows = batch_df.select(
            "flight_id", "callsign", "airline_icao", "aircraft_model",
            "origin", "destination",
            "latitude", "longitude", "altitude_ft", "speed_kn",
            "precipitation_mm", "wind_knots", "visibility_m",
            "delay_probability", "predicted_delay", "delay_category",
            "risk_score_manual", "hour_utc", "flight_phase"
        ).collect()

        pipe = r.pipeline()
        for row in rows:
            key  = f"flight:{row['flight_id']}"
            data = {
                "flight_id":        row["flight_id"]        or "",
                "callsign":         row["callsign"]          or "",
                "airline":          row["airline_icao"]      or "",
                "aircraft":         row["aircraft_model"]    or "",
                "origin":           row["origin"]            or "",
                "destination":      row["destination"]       or "",
                "lat":              str(row["latitude"]      or 0),
                "lon":              str(row["longitude"]     or 0),
                "altitude_ft":      str(row["altitude_ft"]   or 0),
                "speed_kn":         str(row["speed_kn"]      or 0),
                "precip_mm":        str(row["precipitation_mm"] or 0),
                "wind_knots":       str(row["wind_knots"]    or 0),
                "visibility_m":     str(row["visibility_m"]  or 0),
                "delay_prob":       f"{row['delay_probability']:.3f}",
                "predicted_delay":  str(row["predicted_delay"] or 0),
                "delay_category":   row["delay_category"]   or "UNKNOWN",
                "risk_score":       f"{row['risk_score_manual']:.2f}",
                "hour_utc":         str(row["hour_utc"]      or 0),
                "flight_phase":     str(row["flight_phase"]  or 0),
                "updated_at":       str(
                    __import__("datetime").datetime.utcnow().isoformat()
                ) + "Z",
            }
            pipe.hset(key, mapping=data)
            pipe.expire(key, 300)   # data expired setelah 5 menit

        # Simpan juga ringkasan statistik batch di key khusus
        stats = {
            "batch_id":         str(batch_id),
            "total_flights":    str(len(rows)),
            "high_risk":        str(sum(1 for r in rows
                                        if r["delay_category"] in ("HIGH","CRITICAL"))),
            "updated_at":       str(
                __import__("datetime").datetime.utcnow().isoformat()
            ) + "Z",
        }
        pipe.hset("stats:latest", mapping=stats)
        pipe.execute()

        print(f"[REDIS] Batch {batch_id}: {len(rows)} penerbangan "
              f"ditulis ke Redis | "
              f"HIGH/CRITICAL: {stats['high_risk']}")

    except Exception as e:
        print(f"[REDIS ERROR] Batch {batch_id}: {e}")


def write_to_kafka_out(df):
    """
    Menulis hasil prediksi ke Kafka topic baru: flight-predictions
    Agar bisa dikonsumsi oleh anggota tim lain.
    """
    output_cols = [
        "flight_id", "callsign", "airline_icao", "origin", "destination",
        "latitude", "longitude", "altitude_ft", "speed_kn",
        "delay_probability", "predicted_delay", "delay_category",
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
            "delay_probability", "delay_category"
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
