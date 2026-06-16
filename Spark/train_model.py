"""
=============================================================
  train_model.py — Training Model Prediksi Keterlambatan
  Anggota 3: Upgrade ke Regression (prediksi menit delay)
=============================================================
  Jalankan SEKALI sebelum spark_processor.py:
      python train_model.py

  Script ini:
  1. Generate data sintetis dengan fitur lengkap
  2. Latih RandomForestRegressor dengan MLlib Pipeline
     (StringIndexer → OneHotEncoder → VectorAssembler → Scaler → RF)
  3. Evaluasi model dengan RMSE dan MAE
  4. Simpan model ke folder ./model_keterlambatan
=============================================================
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    FloatType, IntegerType, StringType
)
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer, OneHotEncoder,
    VectorAssembler, StandardScaler
)
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

MODEL_PATH = os.getenv("MODEL_PATH", "./model_keterlambatan")

# ── Daftar aircraft dan airline untuk data sintetis ──────────
AIRCRAFT_MODELS = [
    "B738", "A320", "B77W", "A332", "A20N",
    "B739", "A321", "B788", "A333", "B734",
]
AIRLINE_ICAOS = [
    "GIA", "LNI", "BTK", "SJY", "CTV",
    "IDX", "AWQ", "TGN", "NAM", "XAX",
]


def create_spark():
    spark = (
        SparkSession.builder
        .appName("TrainDelayRegressionModel")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def generate_synthetic_data(spark, n_samples=50000):
    """
    Generate data training sintetis untuk model regresi.

    Target: delay_minutes (float)
      - Distribusi realistis: kebanyakan -5 s/d 30, outlier sampai 180
      - Negatif = lebih awal dari jadwal

    Fitur wajib:
      - aircraft_model (string) → akan di-StringIndex + OHE
      - airline_icao   (string) → akan di-StringIndex + OHE
      - weather_score  (float 0–1, komposit kondisi cuaca)
      - traffic_density (int 1–10)
      - distance_km    (float, jarak rute penerbangan)
      - route_deviation (float, deviasi dari rute optimal)

    Fitur tambahan (cuaca & penerbangan):
      - precipitation_mm, wind_knots, visibility_m, weather_code
      - altitude_ft, speed_kn, heading_deg
      - hour_utc, day_of_week
    """
    import random
    random.seed(42)

    schema = StructType([
        StructField("aircraft_model",    StringType(),  False),
        StructField("airline_icao",      StringType(),  False),
        StructField("precipitation_mm",  FloatType(),   False),
        StructField("wind_knots",        FloatType(),   False),
        StructField("visibility_m",      FloatType(),   False),
        StructField("weather_code",      IntegerType(), False),
        StructField("altitude_ft",       IntegerType(), False),
        StructField("speed_kn",          IntegerType(), False),
        StructField("heading_deg",       IntegerType(), False),
        StructField("hour_utc",          IntegerType(), False),
        StructField("day_of_week",       IntegerType(), False),
        StructField("weather_score",     FloatType(),   False),
        StructField("traffic_density",   IntegerType(), False),
        StructField("distance_km",       FloatType(),   False),
        StructField("route_deviation",   FloatType(),   False),
        StructField("delay_minutes",     FloatType(),   False),
    ])

    # Bobot delay per aircraft (beberapa model lebih tua / kurang reliabel)
    aircraft_delay_bias = {
        "B738": -2.0, "A320": -1.5, "B77W": 0.0, "A332": 0.5, "A20N": -3.0,
        "B739": -1.0, "A321": -1.0, "B788": -2.5, "A333": 1.0, "B734": 3.0,
    }
    # Bobot delay per airline (kualitas operasional berbeda)
    airline_delay_bias = {
        "GIA": -2.0, "LNI":  3.0, "BTK": 1.0, "SJY": -1.5, "CTV": 2.0,
        "IDX":  0.5, "AWQ":  1.5, "TGN": 0.0, "NAM":  0.5, "XAX": -1.0,
    }

    rows = []
    for _ in range(n_samples):
        # ── Random fitur ──────────────────────────────────
        ac_model   = random.choice(AIRCRAFT_MODELS)
        al_icao    = random.choice(AIRLINE_ICAOS)

        # Cuaca: gunakan beta distribution agar kebanyakan cuaca baik
        weather_score = round(random.betavariate(2, 5), 3)

        precip     = round(weather_score * random.uniform(0, 50), 1)
        wind       = round(random.uniform(0, 60), 1)
        visibility = round(max(100, 15000 * (1 - weather_score)
                               + random.gauss(0, 1500)), 0)
        # WMO weather code: cuaca baik = 0-3, hujan = 61-65, badai = 80-99
        if weather_score > 0.7:
            wcode = random.choice([61, 63, 65, 80, 82, 95, 96, 99])
        elif weather_score > 0.3:
            wcode = random.choice([1, 2, 3, 51, 53, 61])
        else:
            wcode = random.choice([0, 0, 0, 1, 2, 3])

        altitude   = random.randint(5000, 45000)
        speed      = random.randint(150, 600)
        heading    = random.randint(0, 360)
        hour       = random.randint(0, 23)
        dow        = random.randint(1, 7)

        traffic_density = random.randint(1, 10)
        # Peak hours → traffic lebih padat
        if hour in range(6, 10) or hour in range(17, 21):
            traffic_density = min(10, traffic_density + random.randint(1, 3))

        distance_km     = round(random.uniform(200, 5000), 1)
        route_deviation = round(max(0.0, random.gauss(5, 10)), 2)

        # ── Hitung delay_minutes (target regresi) ─────────
        delay = random.gauss(5, 8)  # base delay

        # Dampak cuaca (faktor utama delay)
        delay += weather_score * random.uniform(15, 45)
        if wind > 30:
            delay += random.uniform(5, 15)
        if visibility < 3000:
            delay += random.uniform(5, 20)

        # Dampak traffic
        delay += (traffic_density - 5) * 1.5

        # Dampak jarak (penerbangan panjang → lebih banyak variasi)
        if distance_km > 3000:
            delay += random.uniform(0, 12)
        elif distance_km < 500:
            delay -= random.uniform(0, 5)

        # Deviasi rute → tambahan delay
        delay += route_deviation * 0.4

        # Bias per aircraft & airline
        delay += aircraft_delay_bias.get(ac_model, 0)
        delay += airline_delay_bias.get(al_icao, 0)

        # Peak hours → congestion
        if hour in range(6, 10) or hour in range(17, 21):
            delay += random.uniform(3, 10)

        # Weekend sedikit lebih baik
        if dow in (1, 7):
            delay -= random.uniform(0, 3)

        # Noise
        delay += random.gauss(0, 4)

        # Clamp ke range realistis: -10 s/d 180 menit
        delay = round(max(-10.0, min(delay, 180.0)), 1)

        rows.append((
            ac_model, al_icao,
            precip, wind, float(visibility), wcode,
            altitude, speed, heading, hour, dow,
            weather_score, traffic_density, distance_km,
            route_deviation, delay,
        ))

    df = spark.createDataFrame(rows, schema)
    print(f"[TRAIN] Data sintetis: {n_samples} baris")
    print(f"[TRAIN] Statistik delay_minutes:")
    df.select(
        F.round(F.mean("delay_minutes"), 2).alias("mean"),
        F.round(F.stddev("delay_minutes"), 2).alias("stddev"),
        F.round(F.min("delay_minutes"), 2).alias("min"),
        F.round(F.max("delay_minutes"), 2).alias("max"),
        F.round(F.expr("percentile_approx(delay_minutes, 0.5)"), 2).alias("median"),
    ).show(truncate=False)

    # Distribusi kategori delay
    df.withColumn("delay_category",
        F.when(F.col("delay_minutes") < 15,  "ON TIME")
         .when(F.col("delay_minutes") <= 60, "MEDIUM DELAY")
         .otherwise("CRITICAL DELAY")
    ).groupBy("delay_category").count().orderBy("delay_category").show()

    return df


def add_features(df):
    """
    Feature engineering identik dengan yang ada di spark_processor.py.
    Memastikan konsistensi antara training dan inference.
    """
    return (
        df
        # ── Fitur biner cuaca ─────────────────────────────
        .withColumn("is_raining",
            (F.col("precipitation_mm") > 0.5).cast(IntegerType()))
        .withColumn("is_high_wind",
            (F.col("wind_knots") > 25).cast(IntegerType()))
        .withColumn("is_low_visibility",
            (F.col("visibility_m") < 3000).cast(IntegerType()))
        .withColumn("is_bad_weather",
            ((F.col("weather_code") >= 60) & (F.col("weather_code") <= 99))
            .cast(IntegerType()))

        # ── Fitur penerbangan ─────────────────────────────
        .withColumn("flight_phase",
            F.when(F.col("altitude_ft") < 15000, 0)
             .when(F.col("altitude_ft") < 35000, 1)
             .otherwise(2))
        .withColumn("is_slow",
            (F.col("speed_kn") < 200).cast(IntegerType()))
        .withColumn("speed_altitude_ratio",
            F.when(F.col("altitude_ft") > 0,
                   F.col("speed_kn") / F.col("altitude_ft") * 1000)
             .otherwise(0.0))

        # ── Fitur waktu ───────────────────────────────────
        .withColumn("is_peak_hour",
            (((F.col("hour_utc") >= 6) & (F.col("hour_utc") <= 9)) |
             ((F.col("hour_utc") >= 17) & (F.col("hour_utc") <= 20)))
            .cast(IntegerType()))
        .withColumn("is_weekend",
            ((F.col("day_of_week") == 1) | (F.col("day_of_week") == 7))
            .cast(IntegerType()))
    )


def build_pipeline():
    """
    ML Pipeline untuk regresi delay_minutes:
      StringIndexer (aircraft_model, airline_icao)
      → OneHotEncoder
      → VectorAssembler (semua fitur numerik + OHE)
      → StandardScaler
      → RandomForestRegressor
    """
    # ── Stage 1: StringIndexer untuk fitur kategorikal ────
    # handleInvalid="keep" agar label baru saat streaming tidak error
    idx_aircraft = StringIndexer(
        inputCol="aircraft_model",
        outputCol="aircraft_model_idx",
        handleInvalid="keep"
    )
    idx_airline = StringIndexer(
        inputCol="airline_icao",
        outputCol="airline_icao_idx",
        handleInvalid="keep"
    )

    # ── Stage 2: OneHotEncoder ─────────────────────────────
    ohe = OneHotEncoder(
        inputCols=["aircraft_model_idx", "airline_icao_idx"],
        outputCols=["aircraft_model_ohe", "airline_icao_ohe"],
        handleInvalid="keep"
    )

    # ── Stage 3: VectorAssembler ───────────────────────────
    numeric_cols = [
        # Fitur wajib Anggota 3
        "weather_score", "traffic_density", "distance_km", "route_deviation",
        # Fitur cuaca mentah
        "precipitation_mm", "wind_knots", "visibility_m", "weather_code",
        # Fitur penerbangan
        "altitude_ft", "speed_kn", "heading_deg",
        "flight_phase", "speed_altitude_ratio",
        # Fitur biner
        "is_raining", "is_high_wind", "is_low_visibility",
        "is_bad_weather", "is_slow",
        # Fitur waktu
        "hour_utc", "day_of_week", "is_peak_hour", "is_weekend",
    ]
    ohe_cols = ["aircraft_model_ohe", "airline_icao_ohe"]

    assembler = VectorAssembler(
        inputCols=numeric_cols + ohe_cols,
        outputCol="features_raw",
        handleInvalid="skip"
    )

    # ── Stage 4: StandardScaler ────────────────────────────
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withMean=True,
        withStd=True
    )

    # ── Stage 5: RandomForestRegressor ─────────────────────
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol="delay_minutes",
        predictionCol="prediction",
        numTrees=100,
        maxDepth=8,
        seed=42,
    )

    return Pipeline(stages=[
        idx_aircraft, idx_airline,
        ohe,
        assembler,
        scaler,
        rf,
    ])


def main():
    print("=" * 60)
    print("  TRAINING MODEL REGRESI PREDIKSI DELAY (MENIT)")
    print("  Anggota 3 — RandomForestRegressor + Pipeline MLlib")
    print("=" * 60)

    spark = create_spark()

    # 1. Generate data training
    print("\n[1/4] Membuat data training sintetis...")
    raw_df = generate_synthetic_data(spark, n_samples=50000)

    # 2. Feature engineering
    print("\n[2/4] Feature engineering...")
    df = add_features(raw_df)
    df.cache()

    # 3. Split train/test
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    print(f"[TRAIN] Train: {train_df.count()} | Test: {test_df.count()}")

    # 4. Training
    print("\n[3/4] Training RandomForestRegressor (50k data, 100 trees)...")
    pipeline = build_pipeline()
    model    = pipeline.fit(train_df)
    print("[TRAIN] Training selesai!")

    # 5. Evaluasi
    print("\n[4/4] Evaluasi model...")
    predictions = model.transform(test_df)

    rmse_eval = RegressionEvaluator(
        labelCol="delay_minutes",
        predictionCol="prediction",
        metricName="rmse"
    )
    mae_eval = RegressionEvaluator(
        labelCol="delay_minutes",
        predictionCol="prediction",
        metricName="mae"
    )
    r2_eval = RegressionEvaluator(
        labelCol="delay_minutes",
        predictionCol="prediction",
        metricName="r2"
    )

    rmse = rmse_eval.evaluate(predictions)
    mae  = mae_eval.evaluate(predictions)
    r2   = r2_eval.evaluate(predictions)

    print("\n" + "─" * 45)
    print(f"  RMSE     : {rmse:.4f} menit")
    print(f"  MAE      : {mae:.4f} menit")
    print(f"  R²       : {r2:.4f}")
    print("─" * 45)

    # Contoh prediksi
    print("\nContoh 15 prediksi (actual vs predicted):")
    predictions.select(
        "aircraft_model", "airline_icao",
        F.round("delay_minutes", 1).alias("actual_min"),
        F.round("prediction", 1).alias("predicted_min"),
        F.round(F.abs(F.col("delay_minutes") - F.col("prediction")), 1)
            .alias("error_min"),
        F.when(F.col("prediction") < 15,  "ON TIME")
         .when(F.col("prediction") <= 60, "MEDIUM DELAY")
         .otherwise("CRITICAL DELAY")
         .alias("pred_category"),
    ).show(15, truncate=False)

    # Distribusi error
    print("Distribusi error prediksi:")
    predictions.select(
        F.round(F.mean(F.abs(F.col("delay_minutes") - F.col("prediction"))), 2)
            .alias("mean_abs_error"),
        F.round(F.expr("percentile_approx(abs(delay_minutes - prediction), 0.5)"), 2)
            .alias("median_abs_error"),
        F.round(F.expr("percentile_approx(abs(delay_minutes - prediction), 0.9)"), 2)
            .alias("p90_abs_error"),
    ).show(truncate=False)

    # 6. Simpan model
    print(f"\n[SAVE] Menyimpan model ke: {MODEL_PATH}")
    model.write().overwrite().save(MODEL_PATH)
    print(f"[SAVE] Model tersimpan! ✓")
    print("\nSekarang jalankan spark_processor.py untuk streaming inference.")

    spark.stop()


if __name__ == "__main__":
    main()
