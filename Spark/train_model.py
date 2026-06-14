"""
=============================================================
  train_model.py — Training Model Prediksi Keterlambatan
=============================================================
  Jalankan SEKALI sebelum spark_processor.py:
      python train_model.py

  Script ini:
  1. Generate data sintetis (karena kita tidak punya dataset
     historis keterlambatan — data real-time saja)
  2. Latih Random Forest Classifier dengan MLlib
  3. Simpan model ke folder ./model_keterlambatan
=============================================================
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

MODEL_PATH = os.getenv("MODEL_PATH", "./model_keterlambatan")


def create_spark():
    spark = (
        SparkSession.builder
        .appName("TrainDelayModel")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def generate_synthetic_data(spark, n_samples=50000):
    """
    Generate data training sintetis yang mencerminkan pola nyata.

    Logika label (terlambat = 1):
    - Hujan deras + visibilitas rendah → kemungkinan delay tinggi
    - Jam sibuk (peak hour) → kemungkinan delay sedang
    - Angin kencang → kemungkinan delay sedang-tinggi
    - Cuaca buruk (weather_code 60-99) → delay tinggi
    """
    import random
    random.seed(42)

    schema = StructType([
        StructField("precipitation_mm",  FloatType(), False),
        StructField("wind_knots",        FloatType(), False),
        StructField("visibility_m",      FloatType(), False),
        StructField("weather_code",      IntegerType(), False),
        StructField("altitude_ft",       IntegerType(), False),
        StructField("speed_kn",          IntegerType(), False),
        StructField("heading_deg",       IntegerType(), False),
        StructField("hour_utc",          IntegerType(), False),
        StructField("day_of_week",       IntegerType(), False),
        StructField("label",             IntegerType(), False),
    ])

    rows = []
    for _ in range(n_samples):
        # Random features
        precip      = random.uniform(0, 50)
        wind        = random.uniform(0, 60)
        visibility  = random.uniform(100, 15000)
        wcode       = random.choice(
            [0]*30 + [1]*10 + [2]*10 + [3]*10 +
            [61,63,65,71,73,80,82,95,96,99]*3
        )
        altitude    = random.randint(5000, 45000)
        speed       = random.randint(150, 600)
        heading     = random.randint(0, 360)
        hour        = random.randint(0, 23)
        dow         = random.randint(1, 7)

        # Hitung skor risiko untuk menentukan label
        risk = 0.0
        if precip > 10:     risk += 3.0
        elif precip > 0.5:  risk += 1.5
        if wind > 30:       risk += 3.0
        elif wind > 20:     risk += 1.5
        if visibility < 1000:  risk += 3.5
        elif visibility < 3000: risk += 2.0
        if wcode >= 80:    risk += 3.5
        elif wcode >= 60:  risk += 2.0
        if hour in range(6, 10) or hour in range(17, 21):
            risk += 1.5
        if dow in (1, 7):  risk += 0.5
        if speed < 180:    risk += 1.0

        # Tambah noise acak
        risk += random.gauss(0, 1.5)

        # Label: terlambat jika risk >= 4.5
        label = 1 if risk >= 4.5 else 0

        rows.append((precip, wind, visibility, wcode, altitude,
                     speed, heading, hour, dow, label))

    df = spark.createDataFrame(rows, schema)
    print(f"[TRAIN] Data sintetis: {n_samples} baris")
    print(f"[TRAIN] Distribusi label:")
    df.groupBy("label").count().show()
    return df


def add_features(df):
    """Tambah feature engineering yang sama dengan spark_processor.py"""
    return (
        df
        .withColumn("is_raining",
            (F.col("precipitation_mm") > 0.5).cast(IntegerType()))
        .withColumn("is_high_wind",
            (F.col("wind_knots") > 25).cast(IntegerType()))
        .withColumn("is_low_visibility",
            (F.col("visibility_m") < 3000).cast(IntegerType()))
        .withColumn("is_bad_weather",
            ((F.col("weather_code") >= 60) & (F.col("weather_code") <= 99))
            .cast(IntegerType()))
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
        .withColumn("is_peak_hour",
            (((F.col("hour_utc") >= 6) & (F.col("hour_utc") <= 9)) |
             ((F.col("hour_utc") >= 17) & (F.col("hour_utc") <= 20)))
            .cast(IntegerType()))
        .withColumn("is_weekend",
            ((F.col("day_of_week") == 1) | (F.col("day_of_week") == 7))
            .cast(IntegerType()))
    )


def build_pipeline():
    """Bangun ML Pipeline: VectorAssembler → Scaler → RandomForest."""
    feature_cols = [
        # Fitur cuaca
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

    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features_raw",
        handleInvalid="skip"
    )

    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withMean=True,
        withStd=True
    )

    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="label",
        numTrees=100,
        maxDepth=8,
        seed=42,
        probabilityCol="probability",
        predictionCol="prediction"
    )

    return Pipeline(stages=[assembler, scaler, rf])


def main():
    print("=" * 60)
    print("  TRAINING MODEL PREDIKSI KETERLAMBATAN PENERBANGAN")
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
    print("\n[3/4] Training Random Forest (50k data, 100 trees)...")
    pipeline = build_pipeline()
    model    = pipeline.fit(train_df)
    print("[TRAIN] Training selesai!")

    # 5. Evaluasi
    print("\n[4/4] Evaluasi model...")
    predictions = model.transform(test_df)

    auc_eval = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )
    acc_eval = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="accuracy"
    )
    f1_eval = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="f1"
    )

    auc = auc_eval.evaluate(predictions)
    acc = acc_eval.evaluate(predictions)
    f1  = f1_eval.evaluate(predictions)

    print("\n" + "─" * 40)
    print(f"  AUC-ROC  : {auc:.4f}")
    print(f"  Accuracy : {acc:.4f}")
    print(f"  F1-Score : {f1:.4f}")
    print("─" * 40)

    # Tampilkan confusion matrix sederhana
    print("\nContoh 10 prediksi:")
    predictions.select(
        "label", "prediction",
        F.round(F.col("probability").getItem(1), 3).alias("prob_delay")
    ).show(10)

    # 6. Simpan model
    print(f"\n[SAVE] Menyimpan model ke: {MODEL_PATH}")
    model.write().overwrite().save(MODEL_PATH)
    print(f"[SAVE] Model tersimpan! ✓")
    print("\nSekarang jalankan spark_processor.py untuk streaming inference.")

    spark.stop()


if __name__ == "__main__":
    main()
