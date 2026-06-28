"""
=============================================================
  Delta Lake Gold Aggregator — Batch Processing
  Anggota 3: Medallion Architecture (Bronze → Silver → Gold)
=============================================================
  Membaca Silver layer (enriched_flights) dan menghasilkan
  4 tabel Gold yang siap digunakan untuk dashboard/BI:

    1. airline_daily_performance  — Performa harian per maskapai
    2. route_daily_statistics     — Statistik harian per rute
    3. hourly_delay_trends        — Tren delay berdasarkan jam
    4. weather_impact_analysis    — Dampak cuaca terhadap delay
=============================================================
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ─── Path Configuration ────────────────────────────────────
DELTA_BASE  = os.getenv("DELTA_BASE", "/app/delta_lake")
SILVER_PATH = f"{DELTA_BASE}/silver/enriched_flights"
GOLD_BASE   = f"{DELTA_BASE}/gold"


def create_spark_session():
    """Membuat SparkSession untuk batch processing Delta Lake."""
    print("[SPARK] Menginisialisasi SparkSession (Gold Aggregator)...")
    spark = (
        SparkSession.builder
        .appName("DeltaLakeGoldAggregator")
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.0.0"
        )
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("[SPARK] SparkSession siap!")
    return spark


def read_silver(spark):
    """Membaca seluruh data dari Silver layer."""
    print(f"[SILVER] Membaca data dari: {SILVER_PATH}")
    return spark.read.format("delta").load(SILVER_PATH)


# ─── Gold Table 1: Airline Daily Performance ───────────────
def gold_airline_daily_performance(silver_df):
    """
    Agregasi performa harian per maskapai.
    Metrik: total flights, avg/max delay, on-time %, FDI,
            penumpang terdampak, estimasi kompensasi.
    """
    return (
        silver_df
        .withColumn("report_date",
            F.to_date(F.from_unixtime(F.col("timestamp"))))
        .groupBy("report_date", "airline_icao")
        .agg(
            F.count("*").alias("total_flights"),
            F.round(F.avg("predicted_delay_minutes"), 1)
                .alias("avg_delay_minutes"),
            F.round(F.max("predicted_delay_minutes"), 1)
                .alias("max_delay_minutes"),
            F.sum(F.when(F.col("delay_category") == "ON TIME", 1)
                   .otherwise(0))
                .alias("on_time_count"),
            F.sum(F.when(F.col("delay_category") == "MEDIUM DELAY", 1)
                   .otherwise(0))
                .alias("medium_delay_count"),
            F.sum(F.when(F.col("delay_category") == "CRITICAL DELAY", 1)
                   .otherwise(0))
                .alias("critical_delay_count"),
            F.round(F.avg("fdi"), 1).alias("avg_fdi"),
            F.sum("affected_passengers")
                .alias("total_affected_passengers"),
            F.sum("estimated_compensation_eur")
                .alias("total_compensation_eur"),
        )
        .withColumn("on_time_percentage",
            F.round(
                F.col("on_time_count") / F.col("total_flights") * 100,
                1
            ))
        .orderBy("report_date", "airline_icao")
    )


# ─── Gold Table 2: Route Daily Statistics ──────────────────
def gold_route_daily_statistics(silver_df):
    """
    Statistik harian per rute (origin → destination).
    Metrik: avg delay, cuaca, traffic density, jumlah critical.
    """
    return (
        silver_df
        # Filter rute yang valid (bukan UNKNOWN/kosong)
        .filter(
            (F.col("origin") != "UNKNOWN") &
            (F.col("destination") != "UNKNOWN") &
            (F.col("origin") != "") &
            (F.col("destination") != "")
        )
        .withColumn("report_date",
            F.to_date(F.from_unixtime(F.col("timestamp"))))
        .groupBy("report_date", "origin", "destination")
        .agg(
            F.count("*").alias("total_flights"),
            F.round(F.avg("predicted_delay_minutes"), 1)
                .alias("avg_delay_minutes"),
            F.round(F.avg("weather_score"), 3)
                .alias("avg_weather_score"),
            F.round(F.avg("traffic_density"), 1)
                .alias("avg_traffic_density"),
            F.sum(F.when(F.col("delay_category") == "CRITICAL DELAY", 1)
                   .otherwise(0))
                .alias("critical_delay_count"),
            F.round(F.avg("distance_km"), 1)
                .alias("avg_distance_km"),
        )
        .orderBy("report_date", F.desc("total_flights"))
    )


# ─── Gold Table 3: Hourly Delay Trends ────────────────────
def gold_hourly_delay_trends(silver_df):
    """
    Tren delay berdasarkan jam UTC.
    Berguna untuk mendeteksi peak hours dan jam-jam rawan delay.
    """
    return (
        silver_df
        .withColumn("report_date",
            F.to_date(F.from_unixtime(F.col("timestamp"))))
        .groupBy("report_date", "hour_utc")
        .agg(
            F.count("*").alias("total_flights"),
            F.round(F.avg("predicted_delay_minutes"), 1)
                .alias("avg_delay_minutes"),
            F.round(
                F.sum(
                    F.when(
                        F.col("delay_category").isin(
                            "MEDIUM DELAY", "CRITICAL DELAY"),
                        1
                    ).otherwise(0)
                ) / F.count("*") * 100,
                1
            ).alias("delayed_percentage"),
            F.round(F.avg("weather_score"), 3)
                .alias("avg_weather_score"),
            F.round(F.avg("traffic_density"), 1)
                .alias("avg_traffic_density"),
        )
        .orderBy("report_date", "hour_utc")
    )


# ─── Gold Table 4: Weather Impact Analysis ────────────────
def gold_weather_impact_analysis(silver_df):
    """
    Analisis dampak cuaca terhadap delay.
    Mengelompokkan berdasarkan bucket cuaca:
      CLEAR    (weather_score < 0.2)
      MODERATE (0.2 ≤ weather_score < 0.5)
      SEVERE   (weather_score ≥ 0.5)
    """
    return (
        silver_df
        .withColumn("report_date",
            F.to_date(F.from_unixtime(F.col("timestamp"))))
        .withColumn("weather_bucket",
            F.when(F.col("weather_score") < 0.2, "CLEAR")
             .when(F.col("weather_score") < 0.5, "MODERATE")
             .otherwise("SEVERE"))
        .groupBy("report_date", "weather_bucket")
        .agg(
            F.count("*").alias("total_flights"),
            F.round(F.avg("predicted_delay_minutes"), 1)
                .alias("avg_delay_minutes"),
            F.round(F.avg("fdi"), 1).alias("avg_fdi"),
            F.round(
                F.sum(
                    F.when(
                        F.col("delay_category") == "CRITICAL DELAY", 1
                    ).otherwise(0)
                ) / F.count("*") * 100,
                1
            ).alias("critical_percentage"),
            F.round(F.avg("route_deviation"), 2)
                .alias("avg_route_deviation"),
        )
        .orderBy("report_date", "weather_bucket")
    )


# ─── Helper: Write Gold Table ─────────────────────────────
def write_gold_table(df, table_name):
    """Menulis DataFrame ke Delta Lake Gold layer (overwrite) dan Redis."""
    path = f"{GOLD_BASE}/{table_name}"
    row_count = df.count()
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )
    print(f"[GOLD] ✅ {table_name}: {row_count} rows → {path}")

    # Simpan juga ke Redis untuk dashboard
    try:
        import redis
        import json
        redis_host = os.getenv("REDIS_HOST", "redis")
        redis_port = int(os.getenv("REDIS_PORT", 6379))
        r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        
        # Convert DataFrame to list of dicts
        # Pyspark toJSON returns RDD of JSON strings
        records = df.toJSON().collect()
        parsed_records = [json.loads(record) for record in records]
        
        r.set(f"gold:{table_name}", json.dumps(parsed_records))
        print(f"[GOLD] ✅ {table_name}: saved to Redis under key gold:{table_name}")
    except Exception as e:
        print(f"[GOLD ERROR] Failed to save {table_name} to Redis: {e}")


def show_gold_summary(spark):
    """Menampilkan ringkasan semua Gold tables."""
    print("\n" + "=" * 60)
    print("  GOLD LAYER SUMMARY")
    print("=" * 60)

    tables = [
        "airline_daily_performance",
        "route_daily_statistics",
        "hourly_delay_trends",
        "weather_impact_analysis",
    ]
    for t in tables:
        path = f"{GOLD_BASE}/{t}"
        try:
            df = spark.read.format("delta").load(path)
            print(f"\n{'─' * 50}")
            print(f"📊 {t} ({df.count()} rows)")
            print(f"{'─' * 50}")
            df.show(5, truncate=False)
        except Exception as e:
            print(f"⚠️  {t}: {e}")


# ─── Main ─────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  DELTA LAKE GOLD AGGREGATOR — Starting...")
    print("=" * 60)

    spark = create_spark_session()

    try:
        # Baca Silver table
        silver_df = read_silver(spark)
        row_count = silver_df.count()
        print(f"[SILVER] Loaded {row_count} rows from Silver layer")

        if row_count == 0:
            print("[GOLD] ⚠️  No data in Silver layer. Skipping.")
            return

        silver_df.cache()  # Cache untuk performa (dipakai 4x)

        # ── Generate semua Gold tables ──────────────────────
        print("\n[GOLD] Generating aggregate tables...\n")

        # 1. Airline daily performance
        print("[GOLD] 1/4 — Airline Daily Performance...")
        airline_perf = gold_airline_daily_performance(silver_df)
        write_gold_table(airline_perf, "airline_daily_performance")

        # 2. Route daily statistics
        print("[GOLD] 2/4 — Route Daily Statistics...")
        route_stats = gold_route_daily_statistics(silver_df)
        write_gold_table(route_stats, "route_daily_statistics")

        # 3. Hourly delay trends
        print("[GOLD] 3/4 — Hourly Delay Trends...")
        hourly_trends = gold_hourly_delay_trends(silver_df)
        write_gold_table(hourly_trends, "hourly_delay_trends")

        # 4. Weather impact analysis
        print("[GOLD] 4/4 — Weather Impact Analysis...")
        weather_impact = gold_weather_impact_analysis(silver_df)
        write_gold_table(weather_impact, "weather_impact_analysis")

        silver_df.unpersist()

        # ── Tampilkan ringkasan ─────────────────────────────
        show_gold_summary(spark)

        print("\n" + "=" * 60)
        print("  ✅ ALL GOLD TABLES GENERATED SUCCESSFULLY!")
        print("=" * 60)

    except Exception as e:
        print(f"\n[GOLD ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()
        print("[INFO] SparkSession ditutup.")


if __name__ == "__main__":
    main()
