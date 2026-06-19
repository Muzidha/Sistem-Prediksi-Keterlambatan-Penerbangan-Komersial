#!/bin/bash
# ─────────────────────────────────────────────────────────
#  entrypoint.sh — Anggota 2+3
#  1. Train model regresi (jika belum ada)
#  2. Jalankan Spark Structured Streaming (Bronze + Silver)
#  3. Jalankan Gold Aggregator secara berkala (background)
# ─────────────────────────────────────────────────────────
set -e

MODEL_DIR="${MODEL_PATH:-/app/model_keterlambatan}"
GOLD_INTERVAL="${GOLD_AGGREGATION_INTERVAL:-300}"  # default 5 menit

# ── Step 1: Training ────────────────────────────────────
# Cek apakah model sudah ada (folder metadata ada = sudah trained)
if [ ! -f "$MODEL_DIR/metadata/part-00000" ]; then
    echo "============================================="
    echo "  MODEL BELUM ADA — Menjalankan training..."
    echo "============================================="
    python train_model.py
    echo ""
    echo "[ENTRYPOINT] Training selesai! Lanjut ke streaming..."
else
    echo "[ENTRYPOINT] Model sudah ada di $MODEL_DIR, skip training."
fi

# ── Step 2: Gold Aggregator (Background Cron) ──────────
# Jalankan delta_aggregator.py secara berkala di background
# untuk menghasilkan Gold tables dari Silver layer.
echo ""
echo "[ENTRYPOINT] Memulai Gold Aggregator (setiap ${GOLD_INTERVAL}s)..."
(
    # Tunggu 60 detik agar streaming sempat menulis data ke Silver
    sleep 60
    while true; do
        echo ""
        echo "[GOLD CRON] $(date '+%Y-%m-%d %H:%M:%S') — Menjalankan aggregation..."
        python delta_aggregator.py 2>&1 || echo "[GOLD CRON] ⚠️  Aggregation gagal, coba lagi nanti."
        echo "[GOLD CRON] Tidur ${GOLD_INTERVAL}s..."
        sleep "$GOLD_INTERVAL"
    done
) &
GOLD_PID=$!
echo "[ENTRYPOINT] Gold Aggregator PID: $GOLD_PID"

# ── Step 3: Streaming Pipeline ─────────────────────────
echo ""
echo "============================================="
echo "  MEMULAI SPARK STRUCTURED STREAMING..."
echo "  Medallion: Bronze → Silver → Gold"
echo "============================================="

# Trap SIGTERM/SIGINT untuk cleanup Gold aggregator
trap "echo '[ENTRYPOINT] Stopping...'; kill $GOLD_PID 2>/dev/null; exit 0" SIGTERM SIGINT

exec spark-submit \
    --master "local[*]" \
    --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0" \
    --conf "spark.sql.shuffle.partitions=4" \
    spark_processor.py
