#!/bin/bash
# ─────────────────────────────────────────────────────────
#  entrypoint.sh — Anggota 2+3
#  1. Train model regresi (jika belum ada)
#  2. Jalankan Spark Structured Streaming
# ─────────────────────────────────────────────────────────
set -e

MODEL_DIR="${MODEL_PATH:-/app/model_keterlambatan}"

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

echo ""
echo "============================================="
echo "  MEMULAI SPARK STRUCTURED STREAMING..."
echo "============================================="

exec spark-submit \
    --master "local[*]" \
    --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0" \
    --conf "spark.sql.shuffle.partitions=4" \
    spark_processor.py
