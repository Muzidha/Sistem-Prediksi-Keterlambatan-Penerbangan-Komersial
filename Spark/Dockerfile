# ─────────────────────────────────────────────────────────
#  Dockerfile — Spark Processing (Anggota 2+3)
#  Base image: Python slim + PySpark + RandomForestRegressor
# ─────────────────────────────────────────────────────────
FROM python:3.11-slim

# Install Java (wajib untuk Spark) + bash untuk entrypoint + dos2unix untuk fix CRLF
RUN apt-get update && apt-get install -y \
    default-jdk \
    curl \
    bash \
    dos2unix \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

# Install Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy source code
COPY spark_processor.py /app/spark_processor.py
COPY train_model.py     /app/train_model.py
COPY entrypoint.sh      /app/entrypoint.sh

# Fix Windows CRLF line endings → Unix LF (penting!)
RUN dos2unix /app/entrypoint.sh && chmod +x /app/entrypoint.sh

WORKDIR /app

# Entrypoint: train model (jika belum ada) → lalu streaming
CMD ["bash", "entrypoint.sh"]