# ─────────────────────────────────────────────────────────
#  Dockerfile — Spark Processing (Anggota 2)
#  Base image: Python slim + install PySpark manual
# ─────────────────────────────────────────────────────────
FROM python:3.11-slim

# Install Java (wajib untuk Spark)
RUN apt-get update && apt-get install -y \
    default-jdk \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

# Install Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy source code
COPY spark_processor.py /app/spark_processor.py
COPY train_model.py     /app/train_model.py

WORKDIR /app

CMD ["spark-submit", \
     "--master", "local[*]", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
     "--conf", "spark.sql.shuffle.partitions=4", \
     "spark_processor.py"]