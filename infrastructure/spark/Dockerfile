FROM apache/spark:3.5.0

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3-pip curl && \
    rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    SRC_ROOT=/app \
    SPARK_IVY_DIR=/tmp/ivy

WORKDIR /app

RUN pip3 install --no-cache-dir \
    happybase==1.2.0 \
    confluent-kafka==2.3.0 \
    pyarrow==14.0.2 \
    pydantic==2.5.3 \
    pydantic-settings==2.1.0 \
    requests==2.31.0 \
    python-dotenv==1.0.0

# Pre-cache Spark-Kafka connector JARs at build time
RUN mkdir -p /tmp/ivy && \
    python3 -c "import os; open('/tmp/precache.py','w').write('print(\"ivy cached\")')" && \
    /opt/spark/bin/spark-submit \
      --master 'local[1]' \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
      --conf spark.jars.ivy=/tmp/ivy \
      --conf spark.ui.enabled=false \
      /tmp/precache.py && \
    rm -f /tmp/precache.py

COPY src/ ./src/

CMD ["python3", "-m", "src.processing.pipeline_runner"]
