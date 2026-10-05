FROM python:3.11-slim-bookworm

# PySpark needs a JVM; pip supplies the matching Spark distribution.
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
ENV PYTHONUNBUFFERED=1 PYSPARK_PYTHON=python
COPY . .
CMD ["python", "producer.py", "--help"]
