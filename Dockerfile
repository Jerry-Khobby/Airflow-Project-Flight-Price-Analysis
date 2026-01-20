# Stage 1: Get Spark
FROM apache/spark:3.5.1-python3 AS spark

# Stage 2: Airflow
FROM apache/airflow:2.8.1-python3.11

USER root

# Copy Spark
COPY --from=spark /opt/spark /opt/spark

# Environment variables
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$SPARK_HOME/bin:$JAVA_HOME/bin

# Install system dependencies (gcc, dev headers)
RUN apt-get update && \
  apt-get install -y --no-install-recommends \
  openjdk-17-jdk \
  default-libmysqlclient-dev \
  gcc \
  python3-dev \
  pkg-config \
  procps && \
  apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt /tmp/requirements.txt

# Switch to airflow user before installing Python packages
USER airflow

# Install Python dependencies as airflow user
RUN pip install --upgrade pip && \
  pip install --timeout=1200 --no-cache-dir \
  mysqlclient==2.2.1 \
  psycopg2-binary==2.9.9 && \
  pip install --timeout=1200 --no-cache-dir -r /tmp/requirements.txt
