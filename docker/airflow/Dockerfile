FROM apache/airflow:2.8.1-python3.11

USER root

# Install system dependencies
RUN apt-get update && \
  apt-get install -y --no-install-recommends \
  gcc \
  g++ \
  default-libmysqlclient-dev \
  pkg-config \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements file
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
  pip install --no-cache-dir -r /requirements.txt

# Set working directory
WORKDIR /opt/airflow