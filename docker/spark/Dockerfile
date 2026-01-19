FROM apache/spark:3.5.1-python3 


USER root

# Install additional dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3-pip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages for Spark
RUN pip3 install --no-cache-dir \
    pyspark==3.5.0 \
    pandas \
    numpy \
    pymysql \
    psycopg2-binary

# Set working directory
WORKDIR /opt/spark-apps

USER 1001

EXPOSE 8080 7077