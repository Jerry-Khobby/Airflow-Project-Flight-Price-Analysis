FROM postgres:14

# Install additional extensions if needed
RUN apt-get update && \
    apt-get install -y --no-install-recommends postgresql-contrib && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set timezone (optional)
ENV TZ=Asia/Dhaka
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Expose PostgreSQL port
EXPOSE 5432