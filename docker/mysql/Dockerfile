
FROM mysql:8.0

# Set default character set and collation
ENV MYSQL_CHARSET=utf8mb4
ENV MYSQL_COLLATION=utf8mb4_unicode_ci

# Copy custom MySQL configuration
COPY my.cnf /etc/mysql/conf.d/custom.cnf

# Set timezone
ENV TZ=Asia/Dhaka
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Expose MySQL port
EXPOSE 3306