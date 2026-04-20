# ==========================================
# STAGE 1: Build the Java Application
# ==========================================
FROM maven:3.9-eclipse-temurin-17 AS builder
WORKDIR /app

# Copy the parent pom.xml and the module poms
COPY pom.xml .
COPY mapreduce-core/pom.xml ./mapreduce-core/
COPY mapreduce-worker/pom.xml ./mapreduce-worker/

# Download dependencies
RUN mvn dependency:go-offline -pl mapreduce-worker -am

# Copy the source code for both modules
COPY mapreduce-core/src ./mapreduce-core/src
COPY mapreduce-worker/src ./mapreduce-worker/src

# Compile, package, AND extract all external libraries
RUN mvn clean package dependency:copy-dependencies -pl mapreduce-worker -am -DskipTests

# ==========================================
# STAGE 2: Create the Production Runtime Image
# ==========================================
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app

# Copy the compiled classes and libraries from the worker module
COPY --from=builder /app/mapreduce-worker/target/classes ./classes
COPY --from=builder /app/mapreduce-worker/target/dependency ./lib

# Set default Environment Variables
ENV RABBITMQ_HOST="host.docker.internal"
ENV RABBITMQ_QUEUE="map_tasks_queue"
ENV MINIO_ENDPOINT="http://host.docker.internal:9000"
ENV MINIO_ROOT_USER="minioadmin"
ENV MINIO_ROOT_PASSWORD="minioadmin"

# Start the Worker Engine using the classpath method
CMD ["java", "-XX:InitialRAMPercentage=50.0", "-XX:MaxRAMPercentage=75.0", "-cp", "classes:lib/*", "com.iliasbolan.App"]