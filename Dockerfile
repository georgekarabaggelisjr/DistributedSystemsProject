# ==========================================
# STAGE 1: Build the Java Application
# ==========================================
FROM maven:3.9-eclipse-temurin-17 AS builder
WORKDIR /app

# Step 1: Copy the pom.xml and download dependencies
# (We do this first so Docker caches the downloads, speeding up future builds!)
COPY pom.xml .
RUN mvn dependency:go-offline

# Step 2: Copy the actual source code
COPY src ./src

# Step 3: Compile the code and extract all external libraries (SLF4J, RabbitMQ, Jackson)
RUN mvn clean compile dependency:copy-dependencies

# ==========================================
# STAGE 2: Create the Production Runtime Image
# ==========================================
# We use a lightweight Alpine JRE to keep the final image incredibly small and secure
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app

# Copy the compiled classes and libraries from the "builder" stage above
COPY --from=builder /app/target/classes ./classes
COPY --from=builder /app/target/dependency ./lib

# Set default Environment Variables (these can be overridden by Kubernetes/Docker Compose)
ENV RABBITMQ_HOST="host.docker.internal"
ENV RABBITMQ_QUEUE="map_tasks_queue"
ENV MINIO_ENDPOINT="http://host.docker.internal:9000"
ENV MINIO_ROOT_USER="minioadmin"
ENV MINIO_ROOT_PASSWORD="minioadmin"

# Start the Worker Engine with Container-Aware Memory Flags
CMD ["java", "-XX:InitialRAMPercentage=50.0", "-XX:MaxRAMPercentage=75.0", "-cp", "classes:lib/*", "com.iliasbolan.App"]