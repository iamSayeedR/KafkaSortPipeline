# ============================================================
# KafkaSortPipeline — Multi-Stage Docker Build
# Stage 1: Maven build  →  fat JAR
# Stage 2: Slim JRE 17  +  Kafka 3.7.1 (KRaft)
# ============================================================

# ---- Stage 1: Build ----------------------------------------
FROM maven:3.9-eclipse-temurin-17 AS build

WORKDIR /build

# Cache dependencies first (layer caching optimisation)
COPY pom.xml .
RUN mvn dependency:go-offline -q || true

# Copy source and build
COPY src ./src
RUN mvn package -DskipTests -q

# ---- Stage 2: Runtime --------------------------------------
FROM eclipse-temurin:17-jre-jammy

# Install required utilities
RUN apt-get update && \
    apt-get install -y --no-install-recommends wget procps && \
    rm -rf /var/lib/apt/lists/*

# Download and install Kafka 3.7.1
ENV KAFKA_VERSION=3.7.1
ENV SCALA_VERSION=2.13
ENV KAFKA_HOME=/opt/kafka

RUN wget -q "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" \
        -O /tmp/kafka.tgz && \
    mkdir -p ${KAFKA_HOME} && \
    tar -xzf /tmp/kafka.tgz -C ${KAFKA_HOME} --strip-components=1 && \
    rm /tmp/kafka.tgz

ENV PATH="${KAFKA_HOME}/bin:${PATH}"

# Application directory
WORKDIR /app

# Copy fat JAR from build stage
COPY --from=build /build/target/kafka-sort-pipeline-1.0-SNAPSHOT.jar /app/kafka-sort-pipeline.jar

# Copy Kafka KRaft configuration
COPY docker/kafka-kraft.properties /app/config/kafka-kraft.properties

# Copy entrypoint and scripts — fix line endings (Windows → Unix)
COPY docker/entrypoint.sh /app/entrypoint.sh
RUN sed -i 's/\r$//' /app/entrypoint.sh && chmod +x /app/entrypoint.sh

COPY docker/scripts/ /app/scripts/
RUN find /app/scripts -type f -name '*.sh' -exec sed -i 's/\r$//' {} + && \
    chmod +x /app/scripts/*.sh

# Environment variables
ENV KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
    KAFKA_HOME=/opt/kafka \
    RECORD_COUNT=50000000 \
    KAFKA_HEAP_OPTS="-Xmx384m -Xms384m"

ENTRYPOINT ["/app/entrypoint.sh"]
