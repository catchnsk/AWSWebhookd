# ============================================================
# Multi-stage build for delivery-listener
# ============================================================

# --- Build stage ---
FROM eclipse-temurin:21-jdk-alpine AS builder

WORKDIR /workspace/app

# Copy maven wrapper and POM first for layer caching
COPY mvnw .
COPY .mvn .mvn
COPY pom.xml .

# Download dependencies (cached unless pom changes)
RUN ./mvnw dependency:go-offline -B

# Copy source and build
COPY src src
RUN ./mvnw package -DskipTests -B

# Extract layered JAR for optimised layers
RUN java -Djarmode=layertools -jar target/*.jar extract --destination target/extracted

# --- Runtime stage ---
FROM eclipse-temurin:21-jre-alpine

# Non-root user for security
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app

# Copy layered JAR contents in dependency order (most stable first)
COPY --from=builder /workspace/app/target/extracted/dependencies/          ./
COPY --from=builder /workspace/app/target/extracted/spring-boot-loader/    ./
COPY --from=builder /workspace/app/target/extracted/snapshot-dependencies/ ./
COPY --from=builder /workspace/app/target/extracted/application/           ./

# Set ownership
RUN chown -R appuser:appgroup /app
USER appuser

# JVM tuning for container-aware GC and memory limits
ENV JAVA_OPTS="\
  -XX:+UseContainerSupport \
  -XX:MaxRAMPercentage=75.0 \
  -XX:+UseG1GC \
  -XX:+UseStringDeduplication \
  -Djava.security.egd=file:/dev/./urandom \
  -Dspring.profiles.active=default"

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD wget -qO- http://localhost:8080/actuator/health/liveness || exit 1

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS org.springframework.boot.loader.launch.JarLauncher"]
