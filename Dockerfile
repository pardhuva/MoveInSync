# ---- Build stage: compile and package the fat jar ----
# Pinned digests are omitted for readability; pin them in a real pipeline.
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build

# Cache dependencies first: this layer only rebuilds when pom.xml changes.
COPY pom.xml .
RUN mvn -q -B -o dependency:go-offline 2>/dev/null || mvn -q -B dependency:go-offline

# Then build. Tests are skipped here because the image build is for shipping, not verification;
# run `mvn test` (or the load test) separately. Nothing in the running image depends on tests.
COPY src ./src
RUN mvn -q -B -DskipTests package

# ---- Run stage: a slim JRE with just the jar ----
FROM eclipse-temurin:17-jre AS run
WORKDIR /app

# Run as a non-root user: a compromised process should not be root in the container.
RUN useradd -r -u 1001 appuser
COPY --from=build /build/target/live-trip-updates.jar app.jar
USER appuser

# The gateway listens on 8080. Overridden per node via SERVER_PORT if needed.
EXPOSE 8080

# Container-aware heap. The fan-out layer is memory-light; most RAM is replay buffers + queues.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75.0 -XX:+UseG1GC"

# A liveness probe hits the actuator health endpoint.
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=3 \
    CMD wget -q -O - http://localhost:8080/actuator/health | grep -q UP || exit 1

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
