# Build stage: Using Maven image to compile the application
FROM maven:3.9-eclipse-temurin-21-alpine AS build
WORKDIR /app

# Copy pom.xml for dependency resolution
COPY pom.xml .

# Copy source code
COPY src ./src

# Build the application
RUN mvn clean package

# Runtime stage: Run the application with minimal image
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Set memory limits and GC settings for JVM
ENV JAVA_OPTS="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp -XX:+UseG1GC -XX:+UseStringDeduplication -XX:MaxGCPauseMillis=200 -XX:InitialRAMPercentage=25.0 -XX:MaxRAMPercentage=50.0"

# Create non-root user for security
RUN addgroup --system appuser && adduser --system --ingroup appuser appuser

# Expose port 8080 for the application
EXPOSE 8080
EXPOSE 8081

# Copy the built JAR from build stage
COPY --from=build /app/target/*.jar app.jar

# Установить gifsicle для эффективной обработки GIF-файлов с низким потреблением памяти
RUN apk update && \
    apk add --no-cache gifsicle && \
    rm -rf /var/cache/apk/*

# Switch to non-root user for security
USER appuser

# Run the application with appropriate settings based on constrained mode
ENTRYPOINT ["sh", "-c", "if [ \"$CONSTRAINED_MODE\" = \"true\" ] ; then java ${JAVA_OPTS_CONSTRAINED} -jar app.jar ; else java ${JAVA_OPTS} -jar app.jar ; fi"] 