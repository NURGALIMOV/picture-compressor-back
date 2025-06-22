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
ENV JAVA_OPTS="-Xms256m -Xmx512m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp -XX:+UseG1GC -XX:+UseStringDeduplication -XX:MaxGCPauseMillis=200"

# Create non-root user for security
RUN addgroup --system appuser && adduser --system --ingroup appuser appuser

# Copy the built JAR from build stage
COPY --from=build /app/target/*.jar app.jar

# Switch to non-root user for security
USER appuser

# Run the application
ENTRYPOINT ["sh", "-c", "java ${JAVA_OPTS} -jar app.jar"] 