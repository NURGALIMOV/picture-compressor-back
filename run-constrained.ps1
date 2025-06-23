$maxRam = "512m"  # 512 MB RAM limit
$javaOpts = "-Xmx$maxRam -Xms256m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./logs -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:+UseStringDeduplication -XX:+DisableExplicitGC -Dspring.profiles.active=constrained"

Write-Host "Starting Spring Boot application in CONSTRAINED MODE with RAM limit: $maxRam"
Write-Host "Note: CPU constraint to 0.1 CPU must be managed externally via Process Lasso or similar tool."

# Stop the application if it's already running
$javaPids = Get-Process -Name "java" -ErrorAction SilentlyContinue | Where-Object { $_.Path -like "*$pwd*" }
if ($javaPids) {
    Write-Host "Stopping existing Java processes..."
    foreach ($proc in $javaPids) {
        Stop-Process -Id $proc.Id -Force
        Write-Host "Stopped Java process with PID $($proc.Id)"
    }
}

# Set environment variables
$env:JAVA_OPTS = $javaOpts
$env:SPRING_PROFILES_ACTIVE = "constrained"
Write-Host "Java options set: $env:JAVA_OPTS"

# Create logs directory if it doesn't exist
if (!(Test-Path -Path "./logs")) {
    New-Item -ItemType Directory -Path "./logs" | Out-Null
    Write-Host "Created logs directory for heap dumps and GC logs"
}

# Run the application with constrained settings
Write-Host "Starting Spring Boot application in constrained mode..."
& ".\mvnw.cmd" "spring-boot:run" "-Dspring-boot.run.jvmArguments=$javaOpts" 