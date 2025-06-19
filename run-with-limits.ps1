$maxRam = "512m"  # 512 MB RAM limit

Write-Host "Starting Spring Boot application with RAM limit: $maxRam"
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

# Start the application with memory constraints
$env:JAVA_OPTS = "-Xmx$maxRam -Xms$maxRam"
Write-Host "Java options set: $env:JAVA_OPTS"

# Run the application
Write-Host "Starting Spring Boot application..."
& ".\mvnw.cmd" "spring-boot:run" 