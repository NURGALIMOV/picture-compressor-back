$url = "http://localhost:8080/api/compress"
$compressionLevel = 0.7
$filePath = "src/test/resources/cf2e1e1d-9c07-4dfa-a7ad-350ca90538bb.gif"
$numRequests = 50
$concurrentRequests = 5
$maxConcurrentJobs = 5
$requestInterval = 500 # milliseconds

# Create results directory
$resultsDir = "load-test-results"
if (-not (Test-Path $resultsDir)) {
    New-Item -ItemType Directory -Path $resultsDir
}

$results = @()
$completedRequests = 0
$failedRequests = 0
$runningJobs = @()
$startTime = Get-Date

function Send-Request {
    param (
        [int]$requestId
    )
    
    $requestStartTime = Get-Date
    try {
        $form = @{
            file = Get-Item -Path $filePath
            compressionLevel = $compressionLevel
        }
        
        $response = Invoke-RestMethod -Uri $url -Method Post -Form $form -TimeoutSec 30
        $requestEndTime = Get-Date
        $duration = ($requestEndTime - $requestStartTime).TotalMilliseconds
        
        return @{
            RequestId = $requestId
            StartTime = $requestStartTime
            EndTime = $requestEndTime
            Duration = $duration
            Success = $true
            Error = $null
        }
    }
    catch {
        $requestEndTime = Get-Date
        $duration = ($requestEndTime - $requestStartTime).TotalMilliseconds
        
        return @{
            RequestId = $requestId
            StartTime = $requestStartTime
            EndTime = $requestEndTime
            Duration = $duration
            Success = $false
            Error = $_.Exception.Message
        }
    }
}

Write-Host "Starting load test with $numRequests requests, $concurrentRequests concurrent requests"

for ($i = 1; $i -le $numRequests; $i++) {
    # Manage concurrent jobs
    while ($runningJobs.Count -ge $maxConcurrentJobs) {
        $completed = $runningJobs | Where-Object { $_.Job.State -ne 'Running' }
        
        foreach ($job in $completed) {
            $result = Receive-Job -Job $job.Job
            $results += $result
            
            if ($result.Success) {
                $completedRequests++
                Write-Host "Request $($result.RequestId) completed in $($result.Duration) ms" -ForegroundColor Green
            }
            else {
                $failedRequests++
                Write-Host "Request $($result.RequestId) failed in $($result.Duration) ms: $($result.Error)" -ForegroundColor Red
            }
            
            # Remove from running jobs
            $runningJobs = $runningJobs | Where-Object { $_.Job.Id -ne $job.Job.Id }
            
            # Clean up the job
            Remove-Job -Job $job.Job -Force
        }
        
        Start-Sleep -Milliseconds 100
    }
    
    # Start a new job
    $job = Start-Job -ScriptBlock ${function:Send-Request} -ArgumentList $i
    $runningJobs += @{
        RequestId = $i
        Job = $job
    }
    
    Write-Host "Started request $i"
    
    # Add delay between requests
    Start-Sleep -Milliseconds $requestInterval
}

# Wait for all remaining jobs to complete
while ($runningJobs.Count -gt 0) {
    $completed = $runningJobs | Where-Object { $_.Job.State -ne 'Running' }
    
    foreach ($job in $completed) {
        $result = Receive-Job -Job $job.Job
        $results += $result
        
        if ($result.Success) {
            $completedRequests++
            Write-Host "Request $($result.RequestId) completed in $($result.Duration) ms" -ForegroundColor Green
        }
        else {
            $failedRequests++
            Write-Host "Request $($result.RequestId) failed in $($result.Duration) ms: $($result.Error)" -ForegroundColor Red
        }
        
        # Remove from running jobs
        $runningJobs = $runningJobs | Where-Object { $_.Job.Id -ne $job.Job.Id }
        
        # Clean up the job
        Remove-Job -Job $job.Job -Force
    }
    
    Start-Sleep -Milliseconds 100
}

$endTime = Get-Date
$totalDuration = ($endTime - $startTime).TotalSeconds

# Calculate statistics
$successfulResults = $results | Where-Object { $_.Success }
if ($successfulResults.Count -gt 0) {
    $avgResponseTime = ($successfulResults | Measure-Object -Property Duration -Average).Average
    $minResponseTime = ($successfulResults | Measure-Object -Property Duration -Minimum).Minimum
    $maxResponseTime = ($successfulResults | Measure-Object -Property Duration -Maximum).Maximum
}
else {
    $avgResponseTime = 0
    $minResponseTime = 0
    $maxResponseTime = 0
}

# Generate report
$report = @"
Load Test Report
=======================================
Date: $(Get-Date)
Duration: $totalDuration seconds
Requests Sent: $numRequests
Concurrent Requests: $concurrentRequests
Requests Completed: $completedRequests
Requests Failed: $failedRequests
Success Rate: $(($completedRequests / $numRequests) * 100)%
Average Response Time: $avgResponseTime ms
Min Response Time: $minResponseTime ms
Max Response Time: $maxResponseTime ms
=======================================
"@

Write-Host $report
$report | Out-File "$resultsDir\load-test-report.txt"

# Export raw results to CSV
$results | Export-Csv -Path "$resultsDir\load-test-results.csv" -NoTypeInformation

Write-Host "Load test completed. Results saved to $resultsDir\" 