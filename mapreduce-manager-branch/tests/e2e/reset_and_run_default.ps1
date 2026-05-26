<#
.SYNOPSIS
    MapReduce Framework - Active-Active Multi-Manager State Reset & E2E Test Runner
.DESCRIPTION
    Tears down the existing local environment, rebuilds Docker images,
    soft-restarts backing infrastructure, launches TWO concurrent Manager API replicas
    to test active-active synchronization, and submits a multi-tenant test job via
    resilient client-side failover routing.
#>

# =============================================================================
# 1. CONFIGURATION
# =============================================================================
$MANAGER_ENDPOINTS   = @(
    "http://127.0.0.1:8000/internal/schedule",
    "http://127.0.0.1:8001/internal/schedule"
)
$K8S_NAMESPACE       = "default"

# Directory Paths
$PROJECT_ROOT        = "C:\Users\hlias\IdeaProjects\mapreduce-manager"
$NODE_ROOT           = "C:\Users\hlias\IdeaProjects\mapreduce-node"
$ESS_DIR             = "C:\Users\hlias\IdeaProjects\mapreduce-ess"
$DOCKER_COMPOSE_PATH = Join-Path $NODE_ROOT "Docker-compose.yml"
$INIT_DB_SQL_PATH    = Join-Path $PROJECT_ROOT "init-db.sql"
$ESS_TEMPLATE_PATH   = Join-Path $PROJECT_ROOT "app\k8s\templates\ess-daemonset.yaml"

# Docker Containers
$POSTGRES_CONTAINER  = "local-postgres"
$RABBIT_CONTAINER    = "local-rabbitmq"
$REDIS_CONTAINER     = "local-redis"

# =============================================================================
# 2. TEARDOWN & CLEANUP
# =============================================================================
Write-Host "`n[INFO] Phase 0: Terminating Services..." -ForegroundColor Cyan

# Kill existing Manager API instances on both ports
foreach ($port in 8000, 8001) {
    $apiProcess = Get-NetTCPConnection -LocalPort $port -ErrorAction SilentlyContinue
    if ($apiProcess) {
        Write-Host "  -> Terminating existing Manager API Replica (Port $port)..." -ForegroundColor DarkGray
        Stop-Process -Id $apiProcess.OwningProcess -Force
    }
}
Start-Sleep -Seconds 2

# Cleanup Kubernetes Resources aggressively
Write-Host "  -> Purging Kubernetes Deployments, Jobs & Pods..." -ForegroundColor DarkGray
kubectl delete -f $ESS_TEMPLATE_PATH --ignore-not-found=true 2>$null
kubectl delete jobs --all --namespace $K8S_NAMESPACE --wait=false 2>$null
kubectl delete pods -l app=worker-node --namespace $K8S_NAMESPACE --force --grace-period=0 2>$null

Write-Host "[SUCCESS] K8s cluster and local ports wiped." -ForegroundColor Green

# =============================================================================
# 3. IMAGE BUILD & SYNC
# =============================================================================
Write-Host "`n[INFO] Phase 1: Rebuilding Java Images (Cache Busted)..." -ForegroundColor Cyan

Push-Location $NODE_ROOT
Write-Host "  -> Building mapreduce-worker:v10..." -ForegroundColor DarkGray
docker build --no-cache -q -t mapreduce-worker:v10 .
Pop-Location

Push-Location $ESS_DIR
Write-Host "  -> Building mapreduce-ess:v2..." -ForegroundColor DarkGray
docker build --no-cache -q -t mapreduce-ess:v2 .
Pop-Location

Write-Host "`n[INFO] Phase 1.5: Syncing Images to Minikube..." -ForegroundColor Yellow
Write-Host "  -> Evicting old images from Minikube registry..." -ForegroundColor DarkGray
minikube image rm mapreduce-worker:v10 2>$null
minikube image rm mapreduce-ess:v2 2>$null

Write-Host "  -> Loading fresh images into Minikube..." -ForegroundColor DarkGray
minikube image load mapreduce-worker:v10
minikube image load mapreduce-ess:v2

# =============================================================================
# 4. INFRASTRUCTURE RESTART
# =============================================================================
Write-Host "`n[INFO] Phase 2: Soft-Restarting Infrastructure..." -ForegroundColor Cyan
Write-Host "  -> Restarting containers (Preserving Volumes)..." -ForegroundColor DarkGray
docker-compose -f $DOCKER_COMPOSE_PATH down 2>$null
docker-compose -f $DOCKER_COMPOSE_PATH up -d --build --force-recreate

Write-Host "  -> Waiting for backing services readiness..." -ForegroundColor DarkGray
do {
    $checkDb = docker exec $POSTGRES_CONTAINER pg_isready -U postgres -d dds_db 2>$null
    if ($LASTEXITCODE -ne 0) { Start-Sleep -Seconds 2 }
} while ($LASTEXITCODE -ne 0)

do {
    $checkMq = docker exec $RABBIT_CONTAINER rabbitmq-diagnostics -q check_running 2>$null
    if ($LASTEXITCODE -ne 0) { Start-Sleep -Seconds 2 }
} while ($LASTEXITCODE -ne 0)

Write-Host "[SUCCESS] Infrastructure is online." -ForegroundColor Green

# =============================================================================
# 5. STATE RESET
# =============================================================================
Write-Host "`n[INFO] Phase 3: Targeted State Reset (Logical Clean)..." -ForegroundColor Cyan

Write-Host "  -> Purging RabbitMQ queues..." -ForegroundColor DarkGray
docker exec $RABBIT_CONTAINER rabbitmqctl purge_queue map_tasks_queue 2>$null
docker exec $RABBIT_CONTAINER rabbitmqctl purge_queue reduce_tasks_queue 2>$null
docker exec $RABBIT_CONTAINER rabbitmqctl purge_queue job_events_queue 2>$null
docker exec $RABBIT_CONTAINER rabbitmqctl purge_queue orchestration_events_queue 2>$null

Write-Host "  -> Flushing Redis cache..." -ForegroundColor DarkGray
docker exec $REDIS_CONTAINER redis-cli FLUSHALL | Out-Null

Write-Host "  -> Recreating Job Table (Class-less Schema)..." -ForegroundColor DarkGray
Get-Content $INIT_DB_SQL_PATH | docker exec -i $POSTGRES_CONTAINER psql -U postgres -d dds_db | Out-Null

# =============================================================================
# 6. ORCHESTRATION LAYER LAUNCH
# =============================================================================
Write-Host "`n[INFO] Phase 4: Launching Active-Active Orchestration Layer..." -ForegroundColor Cyan

Write-Host "  -> Deploying ESS DaemonSet..." -ForegroundColor DarkGray
kubectl apply -f $ESS_TEMPLATE_PATH | Out-Null

# Launch Replica 1 on Port 8000
Write-Host "  -> Starting Manager API Replica 1 (Port 8000)..." -ForegroundColor DarkGray
Start-Process powershell -WindowStyle Normal -ArgumentList "-NoExit", "-ExecutionPolicy Bypass", "-Command", "cd '$PROJECT_ROOT'; .\.venv\Scripts\Activate.ps1; python -m uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload"

# Launch Replica 2 on Port 8001
Write-Host "  -> Starting Manager API Replica 2 (Port 8001)..." -ForegroundColor DarkGray
Start-Process powershell -WindowStyle Normal -ArgumentList "-NoExit", "-ExecutionPolicy Bypass", "-Command", "cd '$PROJECT_ROOT'; .\.venv\Scripts\Activate.ps1; python -m uvicorn app.main:app --host 127.0.0.1 --port 8001 --reload"

# Wait for both FastAPI instances to expose their documentation endpoints
foreach ($endpoint in $MANAGER_ENDPOINTS) {
    # FIX: Changed [url] to [uri] to correctly parse network string attributes
    $port = ([uri]$endpoint).Port
    $apiReady = $false
    while (-not $apiReady) {
        try {
            # FIX: Changed to -ErrorAction Stop so connection failures hit the catch block
            $checkApi = Invoke-WebRequest -Uri "http://127.0.0.1:$port/docs" -Method Get -ErrorAction Stop
            if ($checkApi.StatusCode -eq 200) { $apiReady = $true }
        } catch {
            Start-Sleep -Seconds 2
        }
    }
}
Write-Host "[SUCCESS] Dual Manager Replicas are live and accepting connections." -ForegroundColor Green

# =============================================================================
# 7. EXECUTE TEST WITH CLIENT-SIDE FAILOVER
# =============================================================================
Write-Host "`n[INFO] Phase 5: Executing Resilient Multi-Tenant Test..." -ForegroundColor Cyan

# Generate deterministic identifiers
$jobId  = [guid]::NewGuid().ToString()
$userId = "22222222-2222-2222-2222-222222222222"

$payload = @{
    job_id           = $jobId;
    user_id          = $userId;
    format           = "TEXT";
    input_filename   = "large_data.txt";
    file_size        = 356000000;
    mapper_filename  = "WordCountMapper.class";
    reducer_filename = "WordCountReducer.class"
} | ConvertTo-Json

$headers = @{ "Idempotency-Key" = $jobId }
$startTime = Get-Date
$jobSubmitted = $false

# Iterate through replicas to find a healthy one for submission
foreach ($url in $MANAGER_ENDPOINTS) {
    try {
        Write-Host "  -> Attempting job submission to Manager at $url..." -ForegroundColor DarkGray
        $response = Invoke-RestMethod -Uri $url -Method Post -Headers $headers -Body $payload -ContentType "application/json"
        Write-Host "[SUCCESS] Job submission accepted by Manager at $url! Provisioning compute..." -ForegroundColor Green
        $jobSubmitted = $true
        break
    } catch {
        Write-Host "  [WARNING] Manager replica at $url is unreachable or failed. Trying next..." -ForegroundColor Yellow
    }
}

if (-not $jobSubmitted) {
    Write-Host "`n[ERROR] Both active-active manager replicas are terminally down!" -ForegroundColor Red
    exit
}

# =============================================================================
# 8. MONITORING LOOP
# =============================================================================
$status = "RUNNING"

while ($status -eq "RUNNING" -or $status -eq "SUBMITTED") {

    # Query database for current job status (Shared source of truth)
    $status = (docker exec $POSTGRES_CONTAINER psql -U postgres -d dds_db -t -A -c "SELECT status FROM jobs WHERE id='$jobId';").Trim()

    # Query Kubernetes for pod metrics
    $allPods = kubectl get pods -n $K8S_NAMESPACE -l job_id=$jobId --no-headers 2>$null
    $podCount = if ($allPods) { ($allPods | Measure-Object).Count } else { 0 }
    
    $runningCount = 0
    if ($allPods) {
        $runningCount = ($allPods | Select-String "Running").Count
    }

    $elapsed = (Get-Date) - $startTime
    $timer   = "{0:mm\:ss}" -f $elapsed

    Write-Host "`r[Elapsed: $timer] Status: $status | Total Pods: $podCount | Running: $runningCount   " -NoNewline -ForegroundColor Yellow

    if ($status -eq "COMPLETED" -or $status -eq "FAILED") { break }
    Start-Sleep -Seconds 2
}

# Final output
$finalColor = if ($status -eq "COMPLETED") { "Green" } else { "Red" }
Write-Host "`n`n[FINISH] Execution Halted. Final Status: $status" -ForegroundColor $finalColor