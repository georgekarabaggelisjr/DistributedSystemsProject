<#
.SYNOPSIS
    MapReduce Framework - Multi-Tenant Concurrent Stress Test (3 Jobs)
.DESCRIPTION
    Tears down the existing local environment, rebuilds Java binaries and Docker images,
    soft-restarts backing infrastructure, launches TWO concurrent Manager API replicas,
    and concurrently submits three disparate MapReduce jobs using resilient client-side
    failover routing to validate multi-tenant isolation and active-active scaling behaviors.
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

Write-Host "  -> Purging Kubernetes Jobs, Pods, and DaemonSets..." -ForegroundColor DarkGray
kubectl delete jobs --all --namespace $K8S_NAMESPACE --wait=false 2>$null
kubectl delete pods -l app=worker-node --namespace $K8S_NAMESPACE --force --grace-period=0 2>$null
kubectl delete daemonset external-shuffle-service --namespace $K8S_NAMESPACE 2>$null

# =============================================================================
# 3. IMAGE BUILD & SYNC
# =============================================================================
Write-Host "`n[INFO] Phase 1: Rebuilding Java Images..." -ForegroundColor Cyan

Push-Location $NODE_ROOT
Write-Host "  -> [DOCKER] Building Worker image (mapreduce-worker:v10)..." -ForegroundColor DarkGray
docker build -q -t mapreduce-worker:v10 .
Pop-Location

Push-Location $ESS_DIR
Write-Host "  -> [MAVEN] Compiling ESS locally..." -ForegroundColor DarkGray
mvn clean package -DskipTests | Out-Null
Write-Host "  -> [DOCKER] Building ESS image (mapreduce-ess:v2)..." -ForegroundColor DarkGray
docker build -q -t mapreduce-ess:v2 .
Pop-Location

Write-Host "`n[INFO] Phase 1.5: Syncing Images to Minikube..." -ForegroundColor Yellow
Write-Host "  -> Evicting old images from Minikube registry..." -ForegroundColor DarkGray
minikube image rm mapreduce-worker:v10 -p mapreduce-cluster 2>$null
minikube image rm mapreduce-ess:v2 -p mapreduce-cluster 2>$null

Write-Host "  -> Loading fresh images into Minikube..." -ForegroundColor DarkGray
minikube image load mapreduce-worker:v10 -p mapreduce-cluster
minikube image load mapreduce-ess:v2 -p mapreduce-cluster

# =============================================================================
# 4. INFRASTRUCTURE RESTART
# =============================================================================
Write-Host "`n[INFO] Phase 2: Soft-Restarting Infrastructure..." -ForegroundColor Cyan
Write-Host "  -> Restarting containers (Preserving Volumes)..." -ForegroundColor DarkGray
docker-compose -f $DOCKER_COMPOSE_PATH down 2>$null
docker-compose -f $DOCKER_COMPOSE_PATH up -d

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
    $port = ([uri]$endpoint).Port
    $apiReady = $false
    while (-not $apiReady) {
        try {
            $checkApi = Invoke-WebRequest -Uri "http://127.0.0.1:$port/docs" -Method Get -ErrorAction Stop
            if ($checkApi.StatusCode -eq 200) { $apiReady = $true }
        } catch {
            Start-Sleep -Seconds 2
        }
    }
}
Write-Host "[SUCCESS] Dual Manager Replicas are live and accepting connections." -ForegroundColor Green

# =============================================================================
# 7. EXECUTE MULTI-TENANT TEST (3 CONCURRENT JOBS)
# =============================================================================
Write-Host "`n[INFO] Phase 5: Executing Concurrent Multi-Tenant Test (3 Jobs)..." -ForegroundColor Cyan

$userId = "22222222-2222-2222-2222-222222222222"
$jobId1 = [guid]::NewGuid().ToString()
$jobId2 = [guid]::NewGuid().ToString()
$jobId3 = [guid]::NewGuid().ToString()

# Helper function for resilient client-side failover submission
function Submit-JobWithFailover {
    param ($PayloadJson, $Headers, $JobName)
    $jobSubmitted = $false
    foreach ($url in $MANAGER_ENDPOINTS) {
        try {
            Invoke-RestMethod -Uri $url -Method Post -Headers $Headers -Body $PayloadJson -ContentType "application/json" | Out-Null
            Write-Host "  [SUCCESS] $JobName Accepted by Manager at $url." -ForegroundColor Green
            $jobSubmitted = $true
            break
        } catch {
            Write-Host "  [WARNING] Manager replica at $url is unreachable. Trying next..." -ForegroundColor Yellow
        }
    }
    if (-not $jobSubmitted) {
        Write-Host "  [ERROR] $JobName submission failed on all active-active replicas!" -ForegroundColor Red
    }
}

# ---------------------------------------------------------
# JOB 1: WordCount (Auto-Scaled)
# ---------------------------------------------------------
$json1 = @{
    job_id = $jobId1; user_id = $userId; format = "TEXT";
    input_filename = "large_data.txt"; file_size = 356000000;
    mapper_filename = "WordCountMapper.class"; reducer_filename = "WordCountReducer.class"
} | ConvertTo-Json
$headers1 = @{ "Idempotency-Key" = $jobId1 }

Submit-JobWithFailover -PayloadJson $json1 -Headers $headers1 -JobName "Job 1 ($jobId1): WordCount Auto-Scaled"

# ---------------------------------------------------------
# JOB 2: Average Rating (Auto-Scaled | ~553 MB)
# ---------------------------------------------------------
$json2 = @{
    job_id = $jobId2; user_id = $userId; format = "CSV";
    input_filename = "average_rating_dataset_500MB.csv"; file_size = 553771607;
    mapper_filename = "AverageRatingMapper.class"; reducer_filename = "AverageRatingReducer.class"
} | ConvertTo-Json
$headers2 = @{ "Idempotency-Key" = $jobId2 }

Submit-JobWithFailover -PayloadJson $json2 -Headers $headers2 -JobName "Job 2 ($jobId2): Average Rating Auto-Scaled"

# ---------------------------------------------------------
# JOB 3: WordCount (Explicit 4 Mappers | 2 Reducers)
# ---------------------------------------------------------
$json3 = @{
    job_id = $jobId3; user_id = $userId; format = "TEXT";
    input_filename = "large_data.txt"; file_size = 356000000;
    mapper_filename = "WordCountMapper.class"; reducer_filename = "WordCountReducer.class";
    num_reducers = 2; num_mappers = 4
} | ConvertTo-Json
$headers3 = @{ "Idempotency-Key" = $jobId3 }

Submit-JobWithFailover -PayloadJson $json3 -Headers $headers3 -JobName "Job 3 ($jobId3): WordCount Explicit Topology"


# =============================================================================
# 8. MONITORING LOOP (DASHBOARD)
# =============================================================================
Start-Sleep -Seconds 2
$startTime = Get-Date
$status1 = "RUNNING"
$status2 = "RUNNING"
$status3 = "RUNNING"

while (($status1 -match "RUNNING|SUBMITTED") -or ($status2 -match "RUNNING|SUBMITTED") -or ($status3 -match "RUNNING|SUBMITTED")) {

    # Query PostgreSQL
    $status1 = (docker exec -i $POSTGRES_CONTAINER psql -U postgres -d dds_db -t -A -c "SELECT COALESCE((SELECT status FROM jobs WHERE id='$jobId1'), 'UNKNOWN');").Trim()
    $status2 = (docker exec -i $POSTGRES_CONTAINER psql -U postgres -d dds_db -t -A -c "SELECT COALESCE((SELECT status FROM jobs WHERE id='$jobId2'), 'UNKNOWN');").Trim()
    $status3 = (docker exec -i $POSTGRES_CONTAINER psql -U postgres -d dds_db -t -A -c "SELECT COALESCE((SELECT status FROM jobs WHERE id='$jobId3'), 'UNKNOWN');").Trim()

    # Get Job 1 Pod metrics
    $allPods1 = kubectl get pods -n $K8S_NAMESPACE -l job_id=$jobId1 --no-headers 2>$null
    $podCount1 = if ($allPods1) { ($allPods1 | Measure-Object).Count } else { 0 }
    $runningCount1 = if ($allPods1) { ($allPods1 | Select-String "Running").Count } else { 0 }

    # Get Job 2 Pod metrics
    $allPods2 = kubectl get pods -n $K8S_NAMESPACE -l job_id=$jobId2 --no-headers 2>$null
    $podCount2 = if ($allPods2) { ($allPods2 | Measure-Object).Count } else { 0 }
    $runningCount2 = if ($allPods2) { ($allPods2 | Select-String "Running").Count } else { 0 }

    # Get Job 3 Pod metrics
    $allPods3 = kubectl get pods -n $K8S_NAMESPACE -l job_id=$jobId3 --no-headers 2>$null
    $podCount3 = if ($allPods3) { ($allPods3 | Measure-Object).Count } else { 0 }
    $runningCount3 = if ($allPods3) { ($allPods3 | Select-String "Running").Count } else { 0 }

    $elapsed = (Get-Date) - $startTime
    $timer = "{0:mm\:ss}" -f $elapsed

    # Clear terminal to create a static dashboard effect
    Clear-Host
    Write-Host "===============================================================" -ForegroundColor Cyan
    Write-Host "    MapReduce Multi-Tenant Test Monitor (3 Concurrent Jobs)" -ForegroundColor White
    Write-Host "    Elapsed Time: $timer" -ForegroundColor Yellow
    Write-Host "===============================================================" -ForegroundColor Cyan

    # Format Job 1 Output
    $color1 = if ($status1 -eq "COMPLETED") { "Green" } elseif ($status1 -eq "FAILED") { "Red" } else { "Yellow" }
    Write-Host "[JOB 1] WordCount (Auto-Scaled)     | ID: $jobId1"
    Write-Host "        Status: $status1 " -ForegroundColor $color1 -NoNewline
    Write-Host "| Workers: Total $podCount1 (Running $runningCount1)"
    Write-Host "---------------------------------------------------------------"

    # Format Job 2 Output
    $color2 = if ($status2 -eq "COMPLETED") { "Green" } elseif ($status2 -eq "FAILED") { "Red" } else { "Yellow" }
    Write-Host "[JOB 2] AverageRating (Auto-Scaled) | ID: $jobId2"
    Write-Host "        Status: $status2 " -ForegroundColor $color2 -NoNewline
    Write-Host "| Workers: Total $podCount2 (Running $runningCount2)"
    Write-Host "---------------------------------------------------------------"

    # Format Job 3 Output
    $color3 = if ($status3 -eq "COMPLETED") { "Green" } elseif ($status3 -eq "FAILED") { "Red" } else { "Yellow" }
    Write-Host "[JOB 3] WordCount (Mappers=4, Reducers=2)    | ID: $jobId3"
    Write-Host "        Status: $status3 " -ForegroundColor $color3 -NoNewline
    Write-Host "| Workers: Total $podCount3 (Running $runningCount3)"
    Write-Host "===============================================================" -ForegroundColor Cyan

    Start-Sleep -Seconds 3
}

Write-Host "`n[INFO] All jobs in the multi-tenant test have concluded." -ForegroundColor Green