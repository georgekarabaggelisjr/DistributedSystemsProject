<#
.SYNOPSIS
    MapReduce Framework - Deep Purge & Cluster Reset
.DESCRIPTION
    Executes a hard wipe of the distributed system's state.
    Destroys active Kubernetes compute resources, flushes all RabbitMQ
    message queues (including the DLQ), wipes the Redis idempotency/capacity
    cache, and optionally truncates the Postgres job history.
#>

# =============================================================================
# 1. CONFIGURATION
# =============================================================================
$K8S_NAMESPACE      = "default"

# Docker Containers
$RABBIT_CONTAINER   = "local-rabbitmq"
$REDIS_CONTAINER    = "local-redis"
$POSTGRES_CONTAINER = "local-postgres"

Write-Host "===============================================================================" -ForegroundColor Cyan
Write-Host " INITIATING DEEP PURGE SEQUENCE" -ForegroundColor Cyan
Write-Host "===============================================================================" -ForegroundColor Cyan

# =============================================================================
# 2. KUBERNETES CLEANUP (Forced Teardown)
# =============================================================================
Write-Host "`n[INFO] Phase 1: Destroying Kubernetes Resources..." -ForegroundColor Cyan

Write-Host "  -> Deleting all Job definitions (Map and Reduce phases)..." -ForegroundColor DarkGray
kubectl delete jobs --all --namespace $K8S_NAMESPACE --wait=false 2>$null

Write-Host "  -> Force-terminating worker Pods immediately..." -ForegroundColor DarkGray
kubectl delete pods -l app=worker-node --namespace $K8S_NAMESPACE --force --grace-period=0 2>$null

Write-Host "[SUCCESS] K8s cluster wiped." -ForegroundColor Green

# =============================================================================
# 3. RABBITMQ MESSAGE FLUSH
# =============================================================================
Write-Host "`n[INFO] Phase 2: Purging RabbitMQ Queues..." -ForegroundColor Cyan

Write-Host "  -> Clearing primary operational queues..." -ForegroundColor DarkGray
docker exec $RABBIT_CONTAINER rabbitmqctl purge_queue map_tasks_queue 2>$null
docker exec $RABBIT_CONTAINER rabbitmqctl purge_queue reduce_tasks_queue 2>$null
docker exec $RABBIT_CONTAINER rabbitmqctl purge_queue job_events_queue 2>$null
docker exec $RABBIT_CONTAINER rabbitmqctl purge_queue orchestration_events_queue 2>$null

Write-Host "  -> Clearing Dead Letter Queue (DLQ)..." -ForegroundColor DarkGray
docker exec $RABBIT_CONTAINER rabbitmqctl purge_queue dlq_tasks 2>$null

Write-Host "[SUCCESS] Event queues flushed." -ForegroundColor Green

# =============================================================================
# 4. REDIS STATE WIPE
# =============================================================================
Write-Host "`n[INFO] Phase 3: Wiping Redis State Memory..." -ForegroundColor Cyan

Write-Host "  -> Flushing in-memory cache to destroy active job tracking states..." -ForegroundColor DarkGray
docker exec $REDIS_CONTAINER redis-cli FLUSHALL | Out-Null

Write-Host "[SUCCESS] Redis state wiped." -ForegroundColor Green

# =============================================================================
# 5. POSTGRES DATABASE RESET (Optional/Soft)
# =============================================================================
Write-Host "`n[INFO] Phase 4: Truncating Job History in Postgres..." -ForegroundColor Cyan

# Note: If you want to keep history during purges, comment out the line below.
Write-Host "  -> Deleting historical records..." -ForegroundColor DarkGray
docker exec $POSTGRES_CONTAINER psql -U postgres -d dds_db -c "DELETE FROM jobs;" | Out-Null

Write-Host "[SUCCESS] Job history truncated." -ForegroundColor Green

Write-Host "`n===============================================================================" -ForegroundColor Cyan
Write-Host " DEEP PURGE COMPLETE. Cluster is ready for new jobs." -ForegroundColor Green
Write-Host "===============================================================================" -ForegroundColor Cyan