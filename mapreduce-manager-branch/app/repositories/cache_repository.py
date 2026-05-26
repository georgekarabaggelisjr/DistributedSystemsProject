"""
Repository module for high-throughput distributed caching operations.

This module acts as the Data Access Layer (DAL) for Redis. It abstracts
raw Redis commands into domain-specific operations, primarily focusing on
the atomic, idempotent operations required to track phase barriers (Map/Reduce)
without incurring database lock contention.

With the introduction of the Peer-to-Peer Shuffle architecture, this module
also acts as the dynamic Directory Service, aggregating network routes
from active Map workers to construct the Reduce phase routing table.
"""

import json
import logging
from typing import List, Optional, cast, Awaitable, Any, Dict

from redis.asyncio import Redis

# Initialize the module-level logger for cache persistence events
logger = logging.getLogger(__name__)


class CacheRepository:
    """
    Data access layer for in-memory distributed tracking and synchronization.

    Utilizes Redis Hashes and Sets (:code:`HSET`, :code:`HVALS`, :code:`SADD`, :code:`SCARD`)
    to map job IDs to their completed tasks and active network locations.

    Phase transitions are guarded by atomic Lua scripts to prevent distributed
    race conditions during concurrent worker callbacks.
    """

    @staticmethod
    async def try_acquire_idempotency_lock(
            idempotency_key: str,
            ttl: int,
            redis_client: Redis
    ) -> tuple[bool, Optional[Dict[str, Any]]]:
        """
        Attempts to atomically acquire an operational lock for an idempotency key.

        If the lock is successfully acquired, returns (True, None).
        If the key already exists (meaning it is actively processing or has cached a response),
        returns (False, cached_data_dict).

        :param idempotency_key: The client-provided unique retry identifier.
        :type idempotency_key: str
        :param ttl: Time-to-live expiration limit in seconds.
        :type ttl: int
        :param redis_client: The active asynchronous Redis client.
        :type redis_client: Redis
        :return: A tuple containing a success flag and any cached response data.
        :rtype: tuple[bool, Optional[Dict[str, Any]]]
        """
        key = f"idempotency:schedule:{idempotency_key}"

        # Atomically set key if Not Exists (NX) to establish lock
        acquired = await redis_client.set(
            key,
            json.dumps({"status": "PROCESSING"}),
            nx=True,
            ex=ttl
        )
        if acquired:
            return True, None

        # Lock acquisition failed; retrieve existing state/cached value
        raw_val = await redis_client.get(key)
        if raw_val:
            decoded = raw_val.decode('utf-8') if isinstance(raw_val, bytes) else raw_val
            return False, json.loads(decoded)

        return False, None

    @staticmethod
    async def cache_idempotent_response(
            idempotency_key: str,
            response_payload: Dict[str, Any],
            ttl: int,
            redis_client: Redis
    ) -> None:
        """
        Caches the final successful orchestration payload for a given idempotency key.

        :param idempotency_key: The client-provided unique retry identifier.
        :type idempotency_key: str
        :param response_payload: The dictionary response to cache for subsequent calls.
        :type response_payload: Dict[str, Any]
        :param ttl: Time-to-live expiration cache limit in seconds.
        :type ttl: int
        :param redis_client: The active asynchronous Redis client.
        :type redis_client: Redis
        """
        key = f"idempotency:schedule:{idempotency_key}"
        await redis_client.set(key, json.dumps(response_payload), ex=ttl)

    @staticmethod
    async def release_idempotency_lock(idempotency_key: str, redis_client: Redis) -> None:
        """
        Removes the idempotency lock in the event of a non-idempotent system exception.

        :param idempotency_key: The client-provided unique retry identifier.
        :type idempotency_key: str
        :param redis_client: The active asynchronous Redis client.
        :type redis_client: Redis
        """
        key = f"idempotency:schedule:{idempotency_key}"
        await redis_client.delete(key)

    @staticmethod
    async def reserve_cluster_capacity(
            job_id: str,
            requested_mappers: int,
            num_reducers: int,
            max_capacity: int,
            redis_client: Redis
    ) -> int:
        """
        Atomically reserves cluster capacity for the current phase.
        Upgraded to Phase-Independent Scaling to maximize Map throughput.
        """
        script = """
        local hash_key = KEYS[1]
        local job_id = ARGV[1]
        local requested_map = tonumber(ARGV[2])
        local max_capacity = tonumber(ARGV[4])

        -- Calculate active reservations across the cluster
        local all_vals = redis.call('HVALS', hash_key)
        local current_total = 0
        for i, v in ipairs(all_vals) do
            current_total = current_total + tonumber(v)
        end

        local available = max_capacity - current_total

        if available < 1 then
            return 0
        end

        -- Phase-Independent Scaling: Give the Map phase as much as it wants
        -- up to the current available cluster limit.
        local granted_map = requested_map
        if requested_map > available then
            granted_map = available
        end

        -- We no longer lock the Reducer space upfront.
        redis.call('HINCRBY', hash_key, job_id, granted_map)

        return granted_map
                """
        key = "cluster:global_capacity_semaphore"

        result = await cast(
            Awaitable[Any],
            redis_client.eval(script, 1, key, job_id, requested_mappers, num_reducers, max_capacity)
        )

        return int(result)

    @staticmethod
    async def release_job_capacity(job_id: str, redis_client: Redis) -> None:
        """
        Releases all cluster capacity currently held by a specific job.
        """
        key = "cluster:global_capacity_semaphore"
        await cast(Awaitable[Any], redis_client.hdel(key, job_id))

    @staticmethod
    async def register_map_completion_atomic(
            job_id: str,
            task_id: str,
            target_total: int,
            redis_client: Redis
    ) -> bool:
        """
        Idempotently records a Map task and atomically evaluates the phase barrier.
        """
        script = """
            local set_key = KEYS[1]
            local task_id = ARGV[1]
            local target_total = tonumber(ARGV[2])

            -- Capture whether this specific call added a new task
            local added = redis.call('SADD', set_key, task_id)
            redis.call('EXPIRE', set_key, 86400)

            local current_count = redis.call('SCARD', set_key)

            -- The barrier is ONLY breached if THIS specific task tipped the scale
            if current_count == target_total and added == 1 then
                return 1
            else
                return 0
            end
            """
        key = f"job:{job_id}:map_completed_set"
        result = await cast(Awaitable[Any], redis_client.eval(script, 1, key, task_id, target_total))
        return result == 1

    @staticmethod
    async def add_worker_endpoint(job_id: str, task_id: str, endpoint: str, redis_client: Redis) -> None:
        """
        Stores a Map worker's network endpoint for the peer-to-peer shuffle phase.
        """
        if not endpoint:
            return

        key = f"job:{job_id}:worker_endpoints"

        async with redis_client.pipeline() as pipe:
            pipe.hset(key, task_id, endpoint)
            pipe.expire(key, 86400)  # type: ignore
            await pipe.execute()

    @staticmethod
    async def get_worker_endpoints(job_id: str, redis_client: Redis) -> List[str]:
        """
        Retrieves the aggregated routing table of all Map worker endpoints.
        """
        key = f"job:{job_id}:worker_endpoints"
        endpoints = await cast(Awaitable[Any], redis_client.hvals(key))

        if not endpoints:
            return []

        return [
            endpoint.decode('utf-8') if isinstance(endpoint, bytes) else endpoint
            for endpoint in endpoints
        ]

    @staticmethod
    async def register_reduce_completion_atomic(
            job_id: str,
            task_id: str,
            target_total: int,
            redis_client: Redis
    ) -> bool:
        """
        Idempotently records a Reduce task and evaluates the final phase barrier.
        """
        script = """
            local set_key = KEYS[1]
            local task_id = ARGV[1]
            local target_total = tonumber(ARGV[2])

            -- Capture whether this specific call added a new task
            local added = redis.call('SADD', set_key, task_id)
            redis.call('EXPIRE', set_key, 86400)

            local current_count = redis.call('SCARD', set_key)

            -- The barrier is ONLY breached if THIS specific task tipped the scale
            if current_count == target_total and added == 1 then
                return 1
            else
                return 0
            end
            """
        key = f"job:{job_id}:reduce_completed_set"
        result = await cast(Awaitable[Any], redis_client.eval(script, 1, key, task_id, target_total))
        return result == 1

    @staticmethod
    async def get_cached_total_chunks(job_id: str, redis_client: Redis) -> Optional[int]:
        """
        Retrieves the cached 'total_chunks' boundary for a job to reduce DB load.
        """
        key = f"job:{job_id}:progress_meta"
        val = await cast(Awaitable[Any], redis_client.hget(key, "total_chunks"))

        if val is None:
            return None

        decoded_val = val.decode('utf-8') if isinstance(val, bytes) else val
        return int(decoded_val)

    @staticmethod
    async def cache_total_chunks(job_id: str, total_chunks: int, redis_client: Redis) -> None:
        """
        Caches the static 'total_chunks' boundary to prevent database hits.
        """
        key = f"job:{job_id}:progress_meta"

        async with redis_client.pipeline() as pipe:
            pipe.hset(key, "total_chunks", str(total_chunks))
            pipe.expire(key, 86400)  # type: ignore
            await pipe.execute()

    @staticmethod
    async def add_to_recovery_waitlist(
            job_id: str,
            lost_map_task_id: str,
            failed_reduce_task_id: str,
            redis_client: Redis
    ) -> None:
        """
        Registers a failed Reducer into the recovery waitlist.
        """
        key = f"job:{job_id}:recovery_waitlist:{lost_map_task_id}"

        async with redis_client.pipeline() as pipe:
            pipe.sadd(key, failed_reduce_task_id)
            pipe.expire(key, 86400)  # type: ignore
            await pipe.execute()

    @staticmethod
    async def pop_from_recovery_waitlist(
            job_id: str,
            completed_map_task_id: str,
            redis_client: Redis
    ) -> List[str]:
        """
        Atomically retrieves and removes waiting Reducers from the waitlist.
        """
        script = """
            local key = KEYS[1]
            local members = redis.call('SMEMBERS', key)
            if #members > 0 then
                redis.call('DEL', key)
            end
            return members
            """
        key = f"job:{job_id}:recovery_waitlist:{completed_map_task_id}"

        failed_reducers = await cast(Awaitable[Any], redis_client.eval(script, 1, key))

        if failed_reducers:
            return [r.decode('utf-8') if isinstance(r, bytes) else r for r in failed_reducers]

        return []

    @staticmethod
    async def increment_task_failure(
            job_id: str,
            task_id: str,
            redis_client: Redis
    ) -> int:
        """
        Atomically increments and returns the failure count for a specific task.
        """
        key = f"job:{job_id}:task_failures"

        async with redis_client.pipeline() as pipe:
            pipe.hincrby(key, task_id, 1)
            pipe.expire(key, 86400)  # type: ignore
            results = await pipe.execute()

        return int(results[0])

    @staticmethod
    async def get_active_job_reservations(redis_client: Redis) -> List[str]:
        """
        Retrieves a list of Job IDs that currently have capacity reserved.

        FIXED BUG: Migrated from scanning legacy standalone keyspace variables
        (redis.keys("job:*:reserved_capacity")) to reading hash fields directly
        from the active cluster-wide multi-tenant semaphore data structure.
        """
        key = "cluster:global_capacity_semaphore"
        fields = await cast(Awaitable[Any], redis_client.hkeys(key))

        if not fields:
            return []

        return [
            f.decode('utf-8') if isinstance(f, bytes) else f
            for f in fields
        ]