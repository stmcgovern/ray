"""
Ray Serve application for routing algorithm study.

Contains Parent and Child deployments that form a simple request chain:
HAProxy -> Parent -> Child
"""

import asyncio
import random
import time

import ray
from ray import serve
from ray.serve.handle import DeploymentHandle

from router_benchmark.configurations import (
    CONCURRENT_PER_REPLICA,
    SIMULATED_LATENCY_MEAN_S,
    SIMULATED_LATENCY_CAP_S,
)


@serve.deployment(
    max_ongoing_requests=CONCURRENT_PER_REPLICA,
    max_queued_requests=-1,
    graceful_shutdown_timeout_s=0.1,
    graceful_shutdown_wait_loop_s=0.1,
)
class ChildDeployment:
    """Child deployment that simulates work with variable latency."""

    def __init__(self):
        self.replica_id = serve.get_replica_context().replica_id.unique_id
        self.node_id = ray.get_runtime_context().get_node_id()
        self.request_count = 0

    async def __call__(
        self, parent_replica_id: str, parent_node_id: str, parent_send_time: float
    ) -> dict:
        child_receive_time = time.time()
        routing_delay_ms = (child_receive_time - parent_send_time) * 1000

        self.request_count += 1

        latency_s = random.expovariate(1 / SIMULATED_LATENCY_MEAN_S)
        latency_s = min(latency_s, SIMULATED_LATENCY_CAP_S)
        await asyncio.sleep(latency_s)

        child_send_time = time.time()

        return {
            "child_replica_id": self.replica_id,
            "child_node_id": self.node_id,
            "parent_replica_id": parent_replica_id,
            "parent_node_id": parent_node_id,
            "simulated_latency_ms": latency_s * 1000,
            "parent_to_child_delay_ms": routing_delay_ms,
            "child_send_time": child_send_time,
        }


@serve.deployment(
    max_ongoing_requests=30,
    max_queued_requests=-1,
    graceful_shutdown_timeout_s=0.1,
    graceful_shutdown_wait_loop_s=0.1,
)
class ParentDeployment:
    """Parent deployment that receives external requests and forwards to Child."""

    def __init__(self, child_handle: DeploymentHandle):
        self.child_handle = child_handle
        self.replica_id = serve.get_replica_context().replica_id.unique_id
        self.node_id = ray.get_runtime_context().get_node_id()
        self.request_count = 0

    async def __call__(self, request_or_send_time=None) -> dict:
        # Handle both HTTP (Starlette Request) and DeploymentHandle (float) calls
        from starlette.requests import Request

        client_send_time = None
        if isinstance(request_or_send_time, Request):
            try:
                body = await request_or_send_time.json()
                client_send_time = body.get("client_send_time")
            except Exception:
                pass
        elif isinstance(request_or_send_time, (int, float)):
            client_send_time = request_or_send_time

        parent_receive_time = time.time()
        self.request_count += 1

        client_to_parent_delay_ms = None
        if client_send_time is not None:
            client_to_parent_delay_ms = (parent_receive_time - client_send_time) * 1000

        parent_send_time = time.time()

        child_response = await self.child_handle.remote(
            self.replica_id, self.node_id, parent_send_time
        )

        parent_receive_child_time = time.time()

        child_send_time = child_response.get("child_send_time")
        child_to_parent_delay_ms = None
        if child_send_time is not None:
            child_to_parent_delay_ms = (parent_receive_child_time - child_send_time) * 1000

        child_response["client_to_parent_delay_ms"] = client_to_parent_delay_ms
        child_response["child_to_parent_delay_ms"] = child_to_parent_delay_ms
        child_response["parent_send_response_time"] = time.time()

        return child_response


# Default Pow2 app — replicas/resources configured via YAML deployments section
app = ParentDeployment.bind(ChildDeployment.bind())
