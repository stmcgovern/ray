from ray import serve
from ray.serve.config import DeploymentActorConfig, RequestRouterConfig
from ray.serve.experimental.capacity_queue import CapacityQueue

from router_benchmark.pow2_app import ChildDeployment, ParentDeployment

_APP_NAME = "routing-study"

_router_config = RequestRouterConfig(
    request_router_class=(
        "ray.serve.experimental.capacity_queue_router:CapacityQueueRouter"
    ),
    request_router_kwargs={
        "capacity_queue_actor_name": "capacity_queue",
        "max_fault_retries": 3,
    },
    initial_backoff_s=0.05,
    backoff_multiplier=2.0,
    max_backoff_s=1.0,
)


def _capacity_queue_actors(deployment_name: str):
    return [
        DeploymentActorConfig(
            name="capacity_queue",
            actor_class=CapacityQueue,
            init_kwargs={
                "acquire_timeout_s": 0.5,
                "token_ttl_s": 5,
                "deployment_id_name": deployment_name,
                "deployment_id_app": _APP_NAME,
            },
            actor_options={"num_cpus": 0},
        ),
    ]


child = ChildDeployment.options(
    request_router_config=_router_config,
    deployment_actors=_capacity_queue_actors("ChildDeployment"),
).bind()

app = ParentDeployment.options(
    request_router_config=_router_config,
    deployment_actors=_capacity_queue_actors("ParentDeployment"),
).bind(child)
