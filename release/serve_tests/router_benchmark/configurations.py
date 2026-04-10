"""
Experiment configuration for routing algorithm study.
"""

import hashlib
import math
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Generator, List, Optional


class Algorithm(Enum):
    POW2 = "pow2"
    RANDOM = "random"
    ROUND_ROBIN = "round_robin"
    CENTRALIZED_POW2 = "centralized_pow2"
    CAPACITY_QUEUE_ROUTER = "capacity_queue_router"

    def get_router_class(self) -> Optional[str]:
        if self == Algorithm.POW2:
            return "ray.serve._private.request_router.pow_2_router:PowerOfTwoChoicesRequestRouter"
        elif self == Algorithm.RANDOM:
            return "src.routers.RandomRequestRouter"
        elif self == Algorithm.ROUND_ROBIN:
            return "src.routers.RoundRobinRequestRouter"
        elif self == Algorithm.CENTRALIZED_POW2:
            return "ray.serve._private.request_router.centralized_router:CentralizedPow2Router"
        elif self == Algorithm.CAPACITY_QUEUE_ROUTER:
            return "ray.serve._private.experimental.capacity_queue_request_router:CapacityQueueRouter"


class Scale(Enum):
    SMALL = "small"
    MEDIUM = "medium"
    LARGE = "large"
    XLARGE = "xlarge"


class Ratio(Enum):
    ONE_TO_ONE = "1:1"
    ONE_TO_TWO = "1:2"
    TWO_TO_ONE = "2:1"


class Topology(Enum):
    PACKED = "packed"
    SPREAD = "spread"


class LoadLevel(Enum):
    LOW = 0.50
    MEDIUM = 0.75
    HIGH = 1.00


# Scale definitions: base replica counts for 1:1 ratio
SCALE_REPLICAS = {
    Scale.SMALL: 8,
    Scale.MEDIUM: 32,
    Scale.LARGE: 128,
    Scale.XLARGE: 512,
}

CONCURRENT_PER_REPLICA = 2
MAX_USERS_PER_TASK = 48
CPU_PER_REPLICA = 1

# Simulated work latency configuration (exponential distribution)
SIMULATED_LATENCY_MEAN_S = 1.0    # 1s mean
SIMULATED_LATENCY_CAP_S = 10.0    # 10s cap

RPS_PER_REPLICA = CONCURRENT_PER_REPLICA * (1 / SIMULATED_LATENCY_MEAN_S)


@dataclass(frozen=True)
class ExperimentConfig:
    algorithm: Algorithm
    scale: Scale
    ratio: Ratio
    topology: Topology
    locality: bool
    load_level: LoadLevel

    @property
    def parent_replicas(self) -> int:
        base = SCALE_REPLICAS[self.scale]
        if self.ratio == Ratio.ONE_TO_TWO:
            return base
        elif self.ratio == Ratio.TWO_TO_ONE:
            return base * 2
        else:
            return base

    @property
    def child_replicas(self) -> int:
        base = SCALE_REPLICAS[self.scale]
        if self.ratio == Ratio.ONE_TO_TWO:
            return base * 2
        elif self.ratio == Ratio.TWO_TO_ONE:
            return base
        else:
            return base

    @property
    def bottleneck_replicas(self) -> int:
        return min(self.parent_replicas, self.child_replicas)

    @property
    def theoretical_max_rps(self) -> int:
        return self.bottleneck_replicas * RPS_PER_REPLICA

    @property
    def target_rps(self) -> int:
        return int(self.theoretical_max_rps * self.load_level.value)

    @property
    def num_concurrent(self) -> int:
        base_concurrent = self.bottleneck_replicas * CONCURRENT_PER_REPLICA
        return int(base_concurrent * self.load_level.value)

    @property
    def total_replicas(self) -> int:
        return self.parent_replicas + self.child_replicas

    @property
    def cpus_required(self) -> int:
        return self.total_replicas * CPU_PER_REPLICA

    @property
    def num_tasks(self) -> int:
        return max(1, math.ceil(self.num_concurrent / MAX_USERS_PER_TASK))

    @property
    def ingress_cpus(self) -> int:
        return self.num_tasks + 1

    @property
    def config_hash(self) -> str:
        config_str = (
            f"{self.algorithm.value}_{self.scale.value}_{self.ratio.value}_"
            f"{self.topology.value}_{self.locality}_{self.load_level.value}"
        )
        return hashlib.md5(config_str.encode()).hexdigest()[:8]

    def to_dict(self) -> dict:
        return {
            "algorithm": self.algorithm.value,
            "scale": self.scale.value,
            "ratio": self.ratio.value,
            "topology": self.topology.value,
            "locality": self.locality,
            "load_level": self.load_level.value,
            "parent_replicas": self.parent_replicas,
            "child_replicas": self.child_replicas,
            "num_concurrent": self.num_concurrent,
            "num_tasks": self.num_tasks,
            "ingress_cpus": self.ingress_cpus,
            "target_rps": self.target_rps,
            "theoretical_max_rps": self.theoretical_max_rps,
            "cpus_required": self.cpus_required,
            "config_hash": self.config_hash,
        }

    def __str__(self) -> str:
        return (
            f"Config[{self.algorithm.value}, {self.scale.value}, {self.ratio.value}, "
            f"{self.topology.value}, locality={self.locality}, load={self.load_level.value}]"
        )


@dataclass
class ExperimentRunConfig:
    config: ExperimentConfig
    experiment_id: str
    run_id: str
    repetition: int = 1
    s3_bucket: str = "abrar-test-bucket-123"
    s3_prefix: str = "routing-study"
    warmup_s: float = 10.0
    duration_s: float = 60.0

    @property
    def s3_experiment_path(self) -> str:
        return f"s3://{self.s3_bucket}/{self.s3_prefix}/{self.experiment_id}"

    @property
    def s3_output_path(self) -> str:
        return f"s3://{self.s3_bucket}/{self.s3_prefix}/{self.experiment_id}/{self.run_id}"

    @property
    def num_concurrent(self) -> int:
        return self.config.num_concurrent

    @property
    def num_tasks(self) -> int:
        return self.config.num_tasks

    @property
    def ingress_cpus(self) -> int:
        return self.config.ingress_cpus

    def to_dict(self) -> dict:
        return {
            "experiment_id": self.experiment_id,
            "run_id": self.run_id,
            "repetition": self.repetition,
            "s3_bucket": self.s3_bucket,
            "s3_prefix": self.s3_prefix,
            "s3_output_path": self.s3_output_path if self.s3_bucket else None,
            "warmup_s": self.warmup_s,
            "duration_s": self.duration_s,
            "config": self.config.to_dict(),
        }

    @staticmethod
    def generate_run_id() -> str:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        short_uuid = uuid.uuid4().hex[:6]
        return f"{timestamp}_{short_uuid}"

    @staticmethod
    def generate_experiment_id(config: ExperimentConfig) -> str:
        return (
            f"{config.algorithm.value}_{config.scale.value}_{config.ratio.value}_"
            f"{config.topology.value}_loc{config.locality}_{int(config.load_level.value * 100)}pct"
        )


def generate_quick_configs() -> List[ExperimentConfig]:
    return [
        ExperimentConfig(
            algorithm=algorithm,
            scale=Scale.SMALL,
            ratio=Ratio.ONE_TO_ONE,
            topology=Topology.PACKED,
            locality=False,
            load_level=LoadLevel.MEDIUM,
        )
        for algorithm in Algorithm
    ]
