"""Domain objects for the virtual compute cluster."""
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class TaskStatus(str, Enum):
    WAITING = 'WAITING'
    ASSIGNED = 'ASSIGNED'
    RUNNING = 'RUNNING'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'


@dataclass
class Task:
    id: int
    cpu: int
    ram: int
    duration: int
    priority: int
    deadline: int
    arrival: int
    status: TaskStatus = TaskStatus.WAITING
    assigned_to: str | None = None
    started_at: int | None = None
    finished_at: int | None = None


@dataclass(frozen=True)
class TaskSpec:
    id: int
    cpu: int
    ram: int
    duration: int
    priority: int
    deadline: int
    arrival: int


@dataclass(frozen=True)
class Message:
    id: int
    sender: str
    receiver: str
    type: str
    task_id: int | None
    payload: dict[str, Any]
    time: int


@dataclass
class WorkerSpec:
    id: str
    cpu: int
    ram: int


DEFAULT_WORKERS = (
    WorkerSpec('W1', 4, 8),
    WorkerSpec('W2', 8, 16),
    WorkerSpec('W3', 12, 16),
    WorkerSpec('W4', 6, 32),
)
