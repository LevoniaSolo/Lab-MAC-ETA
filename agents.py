from dataclasses import dataclass
from models import Task, TaskStatus, WorkerSpec


def score(cpu_total: int, ram_total: int, cpu_used: int, ram_used: int, task: Task) -> tuple[float, float]:
    cpu_left = cpu_total - cpu_used - task.cpu
    ram_left = ram_total - ram_used - task.ram
    if cpu_left < 0 or ram_left < 0:
        raise ValueError('Insufficient resources')
    load = ((cpu_used + task.cpu) / cpu_total + (ram_used + task.ram) / ram_total) / 2
    imbalance = abs(cpu_left / cpu_total - ram_left / ram_total)
    return load, imbalance


class Worker:
    def __init__(self, spec: WorkerSpec):
        self.id = spec.id
        self.cpu = spec.cpu
        self.ram = spec.ram
        self.running: dict[int, Task] = {}

    @property
    def cpu_used(self) -> int:
        return sum(t.cpu for t in self.running.values())

    @property
    def ram_used(self) -> int:
        return sum(t.ram for t in self.running.values())

    def can_accept(self, task: Task) -> bool:
        return self.cpu_used + task.cpu <= self.cpu and self.ram_used + task.ram <= self.ram

    def status(self) -> dict:
        return {'cpu': self.cpu, 'ram': self.ram, 'cpu_used': self.cpu_used, 'ram_used': self.ram_used}

    def proposal(self, task: Task) -> tuple[float, float] | None:
        if not self.can_accept(task):
            return None
        return score(self.cpu, self.ram, self.cpu_used, self.ram_used, task)

    def accept(self, task: Task, now: int) -> bool:
        if not self.can_accept(task):
            return False
        task.assigned_to = self.id
        task.status = TaskStatus.RUNNING
        task.started_at = now
        self.running[task.id] = task
        return True

    def complete(self, now: int) -> list[Task]:
        completed = [t for t in self.running.values() if t.started_at is not None and t.started_at + t.duration <= now]
        for task in completed:
            del self.running[task.id]
            task.finished_at = now
            task.status = TaskStatus.COMPLETED
        return completed


class Client:
    id = 'G'

    def __init__(self):
        self.results: dict[int, dict] = {}

    def receive(self, task_id: int, result: dict) -> None:
        self.results[task_id] = result


class Coordinator:
    id = 'C'

    def __init__(self, mode: str):
        if mode not in {'central', 'cnp'}:
            raise ValueError(f'Unknown mode: {mode}')
        self.mode = mode
        self.pending: list[int] = []

    def order(self, tasks: dict[int, Task]) -> list[int]:
        return sorted(self.pending, key=lambda tid: (-tasks[tid].priority, tasks[tid].arrival, tid))
