import csv
import random
from pathlib import Path
from models import TaskSpec
from simulation import Simulation


def workload(count: int, seed: int, interval: int) -> list[TaskSpec]:
    rng = random.Random(seed)
    result = []
    for i in range(count):
        arrival = i * interval
        duration = rng.randint(3, 9)
        result.append(TaskSpec(
            id=i + 1,
            cpu=rng.randint(1, 4),
            ram=rng.choice((2, 4, 6, 8)),
            duration=duration,
            priority=rng.randint(1, 3),
            deadline=arrival + duration + 18,
            arrival=arrival,
        ))
    return result


def compare(count: int = 20, seeds: tuple[int, ...] = (11, 22, 33), output: str = 'results', coordinator_capacity: int = 5, max_negotiations: int = 4) -> list[dict]:
    directory = Path(output)
    directory.mkdir(parents=True, exist_ok=True)
    rows = []
    for scenario, interval in [('low', 9), ('medium', 4), ('high', 1)]:
        for seed in seeds:
            specs = workload(count, seed, interval)
            for mode in ('central', 'cnp'):
                result = Simulation(specs, mode, coordinator_capacity=coordinator_capacity, max_negotiations=max_negotiations).run()
                row = {'scenario': scenario, 'seed': seed, 'coordinator_capacity': coordinator_capacity, 'max_negotiations': max_negotiations, **result.metrics()}
                rows.append(row)
    with (directory / 'metrics.csv').open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return rows


def summary(rows: list[dict]) -> None:
    print(f"{'LOAD':<9} {'MODE':<9} {'FINISH':>8} {'WAIT':>8} {'MSG':>8} {'COORD MSG':>11} {'CPU%':>8}")
    for scenario in ('low', 'medium', 'high'):
        for mode in ('central', 'cnp'):
            group = [r for r in rows if r['scenario'] == scenario and r['mode'] == mode]
            avg = lambda key: sum(r[key] for r in group) / len(group)
            print(f"{scenario:<9} {mode:<9} {avg('makespan'):>8.1f} {avg('avg_wait'):>8.1f} {avg('messages_total'):>8.1f} {avg('messages_coordination'):>11.1f} {100*avg('cpu_utilization'):>7.1f}%")
