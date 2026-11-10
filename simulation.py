from collections import defaultdict, deque
from dataclasses import dataclass
from agents import Client, Coordinator, Worker, score
from models import DEFAULT_WORKERS, Message, Task, TaskSpec, TaskStatus, WorkerSpec

COORD_TYPES = {'STATUS_REQUEST', 'STATUS_RESPONSE', 'CFP', 'PROPOSAL',
               'PROPOSAL_REJECT', 'TASK_OFFER', 'TASK_ACCEPT', 'TASK_REJECT'}


@dataclass
class RunResult:
    mode: str
    tasks: list[Task]
    messages: list[Message]
    events: list[str]
    cpu_utilization: float
    ram_utilization: float
    finish_time: int
    coordination_ticks: int
    max_parallel_negotiations: int

    def metrics(self) -> dict:
        done = [t for t in self.tasks if t.status == TaskStatus.COMPLETED]
        return {
            'mode': self.mode,
            'completed': len(done),
            'total': len(self.tasks),
            'makespan': self.finish_time,
            'avg_wait': round(sum(t.started_at - t.arrival for t in done) / len(done), 3) if done else 0,
            'avg_response': round(sum(t.finished_at - t.arrival for t in done) / len(done), 3) if done else 0,
            'messages_total': len(self.messages),
            'messages_coordination': sum(m.type in COORD_TYPES for m in self.messages),
            'coordination_ticks': self.coordination_ticks,
            'avg_coordination_ticks': round(self.coordination_ticks / len(done), 3) if done else 0,
            'max_parallel_negotiations': self.max_parallel_negotiations,
            'cpu_utilization': round(self.cpu_utilization, 4),
            'ram_utilization': round(self.ram_utilization, 4),
            'deadline_met': sum(t.finished_at <= t.deadline for t in done),
        }


class Simulation:
    def __init__(self, specs: list[TaskSpec], mode: str,
                 workers: tuple[WorkerSpec, ...] = DEFAULT_WORKERS,
                 verbose: bool = False, coordinator_capacity: int = 5,
                 max_negotiations: int = 4):
        if coordinator_capacity < 1 or max_negotiations < 1:
            raise ValueError('Capacity and parallel negotiations must be positive')
        self.tasks = {s.id: Task(**vars(s)) for s in specs}
        if len(self.tasks) != len(specs):
            raise ValueError('Task IDs must be unique')
        if any(s.arrival < 0 or s.duration <= 0 or s.cpu <= 0 or s.ram <= 0 for s in specs):
            raise ValueError('Invalid task parameters')
        self.workers = {s.id: Worker(s) for s in workers}
        if len(self.workers) != len(workers) or not workers:
            raise ValueError('Workers must be nonempty and have unique IDs')
        for t in self.tasks.values():
            if not any(t.cpu <= w.cpu and t.ram <= w.ram for w in self.workers.values()):
                raise ValueError(f'Task {t.id} cannot fit any worker even when idle')
        self.client = Client()
        self.coordinator = Coordinator(mode)
        self.mode = mode
        self.verbose = verbose
        self.coordinator_capacity = coordinator_capacity
        self.max_negotiations = max_negotiations
        self.now = 0
        self.messages: list[Message] = []
        self.events: list[str] = []
        self.message_id = 0
        self.cpu_area = 0
        self.ram_area = 0
        self.coordination_ticks = 0
        self.peak_negotiations = 0
        self.arrivals = defaultdict(list)
        for s in specs:
            self.arrivals[s.arrival].append(s.id)
        self.transit: dict[int, list[Message]] = defaultdict(list)
        self.c_inbox: deque[Message] = deque()
        self.c_outbox: deque[tuple[str, str, int | None, dict]] = deque()
        self.rounds: dict[int, dict] = {}
        self.retry_after: dict[int, int] = {}
        self.first_solicitation: dict[int, int] = {}

    def log(self, line: str) -> None:
        entry = f't={self.now:03d} {line}'
        self.events.append(entry)
        if self.verbose:
            print(entry)

    def send(self, sender: str, receiver: str, kind: str,
             task_id: int | None = None, **payload) -> None:
        self.message_id += 1
        msg = Message(self.message_id, sender, receiver, kind, task_id, payload, self.now)
        self.messages.append(msg)
        self.transit[self.now + 1].append(msg)
        if self.verbose:
            self.log(f'MSG {sender}->{receiver} {kind} task={task_id}')

    def queue_c(self, receiver: str, kind: str, task_id: int | None = None, **payload) -> None:
        self.c_outbox.append(('C', receiver, kind, task_id, payload))

    def receive_worker(self, msg: Message) -> None:
        worker = self.workers[msg.receiver]
        task = self.tasks[msg.task_id]
        if msg.type == 'STATUS_REQUEST':
            self.send(worker.id, 'C', 'STATUS_RESPONSE', task.id, **worker.status())
        elif msg.type == 'CFP':
            bid = worker.proposal(task)
            if bid is None:
                self.send(worker.id, 'C', 'TASK_REJECT', task.id, reason='insufficient_resources')
            else:
                self.send(worker.id, 'C', 'PROPOSAL', task.id, load=bid[0], balance=bid[1])
        elif msg.type == 'TASK_OFFER':
            if worker.accept(task, self.now):
                self.send(worker.id, 'C', 'TASK_ACCEPT', task.id)
                self.send(worker.id, 'C', 'TASK_START', task.id)
                self.log(f'TASK {task.id} START on {worker.id} (CPU={task.cpu}, RAM={task.ram})')
            else:
                self.send(worker.id, 'C', 'TASK_REJECT', task.id, reason='resources_changed')
        elif msg.type == 'PROPOSAL_REJECT':
            pass
        else:
            raise ValueError(f'Unexpected worker message {msg.type}')

    def receive_c(self, msg: Message) -> None:
        tid = msg.task_id
        if msg.type == 'NEW_TASK':
            self.coordinator.pending.append(tid)
            self.log(f'TASK {tid} ARRIVE')
        elif msg.type in ('STATUS_RESPONSE', 'PROPOSAL', 'TASK_REJECT'):
            round_ = self.rounds.get(tid)
            if round_ is None:
                return
            if msg.type == 'TASK_REJECT' and round_['phase'] == 'offered':
                if msg.sender == round_['winner']:
                    self.log(f'TASK {tid} REJECTED by {msg.sender}; retry')
                    del self.rounds[tid]
                    self.retry_after[tid] = self.now + 2
                return
            if round_['phase'] != 'collect' or msg.sender in round_['responses']:
                return
            if msg.type == 'STATUS_RESPONSE':
                task = self.tasks[tid]
                p = msg.payload
                if p['cpu_used'] + task.cpu <= p['cpu'] and p['ram_used'] + task.ram <= p['ram']:
                    bid = score(p['cpu'], p['ram'], p['cpu_used'], p['ram_used'], task)
                else:
                    bid = None
            elif msg.type == 'PROPOSAL':
                bid = (msg.payload['load'], msg.payload['balance'])
            else:
                bid = None
            round_['responses'][msg.sender] = bid
        elif msg.type == 'TASK_ACCEPT':
            round_ = self.rounds.get(tid)
            if round_ and round_['phase'] == 'offered' and round_['winner'] == msg.sender:
                self.coordinator.pending.remove(tid)
                self.coordination_ticks += self.now - self.first_solicitation.pop(tid)
                del self.rounds[tid]
        elif msg.type == 'TASK_START':
            pass
        elif msg.type == 'TASK_COMPLETE':
            self.queue_c('G', 'TASK_RESULT', tid, result='SUCCESS', worker=msg.sender,
                         finished_at=msg.payload['finished_at'])
        else:
            raise ValueError(f'Unexpected coordinator message {msg.type}')

    def advance_rounds(self) -> None:
        for tid, round_ in list(self.rounds.items()):
            if round_['phase'] != 'collect' or len(round_['responses']) < len(self.workers):
                continue
            choices = [(v[0], v[1], wid) for wid, v in round_['responses'].items() if v is not None]
            if not choices:
                del self.rounds[tid]
                self.retry_after[tid] = self.now + 2
                self.log(f'TASK {tid} WAITING: no available worker')
                continue
            _, _, winner = min(choices)
            round_['winner'] = winner
            round_['phase'] = 'offered'
            # The winning offer is sent before the rejection notices.
            self.queue_c(winner, 'TASK_OFFER', tid)
            if self.mode == 'cnp':
                for wid, bid in round_['responses'].items():
                    if wid != winner and bid is not None:
                        self.queue_c(wid, 'PROPOSAL_REJECT', tid)

    def start_rounds(self) -> None:
        available = self.max_negotiations - len(self.rounds)
        if available <= 0:
            return
        for tid in self.coordinator.order(self.tasks):
            if available == 0:
                break
            if tid in self.rounds or self.retry_after.get(tid, 0) > self.now:
                continue
            if self.tasks[tid].status != TaskStatus.WAITING:
                continue
            self.rounds[tid] = {'phase': 'collect', 'responses': {}, 'winner': None}
            self.first_solicitation.setdefault(tid, self.now)
            kind = 'STATUS_REQUEST' if self.mode == 'central' else 'CFP'
            for wid in self.workers:
                self.queue_c(wid, kind, tid, cpu=self.tasks[tid].cpu,
                             ram=self.tasks[tid].ram, duration=self.tasks[tid].duration)
            available -= 1
        self.peak_negotiations = max(self.peak_negotiations, len(self.rounds))

    def run(self, max_ticks: int = 10000) -> RunResult:
        if not self.tasks:
            return RunResult(self.mode, [], [], [], 0, 0, 0, 0, 0)
        last_arrival = max(t.arrival for t in self.tasks.values())
        for now in range(max_ticks):
            self.now = now
            for worker in self.workers.values():
                for task in worker.complete(now):
                    self.send(worker.id, 'C', 'TASK_COMPLETE', task.id, finished_at=now)
                    self.log(f'TASK {task.id} COMPLETE on {worker.id}')
            for tid in self.arrivals[now]:
                self.send('G', 'C', 'NEW_TASK', tid)
            for msg in self.transit.pop(now, []):
                if msg.receiver == 'C':
                    self.c_inbox.append(msg)
                elif msg.receiver == 'G':
                    self.client.receive(msg.task_id, {'status': 'COMPLETED',
                        'worker': msg.payload['worker'], 'finished_at': msg.payload['finished_at']})
                else:
                    self.receive_worker(msg)
            budget = self.coordinator_capacity
            inbound_first = (now % 2 == 0)
            while budget and (self.c_inbox or self.c_outbox):
                if (inbound_first and self.c_inbox) or not self.c_outbox:
                    self.receive_c(self.c_inbox.popleft())
                else:
                    sender, receiver, kind, tid, payload = self.c_outbox.popleft()
                    self.send(sender, receiver, kind, tid, **payload)
                budget -= 1
                inbound_first = not inbound_first
            self.advance_rounds()
            self.start_rounds()
            while budget and self.c_outbox:
                sender, receiver, kind, tid, payload = self.c_outbox.popleft()
                self.send(sender, receiver, kind, tid, **payload)
                budget -= 1
            self.cpu_area += sum(w.cpu_used for w in self.workers.values())
            self.ram_area += sum(w.ram_used for w in self.workers.values())
            if (now >= last_arrival and len(self.client.results) == len(self.tasks)
                    and not self.rounds and not self.coordinator.pending):
                total_cpu = sum(w.cpu for w in self.workers.values())
                total_ram = sum(w.ram for w in self.workers.values())
                span = now + 1
                return RunResult(self.mode, list(self.tasks.values()), self.messages,
                                 self.events, self.cpu_area / (span * total_cpu),
                                 self.ram_area / (span * total_ram), now,
                                 self.coordination_ticks, self.peak_negotiations)
        raise RuntimeError(f'Simulation did not finish in {max_ticks} ticks')
