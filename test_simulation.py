import unittest
from agents import score
from experiments import workload
from models import Task, TaskSpec, TaskStatus, WorkerSpec
from simulation import Simulation


class SimulationTests(unittest.TestCase):
    def test_both_modes_finish_and_return_results(self):
        specs = workload(15, 7, 2)
        for mode in ('central', 'cnp'):
            sim = Simulation(specs, mode)
            result = sim.run()
            self.assertEqual(len(sim.client.results), 15)
            self.assertTrue(all(t.status == TaskStatus.COMPLETED for t in result.tasks))
            self.assertTrue(any(m.type == 'TASK_RESULT' for m in result.messages))

    def test_resource_limits_and_parallelism(self):
        specs = [TaskSpec(i, 1, 2, 30, 1, 100, 0) for i in range(1, 9)]
        sim = Simulation(specs, 'cnp')
        result = sim.run()
        self.assertEqual(result.metrics()['completed'], 8)
        self.assertTrue(any(len([t for t in result.tasks if t.assigned_to == w and t.started_at <= tick < t.finished_at]) > 1
                            for w in sim.workers for tick in range(result.finish_time)))

    def test_limited_coordinator_capacity(self):
        specs = workload(20, 42, 0)
        for capacity in (1, 2, 5):
            for mode in ('central', 'cnp'):
                sim = Simulation(specs, mode, coordinator_capacity=capacity)
                result = sim.run()
                self.assertEqual(result.metrics()['completed'], 20)
                self.assertEqual(len(sim.client.results), 20)

    def test_cnp_extra_messages_can_affect_time(self):
        specs = workload(25, 42, 0)
        central = Simulation(specs, 'central', coordinator_capacity=2).run()
        cnp = Simulation(specs, 'cnp', coordinator_capacity=2).run()
        self.assertGreater(cnp.metrics()['messages_coordination'], central.metrics()['messages_coordination'])
        self.assertGreater(cnp.finish_time, central.finish_time)

    def test_identical_tasks_and_worker_constraints(self):
        specs = workload(30, 19, 1)
        for mode in ('central', 'cnp'):
            sim = Simulation(specs, mode)
            result = sim.run()
            self.assertEqual(set(sim.client.results), {s.id for s in specs})
            self.assertTrue(all(t.finished_at >= t.started_at + t.duration for t in result.tasks))

    def test_score_balance(self):
        task = Task(1, 2, 4, 5, 1, 10, 0)
        self.assertEqual(score(8, 16, 0, 0, task), (0.25, 0.0))

    def test_no_overcommit(self):
        specs = workload(30, 10, 0)
        for mode in ('central', 'cnp'):
            result = Simulation(specs, mode).run()
            for w in ('W1', 'W2', 'W3', 'W4'):
                cpu, ram = {'W1': (4, 8), 'W2': (8, 16), 'W3': (12, 16), 'W4': (6, 32)}[w]
                for tick in range(result.finish_time + 1):
                    active = [t for t in result.tasks if t.assigned_to == w and t.started_at <= tick < t.finished_at]
                    self.assertLessEqual(sum(t.cpu for t in active), cpu)
                    self.assertLessEqual(sum(t.ram for t in active), ram)


if __name__ == '__main__':
    unittest.main()
