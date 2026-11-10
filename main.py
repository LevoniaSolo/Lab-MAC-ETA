import argparse
import csv
from pathlib import Path
from experiments import compare, summary, workload
from simulation import Simulation


def main():
    parser = argparse.ArgumentParser(description='Multi-agent computing resource simulator (Lab 2)')
    parser.add_argument('--mode', choices=('central', 'cnp', 'compare'), default='compare')
    parser.add_argument('--count', type=int, default=20)
    parser.add_argument('--seed', type=int, default=11)
    parser.add_argument('--interval', type=int, default=3, help='Ticks between arrivals in single-mode runs')
    parser.add_argument('--output', default='results')
    parser.add_argument('--verbose', action='store_true')
    parser.add_argument('--capacity', type=int, default=5, help='Coordinator message operations per tick')
    parser.add_argument('--parallel', type=int, default=4, help='Maximum concurrent allocation negotiations')
    args = parser.parse_args()
    if args.count <= 0 or args.interval < 0 or args.capacity <= 0 or args.parallel <= 0:
        parser.error('--count, --capacity, --parallel must be positive; --interval nonnegative')
    if args.mode == 'compare':
        rows = compare(count=args.count, seeds=(args.seed, args.seed + 1, args.seed + 2), output=args.output, coordinator_capacity=args.capacity, max_negotiations=args.parallel)
        summary(rows)
        print(f'\nFull results: {args.output}/metrics.csv')
    else:
        result = Simulation(workload(args.count, args.seed, args.interval), args.mode, verbose=args.verbose, coordinator_capacity=args.capacity, max_negotiations=args.parallel).run()
        print('\nMETRICS')
        for key, value in result.metrics().items():
            print(f'  {key}: {value}')
        out = Path(args.output)
        out.mkdir(parents=True, exist_ok=True)
        with (out / f'{args.mode}_messages.csv').open('w', newline='', encoding='utf-8') as handle:
            writer = csv.writer(handle)
            writer.writerow(['id', 'time', 'sender', 'receiver', 'type', 'task_id', 'payload'])
            for m in result.messages:
                writer.writerow([m.id, m.time, m.sender, m.receiver, m.type, m.task_id, repr(m.payload)])
        (out / f'{args.mode}_events.txt').write_text('\n'.join(result.events) + '\n', encoding='utf-8')
        print(f'Logs: {out}/{args.mode}_messages.csv, {out}/{args.mode}_events.txt')


if __name__ == '__main__':
    main()
