"""Simple comparison of the original Bully algorithm and batched optimization.

Put beside Bully_Original.py and Bully_Optimized.py.
Run: python test_bully.py
"""
import contextlib
import io
import statistics

import Bully_Original as original
import Bully_Optimized as optimized

# Keep these identical for both implementations.
TIMEOUT = 0.01
REPEATS = 3
for module in (original, optimized):
    module.ELECTION_TIMEOUT = TIMEOUT

# Include cases in which optimized reduces election-specific RPCs,
# alongside cases in which it does worse.
SCENARIOS = [
    (5, []),
    (5, [3]),
    (5, [5]),
    (5, [4, 5]),
    (10, []),
    (10, [5]),
    (10, [10]),
    (10, [9, 10]),
    (20, []),
    (20, [10]),
    (20, [20]),
    (20, [18, 19, 20]),
    (30, []),
    (30, [18]),
    (30, [30]),
    (30, [25, 26, 27, 28, 29, 30]),
    (40, []),
    (40, [20]),
    (40, [40]),
    (40, [37, 38, 39, 40]),
    (50, []),
    (50, [30]),
    (50, [50]),
    (50, [47, 48, 49, 50]),
]


def run_once(module, count, crashed):
    system = module.System()
    for node_id in range(1, count + 1):
        system.addNode(module.Node(node_id, system))
    for node_id in crashed:
        system.getNode(node_id).crash()

    # Suppress lengthy per-node log; the result table is enough.
    with contextlib.redirect_stdout(io.StringIO()):
        elapsed = system.time_until_normal(max_seconds=10)

    alive = [node for node in system.getNodes() if node.state != module.State.DOWN]
    expected = max(node.id for node in alive)
    correct = all(node.state == module.State.NORMAL and node.leader_id == expected
                  for node in alive)
    return elapsed * 1000, system.election_message_count, system.monitoring_message_count, system.message_count, correct


def main():
    print(f"Timeout: {TIMEOUT} s | Repetitions: {REPEATS} | times shown as average ms")
    print(f"{'N':>3}  {'Crashed':<24} {'Version':<10} {'Time ms':>9} {'Election':>9} {'Monitor':>8} {'Total':>7} {'Correct':>8}")
    print('-' * 94)
    for count, crashed in SCENARIOS:
        results = {}
        for label, module in [('Original', original), ('Optimized', optimized)]:
            try:
                runs = [run_once(module, count, crashed) for _ in range(REPEATS)]
                mean = lambda pos: statistics.mean(item[pos] for item in runs)
                results[label] = (mean(0), mean(1), mean(2), mean(3), all(item[4] for item in runs))
                tm, elec, monitor, total, correct = results[label]
                print(f"{count:>3}  {str(crashed):<24} {label:<10} {tm:>9.2f} {elec:>9.1f} {monitor:>8.1f} {total:>7.1f} {str(correct):>8}")
            except TimeoutError:
                print(f"{count:>3}  {str(crashed):<24} {label:<10} {'TIMEOUT':>9}")
        if len(results) == 2:
            orig, opt = results['Original'], results['Optimized']
            print('     Difference (optimized - original): '
                  f'election {opt[1]-orig[1]:+.1f}, total {opt[3]-orig[3]:+.1f}, time {opt[0]-orig[0]:+.2f} ms')
        print('-' * 94)


if __name__ == '__main__':
    main()
