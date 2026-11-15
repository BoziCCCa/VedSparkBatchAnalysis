"""Minimum viable benchmark harness.

Runs 3 jobs x 4 modes x 3 runs, records wall-clock seconds per submit
to a CSV, and prints a median summary table at the end.

Jobs : clean.py, q_speed_by_hour.py, q_trip_distance.py
Modes: local, cluster-1, cluster-2, cluster-3

HDFS must already be running (./start.sh starts it). The harness
manages the Spark cluster size itself between modes.

Run from the project root:
    .\.conda-env\python.exe benchmarks\run_benchmark.py

After it finishes, open notebooks/02_benchmark_results.ipynb to view
the median table and chart.
"""
import csv
import statistics
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RESULTS = Path(__file__).resolve().parent / "results.csv"

JOBS = ["clean.py", "q_speed_by_hour.py", "q_trip_distance.py"]
MODES = ["local", "cluster-1", "cluster-2", "cluster-3"]
RUNS_PER_CELL = 3

CLUSTER_WARMUP_SEC = 10


def sh(cmd, check=True, quiet=False):
    """Run a command with the project root as cwd."""
    stdout = subprocess.DEVNULL if quiet else None
    stderr = subprocess.DEVNULL if quiet else None
    return subprocess.run(cmd, cwd=ROOT, check=check,
                          stdout=stdout, stderr=stderr)


def stop_cluster():
    sh(["docker", "compose", "-f", "docker-compose.spark.yml", "down"],
       quiet=True, check=False)


def start_cluster(workers: int):
    services = ["spark-master", "spark-history-server"] + [
        f"spark-worker-{i}" for i in range(1, workers + 1)
    ]
    # Remove any workers above the requested count
    for i in range(workers + 1, 4):
        sh(["docker", "rm", "-f", f"spark-worker-{i}"], quiet=True, check=False)
    sh(["docker", "compose", "-f", "docker-compose.spark.yml", "up", "-d"] + services)
    time.sleep(CLUSTER_WARMUP_SEC)


def set_mode(mode: str):
    """Shape the cluster for the given mode."""
    if mode == "local":
        # Free the memory used by cluster containers
        stop_cluster()
        return
    workers = int(mode.split("-")[1])
    start_cluster(workers)


def submit(mode: str, job: str) -> float:
    """Submit one run, return wall-clock seconds."""
    # --service-ports is needed so the Spark UI at 4040 is reachable from the host
    if mode == "local":
        cmd = ["docker", "compose", "-f", "docker-compose.standalone.yml", "run",
               "--rm", "--service-ports", "spark-standalone", f"/app/{job}"]
    else:
        cmd = ["docker", "compose", "-f", "docker-compose.app.yml", "run",
               "--rm", "--service-ports", "spark-app", f"/app/{job}"]

    t0 = time.perf_counter()
    sh(cmd, quiet=True)
    return time.perf_counter() - t0


def write_csv(rows):
    with RESULTS.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["mode", "job", "run", "seconds"])
        writer.writerows(rows)


def print_summary(rows):
    medians = {}
    for mode in MODES:
        for job in JOBS:
            samples = [r[3] for r in rows if r[0] == mode and r[1] == job]
            medians[(mode, job)] = statistics.median(samples) if samples else float("nan")

    col_width = 18
    header = f"{'mode':<{col_width}}" + "".join(f"{job:>{col_width}}" for job in JOBS)
    print()
    print(header)
    print("-" * len(header))
    for mode in MODES:
        line = f"{mode:<{col_width}}"
        for job in JOBS:
            line += f"{medians[(mode, job)]:>{col_width - 2}.1f} s"
        print(line)


def main():
    print(f"root: {ROOT}")
    print(f"jobs: {JOBS}")
    print(f"modes: {MODES}")
    print(f"runs per cell: {RUNS_PER_CELL}")

    rows = []
    for mode in MODES:
        print(f"\n>>> mode: {mode}")
        set_mode(mode)
        for job in JOBS:
            for run in range(1, RUNS_PER_CELL + 1):
                elapsed = submit(mode, job)
                print(f"    {job:<24} run {run}: {elapsed:>6.1f} s")
                rows.append((mode, job, run, round(elapsed, 2)))
                # Checkpoint CSV after every run so a crash doesn't lose data
                write_csv(rows)

    print_summary(rows)
    print(f"\nRaw results written to {RESULTS}")


if __name__ == "__main__":
    main()
