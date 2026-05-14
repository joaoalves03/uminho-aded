import csv
import json
import logging
import os
import signal
import socket
import subprocess
import threading
import time
from pathlib import Path
from statistics import mean, stdev
from typing import List, Tuple, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from config import (
    PROMPTS,
    LLAMA_SERVER_FLAGS,
    COMPLETION_PARAMS,
    SUMMARY_METRICS,
    SERVER_PORT,
    WORKER_SAMPLE_INTERVAL,
    MODEL,
    MODEL_KEY,
    RUN_DIR,
    THREADS,
    N_TRIALS,
    MAX_TOKENS,
    WORKER_PORT,
    IB_IFACE,
    SIF_IMAGE,
    LLAMA_BIN,
    SING_BINDS,
    URL,
    SSH,
    CONCURRENCY,
)


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    handlers=[logging.FileHandler(RUN_DIR / "bench.py.log"), logging.StreamHandler()],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Node discovery (SLURM)
# ---------------------------------------------------------------------------
def get_nodes() -> Tuple[str, List[str]]:
    """Return (head_node, [worker_nodes]) from SLURM_JOB_NODELIST."""
    nodelist = os.environ.get("SLURM_JOB_NODELIST", "")
    if not nodelist:
        return "localhost", []
    result = subprocess.run(
        ["scontrol", "show", "hostnames", nodelist],
        capture_output=True,
        text=True,
        check=True,
    )
    nodes = result.stdout.strip().splitlines()
    return nodes[0], nodes[1:]


def get_ib_ip(node: str) -> str:
    """Return InfiniBand IP of *node* via SSH."""
    cmd = f"ip -o -4 addr show dev {IB_IFACE} | awk '{{print $4}}' | cut -d/ -f1"
    r = subprocess.run(SSH + [node, cmd], capture_output=True, text=True, timeout=15)
    ip = r.stdout.strip()
    if not ip:
        raise RuntimeError(f"No IB IP found on {node} ({IB_IFACE})")
    return ip


# ---------------------------------------------------------------------------
# Worker management (rpc-server on remote nodes)
# ---------------------------------------------------------------------------
def start_worker(node: str) -> str:
    """Launch rpc-server on *node* via SSH. Return its IB IP."""
    ip = get_ib_ip(node)

    # Build environment exports (no more EXTRA_ENV)
    env_exports = f"export OMP_NUM_THREADS={THREADS} OPENBLAS_NUM_THREADS={THREADS}"

    # Singularity command – note the quotes around --bind argument
    singularity_cmd = (
        f"singularity exec --bind '{SING_BINDS}' {SIF_IMAGE} "
        f"{LLAMA_BIN}/rpc-server --host {ip} --port {WORKER_PORT} --device CPU"
    )

    # Compose full command: exports, then run singularity in background, redirect logs
    full_cmd = (
        f"{env_exports} ; nohup {singularity_cmd} > {RUN_DIR}/worker_{node}.log 2>&1 &"
    )

    # Execute via SSH, using bash to interpret the command string
    subprocess.run(SSH + [node, "bash", "-c", full_cmd], check=True, timeout=30)
    log.info("Worker %s started on %s:%s", node, ip, WORKER_PORT)
    return ip


def wait_worker(ip: str, timeout: int = 60):
    """Block until worker at *ip:WORKER_PORT* accepts connections."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            socket.create_connection((ip, WORKER_PORT), timeout=2).close()
            return
        except OSError:
            time.sleep(1)
    raise RuntimeError(f"Worker {ip}:{WORKER_PORT} not ready after {timeout}s")


def stop_worker(node: str):
    """Kill rpc-server on remote node."""
    subprocess.run(
        SSH + [node, f"pkill -f 'rpc-server.*{WORKER_PORT}' || true"],
        capture_output=True,
    )
    log.info("Worker %s stopped", node)


# ---------------------------------------------------------------------------
# llama-server (head node)
# ---------------------------------------------------------------------------
def build_server_cmd(rpc_nodes: str) -> List[str]:
    """Compile the singularity exec command for llama-server."""
    cmd = [
        "singularity",
        "exec",
        "--bind",
        SING_BINDS,
        SIF_IMAGE,
        f"{LLAMA_BIN}/llama-server",
        "-m",
        MODEL,
        "--host",
        "0.0.0.0",
        "--port",
        str(SERVER_PORT),
    ]
    for flag, value in LLAMA_SERVER_FLAGS.items():
        if flag in ("--threads", "--threads-batch"):
            cmd += [flag, str(THREADS)]
        elif value is None:
            cmd.append(flag)  # boolean flag
        else:
            cmd += [flag, str(value)]
    if rpc_nodes:
        cmd += ["--rpc", rpc_nodes]
    return cmd


def start_server(rpc_nodes: str) -> subprocess.Popen:
    """Launch llama-server and return the Popen object."""
    cmd = build_server_cmd(rpc_nodes)
    proc = subprocess.Popen(
        cmd,
        stdout=open(RUN_DIR / "server.log", "w"),
        stderr=subprocess.STDOUT,
        env={**os.environ},
    )
    mode = f"rpc={rpc_nodes}" if rpc_nodes else "single-node"
    log.info("llama-server PID %d (%s)", proc.pid, mode)
    return proc


def wait_server(timeout: int = 300):
    """Block until llama-server health endpoint responds."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if requests.get(f"{URL}/health", timeout=2).status_code == 200:
                log.info("llama-server ready")
                return
        except requests.RequestException:
            pass
        time.sleep(1)
    raise RuntimeError("llama-server not ready after timeout")


def find_llama_server_pid(parent_pid: int, timeout: int = 30) -> int:
    """
    Find the child process of parent_pid that runs the actual llama-server binary.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            # Get child PIDs of parent_pid
            result = subprocess.run(
                ["ps", "--ppid", str(parent_pid), "-o", "pid=", "--no-headers"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                child_pids = result.stdout.strip().split()
                for pid in child_pids:
                    # Check the command line of each child
                    cmd_result = subprocess.run(
                        ["ps", "-p", pid, "-o", "cmd=", "--no-headers"],
                        capture_output=True,
                        text=True,
                        timeout=5,
                    )
                    if (
                        cmd_result.returncode == 0
                        and "llama-server" in cmd_result.stdout
                    ):
                        # Exclude the parent itself by checking it's not the singularity command
                        if "singularity" not in cmd_result.stdout:
                            return int(pid)
        except subprocess.TimeoutExpired:
            pass
        time.sleep(1)
    raise RuntimeError(f"Could not find llama-server child of parent PID {parent_pid}")


# ---------------------------------------------------------------------------
# Resource monitoring (CPU + network on all nodes, memory on head)
# ---------------------------------------------------------------------------
def read_local_stats() -> Dict[str, float]:
    """Return dict with local CPU% and IB rx/tx bytes."""
    stats = {"cpu": 0.0, "rx": 0, "tx": 0}
    try:
        line = Path("/proc/stat").read_text().splitlines()[0].split()
        total = sum(int(x) for x in line[1:])
        idle = int(line[4])
        stats["cpu"] = round((1 - idle / total) * 100, 1)
    except Exception:
        pass
    try:
        stats["rx"] = int(
            Path(f"/sys/class/net/{IB_IFACE}/statistics/rx_bytes").read_text()
        )
        stats["tx"] = int(
            Path(f"/sys/class/net/{IB_IFACE}/statistics/tx_bytes").read_text()
        )
    except Exception:
        pass
    return stats


def read_remote_stats(node: str) -> Dict[str, float]:
    """SSH into *node* and return the same dict as read_local_stats."""
    cmd = (
        "cpu=$(awk '/^cpu /{i=$5;t=0;for(j=2;j<=NF;j++)t+=$j;"
        'printf "%.1f",(1-i/t)*100}\' /proc/stat); '
        f"rx=$(cat /sys/class/net/{IB_IFACE}/statistics/rx_bytes 2>/dev/null||echo 0); "
        f"tx=$(cat /sys/class/net/{IB_IFACE}/statistics/tx_bytes 2>/dev/null||echo 0); "
        "echo $cpu $rx $tx"
    )
    try:
        r = subprocess.run(
            SSH + [node, cmd], capture_output=True, text=True, timeout=10
        )
        p = r.stdout.strip().split()
        if len(p) == 3:
            return {"cpu": float(p[0]), "rx": int(p[1]), "tx": int(p[2])}
    except Exception:
        pass
    return {"cpu": 0.0, "rx": 0, "tx": 0}


def monitor_nodes(nodes: List[str], stop: threading.Event) -> List[Dict[str, Any]]:
    """
    Run in a thread: sample CPU and net counters on *nodes* (local if empty)
    until *stop* is set, then return a list of dicts with aggregated stats.

    All nodes are sampled in parallel to avoid slow sequential SSH calls.
    """
    # Choose the reader: local ignores dummy key, remote takes node name
    if nodes:

        def read_func(node: str):
            return read_remote_stats(node)
    else:

        def read_func(_node: str):
            return read_local_stats()

    keys = nodes if nodes else ["__local__"]
    samples = {k: [] for k in keys}

    # Read initial counters (t0) in parallel
    with ThreadPoolExecutor(max_workers=len(keys)) as pool:
        futures = {pool.submit(read_func, k): k for k in keys}
        t0 = {futures[f]: f.result() for f in as_completed(futures)}

    # Sampling loop – each iteration collects one set of readings from all nodes
    while not stop.is_set():
        with ThreadPoolExecutor(max_workers=len(keys)) as pool:
            futures = {pool.submit(read_func, k): k for k in keys}
            for f in as_completed(futures):
                k = futures[f]
                samples[k].append(f.result())
        stop.wait(WORKER_SAMPLE_INTERVAL)

    # Read final counters (t1) in parallel after the loop
    with ThreadPoolExecutor(max_workers=len(keys)) as pool:
        futures = {pool.submit(read_func, k): k for k in keys}
        t1 = {futures[f]: f.result() for f in as_completed(futures)}

    # Build result list (same as before)
    result = []
    for k in keys:
        cpus = [s["cpu"] for s in samples[k]]
        result.append(
            {
                "node": k if k != "__local__" else "head",
                "cpu_pct": round(mean(cpus), 1) if cpus else 0.0,
                "net_rx_kb": round((t1[k]["rx"] - t0[k]["rx"]) / 1024, 1),
                "net_tx_kb": round((t1[k]["tx"] - t0[k]["tx"]) / 1024, 1),
            }
        )
    return result


def monitor_memory(pid: int, stop: threading.Event):
    """Log VmRSS of *pid* every second into resources.csv."""
    path = Path(f"/proc/{pid}/status")
    out_path = RUN_DIR / "resources.csv"
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["timestamp_ms", "vmrss_kb"])
        while not stop.is_set():
            try:
                for line in path.read_text().splitlines():
                    if line.startswith("VmRSS"):
                        w.writerow([int(time.time() * 1000), int(line.split()[1])])
                        f.flush()
                        break
            except FileNotFoundError:
                break
            time.sleep(1)


def get_peak_memory(run_dir: Path) -> float:
    """Read resources.csv and return peak VmRSS in MB (megabytes)."""
    path = run_dir / "resources.csv"
    if not path.exists():
        return 0.0
    peak_kb = 0.0
    with open(path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                val = float(row["vmrss_kb"])
                if val > peak_kb:
                    peak_kb = val
            except (ValueError, KeyError):
                continue
    return round(peak_kb / 1024, 1)  # convert KB to MB


# ---------------------------------------------------------------------------
# Single inference measurement (streaming)
# ---------------------------------------------------------------------------
def _monitor_wrapper(nodes, stop, result_holder):
    """Target for the monitoring thread: runs monitor_nodes and stores output."""
    result_holder["data"] = monitor_nodes(nodes, stop)


def measure(prompt: str, worker_nodes: List[str]) -> Dict[str, Any]:
    """Send one streaming chat completion request and return metrics."""
    stop_event = threading.Event()
    mon_out = {}

    # Monitoring thread (identical to original, using _monitor_wrapper)
    mon_thread = threading.Thread(
        target=_monitor_wrapper, args=(worker_nodes, stop_event, mon_out), daemon=True
    )
    mon_thread.start()

    t_start = time.time()
    t_first = None
    content_chunks = []
    server_timings = {}
    finish_reason = "unknown"

    try:
        with requests.post(
            f"{URL}/v1/chat/completions",
            json={
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": MAX_TOKENS,
                "stream": True,
                **COMPLETION_PARAMS,
            },
            stream=True,
            timeout=300,
        ) as resp:
            for raw in resp.iter_lines():
                if not raw:
                    continue
                line = raw.decode() if isinstance(raw, bytes) else raw
                if not line.startswith("data:"):
                    continue

                payload = line[5:].strip()
                if payload == "[DONE]":
                    continue

                data = json.loads(payload)

                # Store timings if present (do NOT skip the chunk)
                if "timings" in data:
                    server_timings = data["timings"]

                choices = data.get("choices", [])
                if not choices:
                    continue

                delta = choices[0].get("delta", {})
                piece = delta.get("content")
                reasoning = delta.get("reasoning_content")

                # Reasoning tokens appear before the final answer
                if reasoning:
                    if t_first is None:
                        t_first = time.time()
                    content_chunks.append(reasoning)

                # Regular answer tokens
                if piece:
                    if t_first is None:
                        t_first = time.time()
                    content_chunks.append(piece)

                if choices[0].get("finish_reason"):
                    finish_reason = choices[0]["finish_reason"]

    except Exception:
        log.exception("Request failed")
        raise
    finally:
        t_end = time.time()
        stop_event.set()
        mon_thread.join(timeout=60)

    stats = mon_out.get("data", [])

    ttft_ms = int((t_first - t_start) * 1000) if t_first else 0
    client_decode = int((t_end - t_first) * 1000) if t_first else 0
    wall_ms = int((t_end - t_start) * 1000)

    # Map server timings (chat-renamed fields)
    ts = server_timings
    return {
        "content": "".join(content_chunks),
        "ttft_ms": ttft_ms,
        "client_decode_ms": client_decode,
        "tokens_predicted": ts.get("predicted_n", 0),
        "tokens_evaluated": ts.get("prompt_n", 0),
        "tokens_cached": ts.get("cache_n", 0),
        "stop_type": finish_reason,  # "stop" or "length"
        "truncated": finish_reason == "length",
        "prompt_ms": ts.get("prompt_ms", 0),
        "prompt_per_token_ms": ts.get("prompt_per_token_ms", 0),
        "prompt_per_second": ts.get("prompt_per_second", 0),
        "predicted_ms": ts.get("predicted_ms", 0),
        "predicted_per_token_ms": ts.get("predicted_per_token_ms", 0),
        "predicted_per_second": ts.get("predicted_per_second", 0),
        "wall_ms": wall_ms,
        "nodes_cpu_pct": ",".join(f"{s['cpu_pct']:.1f}" for s in stats),
        "nodes_net_rx_kb": ",".join(f"{s['net_rx_kb']:.1f}" for s in stats),
        "nodes_net_tx_kb": ",".join(f"{s['net_tx_kb']:.1f}" for s in stats),
    }


# ---------------------------------------------------------------------------
# Output handling
# ---------------------------------------------------------------------------
RESULTS_FIELDS = [
    "trial",
    "prompt_id",
    "concurrency",
    "n",
    "model",
    "engine",
    "prompt_length",
    "ttft_ms",
    "client_decode_ms",
    "tokens_predicted",
    "tokens_evaluated",
    "tokens_cached",
    "stop_type",
    "truncated",
    "prompt_ms",
    "prompt_per_token_ms",
    "prompt_per_second",
    "predicted_ms",
    "predicted_per_token_ms",
    "predicted_per_second",
    "wall_ms",
    "nodes_cpu_pct",
    "nodes_net_rx_kb",
    "nodes_net_tx_kb",
    "peak_memory_mb",
]


def write_results_csv(rows: List[Dict], run_dir: Path):
    """Write results.csv from a list of rows, optionally adding peak_memory_mb column."""
    path = run_dir / "results.csv"

    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=RESULTS_FIELDS)
        w.writeheader()
        w.writerows(rows)


def write_summary(rows: List[Dict], run_dir: Path):
    extra_cols = ["model", "engine", "prompt_length", "concurrency", "peak_memory_mb"]

    fields = (
        extra_cols
        + ["prompt_id"]
        + [f"{m}_mean" for m in SUMMARY_METRICS]
        + [f"{m}_std" for m in SUMMARY_METRICS]
    )

    path = run_dir / "summary.csv"
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        groups = {}
        for r in rows:
            key = (r["prompt_id"], r["concurrency"])
            groups.setdefault(key, []).append(r)
        for (pid, conc), subset in sorted(groups.items()):
            # Take metadata from the first row of the group
            first = subset[0]
            row = {col: first.get(col, "") for col in extra_cols}
            row["prompt_id"] = pid
            row["concurrency"] = conc
            # peak_memory_mb is already set in each row (either float or "N/A")
            # Use the value from the first row (all rows in the group have the same)
            row["peak_memory_mb"] = first.get("peak_memory_mb", "N/A")
            for m in SUMMARY_METRICS:
                vals = [float(r[m]) for r in subset]
                row[f"{m}_mean"] = round(mean(vals), 2)
                row[f"{m}_std"] = round(stdev(vals), 2) if len(vals) > 1 else 0.0
            w.writerow(row)
    log.info("summary.csv written")


def write_responses(entries: List[Dict], run_dir: Path):
    """Write responses.json with full generated texts."""
    path = run_dir / "responses.json"
    path.write_text(json.dumps(entries, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("responses.json written")


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------
def main():
    head, worker_nodes = get_nodes()
    log.info("Head node     : %s", head)
    log.info("Worker nodes  : %s", worker_nodes or "none")
    log.info("Model         : %s", MODEL_KEY)
    log.info("Threads       : %d", THREADS)
    log.info("Output dir    : %s", RUN_DIR)
    log.info("Concurrency: %s", CONCURRENCY)

    # Start workers
    worker_info = []  # (node, ip)
    for node in worker_nodes:
        ip = start_worker(node)
        worker_info.append((node, ip))
    for node, ip in worker_info:
        wait_worker(ip)

    rpc_nodes = ",".join(f"{ip}:{WORKER_PORT}" for _, ip in worker_info)

    # Start server
    server = start_server(rpc_nodes)
    time.sleep(2)
    real_pid = find_llama_server_pid(server.pid)
    log.info("Real llama-server PID = %d", real_pid)
    stop_mem = threading.Event()
    mem_thread = threading.Thread(
        target=monitor_memory, args=(real_pid, stop_mem), daemon=True
    )
    mem_thread.start()

    rows = []
    responses = []

    try:
        wait_server()

        # Warm-up (run once for the server, not per concurrency level)
        requests.post(
            f"{URL}/completion",
            json={"prompt": "Hello", "n_predict": 8, "stream": False},
            timeout=30,
        )
        log.info("Warm-up done")

        log.info("=== Concurrency level: %d ===", CONCURRENCY)

        # Optional: short warm-up at this concurrency (small request)
        # to let server adjust internal scheduling
        with ThreadPoolExecutor(max_workers=CONCURRENCY) as warmup_pool:
            warmup_futures = [
                warmup_pool.submit(measure, "Hello", worker_nodes)
                for _ in range(min(CONCURRENCY, 4))
            ]
            for f in as_completed(warmup_futures):
                try:
                    f.result()
                except Exception:
                    pass

        for trial in range(1, N_TRIALS + 1):
            log.info("Trial %d/%d, concurrency=%d", trial, N_TRIALS, CONCURRENCY)

            # For each prompt, send CONCURRENCY parallel requests
            for pid, prompt in enumerate(PROMPTS, 1):
                with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
                    futures = [
                        executor.submit(measure, prompt, worker_nodes)
                        for _ in range(CONCURRENCY)
                    ]
                    for future in as_completed(futures):
                        measurement = future.result()
                        content = measurement.pop("content")

                        responses.append(
                            {
                                "trial": trial,
                                "prompt_id": pid,
                                "concurrency": CONCURRENCY,
                                "prompt": prompt,
                                "response": content,
                            }
                        )

                        engine = (
                            "turboquant"
                            if os.environ.get("ENABLE_TURBOQUANT") == "TRUE"
                            else "vanilla"
                        )

                        row = {
                            "trial": trial,
                            "prompt_id": pid,
                            "concurrency": CONCURRENCY,
                            "n": int(os.environ.get("NODES", "1")),
                            "model": MODEL_KEY,
                            "engine": engine,
                            "prompt_length": os.environ.get("PROMPT_LENGTH", "unknown"),
                            **measurement,
                        }
                        rows.append(row)

                        log.info(
                            "  p%d (conc=%d) ttft=%dms pred=%dms tpot=%.1fms tokens=%d",
                            pid,
                            CONCURRENCY,
                            measurement["ttft_ms"],
                            measurement["predicted_ms"],
                            measurement["predicted_per_token_ms"],
                            measurement["tokens_predicted"],
                        )

    finally:
        # Cleanup
        stop_mem.set()
        server.send_signal(signal.SIGTERM)
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
        for node, _ in worker_info:
            stop_worker(node)

    # Write outputs
    peak_memory_mb = get_peak_memory(RUN_DIR)
    for row in rows:
        if row["n"] > 1:
            row["peak_memory_mb"] = "N/A"
        else:
            row["peak_memory_mb"] = peak_memory_mb
    write_results_csv(rows, RUN_DIR)
    write_summary(rows, RUN_DIR)
    write_responses(responses, RUN_DIR)
    log.info("Benchmark finished. Results in %s", RUN_DIR)


if __name__ == "__main__":
    main()
