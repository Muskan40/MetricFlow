"""Standalone load generator: posts batches of logs and metrics to the ingestion API.

Run once and exit. Volume, target, and batch size are configurable by env var
or CLI flag, so this can be pointed at nginx, at the ingestion service directly,
or at a deployed host.
"""

import argparse
import json
import os
import random
import sys
import time
import urllib.error
import urllib.request

DEFAULT_TARGET = os.getenv("TARGET_URL", "http://nginx_service")

SERVICES = [
    "api-gateway",
    "auth-service",
    "ingestion-service",
    "payment-worker",
    "search-indexer",
]

# Weighted so healthy traffic dominates, as in real systems.
LEVELS = (
    ["DEBUG"] * 2 + ["INFO"] * 12 + ["WARN"] * 4 + ["ERROR"] * 2 + ["CRITICAL"] * 1
)

MESSAGES = {
    "DEBUG": [
        "cache lookup for key user:{n}",
        "acquired connection from pool (idle={n})",
        "parsed request body in {n}ms",
    ],
    "INFO": [
        "handled GET /v1/items in {n}ms",
        "published {n} messages to queue",
        "health check passed",
        "user session created (uid={n})",
    ],
    "WARN": [
        "upstream latency {n}ms exceeds soft limit",
        "connection pool near capacity ({n} in use)",
        "retrying request (attempt {n})",
        "deprecated endpoint called {n} times",
    ],
    "ERROR": [
        "connection timeout to upstream after {n}ms",
        "failed to persist record id={n}",
        "unhandled exception in worker {n}",
    ],
    "CRITICAL": [
        "disk usage critical at {n}%",
        "database unreachable, {n} requests dropped",
    ],
}


def make_log(now: float) -> dict:
    level = random.choice(LEVELS)
    template = random.choice(MESSAGES[level])
    return {
        "message": template.format(n=random.randint(1, 999)),
        "level": level,
        "service": random.choice(SERVICES),
        # Spread over the last hour so time-range filters have something to cut.
        "timestamp": round(now - random.uniform(0, 3600), 3),
    }


def make_metric(now: float) -> dict:
    # Occasionally emit a hot host so the UI's high-utilization styling shows up.
    hot = random.random() < 0.15
    return {
        "service": random.choice(SERVICES),
        "cpu_utilization": round(random.uniform(70, 99) if hot else random.uniform(2, 65), 2),
        "ram_utilization": round(random.uniform(75, 97) if hot else random.uniform(20, 70), 2),
        "disk_utilization": round(random.uniform(10, 92), 2),
        "timestamp": round(now - random.uniform(0, 3600), 3),
    }


def post(url: str, payload: list, timeout: float = 15.0) -> int:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.status


def wait_for(url: str, retries: int = 30, delay: float = 2.0) -> bool:
    """Poll /health until the ingestion service is accepting requests."""
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=5) as response:
                if response.status == 200:
                    return True
        except (urllib.error.URLError, OSError):
            pass
        print(f"waiting for {url} ({attempt}/{retries})", flush=True)
        time.sleep(delay)
    return False


def send_all(target: str, path: str, factory, count: int, batch_size: int) -> int:
    """Post `count` generated records in batches. Returns the number accepted."""
    url = target.rstrip("/") + path
    sent = 0

    for start in range(0, count, batch_size):
        size = min(batch_size, count - start)
        payload = [factory(time.time()) for _ in range(size)]
        try:
            status = post(url, payload)
            sent += size
            print(f"{path}: sent {sent}/{count} (HTTP {status})", flush=True)
        except urllib.error.HTTPError as exc:
            print(f"{path}: HTTP {exc.code} — {exc.read()[:200]!r}", file=sys.stderr)
        except (urllib.error.URLError, OSError) as exc:
            print(f"{path}: request failed — {exc}", file=sys.stderr)

    return sent


def main() -> int:
    parser = argparse.ArgumentParser(description="Push sample logs and metrics.")
    parser.add_argument("--target", default=DEFAULT_TARGET, help="base URL of the API")
    parser.add_argument("--logs", type=int, default=int(os.getenv("LOG_COUNT", "0")))
    parser.add_argument("--metrics", type=int, default=int(os.getenv("METRIC_COUNT", "0")))
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("BATCH_SIZE", "25")))
    parser.add_argument("--seed", type=int, default=None, help="seed for reproducible data")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    # Default to a random 100-200 per stream when no count is given.
    log_count = args.logs or random.randint(100, 200)
    metric_count = args.metrics or random.randint(100, 200)

    print(f"target={args.target} logs={log_count} metrics={metric_count}", flush=True)

    if not wait_for(args.target.rstrip("/") + "/health"):
        print("ingestion service never became healthy", file=sys.stderr)
        return 1

    logs_sent = send_all(args.target, "/logs", make_log, log_count, args.batch_size)
    metrics_sent = send_all(args.target, "/metrics", make_metric, metric_count, args.batch_size)

    print(f"done: {logs_sent} logs, {metrics_sent} metrics", flush=True)
    return 0 if (logs_sent == log_count and metrics_sent == metric_count) else 1


if __name__ == "__main__":
    sys.exit(main())
