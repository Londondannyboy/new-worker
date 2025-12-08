#!/usr/bin/env python3
"""
Worker Entrypoint

Selects which worker to run based on WORKER_TYPE env var.
"""

import os
import sys
import subprocess

WORKER_TYPE = os.getenv("WORKER_TYPE", "content")

WORKERS = {
    "content": "apps/new_content_worker/worker.py",
    "job": "apps/new_job_worker/worker.py",
}

if __name__ == "__main__":
    worker_path = WORKERS.get(WORKER_TYPE)
    if not worker_path:
        print(f"Unknown WORKER_TYPE: {WORKER_TYPE}")
        print(f"Valid options: {list(WORKERS.keys())}")
        sys.exit(1)

    print(f"Starting {WORKER_TYPE} worker...")
    os.execvp("python", ["python", worker_path])
