import os
import json
from pathlib import Path

with open("prompts.json", "r") as f:
    ALL_PROMPTS = json.load(f)

LENGTH = os.environ.get("PROMPT_LENGTH", "__all__")
if LENGTH == "__all__":
    PROMPTS = [p for cat in ALL_PROMPTS.values() for p in cat]
else:
    PROMPTS = ALL_PROMPTS[LENGTH]

THREADS = int(os.environ["THREADS"])
try:
    CONCURRENCY = int(os.environ.get("CONCURRENCY_LEVELS", "1"))
except ValueError:
    CONCURRENCY = 1

if os.environ.get("ENABLE_TURBOQUANT") == "TRUE":
    LLAMA_SERVER_FLAGS = {
        "--cache-type-k": "turbo3",
        "--cache-type-v": "turbo4",
        "--flash-attn": "on",
        "--split-mode": "row",
        "--threads": str(THREADS),
        "--threads-batch": str(THREADS),
        "--ctx-size": "8192",
        "--batch-size": "512",
        "--ubatch-size": "512",
        "--no-mmap": None,
        "--mlock": None,
        "--numa": "distribute",
        "--parallel": str(CONCURRENCY),
    }
else:
    LLAMA_SERVER_FLAGS = {
        "--threads": str(THREADS),
        "--threads-batch": str(THREADS),
        "--ctx-size": "8192",
        "--batch-size": "512",
        "--ubatch-size": "512",
        "--no-mmap": None,
        "--mlock": None,
        # "--no-cache-prompt": None,
        "--numa": "distribute",
        "--parallel": str(CONCURRENCY),
        "--split-mode": "row",
    }


COMPLETION_PARAMS = {
    "temperature": 0.7,
    "top_p": 0.9,
    "top_k": 40,
    "min_p": 0.05,
}


SUMMARY_METRICS = [
    "ttft_ms",
    "client_decode_ms",
    "tokens_predicted",
    "tokens_evaluated",
    "tokens_cached",
    "prompt_ms",
    "prompt_per_token_ms",
    "prompt_per_second",
    "predicted_ms",
    "predicted_per_token_ms",
    "predicted_per_second",
    "wall_ms",
]

SERVER_PORT = 8080
WORKER_SAMPLE_INTERVAL = 1.0

MODEL = os.environ["MODEL"]
MODEL_KEY = os.environ["MODEL_KEY"]
RUN_DIR = Path(os.environ["RUN_DIR"])
PROJECT = Path(os.environ["PROJECT_ROOT"])
THREADS = int(os.environ["THREADS"])
N_TRIALS = int(os.environ["N_TRIALS"])
MAX_TOKENS = int(os.environ["MAX_TOKENS"])
WORKER_PORT = int(os.environ.get("WORKER_PORT", 50052))
IB_IFACE = os.environ.get("IB_IFACE", "ib0")
SIF_IMAGE = os.environ["SIF_IMAGE"]
LLAMA_BIN = os.environ["LLAMA_BIN_DIR"]
SING_BINDS = os.environ.get("SINGULARITY_BINDS", "/projects:/projects")
VENV = str(PROJECT / ".venv")

URL = f"http://localhost:{SERVER_PORT}"
SSH = ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes"]
