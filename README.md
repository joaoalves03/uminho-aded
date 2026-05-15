# ADED Project 2 — LLM Inference Benchmark on Deucalion ARM Partition

ARM CPU (Fujitsu A64FX 700) inference benchmark for LLM models (llama.cpp and turboquant), supporting single-node and multi-node execution via SLURM using Singularity containers.

---

## Project structure

```
uminho-aded-proj2/
├── benchmark/
│   ├── submit.sh               # Main script — submits SLURM jobs
│   ├── benchmark.sh            # Job script (executed by SLURM)
│   ├── benchmark.py            # Benchmark logic (Python)
│   ├── config.py               # Configuration read from environment variables
│   ├── prompts.json            # Categorised prompts (short / medium / long)
│   ├── join_results.py         # Aggregates results (standard mode)
│   └── join_results_concurrency.py  # Aggregates results (concurrency mode)
├── plot/
│   ├── concurrency.py          # Concurrency plots
│   ├── memory.py               # Memory plots
│   ├── ttft_scaling.py         # TTFT scaling plots
│   ├── ttft_tpot.py            # TTFT vs TPOT plots
│   └── out/                    # Generated SVGs
├── pyproject.toml
└── uv.lock
```

---

## Prerequisites

### On Deucalion (login node)

- Cluster access with an active SLURM account,
- Model files (`.gguf`) and containers (`.sif`) are already available at:
  - Models: `/projects/F202500010HPCVLABUMINHO/uminhocp010-public/models/`
  - Containers: `/projects/F202500010HPCVLABUMINHO/uminhocp010-public/containers/`

### For plotting (local or login node)

Python 3.13+ with the dependencies from `pyproject.toml`. Using `uv` is recommended:

```bash
uv sync
```

Or with pip:

```bash
pip install matplotlib pandas seaborn
```

---

## Configuring `submit.sh`

Before submitting, edit the variables at the top of `benchmark/submit.sh` file:

| Variable | Description | Example |
|---|---|---|
| `ACCOUNT` | SLURM project account | `f202500001hpcvlabepicurea` |
| `PARTITION` | Cluster partition | `normal-arm` |
| `TIME` | Maximum time per job | `01:30:00` |
| `EXCLUDE` | Nodes to exclude (optional) | `cna[0001-0016]` |
| `MODELS` | List of GGUF models to test | see file |
| `NODES` | Number of nodes per job | `(1)` or `(1 2)` |
| `THREADS` | CPUs per task | `(46)` |
| `TRIALS` | Repetitions per configuration | `(1)` |
| `MAX_TOKENS` | Maximum tokens to generate | `(1024)` |
| `PROMPT_LENGTHS` | Prompt length categories | `("short" "medium" "long")` |

---

## Submitting jobs

All commands must be run from the **login node** of Deucalion, inside the `benchmark/` directory:

```bash
cd benchmark/
```

### Make scripts executables

```bash
chmod +x submit.sh benchmark.sh
```


### Default run (Cartesian product of all parameters)

This is secure to execute in login node since it just send jobs to SLURM:

```bash
./submit.sh
```

Submits one SLURM job per combination of model × nodes × threads × trials × max_tokens × prompt length, with concurrency = 1.

### With TurboQuant enabled

Uses the `llama-cpp-turboquant-openblas-native.sif` container and enables optimised cache flags:

```bash
./submit.sh --turbo
```

### Concurrency sweep

Tests multiple levels of simultaneous requests to the llama.cpp server. Result directories get a `N` suffix:

```bash
./submit.sh --concurrency 1,2,4,8
```

### Combining options

```bash
./submit.sh --turbo --concurrency 1,2,4,8
```

---

## What happens after submission

Each job creates a results directory under `benchmark/results/` named:

```
<model>__n<nodes>__t<threads>__tr<trials>__mt<max_tokens>__tq<TRUE|FALSE>__<length>[__c<concurrency>]
```

Inside each directory:

| File | Contents |
|---|---|
| `bench.out` | SLURM log (job stdout/stderr) |
| `bench.py.log` | Detailed Python log |
| `results.csv` | Per-trial metrics (TTFT, tokens/s, etc.) |
| `summary.json` | Mean and standard deviation of all metrics |

Monitor active jobs:

```bash
watch squeue -u $USER
```


---

## Aggregating results

Once jobs have finished, merge all results into a single CSV:

### Standard mode (no concurrency sweep)

```bash
cd benchmark/
python join_results.py --results-dir results/ --output ../plot/full.csv
```

### Concurrency mode

```bash
cd benchmark/
python join_results_concurrency.py --results-dir results/ --output ../plot/concurrency.csv
```

---

## Generating plots

From the project root (with the virtual environment active):

```bash
cd plot/

# Latency and throughput (TTFT and TPOT)
python ttft_tpot.py

# TTFT as a function of prompt length
python ttft_scaling.py

# Concurrency heatmap
python concurrency.py

# Memory comparison
python memory.py
```

SVGs are saved to `plot/out/`.

---

## Important notes

- `submit.sh` **refuses to run inside a job allocation** — always run it from the login node.
- The script checks that model and container files exist before submitting each job.
- For **multi-node** runs (`NODES > 1`), inter-node communication uses InfiniBand (`ib0`) on port `50052`. The head node acts as the llama.cpp server; secondary nodes act as RPC workers.
- The benchmark uses **Singularity** to run llama.cpp inside the container — no manual llama.cpp installation is required.

## Work Assignemt 2 ADED done by:
- João Alves
- Luís Carmo
- Miguel Cruz

