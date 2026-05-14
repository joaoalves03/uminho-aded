#!/usr/bin/env bash
set -euo pipefail

# ================================================================
# SLURM job launcher for LLM inference benchmarks (llama.cpp)
# ================================================================
# Run from login node only.
# Edit the arrays below to control the parameter sweep.

# -- SLURM / cluster settings ---------------------------------
ACCOUNT="f202500001hpcvlabepicurea"
PARTITION="normal-arm"
TIME="01:30:00"
EXCLUDE=""                       # e.g. "cna[0001-0016]"

# -- Models (GGUF files in MODELS_DIR) ------------------------
MODELS=(
    "Meta-Llama-3.1-8B-Instruct-Q4_K_M.gguf"
    "Qwen3-8B-DeepSeek-v3.2-Speciale-Distill.q4_k_m.gguf"
    "granite-4.1-8b-q4_k_m.gguf"
)
MODELS_DIR="/projects/F202500010HPCVLABUMINHO/uminhocp010-public/models"

# -- Experiment parameters (Cartesian product) ----------------
NODES=(1)                 # total nodes; 1 = single‑node, >1 = head + workers
THREADS=(46)                    # CPUs per task
TRIALS=(1)                      # repetitions per configuration
MAX_TOKENS=(1024)               # max generation tokens
PROMPT_LENGTHS=("short" "medium" "long")

# -- Container -------------------------------------------------
SIF_IMAGE_STANDARD="/projects/F202500010HPCVLABUMINHO/uminhocp010-public/containers/llama-cpp-openblas-native.sif"
SIF_IMAGE_TURBO="/projects/F202500010HPCVLABUMINHO/uminhocp010-public/containers/llama-cpp-turboquant-openblas-native.sif"
LLAMA_BIN_DIR="/opt/llama.cpp/build/bin"    # inside the container
SINGULARITY_BINDS="/projects:/projects"

# -- Worker communication (multi‑node) -------------------------
WORKER_PORT=50052
IB_IFACE="ib0"

# Defaults (overridden by command line)
CONCURRENCY_LEVELS=""           # empty means not set
ENABLE_TURBOQUANT="FALSE"

# ================================================================
# Parse command line arguments
# ================================================================
while [[ $# -gt 0 ]]; do
    case $1 in
        --concurrency)
            CONCURRENCY_LEVELS="$2"
            shift 2
            ;;
        --turbo)
            ENABLE_TURBOQUANT="TRUE"
            shift
            ;;
        --help)
            echo "Usage: $0 [--turbo] [--concurrency 1,4,8,16]"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Safety: refuse to run inside a job allocation
if [[ -n "${SLURM_JOB_ID:-}" ]]; then
    echo "ERROR: This script must be run from a login node, not inside a job." >&2
    exit 1
fi

ROOT="$(cd "$(dirname "$0")" && pwd)"
BENCH_SCRIPT="$ROOT/benchmark.sh"
[[ -x "$BENCH_SCRIPT" ]] || { echo "ERROR: benchmark.sh not found or not executable: $BENCH_SCRIPT" >&2; exit 1; }

EXCLUDE_FLAG=""
[[ -n "$EXCLUDE" ]] && EXCLUDE_FLAG="--exclude=$EXCLUDE"

# Build submission function
submit_run() {
    local model="$1" nodes="$2" threads="$3" trials="$4" max_tokens="$5" length="$6" concurrency="$7"

    local model_path="$MODELS_DIR/$model"
    [[ -f "$model_path" ]] || { echo "SKIP: model not found – $model_path" >&2; return 1; }

    local sif_image
    if [[ "$ENABLE_TURBOQUANT" == "TRUE" ]]; then
        sif_image="$SIF_IMAGE_TURBO"
    else
        sif_image="$SIF_IMAGE_STANDARD"
    fi
    [[ -f "$sif_image" ]] || { echo "ERROR: SIF image not found: $sif_image" >&2; return 1;}

    local model_key="${model%.gguf}"
    local concurrency_suffix=""
    local export_concurrency="1"

    if [[ -n "$concurrency" ]]; then
        # User provided --concurrency: add __cXX suffix and set exact concurrency value
        concurrency_suffix="__c${concurrency}"
        export_concurrency="$concurrency"
    else
        # No --concurrency flag: use default 1, but no suffix in directory name
        export_concurrency="1"
    fi

    local run_name="${model_key}__n${nodes}__t${threads}__tr${trials}__mt${max_tokens}__tq${ENABLE_TURBOQUANT}__${length}${concurrency_suffix}"
    local run_dir="$ROOT/results/$run_name"
    mkdir -p "$run_dir"

    # Pack everything into a comma-separated export string
    local export_vars
    export_vars="MODEL=$model_path,MODEL_KEY=$model_key"
    export_vars+=",NODES=$nodes,THREADS=$threads,N_TRIALS=$trials,MAX_TOKENS=$max_tokens"
    export_vars+=",WORKER_PORT=$WORKER_PORT,IB_IFACE=$IB_IFACE"
    export_vars+=",SIF_IMAGE=$sif_image,LLAMA_BIN_DIR=$LLAMA_BIN_DIR"
    export_vars+=",SINGULARITY_BINDS=$SINGULARITY_BINDS"
    export_vars+=",RUN_DIR=$run_dir,PROJECT_ROOT=$ROOT"
    export_vars+=",PROMPT_LENGTH=$length"
    export_vars+=",ENABLE_TURBOQUANT=${ENABLE_TURBOQUANT}"
    export_vars+=",CONCURRENCY_LEVELS=$export_concurrency"   # single integer

    # Submit job
    local job_id
    job_id=$(sbatch --parsable \
        --account="$ACCOUNT" --partition="$PARTITION" \
        --nodes="$nodes" --ntasks="$nodes" --cpus-per-task="$threads" \
        --time="$TIME" \
        -o "$run_dir/bench.out" \
        $EXCLUDE_FLAG \
        --job-name="bench_${model_key}_n${nodes}_t${threads}_tr${trials}_mt${max_tokens}_${length}_tq${ENABLE_TURBOQUANT}${concurrency_suffix}" \
        --export="ALL,$export_vars" \
        "$BENCH_SCRIPT")

    echo "[OK] $run_name  job=$job_id"
}

# Cartesian product over all parameter arrays
for MODEL in "${MODELS[@]}"; do
    for N in "${NODES[@]}"; do
        for T in "${THREADS[@]}"; do
            for TR in "${TRIALS[@]}"; do
                for MT in "${MAX_TOKENS[@]}"; do
                    for PL in "${PROMPT_LENGTHS[@]}"; do
                        if [[ -n "$CONCURRENCY_LEVELS" ]]; then
                            # User gave --concurrency: loop over values
                            IFS=',' read -ra CONC_ARRAY <<< "$CONCURRENCY_LEVELS"
                            for CONC in "${CONC_ARRAY[@]}"; do
                                submit_run "$MODEL" "$N" "$T" "$TR" "$MT" "$PL" "$CONC"
                                sleep 0.5
                            done
                        else
                            # No --concurrency flag: submit one job without suffix
                            submit_run "$MODEL" "$N" "$T" "$TR" "$MT" "$PL" ""
                            sleep 0.5
                        fi
                    done
                done
            done
        done
    done
done