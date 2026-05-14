#!/bin/bash
set -euo pipefail

cd "$PROJECT_ROOT"

ml Python-bundle-PyPI/2025.04-GCCcore-14.2.0
ml Python/3.13.5-GCCcore-14.3.0

export OMP_NUM_THREADS="$THREADS"
export OPENBLAS_NUM_THREADS="$THREADS"

exec python "$PROJECT_ROOT/benchmark.py"
