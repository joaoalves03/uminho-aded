#!/bin/bash
#SBATCH --job-name=llama_cpp
#SBATCH --account=F202500010HPCVLABUMINHOa
#SBATCH --partition=normal-arm
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=48
#SBATCH --time=00:30:00

mkdir -p results

module use /projects/F202500010HPCVLABUMINHO/uminhocp010-public/lmod/modules

#ml llama.cpp/git-3bd9aa1
ml llama.cpp/turboquant

MODEL="/projects/F202500010HPCVLABUMINHO/uminhocp010-public/models/Meta-Llama-3.1-8B-Instruct-Q4_K_M.gguf"

HOST="0.0.0.0"
PORT="8080"

THREADS="${SLURM_CPUS_PER_TASK:-$(nproc)}"
export OMP_NUM_THREADS="$THREADS"
export OPENBLAS_NUM_THREADS="$THREADS"

echo "Running llama.cpp"
echo "Node: $(hostname)"
echo "Threads: $THREADS"

llama-server \
  -m "$MODEL" \
  --host "$HOST" \
  --port "$PORT" \
  --threads "$THREADS" \
  --threads-batch "$THREADS" \
  --cache-type-k turbo3 \
  --cache-type-v turbo3

sleep infinity
