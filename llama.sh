#!/bin/bash
#SBATCH --job-name=llama_cpp
#SBATCH --account=F202500010HPCVLABUMINHOa
#SBATCH --partition=normal-arm
#SBATCH --nodes=1
#SBATCH --exclude=cna[0001-0016]
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=48
#SBATCH --time=00:30:00

extract_timings_csv() {
  jq -r '[.timings.prompt_ms,
            .timings.prompt_per_token_ms,
            .timings.prompt_per_second,
            .timings.predicted_ms,
            .timings.predicted_per_token_ms,
            .timings.predicted_per_second] | @csv'
}

mkdir -p results

ml llama.cpp/20251110-foss-2023a

MODEL="/projects/F202500010HPCVLABUMINHO/uminhocp010/ADED/models/qwen2.5-1.5b-instruct-q4_k_m.gguf"

HOST="0.0.0.0"
PORT="8080"

THREADS="${SLURM_CPUS_PER_TASK:-$(nproc)}"
export OMP_NUM_THREADS="$THREADS"
export OPENBLAS_NUM_THREADS="$THREADS"

echo "[INFO] Running llama.cpp"
echo "[INFO] Node: $(hostname)"
echo "[INFO] Threads: $THREADS"

llama-server \
  -m "$MODEL" \
  --host "$HOST" \
  --port "$PORT" \
  --threads "$THREADS" \
  --threads-batch "$THREADS" \
  >server.log 2>&1 &
SERVER_PID=$!

echo "Server PID: $SERVER_PID"

until [ "$(curl -s -o /dev/null -w "%{http_code}" http://localhost:$PORT/health)" = "200" ]; do
  sleep 1
done

echo "Server ready"

echo "prompt_ms,prompt_per_token_ms,prompt_per_second,predicted_ms,predicted_per_token_ms,predicted_per_second" | tee -a results/results_$SLURM_JOB_ID.csv

for i in {1..10}; do
  curl -s http://localhost:$PORT/v1/chat/completions \
    -H "Content-Type: application/json" \
    -d '{
    "messages":[{"role": "user","content": "Explain what distributed inference is." }],
    "max_tokens": 128
  }' | extract_timings_csv | tee -a results/results_$SLURM_JOB_ID.csv
  echo ""
done

echo "Done"

kill $SERVER_PID
wait $SERVER_PID 2>/dev/null
