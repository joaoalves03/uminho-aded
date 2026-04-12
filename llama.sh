#!/bin/bash
#SBATCH --job-name=llama_cpp
#SBATCH --account=F202500010HPCVLABUMINHOa
#SBATCH --partition=normal-arm
#SBATCH --nodes=1
#SBATCH --exclude=cna[0001-0016]
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=48
#SBATCH --time=00:30:00

mkdir -p results

ml llama.cpp/20251110-foss-2023a

MODEL="/projects/F202500010HPCVLABUMINHO/uminhocp010-public/models/qwen2.5-1.5b-instruct-q4_k_m.gguf"
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
  --cache-ram 0 \
  >server.log 2>&1 &
SERVER_PID=$!
echo "Server PID: $SERVER_PID"

until [ "$(curl -s -o /dev/null -w "%{http_code}" http://localhost:$PORT/health)" = "200" ]; do
  sleep 1
done
echo "Server ready"

echo "[INFO] Warm-up..."
curl -s http://localhost:$PORT/v1/chat/completions \
    -H "Content-Type: application/json" \
    -d '{"messages":[{"role":"user","content":"Hello"}],"max_tokens":16}' > /dev/null

while true; do
    echo "$(date +%s%3N),$(grep VmRSS /proc/$SERVER_PID/status | awk '{print $2}')" \
        >> results/resources_$SLURM_JOB_ID.csv
    sleep 1
done &
MONITOR_PID=$!

echo "trial,prompt_id,ttft_ms,decode_ms,tpot_ms,tokens_generated" > results/results_$SLURM_JOB_ID.csv

PROMPTS=(
    "What is the capital of France?"
    "Explain the key differences between machine learning and deep learning, and provide an example of each."
    "The transformer architecture, introduced in the paper Attention is All You Need by Vaswani et al. in 2017, has become the foundation for most modern large language models. Unlike recurrent neural networks, which process sequences sequentially, transformers process all tokens in parallel using self-attention mechanisms. This allows better parallelisation during training and has proven more effective at capturing long-range dependencies in text. When deploying transformers for inference on CPU, the main bottlenecks are the model large memory footprint and the repeated loading of weights during the decoding phase. Based on this context, what are the main challenges when running large transformer models on CPU-only systems, and how might quantisation help address these challenges?"
)

for trial in {1..3}; do
    echo "[INFO] Trial $trial"
    for i in "${!PROMPTS[@]}"; do
        prompt="${PROMPTS[$i]}"
        prompt_id=$((i+1))

        t_start=$(date +%s%3N)
        token_count=0
        t_first=""

        while IFS= read -r line; do
            [[ "$line" != data:* ]] && continue
            data="${line#data: }"
            [[ "$data" == "[DONE]" ]] && break
            if [[ -z "$t_first" ]]; then
                t_first=$(date +%s%3N)
            fi
            token_count=$((token_count + 1))
        done < <(curl -s -N http://localhost:$PORT/v1/chat/completions \
            -H "Content-Type: application/json" \
            -d "{\"messages\":[{\"role\":\"user\",\"content\":\"$(echo "$prompt" | sed 's/"/\\"/g')\"}],\"max_tokens\":128,\"stream\":true}")

        t_end=$(date +%s%3N)
        ttft=$(( t_first - t_start ))
        decode=$(( t_end - t_first ))
        tpot=0
        if [[ $token_count -gt 1 ]]; then
            tpot=$(( decode / (token_count - 1) ))
        fi

        echo "$trial,$prompt_id,$ttft,$decode,$tpot,$token_count" | tee -a results/results_$SLURM_JOB_ID.csv
    done
done

echo "Done"

echo "[INFO] Computing summary statistics..."
awk -F',' '
NR==1 { next }
{
    trial=$1; pid=$2; ttft=$3; decode=$4; tpot=$5; tokens=$6
    throughput = (decode > 0) ? (tokens / (decode / 1000.0)) : 0
    goodput = (ttft < 2000 && tpot < 200) ? 1 : 0
    total++
    good += goodput
    sum_ttft += ttft
    sum_tpot += tpot
    sum_throughput += throughput
}
END {
    print "---"
    print "Requests:", total
    printf "Avg TTFT:       %.1f ms\n", sum_ttft / total
    printf "Avg TPOT:       %.1f ms\n", sum_tpot / total
    printf "Avg Throughput: %.2f tokens/s\n", sum_throughput / total
    printf "Goodput:        %.1f%%\n", (good / total) * 100
}
' results/results_$SLURM_JOB_ID.csv | tee results/summary_$SLURM_JOB_ID.txt

kill $MONITOR_PID
kill $SERVER_PID
wait $SERVER_PID 2>/dev/null