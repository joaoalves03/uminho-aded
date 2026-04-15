#!/bin/bash

extract_timings_csv() {
  jq -r '[.timings.prompt_ms,
            .timings.prompt_per_token_ms,
            .timings.prompt_per_second,
            .timings.predicted_ms,
            .timings.predicted_per_token_ms,
            .timings.predicted_per_second] | @csv'
}

mkdir -p results

PORT="8080"

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
