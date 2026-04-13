#!/bin/bash
#SBATCH --time=00:10:00
#SBATCH --partition=dev-arm
#SBATCH -A f202500010hpcvlabuminhoa
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=48
#SBATCH --exclusive

ml use /projects/F202500010HPCVLABUMINHO/uminhocp010-public/lmod/modules
ml uv/0.10.10
ml Python/3.13.5-GCCcore-14.3.0

export UV_CACHE_DIR=/projects/F202500010HPCVLABUMINHO/uminhocp013/.cache/uv

if [ ! -d .venv ]; then
    echo "$(date) — creating venv..."
    python -m venv .venv
    source .venv/bin/activate
    uv pip install -r requirements.txt
    echo "$(date) — done"
else
    echo "$(date) — venv already exists, skipping"
fi

.venv/bin/jupyter nbconvert --to notebook --execute analysis.ipynb --output analysis_out.ipynb
echo "$(date) — notebook done"