#!/bin/sh
#SBATCH --time=00:05:00
#SBATCH --partition=normal-arm
#SBATCH -A f202500010hpcvlabuminhoa
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=48
#SBATCH --exclusive

ml Python/3.13.5-GCCcore-14.3.0
ml Java/17.0.6

echo "Running on nodes:"
scontrol show hostname $SLURM_NODELIST

source .venv/bin/activate

python statsEHPC_v2_init.py -m Jan
