#!/bin/sh
#SBATCH --time=00:05:00
#SBATCH --partition=dev-arm
#SBATCH -A f202500010hpcvlabuminhoa
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=48
#SBATCH --exclusive

ml Python/3.13.5-GCCcore-14.3.0

echo "Running on nodes:"
scontrol show hostname $SLURM_NODELIST

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt


mkdir ./spark
mkdir ./spark/outdir
mkdir ./spark/sparkevents
mkdir ./spark/sparkevents/local

