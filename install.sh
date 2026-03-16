#!/bin/sh
#SBATCH --time=00:05:00
#SBATCH --partition=dev-arm
#SBATCH -A f202500010hpcvlabuminhoa
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=48
#SBATCH --exclusive


ml Python/3.13.5-GCCcore-14.3.0

LOCALDIR=/tmp/$SLURM_JOB_ID

mkdir -p $LOCALDIR/venv

# create venv on /tmp
echo "$(date) — creating venv..."
python -m venv $LOCALDIR/venv
source $LOCALDIR/venv/bin/activate

# faster install with uv
pip install uv
uv pip install -r requirements.txt

# build squashfs, write directly to pwd (PFS)
echo "$(date) — building squashfs..."
mksquashfs $LOCALDIR/venv venv.sqsh \
    -comp lz4 \
    -processors $SLURM_CPUS_PER_TASK \
    -noappend
echo "$(date) — done: $(ls -lh venv.sqsh)"
