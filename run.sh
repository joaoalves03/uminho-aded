#!/bin/sh
#SBATCH --time=00:05:00
#SBATCH --partition=dev-arm
#SBATCH -A f202500010hpcvlabuminhoa
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=48
#SBATCH --exclusive

ml Python/3.13.5-GCCcore-14.3.0
ml Java/17.0.6

PROJECTDIR=$(pwd)
LOCALDIR=/tmp/$SLURM_JOB_ID

# ── Mode selection ─────────────────────────────────────────────────────────────
# MOUNT_MODE=0 — mount sqfs directly from pwd (PFS), no copy
# MOUNT_MODE=1 — copy sqfs from pwd to /tmp then mount
MOUNT_MODE=0
# ──────────────────────────────────────────────────────────────────────────────

if [ "$MOUNT_MODE" = "1" ]; then
    mkdir -p $LOCALDIR/venv

    echo "$(date) — copying squashfs to /tmp..."
    cp $PROJECTDIR/venv.sqsh $LOCALDIR/
    echo "$(date) — done"

    echo "$(date) — mounting from /tmp..."
    squashfuse $LOCALDIR/venv.sqsh $LOCALDIR/venv
    echo "$(date) — mounted"

    VENV_DIR=$LOCALDIR/venv
else
    mkdir -p $LOCALDIR/venv

    echo "$(date) — mounting squashfs directly from pwd..."
    squashfuse $PROJECTDIR/venv.sqsh $LOCALDIR/venv
    echo "$(date) — mounted"

    VENV_DIR=$LOCALDIR/venv
fi

export VIRTUAL_ENV=$VENV_DIR
export PATH=$VENV_DIR/bin:$PATH
export PYSPARK_PYTHON=$VENV_DIR/bin/python3
export PYSPARK_DRIVER_PYTHON=$VENV_DIR/bin/python3

time numactl --physcpubind=0-$(($SLURM_CPUS_PER_TASK - 1)) --membind=0 \
    $VENV_DIR/bin/python \
    $PROJECTDIR/statsEHPC_v2_init.py -m Jan -y 2025

# results back
rsync -a --no-compress \
    $LOCALDIR/spark/outdir/ \
    $PROJECTDIR/spark/outdir/ 2>/dev/null || true

# cleanup
#fusermount -u $VENV_DIR
#rm -rf $LOCALDIR
