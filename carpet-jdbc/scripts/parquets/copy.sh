#!/bin/sh

# Usage: ./copy.sh <YYYYMMDD>
# Example: ./copy.sh 20251015

if [ $# -eq 0 ]; then
  echo "Error: Date parameter required"
  echo "Usage: $0 <date>"
  echo "Example: $0 20251015"
  exit 1
fi

DATE="$1"

# Use environment variables if set, otherwise defaults
SRC_BASE="${SRC_BASE:-huawei_obs_uat:/uat-sunline-obs/6666/report/}"
DST_BASE="${DST_BASE:-cbs_s3:/etl-sunline-mastertables/raw/corebanking/}"

SRC="${SRC_BASE}${DATE}/"
DST="${DST_BASE}${DATE}/"

echo "Source: $SRC"
echo "Destination (flat): $DST"
echo ""

# Ensure the destination dir exists when DST is local path
# (No-op for cloud remotes; safe to run either way)
./rclone mkdir "$DST" >/dev/null 2>&1 || true

# List all parquet files recursively, then flatten-copy each one into $DST.
# - lsf: list files
# - -R: recursive
# - --files-only: files only
# - --include "*.parquet": filter parquet files
# - copyto: allows specifying an exact destination object name (i.e., no subdirs)
./rclone lsf -R --files-only --include "*.parquet" "$SRC" | while IFS= read -r f; do
  base="$(basename "$f")"
  echo "Copying: $f -> ${DST}${base}"
  ./rclone copyto "$SRC$f" "${DST}${base}" \
    -v \
    --retries=5 \
    --progress \
    --s3-disable-checksum \
    --s3-no-check-bucket \
    --s3-upload-cutoff 0 \
    --ignore-checksum \
    --size-only
done