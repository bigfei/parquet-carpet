#!/bin/sh

# Usage: ./rclone.sh <YYYYMMDD>
# Example: ./rclone.sh 20260203

if [ $# -eq 0 ]; then
  echo "Error: Date parameter required"
  echo "Usage: $0 <date>"
  echo "Example: $0 20260203"
  exit 1
fi

DATE="$1"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Local export directory (date-based)
SRC_DIR="${SCRIPT_DIR}/exports/${DATE}/"

# Destination (rclone remote)
DST_BASE="${DST_BASE:-cbs_s3:/etl-sunline-mastertables/raw/corebanking/}"
DST="${DST_BASE}${DATE}/"

AWS_ACCOUNT_ID="431312751623"
AWS_KMS_KEY_ID="e6df0452-a104-4bc2-ae99-385abd703578"

echo "Source (Local): $SRC_DIR"
echo "Destination: $DST"
echo ""

if [ ! -d "$SRC_DIR" ]; then
  echo "Error: Source directory not found: $SRC_DIR"
  exit 1
fi

# Ensure destination exists
rclone mkdir "$DST" >/dev/null 2>&1 || true

# Copy parquet + metadata files
find "$SRC_DIR" -type f \( -name "*.parquet" -o -name "*.parquet.metadata" \) | while IFS= read -r f; do
  base="$(basename "$f")"
  echo "Copying: $f -> ${DST}${base}"
  rclone copyto "$f" "${DST}${base}" \
    -v \
    --retries=5 \
    --progress \
    --s3-disable-checksum \
    --s3-no-check-bucket \
    --s3-upload-cutoff 0 \
    --ignore-checksum \
    --s3-server-side-encryption aws:kms \
    --s3-sse-kms-key-id arn:aws:kms:ap-southeast-1:${AWS_ACCOUNT_ID}:key/${AWS_KMS_KEY_ID} \
    --size-only
done

echo ""
echo "Creating SUCCESS.txt marker..."
echo -n "" | rclone rcat -vv --s3-no-check-bucket \
  --s3-server-side-encryption aws:kms \
  --s3-sse-kms-key-id arn:aws:kms:ap-southeast-1:${AWS_ACCOUNT_ID}:key/${AWS_KMS_KEY_ID} \
  "${DST}SUCCESS.txt"

echo "Transfer complete!"
