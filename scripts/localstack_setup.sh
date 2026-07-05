#!/usr/bin/env bash
# Provision the streaming path inside LocalStack. Runs automatically on boot
# (mounted into /etc/localstack/init/ready.d) and is safe to run again by hand:
#   docker compose exec localstack bash /etc/localstack/init/ready.d/setup.sh
set -euo pipefail

STREAM="perishables-events"
BUCKET="perishables-lake"
FIREHOSE="perishables-firehose"
REGION="us-east-1"
ROLE_ARN="arn:aws:iam::000000000000:role/firehose-role"

echo "→ creating Kinesis stream: ${STREAM}"
awslocal kinesis create-stream --stream-name "${STREAM}" --shard-count 1 || true
awslocal kinesis wait stream-exists --stream-name "${STREAM}"

echo "→ creating S3 bucket: ${BUCKET}"
awslocal s3 mb "s3://${BUCKET}" || true

echo "→ creating Firehose delivery stream: ${FIREHOSE} → s3://${BUCKET}/bronze/stream/"
awslocal firehose create-delivery-stream \
  --delivery-stream-name "${FIREHOSE}" \
  --delivery-stream-type DirectPut \
  --extended-s3-destination-configuration "{
    \"RoleARN\": \"${ROLE_ARN}\",
    \"BucketARN\": \"arn:aws:s3:::${BUCKET}\",
    \"Prefix\": \"bronze/stream/!{partitionKeyFromQuery:event_type}/dt=!{timestamp:yyyy-MM-dd}/\",
    \"ErrorOutputPrefix\": \"bronze/stream/_errors/\",
    \"BufferingHints\": {\"IntervalInSeconds\": 60, \"SizeInMBs\": 5},
    \"CompressionFormat\": \"GZIP\"
  }" || true

# --- Lambda: validate/enrich, triggered by the Kinesis stream ----------------
echo "→ packaging and deploying the enrich Lambda"
TMPDIR="$(mktemp -d)"
cp ingestion/stream/enrich_lambda.py ingestion/stream/schema.py ingestion/stream/aws.py "${TMPDIR}/" 2>/dev/null || \
  cp /work/ingestion/stream/*.py "${TMPDIR}/"
( cd "${TMPDIR}" && zip -q function.zip ./*.py )

awslocal lambda create-function \
  --function-name perishables-enrich \
  --runtime python3.12 \
  --handler enrich_lambda.handler \
  --zip-file "fileb://${TMPDIR}/function.zip" \
  --role "arn:aws:iam::000000000000:role/lambda-role" \
  --environment "Variables={FIREHOSE_STREAM=${FIREHOSE}}" \
  --timeout 60 || true

STREAM_ARN="$(awslocal kinesis describe-stream --stream-name "${STREAM}" \
  --query 'StreamDescription.StreamARN' --output text)"

echo "→ wiring Kinesis → Lambda event source mapping"
awslocal lambda create-event-source-mapping \
  --function-name perishables-enrich \
  --event-source-arn "${STREAM_ARN}" \
  --starting-position TRIM_HORIZON \
  --batch-size 100 || true

echo "✓ streaming path provisioned in LocalStack"