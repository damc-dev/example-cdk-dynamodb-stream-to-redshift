#!/usr/bin/env bash

set -e

export AWS_PAGER=""

log() {
    echo "$(date -u +"%Y-%m-%dT%H:%M:%S.%N") $*"
}

wait_for_dynamodb_export() {
    local export_arn="${1}"
    local status="NONE"
    local result=""
    
    while true; do
        result="$(aws dynamodb describe-export --export-arn "${export_arn}")"
        status="$(echo "${result}" | jq -r '.ExportDescription.ExportStatus')"
        log "export-arn: ${export_arn}, status: ${status}"

        if [[ "${status}" == "FAILED" ]]; then
            log "DynamoDB export failed, exiting..."
            log "${result}"
            exit 9
        fi

        if [[ "${status}" != "IN_PROGRESS" ]]; then
            break
        fi
        sleep 10
    done
}


script_dir="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
output_file="${OUTPUT_FILE:-${script_dir}/../outputs.json}"

stack_name="ExampleCdkDynamodbStreamToRedshiftStack"

if [ -f "${output_file}" ]; then
    dynamodb_table="$(jq -r '.'${stack_name}'.DynamoTableName' "${output_file}")"
    dynamodb_table_arn="$(jq -r '.'${stack_name}'.DynamoTableArn' "${output_file}")"
    dyanmodb_backup_bucket="$(jq -r '.'${stack_name}'.BackupBucketName' "${output_file}")"
    cluster_id="$(jq -r '.'${stack_name}'.RedshiftClusterId' "${output_file}")"
    assume_role="$(jq -r '.'${stack_name}'.RedshiftAssumeRoleArn' "${output_file}")"
    database_name="$(jq -r '.'${stack_name}'.RedshiftDefaultDatabaseName' "${output_file}")"
    database_username="$(jq -r '.'${stack_name}'.RedshiftMasterUsername' "${output_file}")"
    kinesis_stream_name="$(jq -r '.'${stack_name}'.ChangeStreamName' "${output_file}")"
fi
dynamodb_table="${dynamodb_table:-${DYNAMODB_TABLE}}"
dynamodb_table_arn="${dynamodb_table_arn:-${DYNAMODB_TABLE_ARN}}"
dyanmodb_backup_bucket="${dyanmodb_backup_bucket:-${DYNAMO_BACKUP_BUCKET}}"
cluster_id="${cluster_id:-${REDSHIFT_CLUSTER_ID}}"
database_name="${database_name:-${REDSHIFT_DB_NAME}}"
database_username="${database_username:-${REDSHIFT_USERNAME}}"
assume_role="${assume_role:-${REDSHIFT_ASSUME_ROLE_ARN}}"
kinesis_stream_name="${kinesis_stream_name:-${KINESIS_STREAM_NAME}}"

s3_prefix="$(date +%Y-%b)"

log "Export table to s3"
log "# This may take awhile, hang in there..."

export_arn="$(aws dynamodb export-table-to-point-in-time \
    --table-arn "${dynamodb_table_arn}" \
    --s3-bucket "${dyanmodb_backup_bucket}" \
    --s3-prefix "${s3_prefix}" \
    --export-format DYNAMODB_JSON \
    --s3-sse-algorithm AES256 \
        | jq -r '.ExportDescription.ExportArn')"

wait_for_dynamodb_export "${export_arn}"