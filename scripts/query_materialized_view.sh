set -e

export AWS_PAGER=""

log() {
    echo "$(date -u +"%Y-%m-%dT%H:%M:%S.%N") $*"
}

pretty_print_statement_result() {
    local statement_execution_id="$1"
    aws redshift-data get-statement-result --id "${statement_execution_id}" | jq '(.ColumnMetadata | map (.name)) as $headers | .Records[] | to_entries | map({($headers[.key]): .value.stringValue}) | add'
}

wait_for_execution_status_change() {
    local statement_execution_id="${1}"
    local status="NONE"
    local result=""
    
    while true; do
        result="$(aws redshift-data describe-statement --id "${statement_execution_id}")"
        status="$(echo "${result}" | jq -r '.Status')"
        log "id: ${statement_execution_id}, status: ${status}"

        if [[ "${status}" == "FAILED" ]]; then
            log "Error occurred during sql execution, exiting..."
            log "${result}"
            exit 7
        fi

        if [[ "${status}" != "STARTED" ]]; then
            break
        fi
    done
}

script_dir="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
output_file="${OUTPUT_FILE:-${script_dir}/../outputs.json}"

stack_name="ExampleCdkDynamodbStreamToRedshiftStack"

if [ -f "${output_file}" ]; then
    dynamodb_table="$(jq -r '.'${stack_name}'.DynamoTableName' "${output_file}")"
    cluster_id="$(jq -r '.'${stack_name}'.RedshiftClusterId' "${output_file}")"
    assume_role="$(jq -r '.'${stack_name}'.RedshiftAssumeRoleArn' "${output_file}")"
    database_name="$(jq -r '.'${stack_name}'.RedshiftDefaultDatabaseName' "${output_file}")"
    database_username="$(jq -r '.'${stack_name}'.RedshiftMasterUsername' "${output_file}")"
    kinesis_stream_name="$(jq -r '.'${stack_name}'.ChangeStreamName' "${output_file}")"
fi
dynamodb_table="${dynamodb_table:-${DYNAMODB_TABLE}}"
cluster_id="${cluster_id:-${REDSHIFT_CLUSTER_ID}}"
database_name="${database_name:-${REDSHIFT_DB_NAME}}"
database_username="${database_username:-${REDSHIFT_USERNAME}}"
assume_role="${assume_role:-${REDSHIFT_ASSUME_ROLE_ARN}}"
kinesis_stream_name="${kinesis_stream_name:-${KINESIS_STREAM_NAME}}"


log "refresh_mv_execution_id"
refresh_mv_execution_id="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user "${database_username}" \
    --cluster-identifier "${cluster_id}" \
    --database "${database_name}" \
    --sql "REFRESH MATERIALIZED VIEW member_quest_data_extract;" \
    | jq -r '.Id')"

wait_for_execution_status_change "${refresh_mv_execution_id}"


log "query_materialized_view"
query_materialized_view="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user "${database_username}" \
    --cluster-identifier "${cluster_id}" \
    --database "${database_name}" \
    --sql "SELECT * FROM member_quest_data_extract LIMIT 5" \
    | jq -r '.Id')"

wait_for_execution_status_change "${query_materialized_view}"

pretty_print_statement_result "${query_materialized_view}"