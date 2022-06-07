set -e

export AWS_PAGER=""

log() {
    echo "$(date -u +"%Y-%m-%dT%H:%M:%S.%N") $*"
}

wait_for_execution_status_change() {
    local statement_execution_id="${1}"
    local status="NONE"
    local result=""
    while true; do
        result="$(aws redshift-data describe-statement --id "${statement_execution_id}")"
        status="$(echo "${result}" | jq -r '.Status')"
        log "id: ${statement_execution_id}, status: ${status}"
        if [[ "${status}" == "FAILED" ]]; then log "${result}"; fi
        if [[ "${status}" != "STARTED" ]]; then break; fi
    done
}

stack_name="ExampleCdkDynamodbStreamToRedshiftStack"

if [ -f "outputs.json" ]; then
    dynamodb_table="$(jq -r '.'${stack_name}'.DynamoTableName' outputs.json)"
    cluster_id="$(jq -r '.'${stack_name}'.RedshiftClusterId' outputs.json)"
    assume_role="$(jq -r '.'${stack_name}'.RedshiftAssumeRoleArn' outputs.json)"
    database_name="$(jq -r '.'${stack_name}'.RedshiftDefaultDatabaseName' outputs.json)"
    database_username="$(jq -r '.'${stack_name}'.RedshiftMasterUsername' outputs.json)"
    kinesis_stream_name="$(jq -r '.'${stack_name}'.ChangeStreamName' outputs.json)"
fi
dynamodb_table="${dynamodb_table:-${DYNAMODB_TABLE}}"
cluster_id="${cluster_id:-${REDSHIFT_CLUSTER_ID}}"
database_name="${database_name:-${REDSHIFT_DB_NAME}}"
database_username="${database_username:-${REDSHIFT_USERNAME}}"
assume_role="${assume_role:-${REDSHIFT_ASSUME_ROLE_ARN}}"
kinesis_stream_name="${kinesis_stream_name:-${KINESIS_STREAM_NAME}}"


member_id="$(uuidgen)"
member_name="Bob"

put_item='{ "pk": { "S": "M_'${member_id}'" }, "sk": { "S": "'${member_name}'" }, "memberId": {"S": "'${member_id}'"}}'
aws dynamodb put-item \
    --table-name "${dynamodb_table}" \
    --item "${put_item}"

log "Created member memberId: ${member_id}"

log "Refresh materialized view to populate it with new records from Kinesis data stream"
refresh_mv_execution_id="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user "${database_username}" \
    --cluster-identifier "${cluster_id}" \
    --database "${database_name}" \
    --sql "REFRESH MATERIALIZED VIEW member_quest_data_extract;" \
    | jq -r '.Id')"

wait_for_execution_status_change "${refresh_mv_execution_id}"

log "Query member from materialized view to get the approximate arrival timestamp the record was loaded into the materialized view"

query_member_from_mv_execution_id="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user "${database_username}" \
    --cluster-identifier "${cluster_id}" \
    --database "${database_name}" \
    --parameters '[{"name": "pk", "value": "M_'${member_id}'"}]' \
    --sql "SELECT approximatearrivaltimestamp FROM member_quest_data_extract where pk = :pk" \
    | jq -r '.Id')"

wait_for_execution_status_change "${query_member_from_mv_execution_id}"

approximate_arrival_timestamp="$(aws redshift-data get-statement-result --id "${query_member_from_mv_execution_id}" | jq '.Records[0][0] | .stringValue')"
log "Found member in materialized view with approximate arrival timestamp of ${approximate_arrival_timestamp}"

log "Execute stored procedure to sync member table with new records in materialized view"

sync_member_execution_id="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user "${database_username}" \
    --cluster-identifier "${cluster_id}" \
    --database "${database_name}" \
    --sql "call incremental_sync_members();" \
    | jq -r '.Id')"


wait_for_execution_status_change "${sync_member_execution_id}"

log "Query member from target table"

get_member_execution_id="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user "${database_username}" \
    --cluster-identifier "${cluster_id}" \
    --database "${database_name}" \
    --parameters '[{"name": "id", "value": "'${member_id}'"}]' \
    --sql "SELECT memberId, approximateUpdateTimestamp, syncTimestamp, (syncTimestamp - approximateUpdateTimestamp) as syncLag FROM member WHERE memberId = :id" \
    | jq -r '.Id')"

wait_for_execution_status_change "${get_member_execution_id}"

log "Query results:"
aws redshift-data get-statement-result --id "${get_member_execution_id}" | jq -r '.Records[0] | "\t memberId: \(.[0].stringValue) \n\t approximateUpdateTimestamp: \(.[1].stringValue) \n\t syncTimestamp: \(.[2].stringValue) \n\t syncLag: \(.[3].stringValue)"' 

exit 0;