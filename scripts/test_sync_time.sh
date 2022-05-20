set -e

log() {
    echo "$(date -u +"%Y-%m-%dT%H:%M:%S.%N") $*"
}

if [ -f "outputs.json" ]; then
    dynamodb_table="$(jq -r '.ExampleCdkDynamodbStreamToRedshiftStack.DynamoTableName' outputs.json)"
fi
dynamodb_table="${dynamodb_table:-${DYNAMODB_TABLE}}"

member_id="$(uuidgen)"
member_name="Bob"

put_item='{ "pk": { "S": "M_'${member_id}'" }, "sk": { "S": "'${member_name}'" }, "memberId": {"S": "'${member_id}'"}}'
aws dynamodb put-item \
    --table-name "${dynamodb_table}" \
    --item "${put_item}"

log "Created member memberId: ${member_id}"

refresh_mv_execution_id="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user admin \
    --cluster-identifier datawarehouseredshift9b5b98e7-ggz7n9waivpt \
    --database new_db \
    --sql "REFRESH MATERIALIZED VIEW member_quest_data_extract;" \
    | jq -r '.Id')"

while true; do
    status="$(aws redshift-data describe-statement --id "${refresh_mv_execution_id}" | jq -r '.Status')"
    log "Refresh MV: ${status}"
    if [[ "${status}" != "STARTED" ]]; then break; fi
done

query_member_from_mv_execution_id="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user admin \
    --cluster-identifier datawarehouseredshift9b5b98e7-ggz7n9waivpt \
    --database new_db \
    --parameters '[{"name": "pk", "value": "M_'${member_id}'"}]' \
    --sql "SELECT approximatearrivaltimestamp FROM member_quest_data_extract where pk = :pk" \
    | jq -r '.Id')"

while true; do
    status="$(aws redshift-data describe-statement --id "${query_member_from_mv_execution_id}" | jq -r '.Status')"
    log "Query Member: ${status}"
    if [[ "${status}" != "STARTED" ]]; then break; fi
done

approximate_arrival_timestamp="$(aws redshift-data get-statement-result --id "${query_member_from_mv_execution_id}" | jq '.Records[0][0] | .stringValue')"
log "Found member in materialized view with approximate arrival timestamp of ${approximate_arrival_timestamp}"

sync_member_execution_id="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user admin \
    --cluster-identifier datawarehouseredshift9b5b98e7-ggz7n9waivpt \
    --database new_db \
    --sql "call incremental_sync_members();" \
    | jq -r '.Id')"

log "sync_member_execution_id: ${sync_member_execution_id}"

while true; do
    status="$(aws redshift-data describe-statement --id "${sync_member_execution_id}" | jq -r '.Status')"
    log "Execution of stored procedure incremental_sync_member: ${status}"
    if [[ "${status}" != "STARTED" ]]; then break; fi
done

get_member_execution_id="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user admin \
    --cluster-identifier datawarehouseredshift9b5b98e7-ggz7n9waivpt \
    --database new_db \
    --parameters '[{"name": "id", "value": "'${member_id}'"}]' \
    --sql "SELECT memberId, approximateUpdateTimestamp, syncTimestamp, (syncTimestamp - approximateUpdateTimestamp) as syncLag FROM member WHERE memberId = :id" \
    | jq -r '.Id')"

log "get_member_execution_id: ${get_member_execution_id}"

while true; do
    status="$(aws redshift-data describe-statement --id "${get_member_execution_id}" | jq -r '.Status')"
    log "Execution of stored procedure incremental_sync_member: ${status}"
    if [[ "${status}" != "STARTED" ]]; then break; fi
done

aws redshift-data get-statement-result --id "${get_member_execution_id}" | jq '.Records' 

exit 0;