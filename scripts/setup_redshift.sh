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

log "enable_case_sensitive_identifier"

enable_case_sensitive_identifier="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user "${database_username}" \
    --cluster-identifier "${cluster_id}" \
    --database "${database_name}" \
    --sql "ALTER USER ${database_username} SET enable_case_sensitive_identifier TO true;" \
    | jq -r '.Id')"
wait_for_execution_status_change "${enable_case_sensitive_identifier}"

log "associate_cluster_iam_role"
aws redshift modify-cluster-iam-roles \
    --cluster-identifier "${cluster_id}" \
    --add-iam-roles "${assume_role}"

log "create_external_kinesis_schema"
create_external_kinesis_schema="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user "${database_username}" \
    --cluster-identifier "${cluster_id}" \
    --database "${database_name}" \
    --sql "DROP SCHEMA IF EXISTS activity_tracking; CREATE EXTERNAL SCHEMA activity_tracking FROM KINESIS IAM_ROLE '${assume_role}';" \
    | jq -r '.Id')"

wait_for_execution_status_change "${create_external_kinesis_schema}"

create_materialized_view_sql=$(cat <<-END
DROP MATERIALIZED VIEW IF EXISTS member_quest_data_extract;

CREATE MATERIALIZED VIEW member_quest_data_extract DISTKEY(5) sortkey(1) AS
    SELECT approximatearrivaltimestamp,
    partitionkey,
    shardid,
    sequencenumber,
    json_extract_path_text(from_varbyte(data, 'utf-8'),'eventID')::varchar(30) as eventID,
    json_extract_path_text(from_varbyte(data, 'utf-8'),'awsRegion')::character(36) as awsRegion,
    json_extract_path_text(from_varbyte(data, 'utf-8'),'eventName')::varchar(20) as eventName,
    json_extract_path_text(from_varbyte(data, 'utf-8'),'userIdentity')::varchar(20) as userIdentity,
    json_extract_path_text(from_varbyte(data, 'utf-8'),'tableName')::varchar(20) as tableName,
    json_extract_path_text(from_varbyte(data, 'utf-8'),'eventSource')::varchar(100) as eventSource,
    json_extract_path_text(from_varbyte(data, 'utf-8'),'dynamodb', 'Keys', 'pk', 'S')::varchar(50) as pk,
    json_extract_path_text(from_varbyte(data, 'utf-8'),'dynamodb', 'Keys', 'sk', 'S')::varchar(50) as sk,
    json_parse(json_extract_path_text(from_varbyte(data, 'utf-8'),'dynamodb')) as eventData
    FROM activity_tracking."${kinesis_stream_name}";
END
)

log "create_materialized_view"
create_materialized_view="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user "${database_username}" \
    --cluster-identifier "${cluster_id}" \
    --database "${database_name}" \
    --sql "${create_materialized_view_sql}" \
    | jq -r '.Id')"

wait_for_execution_status_change "${create_materialized_view}"


log "refresh_mv #so there is data for an initial load of tables"
refresh_mv_execution_id="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user "${database_username}" \
    --cluster-identifier "${cluster_id}" \
    --database "${database_name}" \
    --sql "REFRESH MATERIALIZED VIEW member_quest_data_extract;" \
    | jq -r '.Id')"

wait_for_execution_status_change "${refresh_mv_execution_id}"

log "create_target_tables_and_initial_load"
create_target_tables_and_initial_load="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user "${database_username}" \
    --cluster-identifier "${cluster_id}" \
    --database "${database_name}" \
    --sql file://./sql/create_target_tables_and_initial_load.sql \
    | jq -r '.Id')"

wait_for_execution_status_change "${create_target_tables_and_initial_load}"

log "incremental_sync_member_quests"
incremental_sync_member_quests="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user "${database_username}" \
    --cluster-identifier "${cluster_id}" \
    --database "${database_name}" \
    --sql file://./sql/incremental_sync_member_quests.sql \
    | jq -r '.Id')"

wait_for_execution_status_change "${incremental_sync_member_quests}"

log "incremental_sync_members"
incremental_sync_members="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user "${database_username}" \
    --cluster-identifier "${cluster_id}" \
    --database "${database_name}" \
    --sql file://./sql/incremental_sync_members.sql \
    | jq -r '.Id')"

wait_for_execution_status_change "${incremental_sync_members}"

log "incremental_sync_quests"
incremental_sync_quests="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user "${database_username}" \
    --cluster-identifier "${cluster_id}" \
    --database "${database_name}" \
    --sql file://./sql/incremental_sync_quests.sql \
    | jq -r '.Id')"

wait_for_execution_status_change "${incremental_sync_quests}"