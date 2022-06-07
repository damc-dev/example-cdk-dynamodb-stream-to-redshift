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
sql_dir="${script_dir}/../sql"
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

log "Enable case sensitive identifier"
log "# This is required because Kinesis stream names are case sensitive"

enable_case_sensitive_identifier="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user "${database_username}" \
    --cluster-identifier "${cluster_id}" \
    --database "${database_name}" \
    --sql "ALTER USER ${database_username} SET enable_case_sensitive_identifier TO true;" \
    | jq -r '.Id')"
wait_for_execution_status_change "${enable_case_sensitive_identifier}"

log "Drop schemas and materialized view if they already exist"

drop_schemas_and_materialized_view="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user "${database_username}" \
    --cluster-identifier "${cluster_id}" \
    --database "${database_name}" \
    --sql "DROP MATERIALIZED VIEW IF EXISTS member_quest_data_extract; DROP SCHEMA IF EXISTS activity_tracking;" \
    | jq -r '.Id')"

wait_for_execution_status_change "${drop_schemas_and_materialized_view}"

log "Create external kinesis schema"
create_external_kinesis_schema="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user "${database_username}" \
    --cluster-identifier "${cluster_id}" \
    --database "${database_name}" \
    --sql "CREATE EXTERNAL SCHEMA activity_tracking FROM KINESIS IAM_ROLE '${assume_role}';" \
    | jq -r '.Id')"

wait_for_execution_status_change "${create_external_kinesis_schema}"

create_materialized_view_sql=$(cat <<-END
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

log "Create materialized view"
create_materialized_view="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user "${database_username}" \
    --cluster-identifier "${cluster_id}" \
    --database "${database_name}" \
    --sql "${create_materialized_view_sql}" \
    | jq -r '.Id')"

wait_for_execution_status_change "${create_materialized_view}"


log "Refresh materialized view to populate it with records from Kinesis data stream"
log "# This is required so there is data for initial load of target tables"

refresh_mv_execution_id="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user "${database_username}" \
    --cluster-identifier "${cluster_id}" \
    --database "${database_name}" \
    --sql "REFRESH MATERIALIZED VIEW member_quest_data_extract;" \
    | jq -r '.Id')"

wait_for_execution_status_change "${refresh_mv_execution_id}"

log "Create target tables and initial data load"
log "# Initial data load is only done for test purposes, in production you would dump your dynamoDB table and do an intial data load from that"
create_target_tables_and_initial_load="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user "${database_username}" \
    --cluster-identifier "${cluster_id}" \
    --database "${database_name}" \
    --sql file://${sql_dir}/create_target_tables_and_initial_load.sql \
    | jq -r '.Id')"

wait_for_execution_status_change "${create_target_tables_and_initial_load}"

log "Create stored procedure: incremental_sync_member_quests"
incremental_sync_member_quests="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user "${database_username}" \
    --cluster-identifier "${cluster_id}" \
    --database "${database_name}" \
    --sql file://${sql_dir}/incremental_sync_member_quests.sql \
    | jq -r '.Id')"

wait_for_execution_status_change "${incremental_sync_member_quests}"

log "create stored procedure: incremental_sync_members"
incremental_sync_members="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user "${database_username}" \
    --cluster-identifier "${cluster_id}" \
    --database "${database_name}" \
    --sql file://${sql_dir}/incremental_sync_members.sql \
    | jq -r '.Id')"

wait_for_execution_status_change "${incremental_sync_members}"

log "create stored procedure: incremental_sync_quests"
incremental_sync_quests="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user "${database_username}" \
    --cluster-identifier "${cluster_id}" \
    --database "${database_name}" \
    --sql file://${sql_dir}/incremental_sync_quests.sql \
    | jq -r '.Id')"

wait_for_execution_status_change "${incremental_sync_quests}"