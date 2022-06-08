#!/usr/bin/env bash

set -e

export AWS_PAGER=""

set -e

usage() {
    echo "Usage: $(basename "$0") -a export_arn [-w]
Does initial load of database table from DynamoDB table dump 
        -a export_arn        
        -w              waits until export is complete, if not specified and export is not complete will exit with error 
Examples:
    $(basename "$0") -a arn:aws:dynamodb:us-east-1:011111111111:table/ExampleCdkDynamodbStreamToRedshiftStack-TableCD117FA1-1C9GCWSX0PYAL/export/01654623004421-ec33419c
"
}

_error() {
    local error_msg=$1
    echo "[ERROR] $error_msg"
    usage
    exit 1
}

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

check_if_dynamodb_export_finished() {
    local export_arn="${1}"    
    local result="$(aws dynamodb describe-export --export-arn "${export_arn}")"
    local status="$(echo "${result}" | jq -r '.ExportDescription.ExportStatus')"
    log "export-arn: ${export_arn}, status: ${status}"

    if [[ "${status}" == "FAILED" ]]; then
        log "[ERROR] DynamoDB export failed, exiting..."
        log "${result}"
        exit 9
    fi

    if [[ "${status}" != "COMPLETED" ]]; then
        log "[ERROR] DynamoDB export is not complete, either specific -w to wait or execute command again once it is complete"
        log "${result}"
        exit 3
    fi
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


export_arn="${export_arn:-${DYNAMODB_TABLE_EXPORT_ARN}}"
wait_flag="false"
while getopts :wa: OPT; do
  case "${OPT}" in
    a) export_arn="${OPTARG}";;
    w) wait_flag="true";;
    *) _error "Unrecognized option specified";;
  esac
done
shift $(( $OPTIND - 1 ))

if [ -z "${export_arn}" ]; then
    _error "no DynamoDB table export arn specified"
fi


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

export_id="$(basename "${export_arn}")"

if [ "${wait_flag}" = "true" ]; then
    log "Waiting for DynamoDB export to complete"
    log "# This may take awhile, hang in there..."    
    wait_for_dynamodb_export "${export_arn}"
else
    check_if_dynamodb_export_finished "${export_arn}"
fi

export_description="$(aws dynamodb describe-export --export-arn "${export_arn}")"
s3_prefix="$(echo "${export_description}" | jq -r '.ExportDescription.S3Prefix')"


data_location="s3://${dyanmodb_backup_bucket}/${s3_prefix}/AWSDynamoDB/${export_id}/data"

log "Drop schemas and tables if they already exist"

drop_schemas_and_tables="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user "${database_username}" \
    --cluster-identifier "${cluster_id}" \
    --database "${database_name}" \
    --sql "DROP TABLE IF EXISTS dump_table" \
    | jq -r '.Id')"

wait_for_execution_status_change "${drop_schemas_and_tables}"


create_dump_table_sql=$(cat <<-END
CREATE TABLE dump_table (
    Item SUPER
);
END
)

log "Create dump table"
create_dump_table="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user "${database_username}" \
    --cluster-identifier "${cluster_id}" \
    --database "${database_name}" \
    --sql "${create_dump_table_sql}" \
    | jq -r '.Id')"

wait_for_execution_status_change "${create_dump_table}"

log "Copy backup into Redshift table"

create_redshift_table_from_backup="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user "${database_username}" \
    --cluster-identifier "${cluster_id}" \
    --database "${database_name}" \
    --sql "COPY dump_table FROM 's3://${dyanmodb_backup_bucket}/${s3_prefix}/AWSDynamoDB/${export_id}/data' REGION 'us-east-1' IAM_ROLE '${assume_role}' FORMAT JSON 'auto ignorecase' gzip ACCEPTINVCHARS ' ' TRUNCATECOLUMNS TRIMBLANKS;" \
    | jq -r '.Id')"

wait_for_execution_status_change "${create_redshift_table_from_backup}"


transform_and_load_member_data_sql=$(cat <<-END
INSERT INTO member (
SELECT
    LTRIM(item.pk."S"::varchar, 'M_' )::varchar as memberId, 
  	item.sk."S"::varchar as memberName,
   	GETDATE()::timestamp as approximateUpdateTimestamp,
  	'INITIAL_LOAD'::varchar as eventName,
    GETDATE()::timestamp as syncTimestamp
FROM
    dump_table
WHERE item.pk."S"::varchar LIKE 'M^_%' escape '^'
);
END
)

log "Transform and load member data"
transform_and_load_member_data="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user "${database_username}" \
    --cluster-identifier "${cluster_id}" \
    --database "${database_name}" \
    --sql "${transform_and_load_member_data_sql}" \
    | jq -r '.Id')"

wait_for_execution_status_change "${transform_and_load_member_data}"

transform_and_load_quest_data_sql=$(cat <<-END
INSERT INTO quest (
SELECT
    LTRIM(item.pk."S"::varchar, 'Q_' )::varchar as questId, 
  	item.sk."S"::varchar as questName,
   	GETDATE()::timestamp as approximateUpdateTimestamp,
  	'INITIAL_LOAD'::varchar as eventName,
    GETDATE()::timestamp as syncTimestamp
FROM
    dump_table
WHERE item.pk."S"::varchar LIKE 'Q^_%' escape '^'
);
END
)

log "Transform and load quest data"
transform_and_load_quest_data="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user "${database_username}" \
    --cluster-identifier "${cluster_id}" \
    --database "${database_name}" \
    --sql "${transform_and_load_quest_data_sql}" \
    | jq -r '.Id')"

wait_for_execution_status_change "${transform_and_load_quest_data}"


transform_and_load_member_quest_data_sql=$(cat <<-END
INSERT INTO member_quest (
SELECT
    LTRIM(item.sk."S"::varchar, 'MQ_' )::varchar as memberQuestId,
    LTRIM(item.pk."S"::varchar, 'MQ#M_' ) as memberId,
   	item."questId"."S"::varchar as questId,
    item."dollarsEarned"."N"::float as dollarsEarned,
   	GETDATE()::timestamp as approximateUpdateTimestamp,
  	'INITIAL_LOAD'::varchar as eventName,
    GETDATE()::timestamp as syncTimestamp
FROM
    dump_table
WHERE item.pk."S"::varchar LIKE 'MQ#%'
);
END
)

log "Transform and load member quest data"
transform_and_load_member_quest_data="$(aws redshift-data execute-statement \
    --region us-east-1 \
    --db-user "${database_username}" \
    --cluster-identifier "${cluster_id}" \
    --database "${database_name}" \
    --sql "${transform_and_load_member_quest_data_sql}" \
    | jq -r '.Id')"

wait_for_execution_status_change "${transform_and_load_member_quest_data}"