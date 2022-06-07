#!/usr/bin/env bash

set -e

usage() {
    echo "Usage: $(basename "$0") [-t dynamodb_table] [-i member_quest_id] [-m member_id] [-q quest_id] 
Creates a member quest 

Examples:
    $(basename "$0") -t dev-member -n 'Bob' -i '53AC8FBF-0187-433C-B322-CEFE4315E46A' -m '588A3BFF-771C-4AA1-8D0B-88675DE8CF31' -q 'B4E234EE-CB7B-4EB9-BE5A-F20A5146A64C' -e '1.50'
"
}

_error() {
    local error_msg=$1
    echo "[ERROR] $error_msg"
    usage
    exit 1
}

id="$(uuidgen)"
member_id=""
quest_id=""
earned_dollars="0"

script_dir="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
output_file="${OUTPUT_FILE:-${script_dir}/../outputs.json}"

if [ -f "${output_file}" ]; then
    dynamodb_table="$(jq -r '.ExampleCdkDynamodbStreamToRedshiftStack.DynamoTableName' "${output_file}")"
fi
dynamodb_table="${dynamodb_table:-${DYNAMODB_TABLE}}"

while getopts :i:m:q:e:t: OPT; do
  case "${OPT}" in
    i) id="${OPTARG}";;
    m) member_id="${OPTARG}";;
    q) quest_id="${OPTARG}";;
    e) earned_dollars="${OPTARG}";;
    t) dynamodb_table="${OPTARG}";;
  esac
done
shift $(( $OPTIND - 1 ))

if [ -z "${dynamodb_table}" ]; then
    _error "no DynamoDB table name specified"
fi

if [ -z "${member_id}" ]; then
    _error "member id is required"
fi

if [ -z "${quest_id}" ]; then
    _error "quest id is required"
fi

put_item='{"dollarsEarned":{"N":"'${earned_dollars}'"},"questId":{"S":"'${quest_id}'"},"sk":{"S":"MQ_'${id}'"},"pk":{"S":"MQ#M_'${member_id}'"}}'
aws dynamodb put-item \
    --table-name "${dynamodb_table}" \
    --item "${put_item}"


item_key='{ "pk": { "S": "MQ#M_'${member_id}'" }, "sk": { "S": "MQ_'${id}'"} }'
aws dynamodb get-item \
    --table-name "${dynamodb_table}" \
    --key "${item_key}" | jq