#!/usr/bin/env bash

set -e

usage() {
    echo "Usage: $(basename "$0") [-t dynamodb_table] [-i quest_id] [-n quest_name] 
Creates a quest 

Examples:
    $(basename "$0") -t dev-quest -n 'Walk 10000 steps'
"
}

_error() {
    local error_msg=$1
    echo "[ERROR] $error_msg"
    usage
    exit 1
}

quest_id="$(uuidgen)"
quest_name="30 minutes of exercise"
if [ -f "outputs.json" ]; then
    dynamodb_table="$(jq -r '.ExampleCdkDynamodbStreamToRedshiftStack.DynamoTableName' outputs.json)"
fi
dynamodb_table="${dynamodb_table:-${DYNAMODB_TABLE}}"

while getopts :i:n:t: OPT; do
  case "${OPT}" in
    i) quest_id="${OPTARG}";;
    n) quest_name="${OPTARG}";;
    t) dynamodb_table="${OPTARG}";;
  esac
done
shift $(( $OPTIND - 1 ))

if [ -z "${dynamodb_table}" ]; then
    _error "no DynamoDB table name specified"
fi

put_item='{ "pk": { "S": "Q_'${quest_id}'" }, "questId": {"S": "'${quest_id}'"}, "sk": { "S": "'${quest_name}'"}}'
aws dynamodb put-item \
    --table-name "${dynamodb_table}" \
    --item "${put_item}"

item_key='{ "pk": { "S": "Q_'${quest_id}'" }, "sk": { "S": "'${quest_name}'"} }'
aws dynamodb get-item \
    --table-name "${dynamodb_table}" \
    --key "${item_key}" | jq