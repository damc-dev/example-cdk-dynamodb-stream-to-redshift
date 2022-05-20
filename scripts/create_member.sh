#!/usr/bin/env bash

set -e

usage() {
    echo "Usage: $(basename "$0") [-t dynamodb_table] [-i member_id] [-n member_name] 
Creates a quest 

Examples:
    $(basename "$0") -t dev-member -n 'Bob' -i '53AC8FBF-0187-433C-B322-CEFE4315E46A'
"
}

_error() {
    local error_msg=$1
    echo "[ERROR] $error_msg"
    usage
    exit 1
}

id="$(uuidgen)"
name="Bob"

if [ -f "outputs.json" ]; then
    dynamodb_table="$(jq -r '.ExampleCdkDynamodbStreamToRedshiftStack.DynamoTableName' outputs.json)"
fi
dynamodb_table="${dynamodb_table:-${DYNAMODB_TABLE}}"

while getopts :i:n:t: OPT; do
  case "${OPT}" in
    i) id="${OPTARG}";;
    n) name="${OPTARG}";;
    t) dynamodb_table="${OPTARG}";;
  esac
done
shift $(( $OPTIND - 1 ))

if [ -z "${dynamodb_table}" ]; then
    _error "no DynamoDB table name specified"
fi

#put_item='{ "pk": { "S": "M_'${id}'" }, "memberId": {"S": "'${id}'"}, "sk": { "S": "'${name}'"}}'
put_item='{ "pk": { "S": "M_'${id}'" }, "sk": { "S": "'${name}'" }, "memberId": {"S": "'${id}'"}}'
aws dynamodb put-item \
    --table-name "${dynamodb_table}" \
    --item "${put_item}"

expression_attribute_values='{":id":{"S":"M_'${id}'"}}'
aws dynamodb query \
    --table-name "${dynamodb_table}" \
    --key-condition-expression "pk = :id" \
    --expression-attribute-values  "${expression_attribute_values}" | jq