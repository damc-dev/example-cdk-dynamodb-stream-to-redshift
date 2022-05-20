#!/usr/bin/env bash

set -e

usage() {
    echo "Usage: $(basename "$0") -i member_quest_id -m member_id -e earned_dollars [-t dynamodb_table] 
Updates a member quest 

Examples:
    $(basename "$0") -t dev-member -i '53AC8FBF-0187-433C-B322-CEFE4315E46A' -m '588A3BFF-771C-4AA1-8D0B-88675DE8CF31' -e '1.50'
"
}

_error() {
    local error_msg=$1
    echo "[ERROR] $error_msg"
    usage
    exit 1
}

id=""
member_id=""
#quest_id=""
earned_dollars="0"

if [ -f "outputs.json" ]; then
    dynamodb_table="$(jq -r '.ExampleCdkDynamodbStreamToRedshiftStack.DynamoTableName' outputs.json)"
fi
dynamodb_table="${dynamodb_table:-${DYNAMODB_TABLE}}"

while getopts :i:m:e:t: OPT; do
  case "${OPT}" in
    i) id="${OPTARG}";;
    m) member_id="${OPTARG}";;
#    q) quest_id="${OPTARG}";;
    e) earned_dollars="${OPTARG}";;
    t) dynamodb_table="${OPTARG}";;
  esac
done
shift $(( $OPTIND - 1 ))

if [ -z "${dynamodb_table}" ]; then
    _error "no DynamoDB table name specified"
fi

if [ -z "${id}" ]; then
    _error "member_quest_id is required"
fi

if [ -z "${member_id}" ]; then
    _error "member_id is required"
fi


if [ -z "${earned_dollars}" ]; then
    _error "earned_dollars is required"
fi

# aws dynamodb execute-statement --statement "UPDATE '${dynamodb_table}'  \
#                                             SET dollarsEarned=${earned_dollars}  \
#                                             WHERE sk='MQ_${id}'"

item_key='{ "pk": { "S": "MQ#M_'${member_id}'" }, "sk": { "S": "MQ_'${id}'"} }'

=aws dynamodb update-item \
    --table-name "${dynamodb_table}" \
    --key "${item_key}" \
    --expression-attribute-values '{":e": {"N": "'${earned_dollars}'"}}' \
    --update-expression "SET dollarsEarned=:e"

# aws dynamodb execute-statement --statement "SELECT * FROM \"${dynamodb_table}\"   \
#                                             WHERE sk=\"MQ_${id}\"" | jq
aws dynamodb get-item \
    --table-name "${dynamodb_table}" \
    --key "${item_key}" | jq