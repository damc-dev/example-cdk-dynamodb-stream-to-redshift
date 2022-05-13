# Welcome to your CDK TypeScript project

This is a blank project for CDK development with TypeScript.

The `cdk.json` file tells the CDK Toolkit how to execute your app.

## Useful commands

* `npm run build`   compile typescript to js
* `npm run watch`   watch for changes and compile
* `npm run test`    perform the jest unit tests
* `cdk deploy`      deploy this stack to your default AWS account/region
* `cdk diff`        compare deployed stack with current state
* `cdk synth`       emits the synthesized CloudFormation template


# Steps

## Under Redshift cluster permissions add the exported RedshiftAssumeRole to Associated IAM roles

## In Redshift Query Run

https://docs.aws.amazon.com/redshift/latest/dg/materialized-view-streaming-ingestion.html


Enable Case Sensitive Identifiers which is required for Kinesis

```sql
-- To create and use case sensitive identifiers
SET enable_case_sensitive_identifier TO true;
```

Create external schema for Kinesis

```sql
CREATE EXTERNAL SCHEMA schema_one
FROM KINESIS
IAM_ROLE { default | 'iam-role-arn' };
```

Create Materialized View

```sql
CREATE MATERIALIZED VIEW activity_tracking_view AS
SELECT ApproximateArrivalTimestamp,
       JSON_PARSE(from_varbyte(Data, 'utf-8')) as Data
FROM activity_tracking."ExampleCdkDynamodbStreamToRedshiftStack-DynamoChangeStreamE7F0EE82-sAMpN3bIRC2S"
WHERE is_utf8(Data) AND is_valid_json(from_varbyte(Data, 'utf-8'));
```

Refresh Materialized View

```sql
REFRESH MATERIALIZED VIEW activity_tracking_view;
```

Select from Materialized View

```sql
SELECT * FROM "new_db"."public"."activity_tracking_view";
```

Select Nested Values from Materialized View

```sql
SELECT
    data."eventName", 
    data."dynamodb"."ApproximateCreationDateTime", 
    data."dynamodb"."Keys"."pk"."S" as pk, 
    data."dynamodb"."Keys"."sk"."S" as sk 
FROM
    "new_db"."public"."activity_tracking_view";
```