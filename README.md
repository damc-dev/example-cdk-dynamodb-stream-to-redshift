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

# Goals

* Show how you can get almost realtime data for analysis and reporting into Redshift
* Show how you can keep data in sync between DynamoDB and Redshift

# TODO

* Show realtime data
    * Create records in DynamoDB, run incremental load into Redshift, query it in Redshift
        * Create member, create multiple quests, create multiple member quests

* Show data sync
    * Update in DynamoDB, run incremental load into Redshift, query it in Redshift
        * Update member quests with dollars earned

* Show reporting
    * Query for dollars earned by quest for a member

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

## Create Target Tables

```sql
CREATE TABLE member (
    memberId VARCHAR,
    memberName VARCHAR,
    eventSequenceNumber VARCHAR
);

CREATE TABLE quest (
    questId VARCHAR,
    questName VARCHAR,
    eventSequenceNumber VARCHAR
);

CREATE TABLE member_quest (
    memberId VARCHAR,
    questId VARCHAR,
    dollarsEarned FLOAT,
    --lastUpdatedTimestamp TIMESTAMP,
    eventSequenceNumber VARCHAR
);
```


### Initial Data Load

```sql
INSERT INTO member (
SELECT
    LTRIM(pk, 'M_' ) as memberId, sk as memberName, sequencenumber as eventSequenceNumber
FROM
    member_quest_data_extract
WHERE SUBSTRING(pk, 1, 2) = 'M_'
);


INSERT INTO quest (
SELECT
    LTRIM(pk, 'Q_' )  as questId, sk as questName, sequencenumber as eventSequenceNumber
FROM
    member_quest_data_extract
WHERE SUBSTRING(pk, 1, 2) = 'Q_'
);

INSERT INTO member_quest (
SELECT
    LTRIM(sk, 'MQ_' ) as memberQuestId,
    LTRIM(pk, 'MQ#M_' ) as memberId,
  	newImage."questId"."S"::varchar as questId,
    newImage."dollarsEarned"."N"::float as dollarsEarned,
  	sequencenumber as eventSequenceNumber
FROM
    member_quest_data_extract
WHERE SUBSTRING(pk, 1, 3) = 'MQ#'
);
```

## Queries

```sql

SELECT q.questName, SUM(dollarsEarned) FROM member_quest, quest as q WHERE q.questId = questId

```




