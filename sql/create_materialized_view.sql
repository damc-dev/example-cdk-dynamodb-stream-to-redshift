-- Set case sensitivity at the cluster level for admin user since Kinesis Stream names are case sensitive
-- 		NOTE: Could probably try and make Kinesis stream name and IAM role lowercase so this isn't needed
ALTER USER admin SET enable_case_sensitive_identifier TO true;

-- Create external schema for accessing Kinesis
CREATE EXTERNAL SCHEMA activity_tracking
FROM KINESIS
IAM_ROLE 'arn:aws:iam::094299891118:role/ExampleCdkDynamodbStreamT-RedshiftAssumeRole91938B-1J3YPQ6ZHT30V';


DROP MATERIALIZED VIEW IF EXISTS member_quest_data_extract;

-- NOTES: 
-- 		* Need to look into whether it would improve performance to put JSON payload into super data type before extracting the data.
-- 		* Need to revisit whether column types and lengths are appropriate for expected payloads


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
    FROM activity_tracking."ExampleCdkDynamodbStreamToRedshiftStack-DynamoChangeStreamE7F0EE82-sAMpN3bIRC2S";
    
-- Query columns of materialized view
select pg_get_cols('member_quest_data_extract');