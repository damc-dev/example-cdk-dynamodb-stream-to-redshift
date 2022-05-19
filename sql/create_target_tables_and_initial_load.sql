-- MEMBER: Create table
DROP TABLE member;

CREATE TABLE member (
    memberId VARCHAR,
    memberName VARCHAR,
  	approximateUpdateTimestamp TIMESTAMP,
  	eventName VARCHAR
);

-- MEMBER: Initial load for testing
--		NOTE: For production would use COPY command to stage and ETL initial data load

INSERT INTO member (
SELECT
    LTRIM(pk, 'M_' ) as memberId, 
  	sk as memberName,
   	TIMESTAMP 'epoch' + eventData."ApproximateCreationDateTime"::BIGINT/1000 *INTERVAL '1 second' as approximateUpdateTimestamp, 
  	eventName
FROM
    member_quest_data_extract
WHERE pk LIKE 'M\\_%' AND eventName = 'INSERT' ORDER BY approximateUpdateTimestamp ASC LIMIT 5
);

SELECT * FROM member;


-- QUEST: Create table

DROP TABLE quest;

CREATE TABLE quest (
    questId VARCHAR,
    questName VARCHAR,
  	approximateUpdateTimestamp TIMESTAMP,
   	eventName VARCHAR
);


-- QUEST: Initial load for testing
--		NOTE: For production would use COPY command to stage and ETL initial data load

INSERT INTO quest (
SELECT
  LTRIM(pk, 'Q_' )  as questId, 
  sk as questName, 
  TIMESTAMP 'epoch' + eventData."ApproximateCreationDateTime"::BIGINT/1000 *INTERVAL '1 second' as approximateUpdateTimestamp, 
  eventName
FROM
    member_quest_data_extract
WHERE pk LIKE 'Q\\_%' AND eventName = 'INSERT' ORDER BY approximateUpdateTimestamp ASC LIMIT 5
);

SELECT * FROM quest;


-- MEMBER QUEST: Create table

drop table member_quest;

CREATE TABLE member_quest (
	memberQuestId VARCHAR,
    memberId VARCHAR,
    questId VARCHAR,
    dollarsEarned FLOAT,
  	approximateUpdateTimestamp TIMESTAMP,
    eventName VARCHAR
);

-- MEMBER QUEST: Initial load for testing
--		NOTE: For production would use COPY command to stage and ETL initial data load
INSERT INTO member_quest (
SELECT
    LTRIM(sk, 'MQ_' ) as memberQuestId,
    LTRIM(pk, 'MQ#M_' ) as memberId,
  	eventData."NewImage"."questId"."S"::varchar as questId,
    eventData."NewImage"."dollarsEarned"."N"::float as dollarsEarned,
    TIMESTAMP 'epoch' + eventData."ApproximateCreationDateTime"::BIGINT/1000 *INTERVAL '1 second' as approximateUpdateTimestamp,
  	eventName
FROM
    member_quest_data_extract
WHERE pk LIKE 'MQ#%' AND eventName = 'INSERT' ORDER BY approximateUpdateTimestamp ASC LIMIT 5
);

SELECT * FROM member_quest;