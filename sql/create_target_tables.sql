-- MEMBER: Create table

DROP TABLE IF EXISTS member;

CREATE TABLE member (
    memberId VARCHAR,
    memberName VARCHAR,
  	approximateUpdateTimestamp TIMESTAMP,
  	eventName VARCHAR,
  	syncTimestamp TIMESTAMP
);

-- QUEST: Create table

DROP TABLE IF EXISTS quest;

CREATE TABLE quest (
    questId VARCHAR,
    questName VARCHAR,
  	approximateUpdateTimestamp TIMESTAMP,
   	eventName VARCHAR,
    syncTimestamp TIMESTAMP
);

-- MEMBER QUEST: Create table

DROP TABLE IF EXISTS member_quest;

CREATE TABLE member_quest (
	memberQuestId VARCHAR,
    memberId VARCHAR,
    questId VARCHAR,
    dollarsEarned FLOAT,
  	approximateUpdateTimestamp TIMESTAMP,
    eventName VARCHAR,
  	syncTimestamp TIMESTAMP
);