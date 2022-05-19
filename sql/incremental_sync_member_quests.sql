CREATE OR REPLACE PROCEDURE incremental_sync_member_quests ()
AS $$
DECLARE
    sql          VARCHAR(MAX) := '';
        max_approximate_arrival_timestamp TIMESTAMP;
        staged_record_count BIGINT :=0;
BEGIN

	-- Get last loaded sequence number from target table

    sql := 'SELECT MAX(approximateArrivalTimestamp) FROM member_quest;';
    EXECUTE sql INTO max_approximate_arrival_timestamp;
    
    IF max_approximate_arrival_timestamp = '1970-01-01' OR max_approximate_arrival_timestamp IS NULL THEN
        RAISE EXCEPTION 'Aborted - `max_approximate_arrival_timestamp` was not retrieved correctly.  Ensure you have done initial data load before attempting incremental sync';
	END IF;
   	RAISE INFO 'Last approximate_arrival_timestamp inserted into member_quest: %', max_approximate_arrival_timestamp;
    
    -- Create temp staging table from target table

    EXECUTE 'DROP TABLE IF EXISTS member_quest_stage;';
    EXECUTE 'CREATE TEMPORARY TABLE member_quest_stage (LIKE member_quest);';
 
    -- Insert (and transform) latest change record for member with sequence number greater then last loaded sequence number into temp staging table


    EXECUTE 'INSERT INTO member_quest_stage ('||
      ' SELECT LTRIM(sk, ''MQ_'' ) as memberQuestId, LTRIM(pk, ''MQ#M_'' ) as memberId,'||
          ' eventData."NewImage"."questId"."S"::varchar as questId,'||
          ' eventData."NewImage"."dollarsEarned"."N"::float as dollarsEarned,'||
          ' approximateArrivalTimestamp,'||
          ' sequencenumber as eventSequenceNumber'||
      ' FROM member_quest_data_extract'||
      ' WHERE pk LIKE ''MQ#%'' AND approximateArrivalTimestamp > '''||max_approximate_arrival_timestamp||''');';

    sql := 'SELECT COUNT(*) FROM member_quest_stage;';
    
    EXECUTE sql INTO staged_record_count;
    RAISE INFO 'Staged member_quest records: %', staged_record_count;
    
    -- In staging table remove all but latest change for record
    
    EXECUTE 'DELETE FROM member_quest_stage where (memberQuestId, approximateArrivalTimestamp) NOT IN (SELECT memberQuestId, MAX(approximateArrivalTimestamp) as approximateArrivalTimestamp FROM member_quest_stage GROUP BY memberQuestId)';

	-- Delete records from target table that also exist in staging table (updated/deleted records)
    
    EXECUTE 'DELETE FROM member_quest using member_quest_stage WHERE member_quest.questId = member_quest_stage.questId';
    
    -- Delete all removed records from target table
    
    EXECUTE 'DELETE FROM member_quest_stage where eventName = ''REMOVE''';

	-- Insert all records from staging table into target table

	EXECUTE 'INSERT INTO member_quest SELECT * FROM member_quest_stage;';

	-- Drop staging table
    
    EXECUTE 'DROP TABLE IF EXISTS member_quest_stage;';

END
$$ LANGUAGE plpgsql;

SELECT count(*), MIN(approximateArrivalTimestamp), MAX(approximateArrivalTimestamp) FROM member_quest;
REFRESH MATERIALIZED VIEW member_quest_data_extract;
call incremental_sync_member_quests();
SELECT message FROM SVL_STORED_PROC_MESSAGES WHERE querytxt = 'call incremental_sync_member_quests();' ORDER BY recordTime DESC;
SELECT count(*), MIN(approximateArrivalTimestamp), MAX(approximateArrivalTimestamp) FROM member_quest;