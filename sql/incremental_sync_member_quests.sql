CREATE OR REPLACE PROCEDURE incremental_sync_member_quests ()
AS $$
DECLARE
    sql          VARCHAR(MAX) := '';
        max_approximate_update_timestamp TIMESTAMP;
        staged_record_count BIGINT :=0;
BEGIN

	-- Get last loaded sequence number from target table

    sql := 'SELECT MAX(approximateUpdateTimestamp) FROM member_quest;';
    EXECUTE sql INTO max_approximate_update_timestamp;
    
    IF max_approximate_update_timestamp = '1970-01-01' OR max_approximate_update_timestamp IS NULL THEN
        RAISE EXCEPTION 'Aborted - `max_approximate_update_timestamp` was not retrieved correctly.  Ensure you have done initial data load before attempting incremental sync';
	END IF;
   	RAISE INFO 'Last approximate_update_timestamp inserted into member_quest: %', max_approximate_update_timestamp;
    
    -- Create temp staging table from target table

    EXECUTE 'DROP TABLE IF EXISTS member_quest_stage;';
    EXECUTE 'CREATE TEMPORARY TABLE member_quest_stage (LIKE member_quest);';
 
    -- Insert (and transform) latest change record for member with sequence number greater then last loaded sequence number into temp staging table


    EXECUTE 'INSERT INTO member_quest_stage ('||
      ' SELECT LTRIM(sk, ''MQ_'' ) as memberQuestId, LTRIM(pk, ''MQ#M_'' ) as memberId,'||
      '		eventData."NewImage"."questId"."S"::varchar as questId,'||
      '		eventData."NewImage"."dollarsEarned"."N"::float as dollarsEarned,'||
      '		TIMESTAMP ''epoch'' + eventData."ApproximateCreationDateTime"::BIGINT/1000 *INTERVAL ''1 second'' as approximateUpdateTimestamp,'||
      '		eventName'||
      ' FROM member_quest_data_extract'||
      ' WHERE pk LIKE ''MQ#%'' AND approximateUpdateTimestamp > '''||max_approximate_update_timestamp||''');';

    sql := 'SELECT COUNT(*) FROM member_quest_stage;';
    
    EXECUTE sql INTO staged_record_count;
    RAISE INFO 'Staged member_quest records: %', staged_record_count;
    
    -- In staging table remove all but latest change for record
    
    EXECUTE 'DELETE FROM member_quest_stage where (memberQuestId, approximateUpdateTimestamp) NOT IN (SELECT memberQuestId, MAX(approximateUpdateTimestamp) as approximateUpdateTimestamp FROM member_quest_stage GROUP BY memberQuestId)';

	-- Delete records from target table that also exist in staging table (updated/deleted records)
    
    EXECUTE 'DELETE FROM member_quest using member_quest_stage WHERE member_quest.questId = member_quest_stage.questId';
    
    -- Delete all removed records from target table
    
    EXECUTE 'DELETE FROM member_quest_stage where eventName = ''REMOVE''';

	-- Insert all records from staging table into target table

	EXECUTE 'INSERT INTO member_quest SELECT memberQuestId, memberId, questId, dollarsEarned, approximateUpdateTimestamp, eventName, GETDATE() as syncTimestamp FROM member_quest_stage;';

	-- Drop staging table
    
    EXECUTE 'DROP TABLE IF EXISTS member_quest_stage;';

END
$$ LANGUAGE plpgsql;

-- SELECT count(*), MIN(approximateUpdateTimestamp), MAX(approximateUpdateTimestamp) FROM member_quest;
-- REFRESH MATERIALIZED VIEW member_quest_data_extract;
-- call incremental_sync_member_quests();
-- SELECT message FROM SVL_STORED_PROC_MESSAGES WHERE querytxt = 'call incremental_sync_member_quests();' ORDER BY recordTime DESC;
-- SELECT count(*), MIN(approximateUpdateTimestamp), MAX(approximateUpdateTimestamp) FROM member_quest;
-- SELECT * FROM member_quest ORDER BY approximateUpdateTimestamp DESC LIMIT 5;