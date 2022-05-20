CREATE OR REPLACE PROCEDURE incremental_sync_quests ()
AS $$
DECLARE
    sql          VARCHAR(MAX) := '';
        max_approximate_update_timestamp TIMESTAMP;
        staged_record_count BIGINT :=0;
BEGIN

	-- Get last loaded sequence number from target table

    sql := 'SELECT MAX(approximateUpdateTimestamp) FROM quest;';
    EXECUTE sql INTO max_approximate_update_timestamp;
    
    IF max_approximate_update_timestamp = '1970-01-01' OR max_approximate_update_timestamp IS NULL THEN
        RAISE EXCEPTION 'Aborted - `max_approximate_update_timestamp` was not retrieved correctly.  Ensure you have done initial data load before attempting incremental sync';
	END IF;
   	RAISE INFO 'Last approximate_update_timestamp inserted into quest: %', max_approximate_update_timestamp;
    
    -- Create temp staging table from target table

    EXECUTE 'DROP TABLE IF EXISTS quest_stage;';
    EXECUTE 'CREATE TEMPORARY TABLE quest_stage (LIKE quest);';
 
    -- Insert (and transform) latest change record for member with sequence number greater then last loaded sequence number into temp staging table

    EXECUTE 'INSERT INTO quest_stage ('||
        ' SELECT'||
        '	LTRIM(pk, ''Q_'' ) as questId,'||
        '	sk as questName,'||
        '	TIMESTAMP ''epoch'' + eventData."ApproximateCreationDateTime"::BIGINT/1000 *INTERVAL ''1 second'' as approximateUpdateTimestamp,'||
        '	eventName'||
        ' FROM member_quest_data_extract'||
        ' WHERE pk LIKE ''Q^_%'' escape ''^'' AND approximateUpdateTimestamp > '''||max_approximate_update_timestamp||''');';

    sql := 'SELECT COUNT(*) FROM quest_stage;';
    
    EXECUTE sql INTO staged_record_count;
    RAISE INFO 'Staged quest records: %', staged_record_count;
    
    -- In staging table remove all but latest change for record
    
    EXECUTE 'DELETE FROM quest_stage where (questId, approximateUpdateTimestamp) NOT IN (SELECT questId, MAX(approximateUpdateTimestamp) as approximateUpdateTimestamp FROM quest_stage GROUP BY questId)';

	-- Delete records from target table that also exist in staging table (updated/deleted records)
    
    EXECUTE 'DELETE FROM quest using quest_stage WHERE quest.questId = quest_stage.questId';
    
    -- Delete all removed records from target table
    
    EXECUTE 'DELETE FROM quest_stage where eventName = ''REMOVE''';

	-- Insert all records from staging table into target table

    EXECUTE 'INSERT INTO quest SELECT questId, questName, approximateUpdateTimestamp, eventName, GETDATE() as syncTimestamp FROM quest_stage;';

	-- Drop staging table
    
    EXECUTE 'DROP TABLE IF EXISTS quest_stage;';

END
$$ LANGUAGE plpgsql;

SELECT count(*), MIN(approximateUpdateTimestamp), MAX(approximateUpdateTimestamp) FROM quest;
REFRESH MATERIALIZED VIEW member_quest_data_extract;
call incremental_sync_quests();
SELECT message FROM SVL_STORED_PROC_MESSAGES WHERE querytxt = 'call incremental_sync_quests();' ORDER BY recordTime DESC;
SELECT count(*), MIN(approximateUpdateTimestamp), MAX(approximateUpdateTimestamp) FROM quest;
SELECT * FROM quest ORDER BY approximateUpdateTimestamp DESC LIMIT 5;