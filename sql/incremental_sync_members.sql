CREATE OR REPLACE PROCEDURE incremental_sync_members ()
AS $$
DECLARE
    sql          VARCHAR(MAX) := '';
        max_approximate_update_timestamp TIMESTAMP;
        staged_record_count BIGINT :=0;
BEGIN

	-- Get last loaded sequence number from target table

    sql := 'SELECT MAX(approximateUpdateTimestamp) FROM member;';
    EXECUTE sql INTO max_approximate_update_timestamp;
    IF max_approximate_update_timestamp = '1970-01-01' OR max_approximate_update_timestamp IS NULL THEN
        RAISE EXCEPTION 'Aborted - `max_approximate_update_timestamp` was not retrieved correctly.  Ensure you have done initial data load before attempting incremental sync';
	END IF;
   	RAISE INFO 'Last approximate_update_timestamp inserted into member: %', max_approximate_update_timestamp;
    
    -- Create temp staging table from target table

    EXECUTE 'DROP TABLE IF EXISTS member_stage;';
    EXECUTE 'CREATE TEMPORARY TABLE member_stage (LIKE member);';
 
    -- Insert (and transform) latest change record for member with sequence number greater then last loaded sequence number into temp staging table
    
    EXECUTE 'INSERT INTO member_stage ('||
    	' SELECT LTRIM(pk, ''M_'' ) as memberId,'||
        '	sk as memberName,'||
        '	TIMESTAMP ''epoch'' + eventData."ApproximateCreationDateTime"::BIGINT/1000 *INTERVAL ''1 second'' as approximateUpdateTimestamp,'||
        '	eventName'||
    	' FROM member_quest_data_extract'||
        ' WHERE pk LIKE ''M\\_%'' AND approximateUpdateTimestamp > '''||max_approximate_update_timestamp||''');';
 
    sql := 'SELECT COUNT(*) FROM member_stage;';
    
    EXECUTE sql INTO staged_record_count;
    RAISE INFO 'Staged member records: %', staged_record_count;
    
    -- In staging table remove all but latest change for record
    
    EXECUTE 'DELETE FROM member_stage where (memberId, approximateUpdateTimestamp) NOT IN (SELECT memberId, MAX(approximateUpdateTimestamp) as approximateUpdateTimestamp FROM member_stage GROUP BY memberId)';

	-- Delete records from target table that also exist in staging table (updated/deleted records)
    
    EXECUTE 'DELETE FROM member using member_stage WHERE member.memberId = member_stage.memberId';
    
    -- Delete all removed records from target table
    
    EXECUTE 'DELETE FROM member_stage where eventName = ''REMOVE''';

	-- Insert all records from staging table into target table

	EXECUTE 'INSERT INTO member SELECT * FROM member_stage;';

	-- Drop staging table
    
    EXECUTE 'DROP TABLE IF EXISTS member_stage;';

END
$$ LANGUAGE plpgsql;

SELECT count(*), MIN(approximateUpdateTimestamp), MAX(approximateUpdateTimestamp) FROM member;
REFRESH MATERIALIZED VIEW member_quest_data_extract;
call incremental_sync_members();
SELECT message FROM SVL_STORED_PROC_MESSAGES WHERE querytxt = 'call incremental_sync_members();' ORDER BY recordTime DESC;
SELECT count(*), MIN(approximateUpdateTimestamp), MAX(approximateUpdateTimestamp) FROM member;
SELECT * FROM member ORDER BY approximateUpdateTimestamp DESC LIMIT 5;