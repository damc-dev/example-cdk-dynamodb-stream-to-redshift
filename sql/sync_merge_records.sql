

CREATE OR REPLACE PROCEDURE sync_merge_records ()
AS $$
DECLARE
    sql          VARCHAR(MAX) := '';
        max_id       BIGINT  := 0;

BEGIN

    sql := 'SELECT MAX(eventSequenceNumber) FROM member;';
    EXECUTE sql INTO max_id;

    EXECUTE 'DROP TABLE IF EXISTS tmp_member_sync_merge;';
    EXECUTE 'CREATE TEMPORARY TABLE tmp_member_sync_merge (LIKE member);';
    EXECUTE 'INSERT INTO tmp_member_sync_merge SELECT * FROM member_quest_data_extract WHERE eventSequenceNumber > '||max_id||';';

END
$$ LANGUAGE plpgsql;


/* Usage Example

    -- Redshift: create logging table
    DROP TABLE IF EXISTS public.sp_logs;
    CREATE TABLE public.sp_logs (
        batch_time   TIMESTAMP
      , source_table VARCHAR
      , target_table VARCHAR
      , sync_column  VARCHAR
      , sync_status  VARCHAR
      , sync_queries VARCHAR
      , row_count    INT);