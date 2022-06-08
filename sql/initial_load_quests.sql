INSERT INTO quest (
SELECT
    LTRIM(item.pk."S"::varchar, 'Q_' )::varchar as questId, 
  	item.sk."S"::varchar as questName,
   	GETDATE()::timestamp as approximateUpdateTimestamp,
  	'INITIAL_LOAD'::varchar as eventName,
    GETDATE()::timestamp as syncTimestamp
FROM
    dump_table
WHERE item.pk."S"::varchar LIKE 'Q^_%' escape '^'
);