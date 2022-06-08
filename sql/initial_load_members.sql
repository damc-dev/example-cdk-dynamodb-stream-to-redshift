INSERT INTO member (
SELECT
    LTRIM(item.pk."S"::varchar, 'M_' )::varchar as memberId, 
  	item.sk."S"::varchar as memberName,
   	GETDATE()::timestamp as approximateUpdateTimestamp,
  	'INITIAL_LOAD'::varchar as eventName,
    GETDATE()::timestamp as syncTimestamp
FROM
    dump_table
WHERE item.pk."S"::varchar LIKE 'M^_%' escape '^'
);