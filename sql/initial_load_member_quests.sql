INSERT INTO member_quest (
SELECT
    LTRIM(item.sk."S"::varchar, 'MQ_' )::varchar as memberQuestId,
    LTRIM(item.pk."S"::varchar, 'MQ#M_' ) as memberId,
   	item."questId"."S"::varchar as questId,
    item."dollarsEarned"."N"::float as dollarsEarned,
   	GETDATE()::timestamp as approximateUpdateTimestamp,
  	'INITIAL_LOAD'::varchar as eventName,
    GETDATE()::timestamp as syncTimestamp
FROM
    dump_table
WHERE item.pk."S"::varchar LIKE 'MQ#%'
);