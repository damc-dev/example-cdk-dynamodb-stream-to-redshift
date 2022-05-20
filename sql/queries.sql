-- Get the lag times between dynamoDB change record and sync into redshift for 5 most recently synced records
SELECT
    approximateUpdateTimestamp, syncTimestamp, (syncTimestamp - approximateUpdateTimestamp
    ) as syncLag 
FROM
member
WHERE syncTimestamp IS NOT NULL
     ORDER BY syncTimestamp DESC LIMIT 5;

-- Get the top 5 largest lag times between dynamoDB change record and sync into redshift today

SELECT
    approximateUpdateTimestamp, syncTimestamp, (syncTimestamp - approximateUpdateTimestamp
    ) as syncLag 
FROM
member
WHERE
   TRUNC(approximateUpdateTimestamp) = CURRENT_DATE
     ORDER BY syncLag DESC LIMIT 5;
    
    
-- Get Rewards Earned by Member

SELECT
    m.memberId, m.memberName, SUM(mq.dollarsEarned
    ) as totalDollarsEarned
FROM
    member_quest as mq, member as m 
WHERE
    mq.memberId = m.memberId
GROUP
    BY m.memberId, m.memberNameORDER BY totalDollarsEarned DESC;

-- Get Rewards Earned by Quest

SELECT
    q.questId, q.questName, SUM(mq.dollarsEarned
    ) as totalDollarsEarned
FROM
    member_quest as mq, quest as q
WHERE
    q.questId = mq.questId 
GROUP
    BY q.questId, q.questName
    ORDER BY totalDollarsEarned DESC;