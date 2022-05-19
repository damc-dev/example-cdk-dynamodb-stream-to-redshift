-- Get Rewards Earned per Member

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

-- Get Rewards Earned per Quest by memberId

SELECT
    q.questId, q.questName, SUM(mq.dollarsEarned
    ) as totalDollarsEarned
FROM
    member_quest as mq, quest as q
WHERE
    q.questId = mq.questId
    AND mq.memberId = ''
GROUP
    BY q.questId, q.questName
    ORDER BY totalDollarsEarned DESC;


-- Get Total Rewards Earned by Day

SELECT
   DATE_TRUNC('day', approximateArrivalTimestamp) as earnDay,
SUM(mq.dollarsEarned) as totalDollarsEarned
FROM
    member_quest as mq
GROUP
    BY earnDay
    ORDER BY totalDollarsEarned DESC;