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