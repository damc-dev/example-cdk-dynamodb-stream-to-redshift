// eslint-disable-next-line import/no-extraneous-dependencies
import { User } from "aws-cdk-lib/aws-iam";
import * as SDK from "aws-sdk";
import { v4 as uuidv4 } from "uuid";

const TABLE_NAME = process.env.TABLE_NAME;
if (!TABLE_NAME) {
  throw new Error("Missing TABLE_NAME environment variable");
}

interface Member {
  id: string;
  name: string;
}

interface Quest {
  id: string;
  name: string;
}

interface MemberQuest {
  id: string;
  dollarsEarned: number;
  memberId: string;
  questId: string;
}

exports.handler = async (event: any) => {
  console.log("Received event: ", event);

  const dynamo = new SDK.DynamoDB();
  const member = generateMember();
  console.log("Generated member:", member);
  const memberReq = mapMemberRequest(member);
  console.log("Member request", memberReq)
  await dynamo.putItem(memberReq).promise();
  console.log("Put member:", memberReq);

  const quest = generateQuest();
  console.log("Generated quest:", quest);
  const questReq = mapQuestRequest(quest);
  await dynamo.putItem(questReq).promise();
  console.log("Put quest:", questReq);

  const memberQuest = generateMemberQuest(member.id, quest.id);
  console.log("Generated member quest:", memberQuest);
  const memberQuestReq = mapMemberQuestRequest(memberQuest);
  await dynamo.putItem(memberQuestReq).promise();
  console.log("Put member quest:", memberQuestReq);
};

function generateMember(): Member {
  const names = [
    "Liam",
    "Olivia",
    "Noah",
    "Emma",
    "Oliver",
    "Charlotte",
    "Elijah",
    "Amelia",
  ];
  return {
    id: uuidv4(),
    name: getRandomItem(names),
  };
}

function mapMemberRequest(member: Member): SDK.DynamoDB.PutItemInput {
  return {
    TableName: TABLE_NAME,
    Item: {
      pk: {
        S: `M_${member.id}`,
      },
      sk: {
        S: member.name,
      },
      memberId: {
        S: member.id
      }
    },
  };
}

function getRandomItem(items: string[]) {
  return items[Math.floor(Math.random() * items.length)];
}
function getRandomNumberBetween(
  min: number,
  max: number,
  decimalPlaces: number
) {
  const rand = Math.random() * (max - min) + min;
  const power = Math.pow(10, decimalPlaces);
  return Math.floor(rand * power) / power;
}

function generateQuest(): Quest {
  const names = [
    "30 minutes of exercise",
    "Walk 10,000 steps",
    "Walk 100,000 steps",
  ];
  return {
    id: uuidv4(),
    name: getRandomItem(names),
  };
}

function mapQuestRequest(quest: Quest): SDK.DynamoDB.PutItemInput {
  return {
    TableName: TABLE_NAME,
    Item: {
      pk: {
        S: `Q_${quest.id}`,
      },
      sk: {
        S: quest.name,
      },
      questId: {
        S: quest.id
      }
    },
  };
}

function generateMemberQuest(memberId: string, questId: string): MemberQuest {
  return {
    id: uuidv4(),
    dollarsEarned: getRandomNumberBetween(1, 100, 2),
    memberId,
    questId,
  };
}

function mapMemberQuestRequest(memberQuest: MemberQuest) {
  return {
    TableName: TABLE_NAME,
    Item: {
      pk: {
        S: `MQ#M_${memberQuest.memberId}`,
      },
      sk: {
        S: `MQ_${memberQuest.id}`,
      },
      questId: {
        S: memberQuest.id,
      },
      dollarsEarned: {
        N: memberQuest.dollarsEarned.toString(),
      },
    },
  };
}
