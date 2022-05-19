import * as SDK from "aws-sdk";

const TABLE_NAME = process.env.TABLE_NAME;
if (!TABLE_NAME) {
  throw new Error("Missing TABLE_NAME environment variable");
}

export class MemberRepository {
  
  constructor(private dynamoDB: SDK.DynamoDB) {
    if (!TABLE_NAME) {
      throw Error("TABLE_NAME is not defined");
    }
    this.dynamoDB = dynamoDB;
  }

  async create(member: Member): Promise<Member> {
    const params: SDK.DynamoDB.PutItemInput = {
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
        },
        memberName: {
          S: member.name
        }
      }
    };

    await this.dynamoDB.putItem(params).promise();
    return member;
  }
}
