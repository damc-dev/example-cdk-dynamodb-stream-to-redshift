import { CfnOutput, Duration, RemovalPolicy, Stack, StackProps } from 'aws-cdk-lib';
import { AttributeType, Table } from 'aws-cdk-lib/aws-dynamodb';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import * as events_targets from 'aws-cdk-lib/aws-events-targets';
import * as events from 'aws-cdk-lib/aws-events';

import { Construct } from 'constructs';
import { join } from 'path';
import { Stream } from 'aws-cdk-lib/aws-kinesis';
import { PolicyStatement, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { Cluster } from '@aws-cdk/aws-redshift-alpha';
import { Vpc } from 'aws-cdk-lib/aws-ec2';
interface ExampleCdkDynamodbStreamToRedshiftStackProps extends StackProps {
  vpc: Vpc
  removalPolicy?: RemovalPolicy
  masterUsername?: string
  defaultDatabaseName?: string
}
export class ExampleCdkDynamodbStreamToRedshiftStack extends Stack {
  constructor(scope: Construct, id: string, props: ExampleCdkDynamodbStreamToRedshiftStackProps) {
    super(scope, id, props);

    const defaultDatabaseName = props.defaultDatabaseName ?? "newdb";
    const masterUsername = props.masterUsername ?? "admin"



    const stream = new Stream(this, 'DynamoChangeStream');

    new CfnOutput(this, 'ChangeStreamName', {
      value: stream.streamName
    });

    // TODO restrict redshift ability to assume this role by specific database users
    // src: https://docs.aws.amazon.com/redshift/latest/mgmt/authorizing-redshift-service.html
    const redshiftAssumeRole = new Role(this, 'RedshiftAssumeRole', {
      assumedBy: new ServicePrincipal('redshift.amazonaws.com'),
    });
    
    const policyStatement = PolicyStatement.fromJson({
      "Sid": "ListStream",
      "Effect": "Allow",
      "Action": [
          "kinesis:ListStreams",
          "kinesis:ListShards"
      ],
      "Resource": "*"
    });
    redshiftAssumeRole.addToPolicy(policyStatement);
    stream.grantRead(redshiftAssumeRole)

    new CfnOutput(this, 'RedshiftAssumeRoleArn', {
      value: redshiftAssumeRole.roleArn
    });

    const table = new Table(this, 'Table', {
      partitionKey: {
        name: 'pk',
        type: AttributeType.STRING
      },
      sortKey: {
        name: 'sk',
        type: AttributeType.STRING
      },
      kinesisStream: stream,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    new CfnOutput(this, 'DynamoTableName', {
      value: table.tableName
    });

    const dataGenerator = new NodejsFunction(this, 'DataGeneratorFunction', {
      entry: join(__dirname, "lambda", "index.ts"),
      bundling: {
        externalModules: [
          'aws-sdk'
        ]
      },
      environment: {
        TABLE_NAME: table.tableName
      }
    });

    table.grantWriteData(dataGenerator);

    new events.Rule(this, 'Tick', {
      schedule: events.Schedule.rate(Duration.minutes(1)),
      targets: [ new events_targets.LambdaFunction(dataGenerator) ],
    });

    const cluster = new Cluster(this, "Redshift", {
      masterUser: {
        masterUsername,
      },
      defaultDatabaseName,
      vpc: props.vpc,
      removalPolicy: props.removalPolicy,
      roles: [
        redshiftAssumeRole
      ]
    });

    new CfnOutput(this, 'RedshiftDefaultDatabaseName', {
      value: defaultDatabaseName
    });

    new CfnOutput(this, 'RedshiftClusterId', {
      value: cluster.clusterName
    });

    new CfnOutput(this, 'RedshiftMasterUsername', {
      value: masterUsername
    });
  }
}