import { CfnOutput, Duration, RemovalPolicy, Stack, StackProps } from 'aws-cdk-lib';
import { AttributeType, Table } from 'aws-cdk-lib/aws-dynamodb';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import * as events_targets from 'aws-cdk-lib/aws-events-targets';
import * as events from 'aws-cdk-lib/aws-events';

import { Construct } from 'constructs';
import { join } from 'path';
import { Stream } from 'aws-cdk-lib/aws-kinesis';
import { PolicyStatement, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { Cluster, ClusterType, NodeType } from '@aws-cdk/aws-redshift-alpha'
import { Vpc } from 'aws-cdk-lib/aws-ec2';
import { Bucket } from 'aws-cdk-lib/aws-s3';

interface ExampleCdkDynamodbStreamToRedshiftStackProps extends StackProps {
  vpc: Vpc
  removalPolicy?: RemovalPolicy
  masterUsername?: string
  defaultDatabaseName?: string
}

export class ExampleCdkDynamodbStreamToRedshiftStack extends Stack {
  constructor(scope: Construct, id: string, props: ExampleCdkDynamodbStreamToRedshiftStackProps) {
    super(scope, id, props);

    // Configure defaults if not passed in as props
    const defaultDatabaseName = props.defaultDatabaseName ?? "newdb";
    const masterUsername = props.masterUsername ?? "admin"

    // If stack is configured with RemovalPolicy.DESTROY, we also want to 
    //  auto delete files in S3 bucket to ensure deletion succeeds
    const autoDeleteObjects = props.removalPolicy === RemovalPolicy.DESTROY

    // S3 Bucket to hold DynamoDB exports
    const dynamodbBackupBucket = new Bucket(this, 'BackupBucket', {
      removalPolicy: props.removalPolicy,
      autoDeleteObjects
    })

    new CfnOutput(this, 'BackupBucketName', {
      value: dynamodbBackupBucket.bucketName
    });

    // Kinesis Data Stream for DynamoDB table change data
    const stream = new Stream(this, 'DynamoChangeStream');

    new CfnOutput(this, 'ChangeStreamName', {
      value: stream.streamName
    });

    // TODO restrict redshift ability to assume this role by specific database users
    // src: https://docs.aws.amazon.com/redshift/latest/mgmt/authorizing-redshift-service.html
    const redshiftAssumeRole = new Role(this, 'RedshiftAssumeRole', {
      assumedBy: new ServicePrincipal('redshift.amazonaws.com'),
    });
    

    // Configure to read from Kinesis stream
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
    stream.grantRead(redshiftAssumeRole);

    // Configure role with permissions to read from dynamodb backup bucket
    dynamodbBackupBucket.grantRead(redshiftAssumeRole);

    new CfnOutput(this, 'RedshiftAssumeRoleArn', {
      value: redshiftAssumeRole.roleArn
    });

    // DynamoDB table
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
      removalPolicy: props.removalPolicy,
      pointInTimeRecovery: true, // Required for export
    });

    new CfnOutput(this, 'DynamoTableName', {
      value: table.tableName
    });

    new CfnOutput(this, 'DynamoTableArn', {
      value: table.tableArn
    });

    // Redshift Cluster
    const cluster = new Cluster(this, "Redshift", {
      
      masterUser: {
        masterUsername,
      },
      defaultDatabaseName,
      vpc: props.vpc,
      removalPolicy: props.removalPolicy,
      roles: [
        redshiftAssumeRole
      ],
      clusterType: ClusterType.SINGLE_NODE,
      nodeType: NodeType.DC2_LARGE
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

    // Create scheduled Lambda to generate data in DynamoDB table for testing
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
  }
}