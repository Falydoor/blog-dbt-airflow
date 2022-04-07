import { Stack, StackProps, aws_mwaa, aws_s3, aws_iam, aws_ec2, aws_s3_deployment } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as aws_redshift from '@aws-cdk/aws-redshift-alpha';

export class BlogDbtAirflowStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const env_name = 'blog-dbt-airflow';

    // IAM Role
    const role = new aws_iam.Role(this, 'MyRole', {
      assumedBy: new aws_iam.CompositePrincipal(
        new aws_iam.ServicePrincipal('airflow.amazonaws.com'),
        new aws_iam.ServicePrincipal('airflow-env.amazonaws.com'))
    });

    // Add Redshift permission
    role.addManagedPolicy(aws_iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonRedshiftFullAccess'));

    // Update role with Airflow permissions
    role.addToPolicy(new aws_iam.PolicyStatement({
      resources: [`arn:aws:logs:${this.region}:${this.account}:log-group:airflow-${env_name}-*`],
      actions: [
        'logs:CreateLogStream', 'logs:CreateLogGroup', 'logs:PutLogEvents', 'logs:GetLogEvents',
        'logs:GetLogRecord', 'logs:GetLogGroupFields', 'logs:GetQueryResults'
      ]
    }));
    role.addToPolicy(new aws_iam.PolicyStatement({
      resources: ['*'],
      actions: ['logs:DescribeLogGroups', 'cloudwatch:PutMetricData']
    }));
    role.addToPolicy(new aws_iam.PolicyStatement({
      resources: [`arn:aws:airflow:${this.region}:${this.account}:environment/airflow-${env_name}`],
      actions: ['airflow:PublishMetrics']
    }));
    role.addToPolicy(new aws_iam.PolicyStatement({
      resources: [`arn:aws:sqs:${this.region}:*:airflow-celery-*`],
      actions: [
        'sqs:ChangeMessageVisibility', 'sqs:DeleteMessage', 'sqs:GetQueueAttributes',
        'sqs:GetQueueUrl', 'sqs:ReceiveMessage', 'sqs:SendMessage'
      ]
    }));
    role.addToPolicy(new aws_iam.PolicyStatement({
      notResources: [`arn:aws:kms:*:${this.account}:key/*`],
      actions: ['kms:Decrypt', 'kms:DescribeKey', 'kms:GenerateDataKey*', 'kms:Encrypt'],
      conditions: {
        'StringLike': { 'kms:ViaService': [`sqs.${this.region}.amazonaws.com`] }
      }
    }));
    role.addToPolicy(new aws_iam.PolicyStatement({
      resources: ['arn:aws:redshift:*:*:dbname:*/*', `arn:aws:redshift:*:*:cluster:${env_name}`],
      actions: ['redshift:GetClusterCredentials', 'redshift:DescribeClusters']
    }));

    // S3 Bucket
    const bucket = new aws_s3.Bucket(this, 'MyBucket', {
      bucketName: env_name,
      versioned: true,
      blockPublicAccess: aws_s3.BlockPublicAccess.BLOCK_ALL
    });

    // Add requirements.txt
    const deployment = new aws_s3_deployment.BucketDeployment(this, 'DeployWebsite', {
      sources: [aws_s3_deployment.Source.asset('./bucket')],
      destinationBucket: bucket
    });

    // Grant full access to role
    bucket.grantReadWrite(role)
    role.addToPolicy(new aws_iam.PolicyStatement({
      resources: [bucket.bucketArn, `${bucket.bucketArn}/*`],
      actions: ['s3:GetBucketPublicAccessBlock']
    }));

    // Redshift Cluster
    const vpc = new aws_ec2.Vpc(this, 'Vpc');
    const default_sg = aws_ec2.SecurityGroup.fromSecurityGroupId(this, 'DefaultSG', vpc.vpcDefaultSecurityGroup);
    const redshift = new aws_redshift.Cluster(this, 'Redshift', {
      masterUser: {
        masterUsername: 'admin',
      },
      clusterName: env_name,
      roles: [role],
      vpc,
      clusterType: aws_redshift.ClusterType.SINGLE_NODE,
      nodeType: aws_redshift.NodeType.DC2_LARGE,
      securityGroups: [default_sg]
    });

    // Airflow
    const airflow = new aws_mwaa.CfnEnvironment(this, 'MyCfnEnvironment', {
      name: env_name,
      airflowVersion: '2.2.2',
      executionRoleArn: role.roleArn,
      networkConfiguration: {
        securityGroupIds: [vpc.vpcDefaultSecurityGroup],
        subnetIds: [vpc.privateSubnets[0].subnetId, vpc.privateSubnets[1].subnetId],
      },
      sourceBucketArn: deployment.deployedBucket.bucketArn,
      dagS3Path: 'dags',
      requirementsS3Path: 'requirements.txt',
      environmentClass: 'mw1.small'
    });
  }
}
