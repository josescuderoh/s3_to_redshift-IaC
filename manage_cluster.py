import pandas as pd
import boto3
import json
import time
import configparser


def read_config_file(filename):
    """
    Interact with config files - Read

    :params filename - the path to the file
    :type string
    """

    config = configparser.ConfigParser()
    config.read_file(open(filename))

    return config

def create_iam_role():
    """
    Create IAM role that provides access to S3 for Redshift
    """    

    # Read params
    input_filename = 'aws.cfg'
    config = read_config_file(input_filename) 

    KEY = config.get('AWS','KEY')
    SECRET = config.get('AWS','SECRET')
    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

    iam = boto3.client('iam',region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)

    try:
        print('Creating IAM role')
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description='Allows Redshift to call data from S3',
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'}
            )
        )

    except Exception as e:
        print(e)

    print('Attaching S3 Readonly access')
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess')['ResponseMetadata']['HTTPStatusCode']

    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    # Populate input file and rewrite
    config.set('DWH', 'DWH_IAM_ROLE_ARN', roleArn)

    with open(input_filename, 'w') as configfile:
        config.write(configfile)

    return 'Role has been created'


def create_cluster(open_traffic=True):
    """
    Create Redshift cluster using Role ARN previously created

    :params open_traffic whether TCP traffic should be enabled (default: True)
    :type boolean
    """

    # Read params
    input_filename = 'aws.cfg'
    config = read_config_file(input_filename)

    KEY = config.get('AWS','KEY')
    SECRET = config.get('AWS','SECRET')
    DWH_CLUSTER_TYPE = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE = config.get("DWH","DWH_NODE_TYPE")
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB = config.get("DWH","DWH_DB")
    DWH_DB_USER = config.get("DWH","DWH_DB_USER")
    DWH_PORT = config.get("DWH","DWH_PORT")
    DWH_DB_PASSWORD = config.get("DWH","DWH_DB_PASSWORD")
    DWH_IAM_ROLE_ARN = config.get('DWH', 'DWH_IAM_ROLE_ARN')

    redshift = boto3.client('redshift',region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)

    try:
        response = redshift.create_cluster(
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            IamRoles=[DWH_IAM_ROLE_ARN]
        )
    except Exception as e:
        print(e)


    while True:

        #Check if cluster was created successfully and retrieve endpoint
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

        if myClusterProps['ClusterStatus'] != 'available':
            print('Cluster is not available yet. Retrying in 30 seconds...')
            time.sleep(30)
        else:
            break

    endpoint = myClusterProps['Endpoint']['Address']

    # Populate output file

    output_filename = 'dwh.cfg'
    config = read_config_file(output_filename)

    config.set('CLUSTER', 'HOST', endpoint)
    config.set('CLUSTER', 'DB_NAME', DWH_DB)
    config.set('CLUSTER', 'DB_USER', DWH_DB_USER)
    config.set('CLUSTER', 'DB_PASSWORD', DWH_DB_PASSWORD)
    config.set('CLUSTER', 'DB_PORT', DWH_PORT)
    config.set('IAM_ROLE', 'ARN', DWH_IAM_ROLE_ARN)

    with open(output_filename, 'w') as configfile:
        config.write(configfile)

    if open_traffic:

        #Allow external TCP inbound traffic

        ec2 = boto3.resource('ec2',region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)

        try:
            vpc = ec2.Vpc(id=myClusterProps['VpcId'])
            defaultSg = list(vpc.security_groups.all())[0]

            defaultSg.authorize_ingress(
                GroupName=defaultSg.group_name,
                CidrIp='0.0.0.0/0',
                IpProtocol='TCP',
                FromPort=int(DWH_PORT),
                ToPort=int(DWH_PORT)
            )
        except Exception as e:
            print(e)
        
        return 'TCP inbound traffic enabled for cluster'
    else:
        return 'Cluster is now available'

def delete_resources():
    """
    Delete all existing resources that were created:
    - Redshift
    - IAM Role
    """

    # Read params
    input_filename = 'aws.cfg'
    config = read_config_file(input_filename)

    KEY = config.get('AWS','KEY')
    SECRET = config.get('AWS','SECRET')
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")    

    iam = boto3.client('iam',region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    redshift = boto3.client('redshift',region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)

    # Delete redshift
    redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)

    # Delete role
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)