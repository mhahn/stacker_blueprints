from awacs.helpers.trust import get_default_assumerole_policy
from stacker.blueprints.base import Blueprint
from troposphere import (
    autoscaling,
    cloudformation as cf,
    ec2,
    elasticloadbalancing as elb,
    logs,
    sqs,
    Base64,
    Equals,
    FindInMap,
    GetAtt,
    If,
    Join,
    Not,
    Ref,
)
from troposphere.autoscaling import (
    Metadata,
    Tag as ASTag,
)
from troposphere.iam import (
    InstanceProfile,
    Policy,
    Role,
)
from troposphere.policies import (
    AutoScalingRollingUpdate,
    AutoScalingScheduledAction,
    UpdatePolicy,
)
from troposphere.route53 import RecordSetType

from ..constants import ALLOWED_INSTANCE_TYPES
from .policies import get_conveyor_policy

CLUSTER_SG_NAME = 'ConveyorSecurityGroup'
CONVEYOR_INSTANCE_PROFILE = 'ConveyorProfile'
ELB_SG_NAME = 'ConveyorELBSecurityGroup'
LAUNCH_CONFIGURATION = 'ConveyorLaunchConfiguration'
LOAD_BALANCER = 'LoadBalancer'
LOG_GROUP = 'LogGroup'
QUEUE = 'Queue'


class Conveyor(Blueprint):

    LOCAL_PARAMETERS = {
        'GitHubToken': {
            'type': str,
            'description': 'GitHub API token to use when creating commit statuses.',
        },
        'SshIdRsa': {
            'type': str,
            'description': 'SSH id_rsa for the bot github user.',
        },
        'SshIdRsaPub': {
            'type': str,
            'description': 'SSH id_rsa.pub for the bot github user.',
        },
        'GitHubSecret': {
            'type': str,
            'description': 'The shared secret that GitHub uses to sign webhook payloads.',
            'default': '',
        },
        'SlackToken': {
            'type': str,
            'description': 'Secret shared with Slack to verify slash command webhooks.',
            'default': '',
        },
        'DataDogApiKey': {
            'type': str,
            'description': 'If provided, metrics will be collected and sent to datadog.',
            'default': '',
        },
        'DockerConfig': {
            'type': str,
            'description': 'Contents of a .docker/config.json file',
            'default': '',
        },
    }

    PARAMETERS = {
        'VpcId': {
            'type': 'AWS::EC2::VPC::Id',
            'description': 'ID of the VPC to launch Conveyor in.',
        },
        'DefaultSG': {
            'type': 'AWS::EC2::SecurityGroup::Id',
            'description': 'Top level security group.',
        },
        'PrivateSubnets': {
            'type': 'List<AWS::EC2::Subnet::Id>',
            'description': 'Subnets to deploy private instances in.',
        },
        'PublicSubnets': {
            'type': 'List<AWS::EC2::Subnet::Id>',
            'description': 'Subnets to deploy public (elb) instances in.',
        },
        'AvailabilityZones': {
            'type': 'List<AWS::EC2::AvailabilityZone::Name>',
            'description': 'Comma delimited list of availability zones. MAX 2',
        },
        'SshKeyName': {
            'type': 'AWS::EC2::KeyPair::KeyName',
            'description': 'The name of the key pair to use to allow SSH access.',
        },
        'TrustedNetwork': {
            'type': 'String',
            'description': 'CIDR block allowed to connect to Conveyor ELB.'
        },
        'SSLCertificateName': {
            'type': 'String',
            'description': (
                'The name of the SSL certificate to attach to the load'
                ' balancer.  Note: If this is set, non-HTTPS access is disabled.'
            ),
            'default': '',
        },
        'ExternalDomain': {
            'type': 'String',
            'description': 'Base domain for the stack.',
            'default': '',
        },
        'Subdomain': {
            'type': 'String',
            'description': (
                'The subdomain you want to make conveyor available on.'
                ' NOTE: This only has an effect if "ExternalDomain" is set.'
            ),
            'default': 'conveyor',
        },
        'Version': {
            'type': 'String',
            'description': 'Version of Conveyor to run.',
            'default': 'master',
        },
        'BuilderImage': {
            'type': 'String',
            'description': 'Docker image to use to perform the build.',
            'default': 'remind101/conveyor-builder',
        },
        'DryRun': {
            'type': 'Number',
            'description': 'Set to 1 to enable dry run mode.',
            'default': '0',
            'allowed_values': ['0', '1'],
        },
        'Reporter': {
            'type': 'String',
            'description': (
                'A Reporter to use to report errors. Default is to'
                ' write errors to stderr.'
            ),
            'default': '',
        },
        'InstanceType': {
            'type': 'String',
            'description': 'EC2 instance type to use for conveyor. Defaults to "t2.small"',
            'default': 't2.small',
            'allowed_values': ALLOWED_INSTANCE_TYPES,
        },
        'EbsOptimized': {
            'type': 'String',
            'description': (
                'Boolean to determine whether or not the instance should be EBS optimized'
            ),
            'default': 'false',
            'allowed_values': ['true', 'false'],
        },
        'ImageName': {
            'type': 'String',
            'description': (
                'The image name to use from the AMIMap (usually found in the'
                ' config file).'
            ),
            'default': 'conveyor',
        },
        'MinCapacity': {
            'type': 'Number',
            'description': 'Minimum number of EC2 instanes in the auto scaling group',
            'default': '1',
        },
        'MaxCapacity': {
            'type': 'Number',
            'description': 'Maximum number of EC2 instances in the auto scaling group',
            'default': '5',
        },
        'DesiredCapacity': {
            'type': 'Number',
            'description': 'Desired number of EC2 instances in the auto scaling group',
            'default': '3',
        },
        'LogGroupRetention': {
            'type': 'Number',
            'description': 'Number of days to retain the logs',
            'default': '7',
        },
    }

    def _setup_listeners(self):
        cert_parts = [
            'arn:aws:iam::',
            Ref('AWS::AccountId'),
            ':server-certificate/',
            Ref('SSLCertificateName'),
        ]
        cert_id = Join('', cert_parts)
        return [elb.Listener(
            LoadBalancerPort=If('NoSSL', '80', '443'),
            InstancePort=8080,
            Protocol=If('NoSSL', 'TCP', 'SSL'),
            InstanceProtocol='TCP',
            SSLCertificateId=If('NoSSL', Ref('AWS::NoValue'), cert_id),
        )]

    def _get_base_url(self):
        parts = [
            If('NoSSL', 'http', 'https'),
            '://',
            Ref('Subdomain'),
            '.',
            Ref('ExternalDomain'),
        ]
        return Join('', parts)

    def _get_conveyor_env_content(self):
        content = [
            'GITHUB_TOKEN=', self.local_parameters['GitHubToken'], '\n',
            'GITHUB_SECRET=', self.local_parameters['GitHubSecret'], '\n',
            'SLACK_TOKEN=', self.local_parameters['SlackToken'], '\n',
            'BASE_URL=', self._get_base_url(), '\n',
            'SQS_QUEUE_URL=', Ref(QUEUE), '\n',
            'QUEUE=sqs://\n',
            'LOGGER=', Join('', ['cloudwatch://', Ref(LOG_GROUP)]), '\n',
            'AWS_REGION=', Ref('AWS::Region'), '\n',
            'DRY=', Ref('DryRun'), '\n',
            'REPORTER=', Ref('Reporter'), '\n',
        ]
        return content

    def _get_datadog_conf_content(self):
        content = [
            '[Main]\n\n',
            'dd_url: https://app.datadoghq.com\n',
            'api_key: ', self.local_parameters['DataDogApiKey'], '\n',
            'tags: "role:conveyor"\n',
            'non_local_traffic: yes\n',
        ]
        return content

    def _generate_user_data(self):
        content = [
            '#!/bin/bash\n',
            # NB: cfn-init can use the logical id instead of the physical id
            'cfn-init -s ', Ref('AWS::StackName'), ' -r ', LAUNCH_CONFIGURATION,
            ' --region ', Ref('AWS::Region'), '\n',
            'docker pull ', Ref('BuilderImage'), '\n',
            'docker create --name data -v /var/run/conveyor:/var/run/conveyor:ro ubuntu:14.04\n',
            '/etc/init.d/datadog-agent start\n',
            'echo ', Ref('Version'),
        ]
        return Base64(Join('', content))

    def _get_cloudformation_init(self):
        return cf.Init({
            'config': cf.InitConfig(
                files=cf.InitFiles({
                    '/etc/env/conveyor.env': cf.InitFile(
                        content=Join('', self._get_conveyor_env_content()),
                        mode='000644',
                        owner='root',
                        group='root',
                    ),
                    '/etc/conveyor/version': cf.InitFile(
                        content=Ref('Version'),
                        mode='000644',
                        owner='root',
                        group='root',
                    ),
                    '/etc/dd-agent/datadog.conf': cf.InitFile(
                        content=Join('', self._get_datadog_conf_content()),
                        mode='000644',
                        owner='root',
                        group='root',
                    ),
                    '/home/ubuntu/.docker/config.json': cf.InitFile(
                        content=self.local_parameters['DockerConfig'],
                        mode='000600',
                        owner='ubuntu',
                        group='ubuntu',
                    ),
                    '/root/.docker/config.json': cf.InitFile(
                        content=self.local_parameters['DockerConfig'],
                        mode='000600',
                        owner='root',
                        group='root',
                    ),
                    '/var/run/conveyor/.docker/config.json': cf.InitFile(
                        content=self.local_parameters['DockerConfig'],
                        mode='000600',
                        owner='root',
                        group='root',
                    ),
                    '/var/run/conveyor/.ssh/id_rsa': cf.InitFile(
                        content=self.local_parameters['SshIdRsa'],
                        mode='000600',
                        owner='root',
                        group='root',
                    ),
                    '/var/run/conveyor/.ssh/id_rsa.pub': cf.InitFile(
                        content=self.local_parameters['SshIdRsaPub'],
                        mode='000600',
                        owner='root',
                        group='root',
                    ),
                }),
            ),
        })

    def create_conditions(self):
        t = self.template
        t.add_condition(
            'NoSSL',
            Equals(Ref('SSLCertificateName'), ''),
        )
        t.add_condition(
            'UseDNS',
            Not(Equals(Ref('ExternalDomain'), '')),
        )

    def create_security_groups(self):
        t = self.template

        t.add_resource(
            ec2.SecurityGroup(
                CLUSTER_SG_NAME,
                GroupDescription=CLUSTER_SG_NAME,
                VpcId=Ref('VpcId'),
            ),
        )
        t.add_resource(
            ec2.SecurityGroup(
                ELB_SG_NAME,
                GroupDescription=ELB_SG_NAME,
                VpcId=Ref('VpcId'),
            ),
        )
        t.add_resource(
            ec2.SecurityGroupIngress(
                'ConveyorELBFromTrusted',
                IpProtocol='tcp',
                FromPort=If('NoSSL', '80', '443'),
                ToPort=If('NoSSL', '80', '443'),
                CidrIp=Ref('TrustedNetwork'),
                GroupId=Ref(ELB_SG_NAME),
            ),
        )
        t.add_resource(
            ec2.SecurityGroupIngress(
                'ConveyorELBToConveyorCluster',
                IpProtocol='tcp',
                FromPort='8080',
                ToPort='8080',
                SourceSecurityGroupId=Ref(ELB_SG_NAME),
                GroupId=Ref(CLUSTER_SG_NAME),
            ),
        )

    def create_load_balancer(self):
        t = self.template
        t.add_resource(
            elb.LoadBalancer(
                LOAD_BALANCER,
                HealthCheck=elb.HealthCheck(
                    Target='TCP:8080',
                    HealthyThreshold=3,
                    UnhealthyThreshold=3,
                    Interval=30,
                    Timeout=5,
                ),
                Listeners=self._setup_listeners(),
                SecurityGroups=[Ref(ELB_SG_NAME)],
                CrossZone='true',
                Subnets=Ref('PublicSubnets'),
            )
        )

        # Setup ELB DNS
        t.add_resource(
            RecordSetType(
                'ConveyorElbDnsRecord',
                Condition='UseDNS',
                HostedZoneName=Join('', [Ref('ExternalDomain'), '.']),
                Comment='Conveyor ELB DNS',
                Name=Join('.', [Ref('Subdomain'), Ref('ExternalDomain')]),
                Type='CNAME',
                TTL='120',
                ResourceRecords=[GetAtt(LOAD_BALANCER, 'DNSName')],
            ),
        )

    def create_log_group(self):
        t = self.template
        t.add_resource(logs.LogGroup(LOG_GROUP, RetentionInDays=Ref('LogGroupRetention')))

    def create_iam_profile(self):
        t = self.template
        t.add_resource(
            Role(
                'ConveyorRole',
                AssumeRolePolicyDocument=get_default_assumerole_policy(),
                Path='/',
                Policies=[
                    Policy(
                        PolicyName='ConveyorPolicy',
                        PolicyDocument=get_conveyor_policy(Ref(LOG_GROUP)),
                    ),
                ],
            ),
        )
        t.add_resource(
            InstanceProfile(
                CONVEYOR_INSTANCE_PROFILE,
                Path='/',
                Roles=[Ref('ConveyorRole')],
            ),
        )

    def create_queue(self):
        t = self.template
        t.add_resource(sqs.Queue(QUEUE))

    def create_autoscaling_group(self):
        t = self.template
        t.add_resource(
            autoscaling.LaunchConfiguration(
                LAUNCH_CONFIGURATION,
                IamInstanceProfile=Ref(CONVEYOR_INSTANCE_PROFILE),
                ImageId=FindInMap('AmiMap', Ref('AWS::Region'), Ref('ImageName')),
                InstanceType=Ref('InstanceType'),
                KeyName=Ref('SshKeyName'),
                UserData=self._generate_user_data(),
                SecurityGroups=[Ref('DefaultSG'), Ref(CLUSTER_SG_NAME)],
                EbsOptimized=Ref('EbsOptimized'),
                Metadata=Metadata(self._get_cloudformation_init()),
            ),
        )
        t.add_resource(
            autoscaling.AutoScalingGroup(
                'ConveyorAutoscalingGroup',
                AvailabilityZones=Ref('AvailabilityZones'),
                LaunchConfigurationName=Ref(LAUNCH_CONFIGURATION),
                MinSize=Ref('MinCapacity'),
                MaxSize=Ref('MaxCapacity'),
                DesiredCapacity=Ref('DesiredCapacity'),
                VPCZoneIdentifier=Ref('PrivateSubnets'),
                LoadBalancerNames=[Ref(LOAD_BALANCER)],
                Tags=[ASTag('Name', 'conveyor', True)],
                UpdatePolicy=UpdatePolicy(
                    AutoScalingScheduledAction=AutoScalingScheduledAction(
                        IgnoreUnmodifiedGroupSizeProperties='true',
                    ),
                    AutoScalingRollingUpdate=AutoScalingRollingUpdate(
                        MinInstancesInService='1',
                        MaxBatchSize='2',
                        WaitOnResourceSignals='false',
                        PauseTime='PT5M',
                    ),
                ),
            ),
        )

    def create_template(self):
        self.create_conditions()
        self.create_security_groups()
        self.create_load_balancer()
        self.create_log_group()
        self.create_iam_profile()
        self.create_queue()
        self.create_autoscaling_group()
