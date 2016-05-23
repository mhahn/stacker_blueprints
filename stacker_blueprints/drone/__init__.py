from awacs.helpers.trust import get_default_assumerole_policy
from stacker.blueprints.base import Blueprint
from troposphere import (
    autoscaling,
    cloudformation as cf,
    ec2,
    elasticloadbalancing as elb,
    Base64,
    Equals,
    FindInMap,
    GetAtt,
    If,
    Join,
    Not,
    Or,
    Output,
    Ref,
)
from troposphere.autoscaling import (
    Metadata,
    Tag as ASTag,
)
from troposphere.iam import (
    InstanceProfile,
    Role,
)
from troposphere.policies import (
    AutoScalingRollingUpdate,
    AutoScalingScheduledAction,
    UpdatePolicy,
)
from troposphere.route53 import RecordSetType

from ..constants import ALLOWED_INSTANCE_TYPES

CLUSTER_SG_NAME = 'DroneSecurityGroup'
DRONE_INSTANCE_PROFILE = 'DroneProfile'
ELB_SG_NAME = 'DroneELBSecurityGroup'
LAUNCH_CONFIGURATION = 'DroneLaunchConfiguration'
LOAD_BALANCER = 'LoadBalancer'

NO_DATABASE_CONFIG = 'NoDatabaseConfig'


class Drone(Blueprint):

    PARAMETERS = {
        'VpcId': {
            'type': 'AWS::EC2::VPC::Id',
            'description': 'ID of the VPC to launch Drone in.',
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
                'The subdomain you want to make drone available on.'
                ' NOTE: This only has an effect if "ExternalDomain" is set.'
            ),
            'default': 'drone',
        },
        'Version': {
            'type': 'String',
            'description': 'Version of Drone to run.',
            'default': '0.4',
        },
        'InstanceType': {
            'type': 'String',
            'description': 'EC2 instance type to use for drone. Defaults to "t2.small"',
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
            'default': 'drone',
        },
        'MinCapacity': {
            'type': 'Number',
            'description': 'Minimum number of EC2 instanes in the auto scaling group',
            'default': '1',
        },
        'MaxCapacity': {
            'type': 'Number',
            'description': 'Maximum number of EC2 instances in the auto scaling group',
            'default': '2',
        },
        'DesiredCapacity': {
            'type': 'Number',
            'description': 'Desired number of EC2 instances in the auto scaling group',
            'default': '1',
        },
        'DataDogApiKey': {
            'type': 'String',
            'description': 'If provided, metrics will be collected and sent to datadog.',
            'default': '',
        },
        'RemoteDriver': {
            'type': 'String',
            'description': (
                'Drone will use your remote for authentication, and will add '
                'webhooks to your projects to facilitate the build process. See '
                'http://readme.drone.io/setup/remotes/ for possible values.'
            ),
        },
        'RemoteConfig': {
            'type': 'String',
            'description': 'Settings for the specified RemoteDriver',
        },
        'DatabaseDriver': {
            'type': 'String',
            'default': '',
            'allowed_values': ['', 'sqlite3', 'postgres', 'mysql'],
            'description': (
                'Database driver to use, see '
                'http://readme.drone.io/setup/database/ for reference.'
            ),
        },
        'DatabaseConfig': {
            'type': 'String',
            'default': '',
            'description': 'Settings for the specified DatabaseDriver',
        },
        'PluginFilter': {
            'type': 'String',
            'default': 'plugins/*',
            'description': (
                'This setting contains a space-separated list of patterns for '
                'determining which plugins can be pulled and ran. By disallowing '
                'open season on plugins, we prevent some classes of malicious '
                'plugins sneaking into builds.'
            ),
        },
        'PluginParams': {
            'type': 'String',
            'default': '',
            'description': (
                'This setting may be used to define some global parameters to '
                ' pass into all plugins on all repositories. Be careful what you '
                ' put in here, since it will be trivial for a malicious developer '
                ' to obtain the values found in this setting.'
            ),
        },
        'Debug': {
            'type': 'String',
            'default': 'false',
            'allowed_values': ['true', 'false'],
            'description': 'Whether or not the drone server should be run in debug mode',
        },
    }

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
        t.add_condition(
            'NoDatabaseDriver',
            Equals(Ref('DatabaseDriver'), ''),
        )
        t.add_condition(
            NO_DATABASE_CONFIG,
            Equals(Ref('DatabaseConfig'), ''),
        )
        t.add_condition(
            'NoPluginFilter',
            Equals(Ref('PluginFilter'), ''),
        )
        t.add_condition(
            'NoPluginParams',
            Equals(Ref('PluginParams'), ''),
        )
        t.add_condition(
            'NoDebug',
            Or(Equals(Ref('Debug'), ''), Equals(Ref('Debug'), 'false')),
        )

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
            InstancePort=8000,
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

    def get_dronerc_content(self):
        content = [
            'REMOTE_DRIVER=', Ref('RemoteDriver'), '\n',
            'REMOTE_CONFIG=', Ref('RemoteConfig'), '\n',
            If('NoDatabaseDriver', '', Join('', ['DATABASE_DRIVER=', Ref('DatabaseDriver'), '\n'])),
            If('NoDatabaseConfig', '', Join('', ['DATABASE_CONFIG=', Ref('DatabaseConfig'), '\n'])),
            If('NoPluginFilter', '', Join('', ['PLUGIN_FILTER=', Ref('PluginFilter'), '\n'])),
            If('NoDebug', '', Join('', ['DEBUG=', Ref('Debug'), '\n'])),
        ]
        return content

    def _get_datadog_conf_content(self):
        content = [
            '[Main]\n\n',
            'dd_url: https://app.datadoghq.com\n',
            'api_key: ', Ref('DataDogApiKey'), '\n',
            'tags: "role:conveyor"\n',
            'non_local_traffic: yes\n',
        ]
        return content

    def generate_user_data_content(self):
        content = [
            '#!/bin/bash', '\n',
            # NB: cfn-init can use the logical id instead of the physical id
            'cfn-init -s ', Ref('AWS::StackName'), ' -r ', LAUNCH_CONFIGURATION,
            ' --region ', Ref('AWS::Region'), '\n',
            'docker create --name data -v /var/lib/drone:/var/lib/drone:ro ubuntu:14.04', '\n',
            '/etc/init.d/datadog-agent start', '\n',
        ]
        return content

    def generate_user_data(self):
        content = self.generate_user_data_content()
        content.extend(['echo version: ', Ref('Version'), '\n'])
        return Base64(Join('', content))

    def _get_cloudformation_init(self):
        return cf.Init({
            'config': cf.InitConfig(
                files=cf.InitFiles({
                    '/etc/drone/dronerc': cf.InitFile(
                        content=Join('', self.get_dronerc_content()),
                        mode='000644',
                        owner='root',
                        group='root',
                    ),
                    '/etc/drone/version': cf.InitFile(
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
                }),
            ),
        })

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
                'DroneELBFromTrusted',
                IpProtocol='tcp',
                FromPort=If('NoSSL', '80', '443'),
                ToPort=If('NoSSL', '80', '443'),
                CidrIp=Ref('TrustedNetwork'),
                GroupId=Ref(ELB_SG_NAME),
            ),
        )
        t.add_resource(
            ec2.SecurityGroupIngress(
                'DroneELBToDroneCluster',
                IpProtocol='tcp',
                FromPort='8000',
                ToPort='8000',
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
                    Target='TCP:8000',
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
                'DroneElbDnsRecord',
                Condition='UseDNS',
                HostedZoneName=Join('', [Ref('ExternalDomain'), '.']),
                Comment='Drone ELB DNS',
                Name=Join('.', [Ref('Subdomain'), Ref('ExternalDomain')]),
                Type='CNAME',
                TTL='120',
                ResourceRecords=[GetAtt(LOAD_BALANCER, 'DNSName')],
            ),
        )

    def create_iam_profile(self):
        t = self.template
        t.add_resource(
            Role(
                'DroneRole',
                AssumeRolePolicyDocument=get_default_assumerole_policy(),
                Path='/',
            ),
        )
        t.add_resource(
            InstanceProfile(
                DRONE_INSTANCE_PROFILE,
                Path='/',
                Roles=[Ref('DroneRole')],
            ),
        )
        t.add_output(Output('IAMRole', Value=Ref('DroneRole')))

    def create_autoscaling_group(self):
        t = self.template
        t.add_resource(
            autoscaling.LaunchConfiguration(
                LAUNCH_CONFIGURATION,
                IamInstanceProfile=Ref(DRONE_INSTANCE_PROFILE),
                ImageId=FindInMap('AmiMap', Ref('AWS::Region'), Ref('ImageName')),
                InstanceType=Ref('InstanceType'),
                KeyName=Ref('SshKeyName'),
                UserData=self.generate_user_data(),
                SecurityGroups=[Ref('DefaultSG'), Ref(CLUSTER_SG_NAME)],
                EbsOptimized=Ref('EbsOptimized'),
                Metadata=Metadata(self._get_cloudformation_init()),
            ),
        )
        t.add_resource(
            autoscaling.AutoScalingGroup(
                'DroneAutoscalingGroup',
                AvailabilityZones=Ref('AvailabilityZones'),
                LaunchConfigurationName=Ref(LAUNCH_CONFIGURATION),
                MinSize=Ref('MinCapacity'),
                MaxSize=Ref('MaxCapacity'),
                DesiredCapacity=Ref('DesiredCapacity'),
                VPCZoneIdentifier=Ref('PrivateSubnets'),
                LoadBalancerNames=[Ref(LOAD_BALANCER)],
                Tags=[ASTag('Name', 'drone', True)],
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
        self.create_iam_profile()
        self.create_autoscaling_group()
