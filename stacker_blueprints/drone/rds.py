from copy import deepcopy

from troposphere import (
    ec2,
    If,
    Join,
    Ref,
)

from . import (
    CLUSTER_SG_NAME,
    NO_DATABASE_CONFIG,
    Drone as Base,
)

PARAMETERS = deepcopy(Base.PARAMETERS)
PARAMETERS['DatabaseConfigPrefix'] = {
    'type': 'String',
    'description': 'Prefix for the database config',
    'default': 'postgres://',
}
PARAMETERS['DatabaseSecurityGroup'] = {
    'type': 'AWS::EC2::SecurityGroup::Id',
    'description': 'Security group for the Drone database',
}
PARAMETERS['DatabaseHost'] = {
    'type': 'String',
    'description': 'Hostname for the Drone database',
}
PARAMETERS['DatabaseUser'] = {
    'type': 'String',
    'description': 'User for the Drone database',
}
PARAMETERS['DatabasePassword'] = {
    'type': 'String',
    'description': 'Password for the Drone database',
}
PARAMETERS['DatabasePort'] = {
    'type': 'String',
    'description': 'Port for the database',
    'default': '5342',
}
PARAMETERS['DatabaseTable'] = {
    'type': 'String',
    'description': 'Database table',
}


class Drone(Base):

    PARAMETERS = PARAMETERS

    def get_dronerc_content(self):
        content = super(Drone, self).get_dronerc_content()
        database_config = [
            'DATABASE_CONFIG=',
            Ref('DatabaseConfigPrefix'),
            Ref('DatabaseUser'), ':', Ref('DatabasePassword'),
            '@', Ref('DatabaseHost'), ':', Ref('DatabasePort'), '/', Ref('DatabaseTable'), '\n',
        ]
        content.append(If(NO_DATABASE_CONFIG, Join('', database_config), ''))
        return content

    def create_template(self):
        super(Drone, self).create_template()
        t = self.template
        t.add_resource(
            ec2.SecurityGroupIngress(
                'DroneDBAccess',
                IpProtocol='tcp',
                FromPort=5432,
                ToPort=5432,
                SourceSecurityGroupId=Ref(CLUSTER_SG_NAME),
                GroupId=Ref('DatabaseSecurityGroup'),
            ),
        )
