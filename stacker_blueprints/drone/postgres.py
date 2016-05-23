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
    'default': '5432',
}
PARAMETERS['DatabaseName'] = {
    'type': 'String',
    'description': 'The name of the database to connect to',
}


class Drone(Base):

    LOCAL_PARAMETERS = {
        'CreateDatabase': {
            'type': bool,
            'default': False,
            'description': (
                'Boolean for whether or not we should configure the instance to'
                ' create the database if it doesn\'t exist. Drone doesn\'t create'
                ' the database for you. This option is useful if you\'re reusing'
                ' another database.'
            ),
        },
    }

    PARAMETERS = PARAMETERS

    def _get_database_url(self, with_table=False):
        content = [
            Ref('DatabaseConfigPrefix'),
            Ref('DatabaseUser'), ':', Ref('DatabasePassword'),
            '@', Ref('DatabaseHost'), ':', Ref('DatabasePort')
        ]
        if with_table:
            content.extend(['/', Ref('DatabaseName')])
        return Join('', content)

    def _generate_create_statement(self):
        statement = [
            ' -tc \\"SELECT 1 FROM pg_database WHERE datname = \'', Ref('DatabaseName'),
            '\'\\" | grep -q 1 || psql ', self._get_database_url(),
            ' -c \\"CREATE DATABASE ', Ref('DatabaseName'), '\\"',
        ]
        return Join('', statement)

    def generate_user_data_content(self):
        content = super(Drone, self).generate_user_data_content()
        if self.local_parameters['CreateDatabase']:
            content.extend([
                'docker run --rm postgres:9.4 bash -c "psql ', self._get_database_url(),
                self._generate_create_statement(), '"', '\n',
            ])
        return content

    def get_dronerc_content(self):
        content = super(Drone, self).get_dronerc_content()
        database_config = ['DATABASE_CONFIG=', self._get_database_url(with_table=True)]
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
