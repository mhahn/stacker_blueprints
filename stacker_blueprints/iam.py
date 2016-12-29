from stacker.blueprints.base import Blueprint
from stacker.blueprints.variables.types import TroposphereType

from troposphere import (
    iam,
    GetAtt,
    Output,
)



class Users(Blueprint):

    VARIABLES = {
        "Users": {
            "type": TroposphereType(iam.User, many=True),
            "description": "IAM Users",
        },
    }

    def create_template(self):
        t = self.template
        variables = self.get_variables()
        for user in variables["Users"]:
            t.add_resource(user)
            t.add_output(Output("{}Arn".format(user.title), Value=GetAtt(user, "Arn")))
