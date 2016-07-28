from stacker.blueprints.base import Blueprint
from troposphere import (
    Output,
    Ref,
)
from troposphere.cloudformation import AWSCustomObject


class CustomEmpireApp(AWSCustomObject):
    resource_type = "Custom::EmpireApp"

    props = {
        "ServiceToken": (basestring, True),
        "Name": (basestring, True),
    }


class App(Blueprint):

    PARAMETERS = {
        "ServiceToken": {
            "type": "String",
            "description": (
                "An SNS Topic to fulfill the custom resource request."
            ),
        },
        "Name": {
            "type": "String",
            "description": "The name of the app.",
        },
    }

    def create_template(self):
        t = self.template
        app = CustomEmpireApp(
            "EmpireApp",
            ServiceToken=Ref("ServiceToken"),
            Name=Ref("Name"),
        )
        t.add_resource(app)
        t.add_output(Output("AppId", Value=Ref(app)))
