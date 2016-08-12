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


class CustomEmpireAppEnvironment(AWSCustomObject):
    resource_type = "Custom::EmpireAppEnvironment"

    props = {
        "ServiceToken": (basestring, True),
        "AppId": (basestring, True),
        "Variables": (list, True),
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


class AppEnvironment(Blueprint):

    BLUEPRINT_PARAMETERS = {
        "ServiceToken": {
            "type": str,
            "description": (
                "An SNS Topic to fulfill the custom resource request."
            ),
        },
        "AppId": {
            "type": str,
            "description": (
                "Id of an Empire App to set the environmental variables in."
            ),
        },
        "Variables": {
            "type": dict,
            "description": (
                "Dict of key:value environment variables to set within the app."
            ),
        },
    }

    def _build_variables(self):
        params = self.get_parameters()
        variables = []
        for key, value in params["Variables"].iteritems():
            variables.append({"Name": key, "Value": value})
        return variables

    def create_template(self):
        t = self.template
        params = self.get_parameters()
        app = CustomEmpireAppEnvironment(
            "EmpireAppEnvironment",
            ServiceToken=params["ServiceToken"],
            AppId=params["AppId"],
            Variables=self._build_variables(),
        )
        t.add_resource(app)
