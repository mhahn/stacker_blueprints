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

    VARIABLES = {
        "ServiceToken": {
            "type": str,
            "description": (
                "An SNS Topic to fulfill the custom resource request."
            ),
        },
        "Name": {
            "type": str,
            "description": "The name of the app.",
        },
    }

    def create_template(self):
        t = self.template
        variables = self.get_variables()
        app = CustomEmpireApp(
            "EmpireApp",
            ServiceToken=variables["ServiceToken"],
            Name=variables["Name"],
        )
        t.add_resource(app)
        t.add_output(Output("AppId", Value=Ref(app)))


class AppEnvironment(Blueprint):

    VARIABLES = {
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
        variables = self.get_variables()
        v = []
        for key, value in variables["Variables"].iteritems():
            v.append({"Name": key, "Value": value})
        return v

    def create_template(self):
        t = self.template
        variables = self.get_variables()
        app = CustomEmpireAppEnvironment(
            "EmpireAppEnvironment",
            ServiceToken=variables["ServiceToken"],
            AppId=variables["AppId"],
            Variables=self._build_variables(),
        )
        t.add_resource(app)
