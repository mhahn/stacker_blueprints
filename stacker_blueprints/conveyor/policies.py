from awacs import (
    logs,
    sqs,
)
from awacs.aws import (
    Allow,
    Policy,
    Statement,
)
from troposphere import Join


def get_conveyor_policy(log_group):
    return Policy(
        Statement=[
            Statement(
                Effect=Allow,
                Action=[
                    sqs.SendMessage,
                    sqs.ReceiveMessage,
                    sqs.DeleteMessage,
                ],
                Resource=['*'],
            ),
            Statement(
                Effect=Allow,
                Action=[
                    logs.CreateLogStream,
                    logs.PutLogEvents,
                    logs.GetLogEvents,
                ],
                Resource=[Join('', ['arn:aws:logs:*:*:log-group:', log_group, ':log-stream:*'])],
            ),
        ],
    )
