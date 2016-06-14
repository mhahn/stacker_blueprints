"""Create an AWS Data Pipeline to export a DynamoDB table to S3.

The following blueprint will create an AWS Data Pipeline that will export data
from a DynamoDB table to an S3 bucket via an EMR cluster.

"""
from stacker.blueprints.base import Blueprint
from troposphere import (
    datapipeline,
    Ref,
)

DEFAULT = 'Default'
SCHEDULE = 'Schedule'
EMR_ACTIVITY = 'EmrActivity'
EMR_CLUSTER = 'EmrCluster'
S3_OUTPUT_LOCATION = 'S3Output'
SOURCE_TABLE = 'SourceTable'


class DynamoDBSnapshot(Blueprint):

    PARAMETERS = {
        'Activate': {
            'type': 'String',
            'allowed_values': ['true', 'false'],
            'description': 'Boolean for whether or not the pipeline should be activated',
        },
        'PipelineLogUri': {
            'type': 'String',
            'description': 'S3 URI to store pipeline logs',
        },
        'FailureAndRerunMode': {
            'type': 'String',
            'description': (
                'Configure how pipeline objects react when a dependency fails or'
                ' is cancelled by the user.'
            ),
            'default': 'cascade',
            'allowed_values': ['none', 'cascade'],
        },
        'ResourceRole': {
            'type': 'String',
            'description': 'The role assumed by resources the pipeline creates',
        },
        'Role': {
            'type': 'String',
            'description': 'The role assumed by the data pipeline to access AWS resources',
        },
        'SchedulePeriod': {
            'type': 'String',
            'description': 'How often the pipeline should run',
        },
        'ScheduleType': {
            'type': 'String',
            'description': 'The schedule type for the pipeline',
            'allowed_values': ['ondemand', 'cron', 'timeseries'],
        },
        'MaximumRetries': {
            'type': 'String',
            'description': 'The number of times to retry the backup',
            'default': '2',
        },
        'TableName': {
            'type': 'String',
            'description': 'The name of the DynamoDB table to create a snapshot of',
        },
        'ThroughputRatio': {
            'type': 'String',
            'description': (
                'The amount of provisioned throughput to consume when creating the snapshot'
            ),
            'default': '0.25',
        },
        'S3OutputLocation': {
            'type': 'String',
            'description': 'The S3 location to store the snapshot',
        },
        'CoreInstanceType': {
            'type': 'String',
            'description': 'The type of instance to use for core nodes',
            'default': 'm3.xlarge',
        },
        'CoreInstanceCount': {
            'type': 'String',
            'description': 'The number of core instances to run',
            'default': '1',
        },
        'MasterInstanceType': {
            'type': 'String',
            'description': 'The type of instance to use for the master node',
            'default': 'm3.xlarge',
        },
        'EmrClusterTerminateAfter': {
            'type': 'String',
            'description': 'How long to allow the EMR cluster to run before terminating it',
            'default': '30 Minutes',
        },
        'StartDateTime': {
            'type': 'String',
            'description': 'The date when the pipeline should be activated',
        },
    }

    def get_default_object(self):
        return datapipeline.PipelineObject(
            Id=DEFAULT,
            Name=DEFAULT,
            Fields=self.get_default_object_fields(),
        )

    def get_default_object_fields(self):
        return [
            datapipeline.ObjectField(
                Key='failureAndRerunMode',
                StringValue=Ref('FailureAndRerunMode'),
            ),
            datapipeline.ObjectField(
                Key='resourceRole',
                StringValue=Ref('ResourceRole'),
            ),
            datapipeline.ObjectField(
                Key='role',
                StringValue=Ref('Role'),
            ),
            datapipeline.ObjectField(
                Key='pipelineLogUri',
                StringValue=Ref('PipelineLogUri'),
            ),
            datapipeline.ObjectField(
                Key='scheduleType',
                StringValue=Ref('ScheduleType'),
            ),
            datapipeline.ObjectField(
                Key='type',
                StringValue='Default',
            ),
            datapipeline.ObjectField(
                Key='schedule',
                RefValue=SCHEDULE,
            ),
        ]

    def get_schedule_object(self):
        return datapipeline.PipelineObject(
            Id=SCHEDULE,
            Name=SCHEDULE,
            Fields=self.get_schedule_object_fields(),
        )

    def get_schedule_object_fields(self):
        return [
            datapipeline.ObjectField(
                Key='startDateTime',
                StringValue=Ref('StartDateTime'),
            ),
            datapipeline.ObjectField(
                Key='period',
                StringValue=Ref('SchedulePeriod'),
            ),
            datapipeline.ObjectField(
                Key='type',
                StringValue='Schedule',
            ),
        ]

    def get_emr_activity_object(self):
        return datapipeline.PipelineObject(
            Id=EMR_ACTIVITY,
            Name=EMR_ACTIVITY,
            Fields=self.get_emr_activity_object_fields(),
        )

    def get_emr_activity_object_fields(self):
        return [
            datapipeline.ObjectField(
                Key='runsOn',
                RefValue=EMR_CLUSTER,
            ),
            datapipeline.ObjectField(
                Key='type',
                StringValue='EmrActivity',
            ),
            datapipeline.ObjectField(
                Key='resizeClusterBeforeRunning',
                StringValue='true',
            ),
            datapipeline.ObjectField(
                Key='output',
                RefValue=S3_OUTPUT_LOCATION,
            ),
            datapipeline.ObjectField(
                Key='input',
                RefValue=SOURCE_TABLE,
            ),
            datapipeline.ObjectField(
                Key='maximumRetries',
                StringValue=Ref('MaximumRetries'),
            ),
            datapipeline.ObjectField(
                Key='step',
                StringValue=(
                    's3://dynamodb-emr-#{myDynamoDBRegion}/emr-ddb-storage-handler/2.1.0/'
                    'emr-ddb-2.1.0.jar,org.apache.hadoop.dynamodb.tools.DynamoDbExport,'
                    '#{output.directoryPath},#{input.tableName},#{input.readThroughputPercent}'
                ),
            )
        ]

    def get_dynamodb_data_node_object(self):
        return datapipeline.PipelineObject(
            Id=SOURCE_TABLE,
            Name=SOURCE_TABLE,
            Fields=self.get_dynamodb_data_node_object_fields(),
        )

    def get_dynamodb_data_node_object_fields(self):
        return [
            datapipeline.ObjectField(
                Key='readThroughputPercent',
                StringValue='#{myDynamoDBReadThroughputRatio}'
            ),
            datapipeline.ObjectField(
                Key='type',
                StringValue='DynamoDBDataNode',
            ),
            datapipeline.ObjectField(
                Key='tableName',
                StringValue='#{myDynamoDBTableName}',
            )
        ]

    def get_s3_output_object(self):
        return datapipeline.PipelineObject(
            Id=S3_OUTPUT_LOCATION,
            Name=S3_OUTPUT_LOCATION,
            Fields=self.get_s3_output_object_fields(),
        )

    def get_s3_output_object_fields(self):
        return [
            datapipeline.ObjectField(
                Key='directoryPath',
                StringValue=(
                    '#{myS3OutputLocation}/#{format(@scheduledStartTime,'
                    ' \'YYYY-MM-dd-HH-mm-ss\')}'
                ),
            ),
            datapipeline.ObjectField(
                Key='type',
                StringValue='S3DataNode',
            ),
        ]

    def get_emr_cluster_object(self):
        return datapipeline.PipelineObject(
            Id=EMR_CLUSTER,
            Name=EMR_CLUSTER,
            Fields=self.get_emr_cluster_object_fields(),
        )

    def get_emr_cluster_object_fields(self):
        return [
            datapipeline.ObjectField(
                Key='bootstrapAction',
                StringValue=(
                    's3://#{myDynamoDBRegion}.elasticmapreduce/bootstrap-actions/configure-hadoop,'
                    ' --yarn-key-value,yarn.nodemanager.resource.memory-mb=11520,'
                    '--yarn-key-value,yarn.scheduler.maximum-allocation-mb=11520,'
                    '--yarn-key-value,yarn.scheduler.minimum-allocation-mb=1440,'
                    '--yarn-key-value,yarn.app.mapreduce.am.resource.mb=2880,--mapred-key-value,'
                    'mapreduce.map.memory.mb=5760,--mapred-key-value,'
                    'mapreduce.map.java.opts=-Xmx4608M,--mapred-key-value,'
                    'mapreduce.reduce.memory.mb=2880,--mapred-key-value,'
                    'mapreduce.reduce.java.opts=-Xmx2304m,--mapred-key-value,'
                    'mapreduce.map.speculative=false'
                )
            ),
            datapipeline.ObjectField(
                Key='coreInstanceCount',
                StringValue=Ref('CoreInstanceCount'),
            ),
            datapipeline.ObjectField(
                Key='coreInstanceType',
                StringValue=Ref('CoreInstanceType'),
            ),
            datapipeline.ObjectField(
                Key='amiVersion',
                StringValue='3.8.0',
            ),
            datapipeline.ObjectField(
                Key='masterInstanceType',
                StringValue=Ref('MasterInstanceType'),
            ),
            datapipeline.ObjectField(
                Key='region',
                StringValue='#{myDynamoDBRegion}',
            ),
            datapipeline.ObjectField(
                Key='type',
                StringValue='EmrCluster',
            ),
            datapipeline.ObjectField(
                Key='terminateAfter',
                StringValue=Ref('EmrClusterTerminateAfter'),
            ),
        ]

    def get_parameter_objects(self):
        return [
            datapipeline.ParameterObject(
                Id='myS3OutputLocation',
                Attributes=[
                    datapipeline.ParameterObjectAttribute(
                        Key='description',
                        StringValue='Output S3 location',
                    ),
                    datapipeline.ParameterObjectAttribute(
                        Key='type',
                        StringValue='AWS::S3::ObjectKey',
                    ),
                ],
            ),
            datapipeline.ParameterObject(
                Id='myDynamoDBRegion',
                Attributes=[
                    datapipeline.ParameterObjectAttribute(
                        Key='description',
                        StringValue='Region containing the DynamoDB table',
                    ),
                    datapipeline.ParameterObjectAttribute(
                        Key='type',
                        StringValue='String',
                    ),
                ],
            ),
            datapipeline.ParameterObject(
                Id='myDynamoDBTableName',
                Attributes=[
                    datapipeline.ParameterObjectAttribute(
                        Key='description',
                        StringValue='Source DynamoDB table name',
                    ),
                    datapipeline.ParameterObjectAttribute(
                        Key='type',
                        StringValue='String',
                    ),
                ],
            ),
            datapipeline.ParameterObject(
                Id='myDynamoDBReadThroughputRatio',
                Attributes=[
                    datapipeline.ParameterObjectAttribute(
                        Key='description',
                        StringValue='DynamoDB read throughput ratio',
                    ),
                    datapipeline.ParameterObjectAttribute(
                        Key='type',
                        StringValue='Double',
                    ),
                ],
            ),
        ]

    def get_parameter_values(self):
        return [
            datapipeline.ParameterValue(
                Id='myDynamoDBRegion',
                StringValue=Ref('AWS::Region'),
            ),
            datapipeline.ParameterValue(
                Id='myDynamoDBTableName',
                StringValue=Ref('TableName'),
            ),
            datapipeline.ParameterValue(
                Id='myDynamoDBReadThroughputRatio',
                StringValue=Ref('ThroughputRatio'),
            ),
            datapipeline.ParameterValue(
                Id='myS3OutputLocation',
                StringValue=Ref('S3OutputLocation'),
            ),
        ]

    def get_pipeline_objects(self):
        return [
            self.get_default_object(),
            self.get_schedule_object(),
            self.get_emr_activity_object(),
            self.get_dynamodb_data_node_object(),
            self.get_s3_output_object(),
            self.get_emr_cluster_object(),
        ]

    def create_template(self):
        t = self.template
        t.add_resource(datapipeline.Pipeline(
            'DynamoDBSnapshot',
            Activate=Ref('Activate'),
            Name='DynamoDBSnapshot',
            ParameterObjects=self.get_parameter_objects(),
            ParameterValues=self.get_parameter_values(),
            PipelineObjects=self.get_pipeline_objects(),
        ))
