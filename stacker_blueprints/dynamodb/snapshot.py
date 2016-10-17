"""Create an AWS Data Pipeline to export a DynamoDB table to S3.

The following blueprint will create an AWS Data Pipeline that will export data
from a DynamoDB table to an S3 bucket via an EMR cluster.

"""
from stacker.blueprints.base import Blueprint
from troposphere import (
    datapipeline,
    Ref,
)

DEFAULT = "Default"
DEFAULT_THROUGHPUT_RATIO = "0.25"
SCHEDULE = "Schedule"
EMR_ACTIVITY = "EmrActivity"
EMR_CLUSTER = "EmrCluster"
S3_OUTPUT_LOCATION = "S3Output"
SOURCE_TABLE = "SourceTable"


def validate_snapshot_config(config):
    required_keys = ["TableName", "S3Output"]
    for key in required_keys:
        if key not in config:
            message = "Missing required key, '%s' from config: %s" % (
                key, config)
            raise ValueError(message)

    config.setdefault("ThroughputRatio", DEFAULT_THROUGHPUT_RATIO)
    return config


def validate_snaphsot_configs(value, **kwargs):
    validated_configs = []
    for config in value:
        validated = validate_snapshot_config(config)
        validated_configs.append(validated)
    return validated_configs


class Snapshot(Blueprint):

    VARIABLES = {
        "Activate": {
            "type": bool,
            "description": (
                "Boolean for whether or not the pipeline should be activated"
            )},
        "PipelineLogUri": {
            "type": str,
            "description": "S3 URI to store pipeline logs"},
        "FailureAndRerunMode": {
            "type": str,
            "description": (
                "Configure how pipeline objects react when a dependency fails "
                "or is cancelled by the user."
            ),
            "default": "cascade",
            "allowed_values": ["none", "cascade"]},
        "ResourceRole": {
            "type": str,
            "description": (
                "The role assumed by resources the pipeline creates"
            )},
        "Role": {
            "type": str,
            "description": (
                "The role assumed by the data pipeline to access AWS "
                "resources"
            )},
        "SchedulePeriod": {
            "type": str,
            "description": "How often the pipeline should run"},
        "ScheduleType": {
            "type": str,
            "description": "The schedule type for the pipeline",
            "allowed_values": ["ondemand", "cron", "timeseries"]},
        "MaximumRetries": {
            "type": str,
            "description": "The number of times to retry the backup",
            "default": "2"},
        "SnapshotConfigs": {
            "type": list,
            "description": (
                "A list of snapshot configs. Snapshot configs are "
                " dictionaires that contain the following keys: 'TableName', "
                "'S3Output', and 'ThroughputRatio'. These values are used to "
                "set up the DynamoDB tables the AWS Data Pipeline will backup "
                "along with the location within S3 where the backup should be "
                "stored."
            ),
            "validator": validate_snaphsot_configs},
        "CoreInstanceType": {
            "type": str,
            "description": "The type of instance to use for core nodes",
            "default": "m3.xlarge"},
        "CoreInstanceCount": {
            "type": str,
            "description": "The number of core instances to run",
            "default": "1"},
        "MasterInstanceType": {
            "type": str,
            "description": "The type of instance to use for the master node",
            "default": "m3.xlarge"},
        "EmrClusterTerminateAfter": {
            "type": str,
            "description": (
                "How long to allow the EMR cluster to run before "
                "terminating it"
            ),
            "default": "30 Minutes"},
        "StartDateTime": {
            "type": str,
            "description": "The date when the pipeline should be activated"},
    }

    def get_default_object(self):
        return datapipeline.PipelineObject(
            Id=DEFAULT,
            Name=DEFAULT,
            Fields=self.get_default_object_fields(),
        )

    def get_default_object_fields(self):
        variables = self.get_variables()
        return [
            datapipeline.ObjectField(
                Key="failureAndRerunMode",
                StringValue=variables["FailureAndRerunMode"],
            ),
            datapipeline.ObjectField(
                Key="resourceRole",
                StringValue=variables["ResourceRole"],
            ),
            datapipeline.ObjectField(
                Key="role",
                StringValue=variables["Role"],
            ),
            datapipeline.ObjectField(
                Key="pipelineLogUri",
                StringValue=variables["PipelineLogUri"],
            ),
            datapipeline.ObjectField(
                Key="scheduleType",
                StringValue=variables["ScheduleType"],
            ),
            datapipeline.ObjectField(
                Key="type",
                StringValue="Default",
            ),
            datapipeline.ObjectField(
                Key="schedule",
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
        variables = self.get_variables()
        return [
            datapipeline.ObjectField(
                Key="startDateTime",
                StringValue=variables["StartDateTime"],
            ),
            datapipeline.ObjectField(
                Key="period",
                StringValue=variables["SchedulePeriod"],
            ),
            datapipeline.ObjectField(
                Key="type",
                StringValue="Schedule",
            ),
        ]

    def create_pipeline_objects(self, config):
        variables = self.get_variables()

        data_node_id = config["TableName"]
        data_node = datapipeline.PipelineObject(
            Id=data_node_id,
            Name=data_node_id,
            Fields=[
                datapipeline.ObjectField(
                    Key="readThroughputPercent",
                    StringValue=config["ThroughputRatio"]),
                datapipeline.ObjectField(
                    Key="type",
                    StringValue="DynamoDBDataNode"),
                datapipeline.ObjectField(
                    Key="tableName",
                    StringValue=config["TableName"])])

        s3_output_id = "{}S3Output".format(config["TableName"])
        path = (
            "%s/#{format(@scheduledStartTime, "
            "\"YYYY-MM-dd-HH-mm-ss\")}"
        ) % config["S3Output"]
        s3_output = datapipeline.PipelineObject(
            Id=s3_output_id,
            Name=s3_output_id,
            Fields=[
                datapipeline.ObjectField(
                    Key="directoryPath",
                    StringValue=path),
                datapipeline.ObjectField(
                    Key="type",
                    StringValue="S3DataNode")])

        emr_step = (
            "s3://dynamodb-emr-#{myRegion}/emr-ddb-storage-handler/2.1.0/"
            "emr-ddb-2.1.0.jar,org.apache.hadoop.dynamodb.tools."
            "DynamoDbExport,"
            "#{output.directoryPath},#{input.tableName},"
            "#{input.readThroughputPercent}"
        )
        emr_activity_id = "{}EmrActivity".format(config["TableName"])
        emr_activity = datapipeline.PipelineObject(
            Id=emr_activity_id,
            Name=emr_activity_id,
            Fields=[
                datapipeline.ObjectField(
                    Key="runsOn",
                    RefValue=EMR_CLUSTER),
                datapipeline.ObjectField(
                    Key="type",
                    StringValue="EmrActivity"),
                datapipeline.ObjectField(
                    Key="resizeClusterBeforeRunning",
                    StringValue="true"),
                datapipeline.ObjectField(
                    Key="output",
                    RefValue=s3_output_id),
                datapipeline.ObjectField(
                    Key="input",
                    RefValue=data_node_id),
                datapipeline.ObjectField(
                    Key="maximumRetries",
                    StringValue=variables["MaximumRetries"]),
                datapipeline.ObjectField(
                    Key="step",
                    StringValue=emr_step)])
        return [data_node, s3_output, emr_activity]

    def get_emr_cluster_object(self):
        return datapipeline.PipelineObject(
            Id=EMR_CLUSTER,
            Name=EMR_CLUSTER,
            Fields=self.get_emr_cluster_object_fields(),
        )

    def get_emr_cluster_object_fields(self):
        variables = self.get_variables()
        bootstrap_action = (
            "s3://#{myRegion}.elasticmapreduce/bootstrap-actions/"
            "configure-hadoop,"
            " --yarn-key-value,yarn.nodemanager.resource.memory-mb=11520,"
            "--yarn-key-value,yarn.scheduler.maximum-allocation-mb=11520,"
            "--yarn-key-value,yarn.scheduler.minimum-allocation-mb=1440,"
            "--yarn-key-value,yarn.app.mapreduce.am.resource.mb=2880,"
            "--mapred-key-value,"
            "mapreduce.map.memory.mb=5760,--mapred-key-value,"
            "mapreduce.map.java.opts=-Xmx4608M,--mapred-key-value,"
            "mapreduce.reduce.memory.mb=2880,--mapred-key-value,"
            "mapreduce.reduce.java.opts=-Xmx2304m,--mapred-key-value,"
            "mapreduce.map.speculative=false"
        )
        return [
            datapipeline.ObjectField(
                Key="bootstrapAction",
                StringValue=bootstrap_action,
            ),
            datapipeline.ObjectField(
                Key="coreInstanceCount",
                StringValue=variables["CoreInstanceCount"],
            ),
            datapipeline.ObjectField(
                Key="coreInstanceType",
                StringValue=variables["CoreInstanceType"],
            ),
            datapipeline.ObjectField(
                Key="amiVersion",
                StringValue="3.8.0",
            ),
            datapipeline.ObjectField(
                Key="masterInstanceType",
                StringValue=variables["MasterInstanceType"],
            ),
            datapipeline.ObjectField(
                Key="region",
                StringValue="#{myRegion}",
            ),
            datapipeline.ObjectField(
                Key="type",
                StringValue="EmrCluster",
            ),
            datapipeline.ObjectField(
                Key="terminateAfter",
                StringValue=variables["EmrClusterTerminateAfter"],
            ),
        ]

    def get_pipeline_parameter_objects(self):
        return [
            datapipeline.ParameterObject(
                Id="myRegion",
                Attributes=[
                    datapipeline.ParameterObjectAttribute(
                        Key="description",
                        StringValue="Region containing the DynamoDB table",
                    ),
                    datapipeline.ParameterObjectAttribute(
                        Key="type",
                        StringValue="String",
                    ),
                ],
            ),
        ]

    def get_pipeline_parameter_values(self):
        return [
            datapipeline.ParameterValue(
                Id="myRegion",
                StringValue=Ref("AWS::Region"),
            ),
        ]

    def get_pipeline_objects(self):
        variables = self.get_variables()
        pipeline = [
            self.get_default_object(),
            self.get_schedule_object(),
            self.get_emr_cluster_object(),
        ]
        for config in variables["SnapshotConfigs"]:
            objects = self.create_pipeline_objects(config)
            pipeline.extend(objects)
        return pipeline

    def create_template(self):
        t = self.template
        variables = self.get_variables()
        t.add_resource(datapipeline.Pipeline(
            "DynamoDBSnapshot",
            Activate=variables["Activate"],
            Name="DynamoDBSnapshot",
            ParameterObjects=self.get_pipeline_parameter_objects(),
            ParameterValues=self.get_pipeline_parameter_values(),
            PipelineObjects=self.get_pipeline_objects(),
        ))
