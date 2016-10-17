import unittest

from stacker_blueprints.dynamodb.snapshot import validate_snaphsot_configs


class Test(unittest.TestCase):

    def test_validate_snapshot_configs_valid(self):
        configs = [
            {
                "TableName": "SomeTable",
                "S3Output": "s3-bucket",
                "ThroughputRatio": "0.25",
            },
            {
                "TableName": "SomeOtherTable",
                "S3Output": "s3-bucket",
            }
        ]
        validated = validate_snaphsot_configs(configs)
        self.assertEqual(validated[0], configs[0])
        # test default ThroughputRatio
        self.assertEqual(validated[1]["ThroughputRatio"], "0.25")

    def test_validate_snapshot_configs_invalid(self):
        configs = [{"S3Output": "s3-bucket"}]
        with self.assertRaises(ValueError):
            validate_snaphsot_configs(configs)
