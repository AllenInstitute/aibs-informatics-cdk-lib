from test.aibs_informatics_cdk_lib.base import CdkBaseTest

from aws_cdk import aws_iam as iam
from aws_cdk.assertions import Match

from aibs_informatics_cdk_lib.constructs_.s3.bucket import EnvBaseBucket


class TestBucket(CdkBaseTest):
    def test__grant_permissions__with_key_pattern(self):
        stack = self.get_dummy_stack("test")

        bucket = EnvBaseBucket(stack, "bucket", self.env_base, bucket_name="my-bucket")
        role1 = iam.Role.from_role_arn(
            stack, "role", role_arn="arn:aws:iam::us-west-2:role/my-role-1"
        )
        role2 = iam.Role.from_role_arn(
            stack, "role2", role_arn="arn:aws:iam::us-west-2:role/my-role-2"
        )
        bucket.grant_permissions(role1, "r")
        bucket.grant_permissions(role2, "rw", objects_key_pattern="another/path/*")

        template = self.get_template(stack)

        template.resource_count_is("AWS::IAM::Policy", 2)

        bucket_arn_match = Match.object_like({"Fn::GetAtt": [Match.any_value(), "Arn"]})

        template.has_resource_properties(
            "AWS::IAM::Policy",
            Match.object_like(
                {
                    "PolicyDocument": Match.object_like(
                        {
                            "Statement": Match.array_with(
                                [
                                    Match.object_like(
                                        {
                                            "Action": [
                                                "s3:GetObject*",
                                                "s3:GetBucket*",
                                                "s3:List*",
                                            ],
                                            "Effect": "Allow",
                                            "Resource": Match.array_with(
                                                [
                                                    bucket_arn_match,
                                                    Match.object_like(
                                                        {
                                                            "Fn::Join": Match.array_with(
                                                                [
                                                                    "",
                                                                    Match.array_with(
                                                                        [
                                                                            bucket_arn_match,
                                                                            "/*",
                                                                        ]
                                                                    ),
                                                                ]
                                                            )
                                                        }
                                                    ),
                                                ]
                                            ),
                                        }
                                    )
                                ]
                            ),
                            "Version": "2012-10-17",
                        }
                    ),
                    "PolicyName": "rolePolicy0555B62D",
                    "Roles": ["my-role-1"],
                }
            ),
        )

        template.has_resource_properties(
            "AWS::IAM::Policy",
            Match.object_like(
                {
                    "PolicyDocument": Match.object_like(
                        {
                            "Statement": Match.array_with(
                                [
                                    Match.object_like(
                                        {
                                            "Action": [
                                                "s3:GetObject*",
                                                "s3:GetBucket*",
                                                "s3:List*",
                                                "s3:DeleteObject*",
                                                "s3:PutObject",
                                                "s3:PutObjectLegalHold",
                                                "s3:PutObjectRetention",
                                                "s3:PutObjectTagging",
                                                "s3:PutObjectVersionTagging",
                                                "s3:Abort*",
                                            ],
                                            "Effect": "Allow",
                                            "Resource": Match.array_with(
                                                [
                                                    bucket_arn_match,
                                                    Match.object_like(
                                                        {
                                                            "Fn::Join": Match.array_with(
                                                                [
                                                                    "",
                                                                    Match.array_with(
                                                                        [
                                                                            bucket_arn_match,
                                                                            "/another/path/*",
                                                                        ]
                                                                    ),
                                                                ]
                                                            )
                                                        }
                                                    ),
                                                ]
                                            ),
                                        }
                                    )
                                ]
                            ),
                            "Version": "2012-10-17",
                        }
                    ),
                    "PolicyName": "role2PolicyF053F9CA",
                    "Roles": ["my-role-2"],
                }
            ),
        )

    def test__grant_permissions__no_role(self):
        stack = self.get_dummy_stack("test")

        bucket = EnvBaseBucket(stack, "bucket", self.env_base, bucket_name="my-bucket")
        bucket.grant_permissions(None, "r", "w", "d")

        template = self.get_template(stack)

        template.resource_count_is("AWS::IAM::Policy", 0)

    def test__grant_permissions__all_permissions(self):
        stack = self.get_dummy_stack("test")

        bucket = EnvBaseBucket(stack, "bucket", self.env_base, bucket_name="my-bucket")
        role = iam.Role.from_role_arn(
            stack, "role", role_arn="arn:aws:iam::us-west-2:role/my-role"
        )
        bucket.grant_permissions(role, "rw", "r", "w", "d")

        template = self.get_template(stack)

        template.resource_count_is("AWS::IAM::Policy", 1)

        bucket_arn_match = Match.object_like({"Fn::GetAtt": [Match.any_value(), "Arn"]})

        template.has_resource_properties(
            "AWS::IAM::Policy",
            Match.object_like(
                {
                    "PolicyDocument": Match.object_like(
                        {
                            "Statement": Match.array_with(
                                [
                                    Match.object_like(
                                        {
                                            "Action": [
                                                "s3:GetObject*",
                                                "s3:GetBucket*",
                                                "s3:List*",
                                                "s3:DeleteObject*",
                                                "s3:PutObject",
                                                "s3:PutObjectLegalHold",
                                                "s3:PutObjectRetention",
                                                "s3:PutObjectTagging",
                                                "s3:PutObjectVersionTagging",
                                                "s3:Abort*",
                                            ],
                                            "Effect": "Allow",
                                            "Resource": Match.array_with(
                                                [
                                                    bucket_arn_match,
                                                    Match.object_like(
                                                        {
                                                            "Fn::Join": Match.array_with(
                                                                [
                                                                    "",
                                                                    Match.array_with(
                                                                        [
                                                                            bucket_arn_match,
                                                                            "/*",
                                                                        ]
                                                                    ),
                                                                ]
                                                            )
                                                        }
                                                    ),
                                                ]
                                            ),
                                        }
                                    )
                                ]
                            ),
                            "Version": "2012-10-17",
                        }
                    ),
                    "PolicyName": "rolePolicy0555B62D",
                    "Roles": ["my-role"],
                }
            ),
        )
