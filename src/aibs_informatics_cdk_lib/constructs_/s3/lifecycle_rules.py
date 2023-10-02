import aws_cdk as cdk
from aibs_informatics_aws_utils.constants.s3 import (
    S3_SCRATCH_KEY_PREFIX,
    S3_SCRATCH_TAGGING_KEY,
    S3_SCRATCH_TAGGING_VALUE,
)
from aws_cdk import aws_s3 as s3


class LifecycleRuleGenerator:
    @classmethod
    def expire_files_with_scratch_tags(
        cls,
        id: str = "expire-files-with-scratch-tags",
        expiration: cdk.Duration = cdk.Duration.days(30),
        tag_key: str = S3_SCRATCH_TAGGING_KEY,
        tag_value: str = S3_SCRATCH_TAGGING_VALUE,
        enabled: bool = True,
        **lifecycle_rule_kwargs,
    ) -> s3.LifecycleRule:
        return s3.LifecycleRule(
            id=id,
            expiration=expiration,
            tag_filters={tag_key: tag_value},
            enabled=enabled,
            **lifecycle_rule_kwargs,
        )

    @classmethod
    def expire_files_under_prefix(
        cls,
        id: str = "expire-files-under-prefix",
        expiration: cdk.Duration = cdk.Duration.days(30),
        prefix: str = S3_SCRATCH_KEY_PREFIX,
        enabled: bool = True,
        **lifecycle_rule_kwargs,
    ) -> s3.LifecycleRule:
        if id is None:
            id = f"expire-files-under-{prefix}"
        return s3.LifecycleRule(
            id=id,
            expiration=expiration,
            prefix=prefix,
            enabled=enabled,
            **lifecycle_rule_kwargs,
        )

    @classmethod
    def use_storage_class_as_default(
        cls,
        id: str = "use-s3-intelligent-tier-as-default",
        storage_class: s3.StorageClass = s3.StorageClass.INTELLIGENT_TIERING,
        transition_after: cdk.Duration = cdk.Duration.days(0),
        enabled: bool = True,
        **lifecycle_rule_kwargs,
    ) -> s3.LifecycleRule:
        return s3.LifecycleRule(
            id=id,
            transitions=[
                s3.Transition(storage_class=storage_class, transition_after=transition_after)
            ],
            enabled=enabled,
            **lifecycle_rule_kwargs,
        )
