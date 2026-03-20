import inspect
import types
from typing import Any, get_args, get_origin

import aws_cdk as cdk
from aibs_informatics_core.env import EnvBase
from aibs_informatics_core.models.db import DBIndex, DBModel
from aibs_informatics_core.utils.modules import load_type_from_qualified_name
from aws_cdk import aws_dynamodb as dynamodb
from constructs import Construct
from pydantic.fields import FieldInfo

from aibs_informatics_cdk_lib.constructs_.base import EnvBaseConstructMixins


class DynamoDBModelTable(dynamodb.Table):
    def __init__(
        self,
        scope: Construct,
        id: str,
        db_model: type[DBModel],
        db_index: type[DBIndex],
        billing_mode: dynamodb.BillingMode = dynamodb.BillingMode.PAY_PER_REQUEST,
        removal_policy: cdk.RemovalPolicy = cdk.RemovalPolicy.DESTROY,
        stream: dynamodb.StreamViewType | None = None,
        **kwargs,
    ) -> None:
        self.db_model_type = db_model
        self.db_index_type = db_index

        partition_key, sort_key = self.get_partition_and_sort_key()

        super().__init__(
            scope,
            id,
            table_name=self.get_table_name(),
            billing_mode=billing_mode,
            partition_key=partition_key,
            sort_key=sort_key,
            removal_policy=removal_policy,
            stream=stream,
            **kwargs,
        )

        for index in self.db_index_type:
            if index != self.db_index_type.get_default_index():
                self.add_global_secondary_index_from_type(index)

    def get_table_name(self) -> str:
        return self.db_index_type.table_name()

    def get_index_name(self, index: DBIndex) -> str:
        if index.index_name is None:
            raise ValueError("Cannot get index name for default index")
        return index.index_name

    def add_global_secondary_index_from_type(self, index: DBIndex) -> None:
        index_name = self.get_index_name(index)
        partition_key, sort_key = self.get_partition_and_sort_key(index)
        projection_type = dynamodb.ProjectionType.ALL
        non_key_attributes = index.non_key_attributes
        if non_key_attributes is not None:
            projection_type = dynamodb.ProjectionType.INCLUDE

        self.add_global_secondary_index(
            index_name=index_name,
            partition_key=partition_key,
            sort_key=sort_key,
            projection_type=projection_type,
            non_key_attributes=non_key_attributes,
        )

    def get_partition_and_sort_key(
        self, db_index: DBIndex | None = None
    ) -> tuple[dynamodb.Attribute, dynamodb.Attribute | None]:
        db_index = db_index or self.db_index_type.get_default_index()
        db_model_fields = self.db_model_type.model_fields
        partition_key = db_index.key_name
        sort_key = db_index.sort_key_name

        partition_field = db_model_fields.get(partition_key)
        if partition_field is None:
            raise ValueError(
                f"Partition key '{partition_key}' not found in model {self.db_model_type.__name__}"
            )
        partition_attr = dynamodb.Attribute(
            name=partition_key, type=self.get_attribute_type(partition_field)
        )

        sort_attr = None
        if sort_key:
            sort_field = db_model_fields.get(sort_key)
            if sort_field is None:
                raise ValueError(
                    f"Sort key '{sort_key}' not found in model {self.db_model_type.__name__}"
                )
            sort_attr = dynamodb.Attribute(name=sort_key, type=self.get_attribute_type(sort_field))
        return partition_attr, sort_attr

    @classmethod
    def get_attribute_type(cls, field: FieldInfo) -> dynamodb.AttributeType:
        _type: Any = field.annotation
        if _type is None:
            return dynamodb.AttributeType.STRING

        origin = get_origin(_type)
        if origin in (types.UnionType, tuple, list, set, dict) or origin is not None:
            args = [arg for arg in get_args(_type) if arg is not type(None)]
            if len(args) == 1:
                _type = args[0]

        if isinstance(_type, str):
            _type = load_type_from_qualified_name(_type)
        if not inspect.isclass(_type):
            _type = _type.__class__

        if issubclass(_type, int):
            return dynamodb.AttributeType.NUMBER
        elif issubclass(_type, bytes):
            return dynamodb.AttributeType.BINARY
        else:
            return dynamodb.AttributeType.STRING


class EnvBaseDBModelTable(DynamoDBModelTable, EnvBaseConstructMixins):
    def __init__(
        self,
        scope: Construct,
        id: str,
        env_base: EnvBase,
        db_model: type[DBModel],
        db_index: type[DBIndex],
        billing_mode: dynamodb.BillingMode = dynamodb.BillingMode.PAY_PER_REQUEST,
        stream: dynamodb.StreamViewType | None = None,
        **kwargs,
    ) -> None:
        self.env_base = env_base
        super().__init__(
            scope,
            id,
            db_model,
            db_index,
            billing_mode,
            cdk.RemovalPolicy.RETAIN if self.is_test_or_prod else cdk.RemovalPolicy.DESTROY,
            stream,
            **kwargs,
        )

    def get_table_name(self) -> str:
        return self.env_base.get_table_name(self.db_index_type.table_name())

    def get_index_name(self, index: DBIndex) -> str:
        index_name = index.get_index_name(self.env_base)
        if index_name is None:
            raise ValueError("Cannot get index name for default index")
        return index_name
