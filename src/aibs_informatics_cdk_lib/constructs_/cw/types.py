import re
from collections import defaultdict
from copy import deepcopy
from math import ceil
from typing import Any, Dict, List, Literal, Optional, Tuple, TypedDict, Union

import aws_cdk as cdk
import constructs
from aibs_informatics_core.env import EnvBase
from aws_cdk import aws_cloudwatch as cw
from aws_cdk import aws_cloudwatch_actions as cw_actions
from aws_cdk import aws_sns as sns

from aibs_informatics_cdk_lib.constructs_.base import EnvBaseConstruct, EnvBaseConstructMixins


class _AlarmMetricConfigOptional(TypedDict, total=False):
    pass


class _AlarmMetricConfigRequired(TypedDict):
    name: str
    threshold: int
    evaluation_periods: int
    datapoints_to_alarm: int
    comparison_operator: Union[cw.ComparisonOperator, str]


class AlarmMetricConfig(_AlarmMetricConfigRequired, _AlarmMetricConfigOptional):
    pass


class _GraphMetricConfigOptional(TypedDict, total=False):
    namespace: str
    metric_expression: str
    dimension_map: Dict[str, str]
    using_metrics: Dict[str, cw.IMetric]
    alarm: AlarmMetricConfig
    axis_side: Literal["left", "right"]
    color: str
    label: str
    unit: cw.Unit


class _GraphMetricConfigRequired(TypedDict):
    metric: Union[str, cw.IMetric]
    statistic: str


class GraphMetricConfig(_GraphMetricConfigRequired, _GraphMetricConfigOptional):
    pass


class _GroupedGraphMetricConfigOptional(TypedDict, total=False):
    namespace: str
    dimension_map: Dict[str, str]
    height: int
    width: int
    left_y_axis: cw.YAxisProps
    right_y_axis: cw.YAxisProps


class _GroupedGraphMetricConfigRequired(TypedDict):
    title: str
    metrics: List[GraphMetricConfig]


class GroupedGraphMetricConfig(
    _GroupedGraphMetricConfigRequired, _GroupedGraphMetricConfigOptional
):
    pass
