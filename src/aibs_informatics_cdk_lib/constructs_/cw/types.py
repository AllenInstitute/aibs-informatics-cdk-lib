from typing import Dict, List, Literal, TypedDict, Union

from aws_cdk import aws_cloudwatch as cw


def to_comparison_operator(value: Union[cw.ComparisonOperator, str]) -> cw.ComparisonOperator:
    if isinstance(value, cw.ComparisonOperator):
        return value
    elif value.lower() in [">=", "greater_than_or_equal_to"]:
        return cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD
    elif value.lower() in [">", "greater_than"]:
        return cw.ComparisonOperator.GREATER_THAN_THRESHOLD
    elif value.lower() in ["<=", "less_than_or_equal_to"]:
        return cw.ComparisonOperator.LESS_THAN_OR_EQUAL_TO_THRESHOLD
    elif value.lower() in ["<", "less_than"]:
        return cw.ComparisonOperator.LESS_THAN_THRESHOLD
    elif value.lower() in ["<>", "out_of_range"]:
        return cw.ComparisonOperator.LESS_THAN_LOWER_OR_GREATER_THAN_UPPER_THRESHOLD
    else:
        return cw.ComparisonOperator(value)


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
