import re
from functools import reduce
from typing import Any, ClassVar, Dict, List, Mapping, Optional, Pattern, Union, cast

import aws_cdk as cdk
import constructs
from aibs_informatics_core.utils.tools.dicttools import remove_null_values
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as stepfn_tasks


def sanitize_parameters(parameters: Dict[str, Any]) -> Dict[str, Any]:
    new_parameters: Dict[str, Any] = {}
    for k, v in parameters.items():
        if isinstance(v, str):
            if v.startswith("$") or v.startswith("States."):
                new_parameters[f"{k}.$"] = v
        elif isinstance(v, dict):
            new_parameters[k] = sanitize_parameters(cast(Dict[str, Any], v))
        else:
            new_parameters[k] = v
    return new_parameters


class JsonReferencePath(str):
    """
    str extension with properties that provide *some* functionality for defining JsonPath reference expressions
    More details: https://github.com/json-path/JsonPath

    Primarily supports "$" reference.


    """

    _EXTRA_PERIODS_PATTERN: ClassVar[Pattern[str]] = re.compile(r"[$.]+")
    _PERIOD_PATTERN: ClassVar[Pattern[str]] = re.compile(r"(?<!\$)\.(?!\$)")
    _PREFIX: ClassVar[str] = "$."
    _SUFFIX: ClassVar[str] = ".$"
    _REF: ClassVar[str] = "$"

    def __new__(cls, content: str):
        return super().__new__(cls, cls.sanitize(content))

    def __add__(self, other):
        return JsonReferencePath(super().__add__("." + other))

    def extend(self, *paths: str) -> "JsonReferencePath":
        return cast(JsonReferencePath, reduce(lambda x, y: x + y, [self, *paths]))

    @property
    def as_key(self) -> str:
        """
        Returns reference path as a key. This appends ".$"
        """
        return f"{self}{self._SUFFIX}" if self else self._REF

    @property
    def as_reference(self) -> str:
        """
        Returns reference path as a value. This prepends "$."
        """
        return f"{self._PREFIX}{self}" if self else self._REF

    @property
    def as_jsonpath_string(self) -> str:
        return sfn.JsonPath.string_at(self.as_reference)

    @property
    def as_jsonpath_object(self) -> cdk.IResolvable:
        return sfn.JsonPath.object_at(self.as_reference)

    @property
    def as_jsonpath_json_to_string(self) -> str:
        return sfn.JsonPath.json_to_string(self.as_jsonpath_object)

    @property
    def as_jsonpath_list(self) -> List[str]:
        return sfn.JsonPath.list_at(self.as_reference)

    @property
    def as_jsonpath_number(self) -> Union[int, float]:
        return sfn.JsonPath.number_at(self.as_reference)

    @classmethod
    def sanitize(cls, s: str) -> str:
        """Sanitizes a string to ensure string has non-consecutive periods and not on the edge."""
        return f'{cls._EXTRA_PERIODS_PATTERN.sub(".", s).strip(".")}'

    @classmethod
    def is_reference(cls, s: Any) -> bool:
        return isinstance(s, JsonReferencePath) or isinstance(s, str) and s.startswith("$")

    @classmethod
    def empty(cls) -> "JsonReferencePath":
        return cls("")
