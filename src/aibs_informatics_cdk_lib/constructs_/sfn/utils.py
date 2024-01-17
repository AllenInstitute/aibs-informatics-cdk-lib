import re
from functools import reduce
from typing import Any, ClassVar, List, Pattern, Union, cast

import aws_cdk as cdk
from aibs_informatics_core.utils.json import JSON
from aws_cdk import aws_stepfunctions as sfn


def convert_reference_paths(parameters: JSON) -> JSON:
    if isinstance(parameters, dict):
        return {k: convert_reference_paths(v) for k, v in parameters.items()}
    elif isinstance(parameters, list):
        return [convert_reference_paths(v) for v in parameters]
    elif isinstance(parameters, str):
        if (
            parameters.startswith("$") or parameters.startswith("States.")
        ) and not parameters.startswith("${Token"):
            return sfn.JsonPath.string_at(parameters)
        else:
            return parameters
    else:
        return parameters


def enclosed_chain(
    id: str, chain: sfn.Chain, result_path: str = "$", output_path: str = "$"
) -> sfn.Chain:

    pre = sfn.Pass(id + " Parallel Prep", result_path=result_path, output_path=output_path)
    parallel = chain.to_single_state(
        id=f"{id} Parallel", input_path=result_path, output_path="$[0]"
    )
    post = sfn.Pass(id + " Parallel Post", result_path=result_path, output_path=output_path)

    return sfn.Chain.start(pre).next(parallel).next(post)


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
