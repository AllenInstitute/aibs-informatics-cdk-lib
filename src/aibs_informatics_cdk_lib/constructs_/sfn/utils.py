import re
from functools import reduce
from typing import Any, ClassVar, Dict, List, Mapping, Optional, Pattern, TypeVar, Union, cast

import aws_cdk as cdk
import constructs
from aibs_informatics_core.utils.json import JSON
from aibs_informatics_core.utils.tools.dicttools import convert_key_case
from aibs_informatics_core.utils.tools.strtools import pascalcase
from aws_cdk import aws_stepfunctions as sfn

T = TypeVar("T")


def convert_reference_paths_in_mapping(parameters: Mapping[str, Any]) -> Mapping[str, Any]:
    return {k: convert_reference_paths(v) for k, v in parameters.items()}


def convert_to_sfn_api_action_case(parameters: T) -> T:
    """Converts a dictionary of parameters to the format expected by the Step Functions for service integration.

    Even if a native API specifies a parameter in camelCase, the Step Functions SDK expects it in pascal case.

    https://docs.aws.amazon.com/step-functions/latest/dg/supported-services-awssdk.html#use-awssdk-integ

    Args:
        parameters (Dict[str, Any]): parameters for SDK action

    Returns:
        Dict[str, Any]: parameters for SDK action in pascal case
    """
    return convert_key_case(parameters, pascalcase)


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
    scope: constructs.Construct,
    id: str,
    definition: sfn.IChainable,
    input_path: Optional[str] = None,
    result_path: Optional[str] = None,
) -> sfn.Chain:
    """Enclose the current state machine fragment within a parallel state.

    Notes:
        - If input_path is not provided, it will default to "$"
        - If result_path is not provided, it will default to input_path

    Args:
        id (str): an identifier for the parallel state
        input_path (Optional[str], optional): input path for the enclosed state.
            Defaults to "$".
        result_path (Optional[str], optional): result path to put output of enclosed state.
            Defaults to same as input_path.

    Returns:
        sfn.Chain: the new state machine fragment
    """
    if input_path is None:
        input_path = "$"
    if result_path is None:
        result_path = input_path

    chain = (
        sfn.Chain.start(definition)
        if not isinstance(definition, (sfn.Chain, sfn.StateMachineFragment))
        else definition
    )

    if isinstance(chain, sfn.Chain):
        parallel = chain.to_single_state(
            id=f"{id} Enclosure", input_path=input_path, result_path=result_path
        )
    else:
        parallel = chain.to_single_state(input_path=input_path, result_path=result_path)
    definition = sfn.Chain.start(parallel)

    if result_path and result_path != sfn.JsonPath.DISCARD:
        restructure = sfn.Pass(
            scope,
            f"{id} Enclosure Post",
            input_path=f"{result_path}[0]",
            result_path=result_path,
        )
        definition = definition.next(restructure)

    return definition


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
