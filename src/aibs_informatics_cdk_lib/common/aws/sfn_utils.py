"""Step Functions utilities for JSON path references.

This module provides utilities for working with Step Functions JSON path expressions.
"""

import re
from functools import reduce
from typing import Any, ClassVar, List, Pattern, Union, cast

import aws_cdk as cdk
from aws_cdk import aws_stepfunctions as sfn


class JsonReferencePath(str):
    """String extension for defining JsonPath reference expressions.

    Provides properties and methods for working with JSON path references
    in AWS Step Functions state machines.

    More details: https://github.com/json-path/JsonPath

    Primarily supports "$" reference.

    Attributes:
        _EXTRA_PERIODS_PATTERN: Regex pattern for multiple periods.
        _PERIOD_PATTERN: Regex pattern for single periods.
        _PREFIX: JSON path prefix ("$.").
        _SUFFIX: JSON path suffix (".$").
        _REF: Root reference ("$").

    Example:
        >>> path = JsonReferencePath("input.data")
        >>> path.as_reference
        '$.input.data'
        >>> path.as_key
        'input.data.$'
    """

    _EXTRA_PERIODS_PATTERN: ClassVar[Pattern[str]] = re.compile(r"[$.]+")
    _PERIOD_PATTERN: ClassVar[Pattern[str]] = re.compile(r"(?<!\$)\.(?!\$)")
    _PREFIX: ClassVar[str] = "$."
    _SUFFIX: ClassVar[str] = ".$"
    _REF: ClassVar[str] = "$"

    def __new__(cls, content: str):
        """Create a new JsonReferencePath.

        Args:
            content (str): The path content to wrap.

        Returns:
            A sanitized JsonReferencePath instance.
        """
        return super().__new__(cls, cls.sanitize(content))

    def __add__(self, other):
        """Concatenate paths with a period separator.

        Args:
            other: The path segment to append.

        Returns:
            A new JsonReferencePath with the appended segment.
        """
        return JsonReferencePath(super().__add__("." + other))

    def extend(self, *paths: str) -> "JsonReferencePath":
        """Extend the path with multiple segments.

        Args:
            *paths (str): Variable number of path segments to append.

        Returns:
            A new JsonReferencePath with all segments appended.
        """
        return cast(JsonReferencePath, reduce(lambda x, y: x + y, [self, *paths]))

    @property
    def as_key(self) -> str:
        """Return the reference path as a key.

        Appends ".$" suffix for use as a state machine key.

        Returns:
            The path formatted as a key.
        """
        return f"{self}{self._SUFFIX}" if self else self._REF

    @property
    def as_reference(self) -> str:
        """Return the reference path as a value.

        Prepends "$." prefix for use as a state machine reference.

        Returns:
            The path formatted as a reference.
        """
        return f"{self._PREFIX}{self}" if self else self._REF

    @property
    def as_jsonpath_string(self) -> str:
        """Return the path as a Step Functions string reference.

        Returns:
            The path wrapped in JsonPath.string_at().
        """
        return sfn.JsonPath.string_at(self.as_reference)

    @property
    def as_jsonpath_object(self) -> cdk.IResolvable:
        """Return the path as a Step Functions object reference.

        Returns:
            The path wrapped in JsonPath.object_at().
        """
        return sfn.JsonPath.object_at(self.as_reference)

    @property
    def as_jsonpath_json_to_string(self) -> str:
        """Return the path as a JSON-to-string conversion.

        Returns:
            The object reference converted to string via JsonPath.json_to_string().
        """
        return sfn.JsonPath.json_to_string(self.as_jsonpath_object)

    @property
    def as_jsonpath_list(self) -> List[str]:
        """Return the path as a Step Functions list reference.

        Returns:
            The path wrapped in JsonPath.list_at().
        """
        return sfn.JsonPath.list_at(self.as_reference)

    @property
    def as_jsonpath_number(self) -> Union[int, float]:
        """Return the path as a Step Functions number reference.

        Returns:
            The path wrapped in JsonPath.number_at().
        """
        return sfn.JsonPath.number_at(self.as_reference)

    @classmethod
    def sanitize(cls, s: str) -> str:
        """Sanitize a string for use as a JSON path.

        Ensures string has non-consecutive periods and no periods at edges.

        Args:
            s (str): The string to sanitize.

        Returns:
            The sanitized string.
        """
        return f'{cls._EXTRA_PERIODS_PATTERN.sub(".", s).strip(".")}'

    @classmethod
    def is_reference(cls, s: Any) -> bool:
        """Check if a value is a JSON path reference.

        Args:
            s (Any): The value to check.

        Returns:
            True if the value is a JsonReferencePath or starts with "$".
        """
        return isinstance(s, JsonReferencePath) or isinstance(s, str) and s.startswith("$")

    @classmethod
    def empty(cls) -> "JsonReferencePath":
        """Create an empty JsonReferencePath.

        Returns:
            An empty JsonReferencePath instance.
        """
        return cls("")
