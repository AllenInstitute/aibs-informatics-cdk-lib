from functools import reduce
from typing import List

from aibs_informatics_test_resources.base import does_not_raise
from pytest import mark, param

from aibs_informatics_cdk_lib.common.aws.sfn_utils import JsonReferencePath


@mark.parametrize(
    "input, expected, raises_error",
    [
        param("simple", "simple", does_not_raise()),
        param("extra..periods", "extra.periods", does_not_raise()),
        param(".", "", does_not_raise()),
        param(".prefixed", "prefixed", does_not_raise()),
        param("suffixed.", "suffixed", does_not_raise()),
        param("$.dollarsign.prefix", "dollarsign.prefix", does_not_raise()),
        param("dollarsign.suffix.$", "dollarsign.suffix", does_not_raise()),
        param("dollarsign.$.middle", "dollarsign.middle", does_not_raise()),
        param("dollarsign.$instring", "dollarsign.instring", does_not_raise()),
    ],
)
def test__JsonReferencePath__normalized(input: str, expected: str, raises_error):
    with raises_error:
        actual = JsonReferencePath(input)

    if expected is not None:
        assert actual == expected


@mark.parametrize(
    "input, expected, raises_error",
    [
        param("simple", "simple.$", does_not_raise()),
        param("extra..periods", "extra.periods.$", does_not_raise()),
        param(".", "$", does_not_raise()),
        param(".prefixed", "prefixed.$", does_not_raise()),
        param("suffixed.", "suffixed.$", does_not_raise()),
        param("$.dollarsign.prefix", "dollarsign.prefix.$", does_not_raise()),
        param("dollarsign.suffix.$", "dollarsign.suffix.$", does_not_raise()),
        param("dollarsign.$.middle", "dollarsign.middle.$", does_not_raise()),
        param("dollarsign.$instring", "dollarsign.instring.$", does_not_raise()),
    ],
)
def test__JsonReferencePath__as_key(input: str, expected: str, raises_error):
    with raises_error:
        actual = JsonReferencePath(input)

    if expected is not None:
        assert actual.as_key == expected


@mark.parametrize(
    "input, expected, raises_error",
    [
        param("simple", "$.simple", does_not_raise()),
        param("extra..periods", "$.extra.periods", does_not_raise()),
        param(".", "$", does_not_raise()),
        param(".prefixed", "$.prefixed", does_not_raise()),
        param("suffixed.", "$.suffixed", does_not_raise()),
        param("$.dollarsign.prefix", "$.dollarsign.prefix", does_not_raise()),
        param("dollarsign.suffix.$", "$.dollarsign.suffix", does_not_raise()),
        param("dollarsign.$.middle", "$.dollarsign.middle", does_not_raise()),
        param("dollarsign.$instring", "$.dollarsign.instring", does_not_raise()),
    ],
)
def test__JsonReferencePath__as_reference(input: str, expected: str, raises_error):
    with raises_error:
        actual = JsonReferencePath(input)

    if expected is not None:
        assert actual.as_reference == expected


@mark.parametrize(
    "input, expected, raises_error",
    [
        param([JsonReferencePath("simple"), "concat"], "simple.concat", does_not_raise()),
        param([JsonReferencePath("dots."), "concat"], "dots.concat", does_not_raise()),
        param([JsonReferencePath("simple"), ".dots"], "simple.dots", does_not_raise()),
    ],
)
def test__JsonReferencePath__add__(input: List[str], expected: str, raises_error):
    with raises_error:
        actual = reduce(lambda x, y: x + y, input)

    if expected is not None:
        assert actual == expected
