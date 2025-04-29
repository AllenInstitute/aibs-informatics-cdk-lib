from mimetypes import init
from typing import Any, Literal, Optional

import constructs
from aws_cdk import aws_stepfunctions as sfn

from aibs_informatics_cdk_lib.common.aws.sfn_utils import JsonReferencePath


class CommonOperation:
    @classmethod
    def merge_defaults(
        cls,
        scope: constructs.Construct,
        id: str,
        defaults: dict[str, Any],
        input_path: str = "$",
        target_path: str = "$",
        result_path: Optional[str] = None,
        order_of_preference: Literal["target", "default"] = "target",
        check_if_target_present: bool = False,
    ) -> sfn.Chain:
        """Wrapper chain that merges input with defaults.

        Notes:
            - reference paths in defaults should be relative to the input path

        Args:
            scope (constructs.Construct): construct scope
            id (str): identifier for the states created
            defaults (dict[str, Any]): default values to merge with input. If any reference paths
                are present in the defaults, they should be relative to the input path.
            input_path (str, optional): Input path of object to merge. De faults to "$".
            target_path (str, optional): target path to merge with. This should be relative to
                the input_path parameter. Defaults to "$".
            result_path (Optional[str], optional): result path to store merged results.
                If not specified, it defaults to the target_path relative to input_path.
                If specified, it is considered an absolute path.
            order_of_preference (Literal["target", "default"], optional): If "target", the target
                path will be merged with the defaults. If "default", the defaults will be merged
                with the target path. Defaults to "target".
            check_if_target_present (bool, optional): If true, check if the target path is present
                in the input. If not, the defaults will be used as the result. Otherwise, the
                defaults will be merged with the target path. This is useful for optional
                parameters that may or may not be present in the input.
                Defaults to False.

        Returns:
            sfn.Chain: the new chain that merges defaults with input
        """
        input_path = JsonReferencePath(input_path)
        target_path = JsonReferencePath(target_path)
        result_path = (
            JsonReferencePath(result_path) if result_path else input_path.extend(target_path)
        )

        pref1 = "$.target" if order_of_preference == "target" else "$.default"
        pref2 = "$.default" if order_of_preference == "target" else "$.target"

        merge_task = sfn.Pass(
            scope,
            "Merge Pass",
            parameters={
                "merged": sfn.JsonPath.json_merge(
                    sfn.JsonPath.object_at(pref2),
                    sfn.JsonPath.object_at(pref1),
                ),
            },
            output_path="$.merged",
        )

        if check_if_target_present:
            # Branch based on presence of the target
            choice = sfn.Choice(scope, "Check Target")
            present_pass = sfn.Pass(
                scope,
                "Target Present",
                parameters={
                    "target": sfn.JsonPath.object_at(target_path.as_reference),
                    "default": defaults,
                },
            )
            not_present_pass = sfn.Pass(
                scope,
                "Target Not Present",
                parameters={
                    "target": {},
                    "default": defaults,
                },
            )
            # Chain both branches into the merge task
            present_pass.next(merge_task)
            not_present_pass.next(merge_task)
            choice.when(
                sfn.Condition.is_present(target_path.as_reference),
                present_pass,
            ).otherwise(not_present_pass)
            chain_start = choice
        else:
            init_pass = sfn.Pass(
                scope,
                "Init Pass",
                parameters={
                    "target": sfn.JsonPath.object_at(target_path.as_reference),
                    "default": defaults,
                },
            )
            init_pass.next(merge_task)
            chain_start = init_pass

        parallel = sfn.Chain.start(chain_start).to_single_state(
            id=id,
            input_path=input_path.as_reference,
            result_path=result_path.as_reference,
        )
        restructure = sfn.Pass(
            scope,
            f"{id} Restructure",
            input_path=f"{result_path.as_reference}[0]",
            result_path=result_path.as_reference,
        )
        return parallel.next(restructure)

    @classmethod
    def enclose_chainable(
        cls,
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
