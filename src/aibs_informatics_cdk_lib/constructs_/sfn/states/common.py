from typing import Any, Optional

import constructs
from aws_cdk import aws_stepfunctions as sfn


class CommonOperation:
    @classmethod
    def merge_defaults(
        cls,
        scope: constructs.Construct,
        id: str,
        defaults: dict[str, Any],
        input_path: str = "$",
        result_path: Optional[str] = None,
    ) -> sfn.Chain:
        """Wrapper chain that merges input with defaults.



        Args:
            scope (constructs.Construct): construct scope
            id (str): identifier for the states created
            defaults (dict[str, Any]): default values to merge with input
            input_path (str, optional): Input path of object to merge. Defaults to "$".
            result_path (Optional[str], optional): result path to store merged results.
                Defaults to whatever input_path is defined as.

        Returns:
            sfn.Chain: the new chain that merges defaults with input
        """
        new_input_path = result_path if result_path is not None else input_path
        init_state = sfn.Pass(
            scope,
            "Merge Defaults",
            parameters={
                "input": sfn.JsonPath.object_at("$"),
                "default": defaults,
            },
        )
        merge = sfn.Pass(
            scope,
            "Merge",
            parameters={
                "merged": sfn.JsonPath.json_merge(
                    sfn.JsonPath.object_at("$.default"), sfn.JsonPath.object_at("$.input")
                ),
            },
            output_path="$.merged",
        )

        parallel = init_state.next(merge).to_single_state(
            id=id, input_path=input_path, result_path=new_input_path
        )
        restructure = sfn.Pass(
            scope,
            f"{id} Restructure",
            input_path=f"{new_input_path}[0]",
            result_path=new_input_path,
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
