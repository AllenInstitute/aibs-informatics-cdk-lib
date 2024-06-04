from typing import cast

import constructs
from aibs_informatics_core.env import EnvBase
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as stepfn_tasks

from aibs_informatics_cdk_lib.constructs_.sfn.fragments.base import EnvBaseStateMachineFragment


class LambdaFunctionFragment(EnvBaseStateMachineFragment):
    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        env_base: EnvBase,
        lambda_function: lambda_.Function,
    ) -> None:
        """Creates a single task state machine fragment for a generic lambda function

                    [Pass]          # Start State. Augments input.
                      ||            #
                  [lambda fn]
               (x)  //   \\ (o)     #
                [Fail]    ||        # Catch state for failed lambda execution
                          ||        #
                        [Pass]      # End state. Reforms output of lambda as sfn output

        Returns:
            StateMachineFragment: the state machine fragment
        """
        super().__init__(scope, id, env_base)

        lambda_task = stepfn_tasks.LambdaInvoke(
            self,
            f"{lambda_function.function_name} Function Execution",
            lambda_function=cast(lambda_.IFunction, lambda_function),
            payload_response_only=True,
        )

        self.definition = sfn.Chain.start(lambda_task)
