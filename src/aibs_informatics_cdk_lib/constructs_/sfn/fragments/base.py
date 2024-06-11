from abc import abstractmethod
from typing import Any, Dict, List, Mapping, Optional, Sequence, TypeVar, Union, cast

import aws_cdk as cdk
import constructs
from aibs_informatics_core.collections import ValidatedStr
from aibs_informatics_core.env import EnvBase
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_logs as logs_
from aws_cdk import aws_stepfunctions as sfn

from aibs_informatics_cdk_lib.common.aws.core_utils import build_lambda_arn
from aibs_informatics_cdk_lib.common.aws.sfn_utils import JsonReferencePath
from aibs_informatics_cdk_lib.constructs_.base import EnvBaseConstructMixins

T = TypeVar("T", bound=ValidatedStr)

F = TypeVar("F", bound="StateMachineFragment")


def create_log_options(
    scope: constructs.Construct,
    id: str,
    env_base: EnvBase,
    removal_policy: Optional[cdk.RemovalPolicy] = None,
    retention: Optional[logs_.RetentionDays] = None,
) -> sfn.LogOptions:
    return sfn.LogOptions(
        destination=logs_.LogGroup(
            scope,
            env_base.get_construct_id(id, "state-loggroup"),
            log_group_name=env_base.get_state_machine_log_group_name(id),
            removal_policy=removal_policy or cdk.RemovalPolicy.DESTROY,
            retention=retention or logs_.RetentionDays.ONE_MONTH,
        )
    )


def create_role(
    scope: constructs.Construct,
    id: str,
    env_base: EnvBase,
    assumed_by: iam.IPrincipal = iam.ServicePrincipal("states.amazonaws.com"),  # type: ignore[assignment]
    managed_policies: Optional[Sequence[Union[iam.IManagedPolicy, str]]] = None,
    inline_policies: Optional[Mapping[str, iam.PolicyDocument]] = None,
    inline_policies_from_statements: Optional[Mapping[str, Sequence[iam.PolicyStatement]]] = None,
    include_default_managed_policies: bool = True,
) -> iam.Role:
    construct_id = env_base.get_construct_id(id, "role")

    if managed_policies is not None:
        managed_policies = [
            iam.ManagedPolicy.from_aws_managed_policy_name(policy)
            if isinstance(policy, str)
            else policy
            for policy in managed_policies
        ]

    if inline_policies is None:
        inline_policies = {}
    if inline_policies_from_statements:
        inline_policies = {
            **inline_policies,
            **{
                name: iam.PolicyDocument(statements=statements)
                for name, statements in inline_policies_from_statements.items()
            },
        }

    return iam.Role(
        scope,
        construct_id,
        assumed_by=assumed_by,  # type: ignore
        managed_policies=[
            *(managed_policies or []),
            *[
                iam.ManagedPolicy.from_aws_managed_policy_name(policy)
                for policy in (
                    [
                        "AWSStepFunctionsFullAccess",
                        "CloudWatchLogsFullAccess",
                        "CloudWatchEventsFullAccess",
                    ]
                    if include_default_managed_policies
                    else []
                )
            ],
        ],
        inline_policies=inline_policies,
    )


class StateMachineMixins(EnvBaseConstructMixins):
    def get_fn(self, function_name: str) -> lambda_.IFunction:
        cache_attr = "_function_cache"
        if not hasattr(self, cache_attr):
            setattr(self, cache_attr, {})
        resource_cache = cast(Dict[str, lambda_.IFunction], getattr(self, cache_attr))
        if function_name not in resource_cache:
            resource_cache[function_name] = lambda_.Function.from_function_arn(
                scope=self.as_construct(),
                id=self.env_base.get_construct_id(function_name, "from-arn"),
                function_arn=build_lambda_arn(
                    resource_type="function",
                    resource_id=self.env_base.get_function_name(function_name),
                ),
            )
        return resource_cache[function_name]

    def get_state_machine_from_name(self, state_machine_name: str) -> sfn.IStateMachine:
        cache_attr = "_state_machine_cache"
        if not hasattr(self, cache_attr):
            setattr(self, cache_attr, {})
        resource_cache = cast(Dict[str, sfn.IStateMachine], getattr(self, cache_attr))
        if state_machine_name not in resource_cache:
            resource_cache[state_machine_name] = sfn.StateMachine.from_state_machine_name(
                scope=self.as_construct(),
                id=self.env_base.get_construct_id(state_machine_name, "from-name"),
                state_machine_name=self.env_base.get_state_machine_name(state_machine_name),
            )
        return resource_cache[state_machine_name]


def create_state_machine(
    scope: constructs.Construct,
    env_base: EnvBase,
    id: str,
    name: Optional[str],
    definition: sfn.IChainable,
    role: Optional[iam.Role] = None,
    logs: Optional[sfn.LogOptions] = None,
    timeout: Optional[cdk.Duration] = None,
) -> sfn.StateMachine:
    return sfn.StateMachine(
        scope,
        env_base.get_construct_id(id),
        state_machine_name=env_base.get_state_machine_name(name) if name else None,
        logs=(
            logs
            or sfn.LogOptions(
                destination=logs_.LogGroup(
                    scope,
                    env_base.get_construct_id(id, "state-loggroup"),
                    log_group_name=env_base.get_state_machine_log_group_name(name or id),
                    removal_policy=cdk.RemovalPolicy.DESTROY,
                    retention=logs_.RetentionDays.ONE_MONTH,
                )
            )
        ),
        role=cast(iam.IRole, role),
        definition_body=sfn.DefinitionBody.from_chainable(definition),
        timeout=timeout,
    )


class StateMachineFragment(sfn.StateMachineFragment):
    @property
    def definition(self) -> sfn.IChainable:
        return self._definition

    @definition.setter
    def definition(self, value: sfn.IChainable):
        self._definition = value

    @property
    def start_state(self) -> sfn.State:
        return self.definition.start_state

    @property
    def end_states(self) -> List[sfn.INextable]:
        return self.definition.end_states

    def enclose(
        self,
        id: Optional[str] = None,
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
        id = id or self.node.id

        if input_path is None:
            input_path = "$"
        if result_path is None:
            result_path = input_path

        chain = (
            sfn.Chain.start(self.definition)
            if not isinstance(self.definition, (sfn.Chain, sfn.StateMachineFragment))
            else self.definition
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
                self,
                f"{id} Enclosure Post",
                input_path=f"{result_path}[0]",
                result_path=result_path,
            )
            definition = definition.next(restructure)

        return definition


class EnvBaseStateMachineFragment(StateMachineFragment, StateMachineMixins):
    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        env_base: EnvBase,
    ) -> None:
        super().__init__(scope, id)
        self.env_base = env_base

    def to_single_state(
        self,
        *,
        prefix_states: Optional[str] = None,
        state_id: Optional[str] = None,
        comment: Optional[str] = None,
        input_path: Optional[str] = None,
        output_path: Optional[str] = "$[0]",
        result_path: Optional[str] = None,
        result_selector: Optional[Mapping[str, Any]] = None,
    ) -> sfn.Parallel:
        return super().to_single_state(
            prefix_states=prefix_states,
            state_id=state_id,
            comment=comment,
            input_path=input_path,
            output_path=output_path,
            result_path=result_path,
            result_selector=result_selector,
        )

    def to_state_machine(
        self,
        state_machine_name: str,
        role: Optional[iam.Role] = None,
        logs: Optional[sfn.LogOptions] = None,
        timeout: Optional[cdk.Duration] = None,
    ) -> sfn.StateMachine:
        if role is None:
            role = create_role(
                self,
                state_machine_name,
                self.env_base,
                managed_policies=self.required_managed_policies,
                inline_policies_from_statements={
                    "default": self.required_inline_policy_statements,
                },
            )
        else:
            for policy in self.required_managed_policies:
                if isinstance(policy, str):
                    policy = iam.ManagedPolicy.from_aws_managed_policy_name(policy)
                role.add_managed_policy(policy)

            for statement in self.required_inline_policy_statements:
                role.add_to_policy(statement)

        return sfn.StateMachine(
            self,
            self.get_construct_id(state_machine_name),
            state_machine_name=self.env_base.get_state_machine_name(state_machine_name),
            logs=logs or create_log_options(self, state_machine_name, self.env_base),
            role=(
                role if role is not None else create_role(self, state_machine_name, self.env_base)
            ),  # type: ignore[arg-type]
            definition_body=sfn.DefinitionBody.from_chainable(self.definition),
            timeout=timeout,
        )

    @property
    def required_managed_policies(self) -> Sequence[Union[iam.ManagedPolicy, str]]:
        return []

    @property
    def required_inline_policy_statements(self) -> Sequence[iam.PolicyStatement]:
        return []


class LazyLoadStateMachineFragment(EnvBaseStateMachineFragment):
    @property
    def definition(self) -> sfn.IChainable:
        try:
            return self._definition
        except AttributeError:
            self.definition = self.build_definition()
            return self.definition

    @definition.setter
    def definition(self, value: sfn.IChainable):
        self._definition = value

    @abstractmethod
    def build_definition(self) -> sfn.IChainable:
        raise NotImplementedError("Must implement")


class TaskWithPrePostStatus(LazyLoadStateMachineFragment):
    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        env_base: EnvBase,
        task: Optional[sfn.IChainable],
    ) -> None:
        super().__init__(scope, id, env_base)
        self.task = task
        self.task_name = id

        self.raw_task_input_path = JsonReferencePath("$")

    @property
    def task(self) -> sfn.IChainable:
        assert self._task, "Task must be set"
        return self._task

    @task.setter
    def task(self, value: Optional[sfn.IChainable]):
        self._task = value

    @property
    def task_name(self) -> str:
        return self._task_name

    @task_name.setter
    def task_name(self, value: str):
        self._task_name = value

    def build_definition(self) -> sfn.IChainable:
        # Should only evaluate once, otherwise errors for duplicate construct will be raised\
        task__augment_input = self.task__augment_input
        task__status_started = self.task__status_started
        task__status_failed = self.task__status_failed
        task__status_completed = self.task__status_completed
        task__pre_run = self.task__pre_run
        task__post_run = self.task__post_run

        # ---------------------------
        # START DEFINITION
        # ---------------------------
        definition = sfn.Pass(
            self,
            f"{self.task_name} Start",
            parameters={
                "input": self.raw_task_input_path.as_jsonpath_object,
                "context": self.task_context,
            },
        )

        # -------------
        # AUGMENT INPUT
        # -------------
        if task__augment_input:
            definition = definition.next(
                sfn.Chain.start(task__augment_input).to_single_state(
                    f"{self.task_name} Augment Input",
                    result_path=self.task_input_path.as_reference,
                    output_path=f"{self.task_input_path.as_reference}[0]",
                )
            )

        # -------------
        # PRE TASK
        # -------------
        if task__status_started:
            definition = definition.next(
                sfn.Chain.start(task__status_started).to_single_state(
                    f"{self.task_name} Status Started", result_path=sfn.JsonPath.DISCARD
                )
            )
        if task__pre_run:
            definition = definition.next(
                sfn.Chain.start(task__pre_run).to_single_state(
                    f"{self.task_name} Pre Run", result_path=sfn.JsonPath.DISCARD
                )
            )

        # -------------
        # TASK
        # -------------
        task_chain = sfn.Chain.start(self.task)
        task_enclosure = task_chain.to_single_state(
            f"{self.task_name} Run",
            input_path=self.task_input_path.as_reference,
            result_path=self.task_result_path.as_reference,
        )
        # fmt: off
        definition = (
            definition
            .next(task_enclosure)
            .next(
                sfn.Pass(
                    self,
                    f"{self.task_name} Run (Restructure)",
                    input_path=f"{self.task_result_path.as_reference}[0]",
                    result_path=self.task_result_path.as_reference,
                )
            )
        )
        # fmt: on

        # -------------
        # TASK FAILED
        # -------------
        if task__status_failed:
            task_enclosure.add_catch(
                sfn.Chain.start(task__status_failed)
                .to_single_state(
                    f"{self.task_name} Status Failed", result_path=sfn.JsonPath.DISCARD
                )
                .next(
                    sfn.Fail(
                        self,
                        f"{self.task_name} Fail State",
                        cause=f"Task {self.task_name} failed during execution.",
                    )
                ),
                result_path=self.task_error_path.as_reference,
            )

        # -------------
        # POST TASK
        # -------------
        if task__post_run:
            definition = definition.next(
                sfn.Chain.start(task__post_run).to_single_state(
                    f"{self.task_name} Post Run", result_path=sfn.JsonPath.DISCARD
                )
            )
        if task__status_completed:
            definition = definition.next(
                sfn.Chain.start(task__status_completed).to_single_state(
                    f"{self.task_name} Status Complete", result_path=sfn.JsonPath.DISCARD
                )
            )

        definition.next(
            sfn.Pass(self, f"{self.task_name} End", input_path=self.task_result_path.as_reference)
        )

        # ---------------------------
        # COMPLETE DEFINITION
        # ---------------------------
        return definition

    @property
    def task_input_path(self) -> JsonReferencePath:
        return JsonReferencePath("input")

    @property
    def task_context(self) -> Dict[str, Any]:
        return {}

    @property
    def task_result_path(self) -> JsonReferencePath:
        return JsonReferencePath("result")

    @property
    def task_error_path(self) -> JsonReferencePath:
        return JsonReferencePath("error")

    @property
    def task_context_path(self) -> JsonReferencePath:
        return JsonReferencePath("context")

    @property
    def task__augment_input(self) -> Optional[sfn.IChainable]:
        """Run right after the sfn.Pass 'START' of the state machine fragment. Can be used
        to dynamically fill out 'contexts' JsonReferencePath for subsequent lambdas/tasks to use.
        NOTE: Outputs of this chain are MAINTAINED
        """
        return None

    @property
    def task__status_started(self) -> Optional[sfn.IChainable]:
        """Runs right before "task__pre_run" if defined. Otherwise runs right before main
        task executes.
        NOTE: Outputs within this chain get DISCARDED
        """
        return None

    @property
    def task__status_completed(self) -> Optional[sfn.IChainable]:
        """Runs right after "task__post_run" if defined. Otherwise runs right after main
        task is completed.
        NOTE: Outputs within this chain get DISCARDED
        """
        return None

    @property
    def task__status_failed(self) -> Optional[sfn.IChainable]:
        """Runs if main task fails during execution"""
        return None

    @property
    def task__pre_run(self) -> Optional[sfn.IChainable]:
        """Runs right before main task executes.
        NOTE: Outputs within this chain get DISCARDED
        """
        return None

    @property
    def task__post_run(self) -> Optional[sfn.IChainable]:
        """Runs right after main task is completed
        NOTE: Outputs within this chain get DISCARDED
        """
        return None
