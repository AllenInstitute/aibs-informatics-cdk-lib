"""Step Functions state machine fragment constructs.

This module provides base classes and utilities for building Step Functions
state machine fragments and workflows.
"""

from abc import abstractmethod
from collections.abc import Mapping, Sequence
from typing import Any, TypeVar, cast

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
    removal_policy: cdk.RemovalPolicy | None = None,
    retention: logs_.RetentionDays | None = None,
) -> sfn.LogOptions:
    """Create log options for a state machine.

    Args:
        scope (constructs.Construct): The construct scope.
        id (str): Identifier for the log group.
        env_base (EnvBase): Environment base for naming.
        removal_policy (Optional[cdk.RemovalPolicy]): Removal policy.
            Defaults to DESTROY.
        retention (Optional[logs_.RetentionDays]): Log retention period.
            Defaults to ONE_MONTH.

    Returns:
        Log options configured with a CloudWatch log group.
    """
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
    managed_policies: Sequence[iam.IManagedPolicy | str] | None = None,
    inline_policies: Mapping[str, iam.PolicyDocument] | None = None,
    inline_policies_from_statements: Mapping[str, Sequence[iam.PolicyStatement]] | None = None,
    include_default_managed_policies: bool = True,
) -> iam.Role:
    """Create an IAM role for a state machine.

    Args:
        scope (constructs.Construct): The construct scope.
        id (str): Identifier for the role.
        env_base (EnvBase): Environment base for naming.
        assumed_by (iam.IPrincipal): Principal that can assume the role.
            Defaults to states.amazonaws.com.
        managed_policies (Optional[Sequence[Union[iam.IManagedPolicy, str]]]):
            Managed policies to attach.
        inline_policies (Optional[Mapping[str, iam.PolicyDocument]]):
            Inline policy documents.
        inline_policies_from_statements (Optional[Mapping[str, Sequence[iam.PolicyStatement]]]):
            Inline policies from statements.
        include_default_managed_policies (bool): Include default Step Functions
            managed policies. Defaults to True.

    Returns:
        The created IAM role.
    """
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
            *(managed_policies or []),  # type: ignore[list-item] # mypy does not understand that ManagedPolicy is an IManagedPolicy
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
    """Mixin class providing state machine helper methods.

    Provides methods for retrieving Lambda functions and state machines
    with caching support.
    """

    def get_fn(self, function_name: str) -> lambda_.IFunction:
        """Get a Lambda function by name.

        Args:
            function_name (str): The function name.

        Returns:
            The Lambda function interface.
        """
        cache_attr = "_function_cache"
        if not hasattr(self, cache_attr):
            setattr(self, cache_attr, {})
        resource_cache = cast(dict[str, lambda_.IFunction], getattr(self, cache_attr))
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
        """Get a state machine by name.

        Args:
            state_machine_name (str): The state machine name.

        Returns:
            The state machine interface.
        """
        cache_attr = "_state_machine_cache"
        if not hasattr(self, cache_attr):
            setattr(self, cache_attr, {})
        resource_cache = cast(dict[str, sfn.IStateMachine], getattr(self, cache_attr))
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
    name: str | None,
    definition: sfn.IChainable,
    role: iam.Role | None = None,
    logs: sfn.LogOptions | None = None,
    timeout: cdk.Duration | None = None,
) -> sfn.StateMachine:
    """Create a state machine from a definition.

    Args:
        scope (constructs.Construct): The construct scope.
        env_base (EnvBase): Environment base for naming.
        id (str): Construct identifier.
        name (Optional[str]): State machine name.
        definition (sfn.IChainable): The state machine definition.
        role (Optional[iam.Role]): IAM role for the state machine.
        logs (Optional[sfn.LogOptions]): Log options.
        timeout (Optional[cdk.Duration]): Execution timeout.

    Returns:
        The created state machine.
    """
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
    """Base class for state machine fragments.

    Provides common functionality for building reusable state machine
    fragments with definition management.
    """

    @property
    def definition(self) -> sfn.IChainable:
        """Get the state machine definition.

        Returns:
            The chainable definition.
        """
        return self._definition

    @definition.setter
    def definition(self, value: sfn.IChainable):
        """Set the state machine definition.

        Args:
            value (sfn.IChainable): The definition to set.
        """
        self._definition = value

    @property
    def start_state(self) -> sfn.State:
        """Get the start state.

        Returns:
            The definition's start state.
        """
        return self.definition.start_state

    @property
    def end_states(self) -> list[sfn.INextable]:
        """Get the end states.

        Returns:
            List of nextable end states.
        """
        return self.definition.end_states

    def enclose(
        self,
        id: str | None = None,
        input_path: str | None = None,
        result_path: str | None = None,
    ) -> sfn.Chain:
        """Enclose the fragment within a parallel state.

        Args:
            id (Optional[str]): Identifier for the parallel state.
                Defaults to the node ID.
            input_path (Optional[str]): Input path for the enclosed state.
                Defaults to "$".
            result_path (Optional[str]): Result path for output.
                Defaults to same as input_path.

        Returns:
            The enclosed state machine fragment as a chain.
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
    """Environment-aware state machine fragment.

    Combines StateMachineFragment with environment base naming conventions
    and state machine mixins.
    """

    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        env_base: EnvBase,
    ) -> None:
        """Initialize an environment-aware state machine fragment.

        Args:
            scope (constructs.Construct): The construct scope.
            id (str): The construct ID.
            env_base (EnvBase): Environment base for resource naming.
        """
        super().__init__(scope, id)
        self.env_base = env_base

    def to_single_state(
        self,
        *,
        prefix_states: str | None = None,
        state_id: str | None = None,
        comment: str | None = None,
        input_path: str | None = None,
        output_path: str | None = "$[0]",
        result_path: str | None = None,
        result_selector: Mapping[str, Any] | None = None,
        arguments: Mapping[str, Any] | None = None,
        parameters: Mapping[str, Any] | None = None,
        query_language: sfn.QueryLanguage | None = None,
        state_name: str | None = None,
        assign: Mapping[str, Any] | None = None,
        outputs: Any = None,
    ) -> sfn.Parallel:
        """Convert the fragment to a single parallel state.

        Args:
            prefix_states (Optional[str]): Prefix for state names.
            state_id (Optional[str]): ID for the parallel state.
            comment (Optional[str]): Comment for the state.
            input_path (Optional[str]): Input path.
            output_path (Optional[str]): Output path. Defaults to "$[0]".
            result_path (Optional[str]): Result path.
            result_selector (Optional[Mapping[str, Any]]): Result selector.

        Returns:
            A parallel state containing the fragment.
        """
        return super().to_single_state(
            prefix_states=prefix_states,
            state_id=state_id,
            comment=comment,
            input_path=input_path,
            output_path=output_path,
            result_path=result_path,
            result_selector=result_selector,
            arguments=arguments,
            parameters=parameters,
            query_language=query_language,
            state_name=state_name,
            assign=assign,
            outputs=outputs,
        )

    def to_state_machine(
        self,
        state_machine_name: str,
        role: iam.Role | None = None,
        logs: sfn.LogOptions | None = None,
        timeout: cdk.Duration | None = None,
    ) -> sfn.StateMachine:
        """Convert the fragment to a complete state machine.

        Args:
            state_machine_name (str): Name for the state machine.
            role (Optional[iam.Role]): IAM role. Creates default if None.
            logs (Optional[sfn.LogOptions]): Log options.
            timeout (Optional[cdk.Duration]): Execution timeout.

        Returns:
            The created state machine.
        """
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
    def required_managed_policies(self) -> Sequence[iam.IManagedPolicy | str]:
        """Get required managed policies for this fragment.

        Returns:
            Sequence of required managed policies.
        """
        return []

    @property
    def required_inline_policy_statements(self) -> Sequence[iam.PolicyStatement]:
        """Get required inline policy statements for this fragment.

        Returns:
            Sequence of required policy statements.
        """
        return []


class LazyLoadStateMachineFragment(EnvBaseStateMachineFragment):
    """State machine fragment with lazy-loaded definition.

    The definition is built on first access via build_definition().
    """

    @property
    def definition(self) -> sfn.IChainable:
        """Get the state machine definition, building if needed.

        Returns:
            The chainable definition.
        """
        try:
            return self._definition
        except AttributeError:
            self.definition = self.build_definition()
            return self.definition

    @definition.setter
    def definition(self, value: sfn.IChainable):
        """Set the state machine definition.

        Args:
            value (sfn.IChainable): The definition to set.
        """
        self._definition = value

    @abstractmethod
    def build_definition(self) -> sfn.IChainable:
        """Build the state machine definition.

        Subclasses must implement this method.

        Returns:
            The built definition.
        """
        raise NotImplementedError("Must implement")


class TaskWithPrePostStatus(LazyLoadStateMachineFragment):
    """State machine fragment with pre/post status tracking.

    Wraps a task with status updates for started, failed, and completed states.

    """

    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        env_base: EnvBase,
        task: sfn.IChainable | None,
    ) -> None:
        """Initialize a task with pre/post status tracking.

        Args:
            scope (constructs.Construct): The construct scope.
            id (str): The construct ID.
            env_base (EnvBase): Environment base for resource naming.
            task (Optional[sfn.IChainable]): The task to wrap.
        """
        super().__init__(scope, id, env_base)
        self.task = task
        self.task_name = id

        self.raw_task_input_path = JsonReferencePath("$")

    @property
    def task(self) -> sfn.IChainable:
        """Get the wrapped task.

        Returns:
            The task.

        Raises:
            AssertionError: If task is not set.
        """
        assert self._task, "Task must be set"
        return self._task

    @task.setter
    def task(self, value: sfn.IChainable | None):
        """Set the wrapped task.

        Args:
            value (Optional[sfn.IChainable]): The task to set.
        """
        self._task = value

    @property
    def task_name(self) -> str:
        """Get the task name.

        Returns:
            The task name.
        """
        return self._task_name

    @task_name.setter
    def task_name(self, value: str):
        """Set the task name.

        Args:
            value (str): The name to set.
        """
        self._task_name = value

    def build_definition(self) -> sfn.IChainable:
        """Build the task definition with status tracking.

        Returns:
            The complete task definition with pre/post status states.
        """
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
        definition: sfn.Chain | sfn.Pass = sfn.Pass(
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
    def task_context(self) -> dict[str, Any]:
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
    def task__augment_input(self) -> sfn.IChainable | None:
        """Run right after the sfn.Pass 'START' of the state machine fragment. Can be used
        to dynamically fill out 'contexts' JsonReferencePath for subsequent lambdas/tasks to use.
        NOTE: Outputs of this chain are MAINTAINED
        """
        return None

    @property
    def task__status_started(self) -> sfn.IChainable | None:
        """Runs right before "task__pre_run" if defined. Otherwise runs right before main
        task executes.
        NOTE: Outputs within this chain get DISCARDED
        """
        return None

    @property
    def task__status_completed(self) -> sfn.IChainable | None:
        """Runs right after "task__post_run" if defined. Otherwise runs right after main
        task is completed.
        NOTE: Outputs within this chain get DISCARDED
        """
        return None

    @property
    def task__status_failed(self) -> sfn.IChainable | None:
        """Runs if main task fails during execution"""
        return None

    @property
    def task__pre_run(self) -> sfn.IChainable | None:
        """Runs right before main task executes.
        NOTE: Outputs within this chain get DISCARDED
        """
        return None

    @property
    def task__post_run(self) -> sfn.IChainable | None:
        """Runs right after main task is completed
        NOTE: Outputs within this chain get DISCARDED
        """
        return None
