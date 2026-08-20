# AIBS Informatics CDK Library

A CDK construct library for AIBS Informatics services. Alongside asset provisioning, it owns
**demand execution**: running a declaratively described containerized scientific workload, and
composing several of them into one piece of work that shares its staged data.

## Language

### Composition

**Demand Execution Group**:
The unit of composition: an ordered list of Phases run as one piece of work, sharing staged data.
A lone Demand Execution is a Group of one.
_Avoid_: pipeline, workflow, multi-step demand execution (the first two are what science teams
call their Nextflow-shaped things; the third puts "demand execution" inside the name of the thing
that contains demand executions)

**Phase**:
A set of Steps within a Group that have no dependencies on each other and may therefore run
concurrently.
_Avoid_: stage (taken twice already — the deployment environment, and stage-in/stage-out), wave,
layer, generation

**Step**:
One member of a Phase. A Step is a Demand Execution; it is not a distinct kind of thing.
_Avoid_: task, sub-execution, job (a job is the compute unit a Step runs on, not the Step)

**Edge**:
A Step's output serving as a later Step's input, without leaving the shared filesystem.
_Avoid_: dependency, link, connection

**Lowering**:
The transformation, performed before submission, from an authored dependency graph into the
ordered Phases that are actually submitted. A Group is always submitted lowered; the graph is
never transmitted.
_Avoid_: compiling, converting, planning, scheduling

### Data placement

**Working Directory**:
The scratch area belonging to one Demand Execution, into which its inputs are staged and in which
its command runs.
_Avoid_: workspace, scratch dir (scratch is the volume role, not this directory)

**Destination**:
A place an output is written after its Step's command succeeds. An output may have more than one.
_Avoid_: target, sink, upload location

**Intermediate Output**:
An output whose only Destinations are inside the Group. It exists to feed a later Step and is
never persisted outside the run.
_Avoid_: temp output, scratch output

**Persisted Output**:
An output with a Destination outside the Group, which therefore survives the run and its cleanup.
_Avoid_: final output, real output, saved output
