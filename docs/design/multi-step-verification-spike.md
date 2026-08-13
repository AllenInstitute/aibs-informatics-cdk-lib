# Multi-Step Demand Execution: exploration handoff

**Status:** exploration. Nothing here is decided, and nothing is implemented.
**Purpose:** capture an idea, and more importantly capture *a process for defining it*, so whoever
picks this up next — including future me — starts from the real questions rather than from scratch.

**Read first:** [`demand-execution.md`](./demand-execution.md) — the design reference for the system
as deployed. Citations here use its conventions (`cdk:`, `lambda:`, `aws-utils:`, `core:`), and are
against the same commits.

> **What this document is not.** It is not a design, an RFC, or a plan. It deliberately stops short
> of choosing. Where I have a leaning I say so and say why, but the open questions are the payload.

---

## The idea

Today one demand execution is one containerized job: stage inputs from S3 onto EFS → run one
container → stage outputs back to S3 → clean up.

The idea is to run **more than one step against the same staged data, without re-transferring it** —
framed as a *pre-check* before the main workload and a *post-check* after it. A verification set of
steps. Those checks might need their own container image, separate from the science image.

Concretely, the shape being explored:

```
stage in  →  [pre-check]  →  main step  →  [post-check]  →  stage out  →  cleanup
             └─────────── all against the same EFS working directory ──────────┘
```

## Why the architecture makes this cheap

Data movement is already bracketed *outside* the workload. The working directory
`{scratch}/{execution_id}` is created before the science job and persists until cleanup, and every
job in the execution mounts the same EFS file system. Nothing about the transfer machinery is
per-job — `setup_configs.data_sync_requests` runs before, `cleanup_configs.data_sync_requests` runs
after, and what happens in between is opaque to them.

So inserting steps between stage-in and stage-out costs **zero extra data movement**, by
construction. That is the whole reason this idea fits: you are not proposing to move data, you are
proposing to look at data that is already sitting there.

The `.demand.env` file is a second gift. It lives in the working directory
(`lambda:handlers/demand/context_manager.py:784-785`) and is sourced by the command's pre-commands.
If multiple steps each source it, **the env file becomes the shared contract between steps** — every
step sees the same `INPUT_BAM`, `OUT_DIR`, `WORKING_DIR`, `TMPDIR`. That is a genuinely nice property
and worth preserving in any design.

---

## First: this is two features, not one

This is the most important thing in the document. The idea bundles two things with wildly different
costs, and separating them changes what you should build.

### Feature A: same-image checks. **Already works today.**

The `command` is a `list[str]` that gets space-joined into a single shell string and run under
`bash -c`:

```python
command_string = " && ".join([" ".join(_) for _ in pre_commands + [command]])   # :792
...
command=["/bin/bash", "-c", command_string],                                    # :810
```

(`lambda:handlers/demand/context_manager.py:792,810`.)

So this is a working multi-step execution **right now, with no platform change**:

```jsonc
"command": ["verify-inputs.sh", "&&", "run.sh", "--in", "${INPUT_BAM}",
            "&&", "verify-outputs.sh", "${OUT_DIR}"]
```

`&&` short-circuits, the nonzero exit propagates out of `bash -c`, the Batch job fails, and the
execution fails. The env-var reference scan that decides what stays inline vs. what goes into
`.demand.env` already unions across the command *and* all pre-commands
(`context_manager.py:774`), so the environment contract holds for chained commands too.

**If this covers your use case, you are done, and the deliverable is documentation, not code.**

### Feature B: different image, different resources, distinguishable outcomes

What Feature A cannot do:

| Cannot | Why it matters |
|---|---|
| Use a different image per step | Your verification tooling has to be baked into the science image |
| Use different CPU/memory/GPU per step | A checksum runs on the GPU box at GPU prices |
| Tell you *which* step failed, from the state machine | Everything surfaces as one failed Batch job; you read container logs to find out |
| Do anything on failure except fail the whole execution | No "verify then decide whether to upload" |
| Record a verification *result* as opposed to a crash | Nowhere for "check ran, and here is the answer" to go |

**That is the actual feature request**: not the ability to run extra commands, but different
images/resources per step and *actionable, distinguishable outcomes*. Worth confirming that framing
before designing anything — if what you want is mostly "which step failed" and "don't upload bad
outputs," those are cheaper to attack than general multi-step orchestration.

### The cost that decides it

Every step that needs its own image is a separate AWS Batch job, which means a job-definition
register → submit → wait → deregister cycle (`cdk:constructs_/sfn/fragments/batch.py:39`), plus
Batch scheduling latency, plus instance provisioning if the queue is cold. For a 3-second checksum
that overhead plausibly dominates the check by one to two orders of magnitude.

**This number should be measured, not guessed** — see P5 below. It is the single input that decides
whether per-step images are worth it, or whether the answer is "bake verification tools into a
combined image and use Feature A."

---

## A process for defining it

Six steps. Each produces an artifact you can put in front of someone.

| | Step | Produces |
|---|---|---|
| **P1** | Write the caller-facing shape first | A JSON document a caller would submit |
| **P2** | Classify the step kinds you actually need | A table of step kinds × the axes that change implementation |
| **P3** | Decide failure semantics per step kind | A matrix of on-failure behaviors — **this is the design** |
| **P4** | Pick the seam | One named extension point, checked against existing ones |
| **P5** | Prototype the cheapest thing that tests the semantics | A measurement and a working/failing example |
| **P6** | Write the non-goals | A list of what v1 will not do |

The ordering is deliberate. The system is declarative — the model *is* the API — so the caller-facing
shape constrains everything downstream. And P3 before P4 because failure semantics are the hard part;
if you pick a seam first you will pick one that cannot express the semantics you need.

---

# Working the process

## P1: Write the caller shape first

If you cannot write the JSON cleanly, the design is not ready. Here is a strawman to argue with,
extending the worked example in the reference doc:

```jsonc
{
  "execution_type": "demo",
  "execution_id": "exec-123",
  "execution_parameters": {
    "params": {
      "input-bam": "s3://bucket/prefix/sample.bam",
      "out_dir": "results/",
      "threads": 8
    },
    "inputs": ["input-bam"],
    "outputs": ["out_dir"],
    "output_s3_prefix": "s3://out-bucket/run1",

    "steps": [
      { "name": "precheck",
        "image": "acct.dkr.ecr…/qc:2",
        "command": ["verify-inputs.sh", "${INPUT_BAM}"],
        "resource_requirements": { "vcpus": 1, "memory": 2048 },
        "on_failure": "fail_execution" },

      { "name": "main",
        "image": "acct.dkr.ecr…/demo:1",
        "command": ["run.sh", "--in", "${INPUT_BAM}", "--out", "${OUT_DIR}"],
        "resource_requirements": { "gpu": 1, "vcpus": 8, "memory": 61440 } },

      { "name": "postcheck",
        "image": "acct.dkr.ecr…/qc:2",
        "command": ["verify-outputs.sh", "${OUT_DIR}"],
        "resource_requirements": { "vcpus": 1, "memory": 2048 },
        "on_failure": "fail_before_upload" }
    ]
  }
}
```

**Now attack it.** Every one of these is a real question, not a nit:

1. **`execution_image` and `command` are currently required, top-level, singular** (`core:models/demand_execution/model.py:17`, `parameters.py:58`). Do steps *replace* them, or *inherit* them as defaults? Backwards compatibility says the single-step shape must keep working — is a one-element `steps` list the same thing as today's shape, exactly?
2. **`DemandResourceRequirements` is per-execution, not per-step** (`core:models/demand_execution/resource_requirements.py:8`). Moving it per-step is the change that unlocks the cost saving. What does a step that omits it inherit?
3. **Which step's outputs get uploaded?** `outputs` and `output_s3_prefix` are execution-level. If `postcheck` writes a QC report, is that an output? Options: outputs stay execution-level and any step may write them; or each step declares its own outputs. The second is more expressive and much more model surface.
4. **What is the job queue per step?** Today the science job's queue is caller-supplied data (`context_manager.py:829-844`). Per-step queues, or one queue for the execution and per-step resources within it?
5. **Ordering and concurrency.** Implicit by list order? Can two steps run in parallel? (Strong leaning: v1 sequential only. Parallel steps mean a dependency graph, and you are then building a workflow engine inside a workflow engine.)
6. **Can a step be conditional?** "Only run postcheck if the main step wrote anything." That is a `Choice` state or handler-side filtering — see P4.
7. **Do steps need their own params?** If yes, read the serialization trap note below before going further.

## P2: Classify the step kinds you actually need

Do not design for "arbitrary steps." Enumerate the kinds you have real use cases for, and score them
on the axes that actually change the implementation:

| Axis | Why it changes the implementation |
|---|---|
| Same image as the main step? | Same → Feature A, nearly free. Different → a whole Batch job. |
| Different CPU/memory/GPU? | Requires per-step resource requirements, and possibly a per-step queue. |
| Reads only, or also writes? | A writing step's output may need staging out, which reorders the graph. |
| Can it fail the execution? | Drives P3, and requires SFN error handling that does not exist today. |
| Always runs, or conditional? | Conditional needs a `Choice` state or handler-side filtering. |
| Needs its own params? | Serialization trap territory. |

My guess is you have two or three kinds, not N: *advisory check* (records a result, never fails the
run), *gate* (fails the run, cheap, runs before the expensive thing), and *verification* (fails
**after** the expensive thing, and the question is what happens to the outputs). If that is right,
design for exactly those three and name them in the model rather than exposing a generic
`on_failure` enum.

## P3: Decide failure semantics. This is the design.

Everything else is plumbing. The interesting case is the one your idea is actually about:

> The post-check fails. The outputs exist on EFS. **Do you upload them?**

Today, the answer is the worst possible one. There is **no `Catch` and no top-level `Retry` on the
`demand-execution` state machine** (`cdk:constructs_/sfn/fragments/informatics/demand_execution.py:1-383`).
A failing post-check would fail the execution *before* stage-out and cleanup — so the outputs sit on
EFS, unuploaded, with no signal beyond a failed execution, until the daily janitor reclaims them
three days later. That is the current behavior for *any* science-job failure, and it is already the
system's sharpest operational edge.

So a candidate vocabulary, with what each actually requires:

| `on_failure` | Semantics | Requires |
|---|---|---|
| `fail_execution` | Today's behavior. Execution fails, no stage-out, no cleanup, data stranded. | nothing new |
| `fail_before_upload` | Fail, skip stage-out, **but still clean up**. Don't strand data. | a `Catch` routing to the cleanup chain |
| `fail_after_upload` | Upload anyway, then fail — you want the bad artifacts for debugging. | reordering, plus a story for how consumers know the data is suspect |
| `record_and_continue` | Advisory. Result recorded, execution proceeds. | somewhere to *put* a result — see below |
| `skip_remaining` | Stop the chain, treat the execution as succeeded. | `Choice` on step result |

**Pick these deliberately and write down the reasoning.** Two traps:

- `fail_after_upload` sounds friendly and is dangerous: uploaded outputs that failed verification look
  identical to good ones. If you want it, it probably needs a quarantine prefix or an object tag, and
  that is its own sub-design. Don't let it in by accident.
- `record_and_continue` is worthless without observability. **No demand execution handler emits any
  metric today**, despite `MetricsMixins` being available on the handler base class
  (`lambda:common/metrics.py:123`), and `lambda:handlers/notifications/` (SES + SNS notifiers) is
  wired to nothing. A check whose result goes only to CloudWatch logs is not a check — it is a log
  line. **If verification is to mean anything operationally, closing that gap is part of this feature,
  not a follow-up.** This may be the most under-appreciated cost in the whole idea.

## P4: Pick the seam

Two implementation paths.

**Path 1 — topology change in CDK.** Add states to `DemandExecutionFragment`
(`cdk:constructs_/sfn/fragments/informatics/demand_execution.py:26`): a pre-check state before
`Submit Batch Job`, a post-check state after. Simple, and rigid: the number and shape of steps is
baked into the deployed state machine, and every change to step structure is a cdk-lib change plus a
redeploy.

**Path 2 — steps as data. Recommended.** Make the scaffolding handler emit an ordered *list* of job
arguments, and have the state machine `Map` over it with `max_concurrency=1`. One topology change,
after which adding or reordering steps is caller data.

Path 2 is recommended because **the precedent already exists in this exact state machine.** Input
transfers are already a `Map` over a list produced by scaffolding:

```
setup_configs.data_sync_requests   →  Map  →  data-sync-v2      (cdk:…/demand_execution.py:215-229)
setup_configs.step_requests        →  Map(max_concurrency=1)  →  submitJob.sync     ← the proposal
```

What Path 2 touches:

- `CreateDefinitionAndPrepareArgsHandler` (`lambda:handlers/batch/create.py:85`) returns one
  `CreateDefinitionAndPrepareArgsResponse` (job_name, job_definition_arn, job_queue_arn, parameters,
  container_overrides). It would need to return a list, or be invoked per step.
- `generate_batch_job_builder` (`lambda:handlers/demand/context_manager.py:630`) builds command +
  environment + mounts for *the* job. Becomes per-step. The env-file split logic
  (`:774-777`) already scans across multiple command lists, which is encouraging.
- `Submit Batch Job` is a raw `CustomState` reading `$.config.scaffolding.setup_results.batch_args`
  (`cdk:…/demand_execution.py:232-259`). Becomes a `Map` iterator over a list.
- `$.tasks.batch_submit_task` holds one Batch result. Becomes per-step, which is what makes "which
  step failed" answerable from the state machine.

**One concrete trap.** `job_name` uses the *strict* execution hash — execution_type + image + command
+ execution_id + sanitized params + inputs + outputs — while `job_definition_name` uses the
non-strict hash of type + image + command only (`context_manager.py:804-809`). Consequences:

- Different image per step → different job definition automatically. Convenient, and free.
- Two steps with the **same image and same command** hash identically → **identical `job_name`**.
  Batch permits duplicate job names, so this is not fatal, but it makes logs and job lookups
  ambiguous. A step name or index has to enter the job name.

**And the serialization trap.** If steps or their params ride on resolvables, note that
`sanitize_serialized_params` collapses every `Resolvable` to a two-position string (`"{remote} @
{local}"` / `"{local} @ {remote}"`) and **silently drops any field not expressible there**
(`core:models/demand_execution/parameters.py:425-448`, `resolvables.py:188-196`). This already blocks
OCSDV-453. Keep per-step data on its own model, not smuggled through resolvables.

> **Coordinate with whoever is on OCSDV-453.** The `aibs-informatics-aws-lambda` checkout in this
> workspace is currently on `feature/OCSDV-453-demand-execution-sync-filters`, which is touching
> exactly this serialization boundary. Whatever they land will shape what is cheap here.

## P5: Prototype the cheapest thing that tests the semantics

Do not build multi-image orchestration to find out how failure handling behaves. Two experiments,
both small:

1. **Measure the overhead that decides Feature A vs. B.** Submit a trivial Batch job to the
   `demand-infra-lambda` queue, warm and cold, and record wall-clock from `submitJob` to terminal
   state. That number decides whether per-step images are worth it. Everything in the "two features"
   section above hangs on it.
2. **Test the failure semantics with Feature A.** Run a demand execution whose command is
   `run.sh && exit 1`. Confirm what you already expect from reading the code: execution fails,
   outputs stay on EFS, nothing uploads, nothing cleans up, no notification. Now you have the
   baseline behavior in front of you, and the `on_failure` table in P3 stops being abstract.

Experiment 2 costs one execution and is worth doing before any design meeting.

## P6: Write the non-goals

A guess at a defensible v1, to argue with:

**In:** sequential steps; per-step image; per-step resource requirements; per-step outcome visible in
the state machine; `fail_execution` and `fail_before_upload`.

**Out (say so explicitly):** parallel steps; conditional steps; per-step inputs/outputs and per-step
staging; `fail_after_upload` and quarantine semantics; retry of an individual step; per-step queues;
resuming a failed execution from a given step.

Every one of those "out" items is a plausible follow-up. Naming them now is what keeps v1 from
becoming a workflow engine.

---

## Before designing anything: `ParamSetPair` may already be this

**Highest-value thing to check first.**

`ParamPair` / `ParamSetPair` (`core:models/demand_execution/param_pair.py:14,60`) express which inputs
feed which outputs. They are fully built out: validated (`parameters.py:116-138`), exposed as
`param_set_pairs` / `job_param_set_pairs` (`:213,251`), caller-settable via `param_pair_overrides`
(`:65`), and they have even been through a deprecation migration — `param_pairs`,
`param_set_pairs`, and `param_set_pair_overrides` are all accepted and rewritten to the current field
name (`:389-421`).

Two findings:

1. **They have no consumer.** A search across `aibs-informatics-aws-lambda`,
   `aibs-informatics-cdk-lib`, and `aibs-informatics-aws-utils` finds zero references. Nothing outside
   `core`'s own models reads them.
2. **The docstrings are written in terms of jobs, plural.** `ParamPair`'s docstring enumerates
   scenarios as *"a single job that has both inputs and outputs (Most common)"*, *"a single job that
   has no inputs or outputs"*, and so on. `ParamSetPair`'s does the same. That phrasing only makes
   sense if an execution could contain more than one job.

And `git log --follow` on `param_pair.py` runs back to the commit `adding demand execution` — this is
**original design surface from the first day of the system, never consumed, that appears to have been
about grouping params per job.**

**Action:** find whoever wrote it and ask what it was for. If multi-job-per-execution was the original
intent, this exploration is resuming a deferred design rather than inventing one, and the existing
model may already be the right shape for expressing which params belong to which step. That is worth a
15-minute conversation before writing any new model.

## The alternative worth rejecting explicitly

**"Just compose several demand executions in an outer state machine."**

Reject it, for a specific reason worth writing down: every demand execution stages in from S3 and
stages out to S3, and its working directory is `{scratch}/{execution_id}` — a different path per
execution — which cleanup deletes at the end. So a second execution cannot see the first's outputs on
EFS. It would re-download from S3, which is exactly the transfer you are trying to avoid.

The content-addressed shared cache does not rescue this either: it is keyed on the remote S3 URI
(`context_manager.py:525`), it is off by default (`isolate_inputs=True`), and it only ever caches
*inputs*. A post-check needs the main step's *outputs*, which never enter it.

This is the argument for why the steps must live inside one execution, and it is the cleanest
justification for the whole idea. Lead with it.

## Questions only you and the team can answer

1. **What are the checks actually checking?** Checksums and file-existence? Schema or format
   validation? Scientific QC metrics? Cost or size guards? This determines whether checks need
   a real toolchain (→ separate image) or are shell one-liners (→ Feature A).
2. **Who consumes a verification result?** A human reading a dashboard, an automated gate, or a
   downstream pipeline? Nothing today can carry that answer anywhere, so whoever consumes it defines
   how much observability work this feature includes.
3. **Is "verification failed" an error or a finding?** If executions are *expected* to fail
   verification sometimes as a normal outcome, then failure-as-crash is wrong and you need result
   reporting — which is a much bigger change than adding steps.
4. **Is there an existing ticket or prior proposal?** DT-9913 covers EFS lifecycle; OCSDV-452/453
   cover sync filtering. Nothing found for multi-step, but it is worth checking Confluence (space
   `DAS`) and asking, given the `ParamSetPair` finding.
5. **Does anything outside these four repos already do this?** `DemandExecutionFragment` is a library
   construct; another stack could be wiring its own multi-step pattern around it already.

## Pointers

| Thing | Where |
|---|---|
| System reference | [`demand-execution.md`](./demand-execution.md) |
| Command assembly (Feature A) | `lambda:handlers/demand/context_manager.py:692-696,774-777,792,810` |
| The one job-submission state | `cdk:constructs_/sfn/fragments/informatics/demand_execution.py:232-259` |
| The Map-over-a-list precedent | `cdk:…/demand_execution.py:215-229` |
| Per-execution job args | `lambda:handlers/batch/create.py:85` |
| Dormant param-pair surface | `core:models/demand_execution/param_pair.py:14,60` |
| Failure semantics today | `demand-execution.md` §12.3 |
| Unused metrics / notifications | `lambda:common/metrics.py:123`, `lambda:handlers/notifications/` |
