# Multi-Step Demand Execution

**Status:** design. Agreed in a design session on 2026-08-19. **Nothing is implemented.**
**Previously:** this file was `multi-step-verification-spike.md`, an exploration of a
*verification* feature (pre-check / post-check around one workload). That scope was
generalized during the design session: verification is now one use case among several, and
the feature is general multi-step composition. The rename is deliberate; the old title
described something narrower than what is designed here.

**Read first:** [`demand-execution.md`](./demand-execution.md) — the design reference for the
system as deployed. Citations here use its conventions (`cdk:`, `lambda:`, `aws-utils:`,
`core:`) and are verified against `main` in each repo.

> **Not in `mkdocs.yml` nav, deliberately.** This repo's docs site publishes to
> <https://alleninstitute.github.io/aibs-informatics-cdk-lib/>. This document names latent
> bugs, a data-loss hazard, and internal ticket IDs. Keep it out of `nav` unless someone
> decides otherwise deliberately.

---

## The 60-second version

Run several containerized steps against the same staged data, without round-tripping
intermediates through S3.

A **Demand Execution Group** is an ordered list of **phases**; a phase is a set of **steps**
that may run concurrently; a **step is an ordinary `DemandExecution`**. Steps pass data by
**edges**: step A's stage-out destination is redirected from `s3://` to an `efs://` path
inside step B's working directory, so the bytes move filesystem-to-filesystem and never
leave EFS. The dependency graph is resolved **caller-side, at compile time**, into the phase
list; the runtime only ever sees a list of lists.

Because a step is a whole `DemandExecution`, per-step image, per-step resource requirements,
per-step job queue, and per-step failure visibility all come for free — those were the
actual feature request, and none of them needs a new model.

---

## What changed from the exploration

The previous version of this document was an exploration handoff. Three of its findings did
not survive, and they are recorded here so nobody re-derives them.

### `ParamSetPair` is not this

The handoff's highest-priority open question was whether `ParamPair` / `ParamSetPair`
(`core:models/demand_execution/param_pair.py`) was a deferred multi-job-per-execution design,
on the evidence that it is fully built out, caller-settable, migrated through deprecated field
names, and has **no consumer outside `core`**. The zero-consumer finding holds on `main` across
all repos. The inference does not: **the author confirms it was for connecting links between
inputs and outputs, not for grouping params per job.**

A supporting detail in the handoff was also wrong. It claimed the docstrings "are written in
terms of jobs, plural." They are not — every scenario clause says *"a single job"*, singular,
and that phrase recurs twelve or more times across `ParamPair`, `ParamSetPair`, `JobParamPair`
and `JobParamSetPair`. The only real signal was that a job-level noun appears in a parameter
model at all, which is weaker evidence than was claimed.

### `outputs_metadata` is a second dead field, and cannot be used here

`DemandExecutionParameters.outputs_metadata: dict[str, dict[str, JSON]]` is declared,
round-trips through serialization, and has **zero read sites on any branch of any repo**. It
looked like a natural home for per-output destination data. It is not usable for that, because
it is **absent from `get_execution_hash`** in both strict and non-strict mode: two executions
differing only in `outputs_metadata` produce an identical `job_name`.

### "Composing separate demand executions does not work" is overturned

The handoff argued this was the cleanest justification for putting steps inside one execution:
every demand execution stages in from S3, uses its own `{scratch}/{execution_id}`, and deletes
it at cleanup, so a second execution cannot see the first's outputs on EFS.

Every clause of that is still true. The design here defeats it by **changing the premise**
rather than contradicting it: the first execution's outputs are not written to S3 and then
re-fetched, they are written *directly into the second execution's working directory* as that
execution's stage-out destination. Nothing has to survive a cleanup, because the transfer
happens before the producing execution's cleanup runs.

---

# Part I: The model

## 1. Vocabulary

Full definitions in `CONTEXT.md` (repo root).

| Term | Meaning |
|---|---|
| **Demand Execution Group** | The unit of composition. Carries a `group_id` and an ordered list of phases. |
| **Phase** | A set of steps with no dependencies between them, which may run concurrently. |
| **Step** | One `DemandExecution`. Not a new type. |
| **Edge** | One step's output becoming another step's input, without passing through S3. |

A standalone `DemandExecution` is a group of one, containing one phase, containing one step.
That equivalence is what makes the backward-compatibility story a single sentence.

## 2. What the caller submits

The converter runs caller-side and resolves everything before submission, so the wire shape
is deliberately dull: every element is a `DemandExecution` valid under today's schema, and
any one of them can be pulled out and submitted by hand to debug it.

```jsonc
{
  "group_id": "grp-1",
  "phases": [
    [
      {
        "execution_type": "demo",
        "execution_id": "grp-1-prep",
        "execution_image": "acct.dkr.ecr…/prep:3",
        "resource_requirements": { "vcpus": 2, "memory": 4096 },
        "execution_parameters": {
          "command": ["prep.sh", "--in", "${RAW_BAM}", "--out", "${CLEAN_BAM}"],
          "params": {
            "raw-bam":   "s3://bucket/prefix/sample.bam",
            "clean-bam": "clean.bam"
          },
          "inputs":  ["raw-bam"],
          "outputs": ["clean-bam"],
          "output_destinations": {
            "clean-bam": ["efs://fs-1:/scratch/grp-1-call/clean.bam"]
          }
        }
      }
    ],
    [
      {
        "execution_type": "demo",
        "execution_id": "grp-1-call",
        "execution_image": "acct.dkr.ecr…/caller:7",
        "resource_requirements": { "gpu": 1, "vcpus": 8, "memory": 61440 },
        "execution_platform": { "aws_batch": { "job_queue_name": "gpu-queue" } },
        "execution_parameters": {
          "command": ["call.sh", "--in", "${CLEAN_BAM}", "--out", "${VARIANTS}"],
          "params": {
            "clean-bam": "clean.bam",
            "variants":  "variants.vcf"
          },
          "outputs": ["variants"],
          "output_s3_prefix": "s3://out-bucket/run1"
        }
      }
    ]
  ]
}
```

Three things to notice.

**The edge is `output_destinations` on the producer, not a reference on the consumer.** Step
`grp-1-call` does not mention `grp-1-prep` at all. It declares `clean-bam` as a plain local
param and expects a file to be there. The converter, which knows both working directories
before either execution starts, wrote the destination into the producer. This keeps steps
independently submittable: `grp-1-call` submitted alone is a valid execution that happens to
require a file already present.

**The intermediate never touches S3.** `grp-1-prep` has no `output_s3_prefix`. Its only
destination is an EFS path. Only the terminal step persists.

**Per-step image, resources and queue are already expressible**, because a step is a
`DemandExecution` and those are already fields on it. Nothing was added for them.

## 3. Why the converter can do this at compile time

The whole design rests on one property: **a working directory is computable before the
execution runs**. `container_working_path` is
`scratch_mount_point.as_mounted_path(execution_id)` — literally `{scratch}/{execution_id}` —
`execution_id` is caller-supplied, and the scratch mount point is fixed at synth time. So the
converter knows exactly where `grp-1-call` will work while it is still building
`grp-1-prep`'s request.

Output S3 paths are equally predictable: `remote_value` is plain concatenation,
`f"{output_s3_prefix}/{param_value}"`, with no hashing and no nondeterminism. Nothing about
edge resolution needs to observe a running execution.

---

# Part II: The decisions

Each of the first three has an ADR. This section states them and their reasoning in one place.

## D1: A step is a demand execution, not a new step type

The alternative was an ordered `steps` list *inside* one `DemandExecution`, with
`execution_image`, `command` and `resource_requirements` moving per-step. That is a smaller
runtime change — the staging bracket and cleanup already sit at execution level and would not
have to move.

It was rejected because making a step a whole `DemandExecution` yields, at no modelling cost,
every capability the exploration listed as the real feature request: per-step image, per-step
resources, per-step **queue** (which the handoff had parked as a non-goal), and per-step
outcome visible in the state machine, since each step is its own child execution.

It also dissolves a trap the handoff called out. Two steps with the same image and command
would have produced an identical `job_name`, because that name comes from the strict execution
hash. The strict hash includes `execution_id`, and ids are now `grp-1-prep` and `grp-1-call` —
distinct by construction. No step index needs to enter the job name.

**The cost is real and is stated in Part V:** each step now pays the full infrastructure
bracket, so a group runs roughly twice the infrastructure jobs that steps-inside-one-execution
would have.

## D2: Edges are redirected stage-out, not transfer steps

An edge is not a new operation. It is the producing step's existing post-execution transfer,
with its destination changed from an `s3://` URI to an `efs://` URI inside the consumer's
working directory.

This was chosen over two alternatives. **S3-mediated references** — B names A's output S3 URI —
already work today with no platform change at all, since output paths are computable in
advance; they were rejected only because they keep the round-trip the feature exists to remove.
**A distinct transfer state between executions** was rejected because it adds a Batch job per
edge, which is precisely the cost being avoided.

Redirected stage-out adds **no new state machine states**, reuses the existing "Transfer Results
FROM Batch Job" `Map`, and — because it runs during the producer's post-execution phase, before
the producer's cleanup — requires no deferral of cleanup anywhere.

The machinery already supports it. `DataSyncOperations.sync` dispatches on
`isinstance(path, S3Path)` and routes anything else to `sync_local_to_local`; `DataSyncTask`
types both `source_path` and `destination_path` as `S3Path | EFSPath | Path` with no cross-field
validator; `data-sync-v2` imposes no scheme constraint and mounts the EFS root access point, so
it can resolve both sides. The blocker is one coercion: `post_execution_data_sync_requests`
hardcodes `destination_path=S3Path(param.remote_value)`.

**Untested combination.** `sync_local_to_local` has seven tests, all against `tmp_path`. **No
test anywhere drives an `efs://` URI through `sync()`**, and nothing in production emits an
EFS-to-EFS request. Note also that `sync_local_to_local` ignores `force`, `size_only` and
`require_lock` — it is an unconditional full copy or `shutil.move`, with no locking.

## D3: The dependency graph is resolved caller-side, at compile time

The caller authors a graph; a pure function in `core` topologically sorts it into phases and
emits the wire shape above. The runtime never sees a graph, only `[[…], […]]`.

The alternative — a state machine that interprets dependencies at runtime — is not "a few more
states." Step Functions topology is fixed at deploy time, so runtime graph execution means
writing a generic DAG interpreter as a state machine. Compile-time resolution is a topological
sort and a cycle check.

What this costs: the platform cannot see authoring intent, so it cannot report "step B was
skipped because A failed" in graph terms; and every caller must upgrade the library to get
converter fixes. What it buys: the graph is unit-testable with no AWS, and the authoring sugar
never becomes platform surface that has to be supported forever.

## D4: Destinations live on the resolvable, after the serializer is fixed

One output may need several destinations — the next step's working directory *and* S3. This
is not currently expressible, and the reason is worth stating precisely, because it is a trap
that passes its own unit tests.

`ResolvableBase` builds and parses exactly two positions, `local` and `remote`.
`DemandExecutionParameters.sanitize_serialized_params` is a `@field_serializer("params")` that
collapses every `Resolvable` value to `to_str()`. So a new field added to a `Resolvable`
**serializes correctly when the resolvable is dumped on its own, and is silently destroyed when
it is dumped inside `params`** — which is the form that actually travels to the state machine.
Demonstrated on `main`:

```text
1) Resolvable serialized ALONE : {'local': 'clean-bam', 'remote': 's3://…',
                                  'extra_remotes': ['efs://…'], 'action': 'DELOCALIZE'}
2) same object INSIDE params   : {'clean-bam': 'clean-bam @ s3://out/run1/clean.bam'}
3) extra_remotes survived?     : False
```

This is the same wall that blocks OCSDV-453.

The decision is to **fix the serializer** — emit `to_dict()` rather than `to_str()` for
resolvable params — and then carry plural destinations on the resolvable, where a destination
conceptually belongs. See ADR 0005. This is sequenced **first**, as independent work.

The considered alternative was a sibling `output_destinations` field on
`DemandExecutionParameters`, outside `params` and therefore out of the serializer's reach. It
is additive and lower-risk, and remains the fallback if the serializer fix proves disruptive.
The worked example in Part I is written in that form because it is the shape that works today;
once the serializer is fixed, the destinations move onto the resolvable.

**Recommendation not yet ratified:** keep `remote: str` as it is and add a plural field beside
it, rather than making `remote` itself plural. Backward compatibility on one shipped field
beats symmetry.

## D5: Execution ids are derived, and truncated to fit

The converter mints `{group_id}-{step_name}`. This is unique within a group by construction,
and it makes `{scratch}/{execution_id}` self-describing on the filesystem, which is what
matters when diagnosing a failed group.

**It is not unique across groups, and nothing anywhere would notice.** There is no registry,
no table, no uniqueness validator; `execution_id` is a bare required `str`, and
`setup_file_system` does not even `mkdir` (the call is commented out), so there is no
`exist_ok=False` to trip. Two executions sharing an id share a working directory verbatim, and
one's cleanup deletes the other's data. This hazard predates the feature; the feature increases
exposure to it by minting ids programmatically at several per group. Cross-group uniqueness is
the caller's responsibility, via `group_id`.

Derived ids are longer than the ids in use today, and the id flows into job names, job
definition names, the ECS task family, and finally the docker volume name — where
`lambda:handlers/demand/naming.py` enforces `ECS_VOLUME_COMPONENT_BUDGET = 139`, because ECS
embeds the task family in the volume name and `amazon-efs-utils` caps the derived path at 246
characters. **The converter truncates with a hash suffix** so the budget is always satisfied.
Truncation costs readability in exactly the cases where names are longest.

## D6: Group members co-locate on one file system

`select_file_system` is on `main` in `aibs-informatics-aws-lambda` and seeds selection with
`f"{execution_id}#scratch"` (salted per role), so retries of one execution land consistently.
Members of a group would therefore scatter across candidate file systems at random, turning
every edge into a cross-filesystem copy.

**`DemandExecution` gains an optional seed field.** When present it is used to seed selection;
otherwise selection falls back to `execution_id`, so standalone behavior is unchanged. The
converter sets it to the `group_id`.

Two notes. This is a change to `main`, not to unmerged work — the reference doc's claim that
`select_file_system` "does not exist on `main`" is stale. And because the seed is derived only
from `execution_id`, which nothing validates, **a caller can already fully determine file-system
placement today** by choosing an id; the seed field makes an existing capability explicit rather
than creating one.

Cross-filesystem edges are not fatal, only expensive. On the PR-61 branch every ecosystem's root
access point is mounted into the same data-sync job, and `get_local_path` resolves each side
independently, so both would resolve. On `main`, with one root config, the unmounted side raises.

## D7: Failure is fail-fast, with group cleanup as a v1 goal

Within a phase, the first failure fails the phase and Step Functions aborts the in-flight
siblings. This is the default behavior of a `Map`, so it is free, and every other policy is
work.

The accepted consequence is that an aborted execution's cleanup does not run. Because each step
owns a working directory, **a group failing partway can strand several working directories at
once**, against a janitor that reclaims after roughly three days — the system's sharpest
operational edge, multiplied.

**A group-level `Catch` routing to a cleanup chain is a goal of the first version**, recorded
here rather than designed. The new outer state machine is the natural and only home for it: it
is the sole component that knows all of a group's working directories, and adding a `Catch`
there changes no existing behavior, since the demand-execution machine has none today.

---

# Part III: What has to change

| Repo | Change | Notes |
|---|---|---|
| `core` | Serialize resolvable params as objects | ADR 0005. Two call sites, both in `core`. Sequenced first, as independent work. |
| `core` | Plural destinations on the resolvable | Depends on the above. |
| `core` | The converter: graph to phases, id minting, edge resolution | Pure function, no AWS. The bulk of the new logic. |
| `core` | Optional selection-seed field on `DemandExecution` | Additive, defaults to `execution_id` behavior. |
| `aws-lambda` | `post_execution_data_sync_requests`: accept non-S3 destinations | Relax `S3Path(param.remote_value)` to the union `DataSyncTask` already declares. |
| `aws-lambda` | `post_execution_data_sync_requests`: emit N requests per output | The loop already returns a list; only the index used for the payload sub-path needs care. |
| `aws-lambda` | `select_file_system`: honor the seed field when present | One line, plus fallback. |
| `cdk-lib` | New outer state machine fragment | Sequential over phases, `Map` within a phase, `StartExecution.sync` into `demand-execution`. |
| `cdk-lib` | Group-level `Catch` to a cleanup chain | The v1 goal in D7. |

`DemandExecutionFragment` itself is untouched. Its input contract — `$` *is* the
`DemandExecution` JSON — stays exactly as it is, which is what keeps every step independently
submittable.

**Precedent for the new fragment already exists in this repo.** `DemandExecutionFragment`
invokes `data-sync-v2` as `sfn.Map(...).iterator(StepFunctionsStartExecution(..., RUN_JOB))`.
The group runtime is that same pattern one level up. One difference worth deciding at
implementation time: **no `Map` in this repo sets `max_concurrency`** — zero hits repo-wide — so
all Maps currently run at Step Functions' default unbounded concurrency. A phase Map probably
should not.

---

# Part IV: Prerequisites and sequencing

1. **Serializer fix** (ADR 0005) lands first, on its own, with its own ticket. It is
   independent of multi-step, it unblocks OCSDV-453, and doing it separately keeps a wire-format
   change from riding in on a feature review.
2. **Coordinate with OCSDV-453.** It is touching the same serialization boundary. Whichever
   lands first shapes the other; they should not be developed in parallel.
3. **Take the P5 measurement** (Part V) before committing to the job-count cost.
4. Model and converter work in `core`, then the handler changes, then the CDK fragment.

---

# Part V: Costs, measured and unmeasured

## The job count went up, not down

At the reference doc's accounting, one execution costs `2 + I + 1 + O + C` Batch jobs — a
typical execution with three inputs and two outputs runs **nine infrastructure jobs around one
science job**.

Because a step is a whole execution, each step pays that bracket. A three-step group with
roughly one input and one output per step runs on the order of **twenty infrastructure jobs
around three science jobs**, roughly double what steps-inside-one-execution would have cost.
Every S3 round-trip for intermediates is eliminated, which was the goal — but this is a
trade, not a saving, and which side wins depends on how large the intermediates are relative to
Batch's startup overhead.

## The measurement that prices it

**This has not been taken.** The claim that Batch startup dominates short steps is reasoning
from the architecture, not a measurement, and it matters more under this design than under the
one the exploration assumed.

```bash
Q=$(aws batch describe-job-queues \
  --query 'jobQueues[?contains(jobQueueName,`demand-infra-lambda`)].jobQueueName' \
  --output text | head -1)

# reuse an image already pullable from the VPC rather than a public registry
IMG=$(aws batch describe-job-definitions --status ACTIVE --max-items 1 \
  --query 'jobDefinitions[0].containerProperties.image' --output text)

aws batch register-job-definition --job-definition-name overhead-probe --type container \
  --container-properties "{\"image\":\"$IMG\",\"command\":[\"true\"],
    \"resourceRequirements\":[{\"type\":\"VCPU\",\"value\":\"1\"},
                              {\"type\":\"MEMORY\",\"value\":\"512\"}]}"

J=$(aws batch submit-job --job-name probe-$(date +%s) --job-queue "$Q" \
  --job-definition overhead-probe --query jobId --output text)

# poll until SUCCEEDED or FAILED, then read Batch's own timestamps
aws batch describe-jobs --jobs "$J" \
  --query 'jobs[0].[status,createdAt,startedAt,stoppedAt]' --output text
```

Overhead is `startedAt - createdAt`, in milliseconds. Run once cold, then again immediately for
warm. Use Batch's recorded timestamps rather than wall-clock around the CLI, so the polling
interval is excluded.

## The cheap semantics experiment

Still worth doing, and still one execution: submit a demand execution whose command is
`run.sh && exit 1`. Confirm the baseline — execution fails, outputs stay on EFS, nothing
uploads, nothing cleans up, no notification. It makes D7 concrete before anyone argues about it.

---

# Part VI: Non-goals for a first version

Every one of these is a plausible follow-up. Naming them is what keeps this from becoming a
workflow engine.

- **Conditional or branching steps.** No `Choice` on step results.
- **Loops, or step lists determined at runtime.** The phase list is fixed at submission.
- **Runtime graph resolution.** The graph is compile-time, always. The runtime sees a list.
- **Step-level retry** beyond the Batch job's existing `attempts=5`.
- **Resuming a failed group** from a given phase.
- **Data references across groups.** Edges are intra-group only.
- **Platform-side `execution_id` uniqueness enforcement.** Real, and separate work — see D5.
- **Quarantine or upload-anyway semantics** for outputs that failed a check.
- **Metrics and notifications.** No handler emits a metric today and
  `lambda:handlers/notifications/` is wired to nothing. Advisory steps that "record a result and
  continue" are therefore out of scope, because there is nowhere to record to.
- **Cross-filesystem edge optimization.** Co-location is the answer for v1.

---

## What could not be determined

- **Whether an EFS-to-EFS transfer through `data-sync-v2` behaves correctly end to end.** The
  code paths exist and dispatch correctly, but no test drives an `efs://` URI through `sync()`,
  and nothing in production emits one. This should be verified before the design is trusted.
- **The Batch overhead number.** See Part V.
- **Whether hardlinking is viable as an edge mechanism.** Producer output and consumer working
  directory sit on the same file system, where `os.link` would make a file appear in two places
  with zero bytes copied and independent refcounted lifetimes. **No repo uses links of any kind**,
  so there is no in-house precedent, and in-place modification by one step would corrupt the
  other's view. Recorded as an unexplored option, not a recommendation.
- **Whether `Uploadable.from_any` can be made to yield a null remote.** It raises today, so an
  output with no destination is unrepresentable. Under this design intermediates always have an
  `efs://` destination, so the question does not block — but it would if a truly destination-less
  output is ever wanted.

## Pointers

| Thing | Where |
|---|---|
| System reference | [`demand-execution.md`](./demand-execution.md) |
| Glossary | `CONTEXT.md` (repo root) |
| The serialization trap | `core:models/demand_execution/parameters.py`, `sanitize_serialized_params` |
| The one job-submission state | `cdk:constructs_/sfn/fragments/informatics/demand_execution.py` |
| The Map-over-a-list precedent | same file, `"Transfer Inputs TO Batch Job"` |
| Output transfer construction | `lambda:handlers/demand/context_manager.py`, `post_execution_data_sync_requests` |
| Working directory derivation | same file, `container_working_path` |
| File system selection | `lambda:handlers/demand/scaffolding.py`, `select_file_system` |
| Name budget | `lambda:handlers/demand/naming.py`, `ECS_VOLUME_COMPONENT_BUDGET` |
| Failure semantics today | `demand-execution.md` §12.3 |
