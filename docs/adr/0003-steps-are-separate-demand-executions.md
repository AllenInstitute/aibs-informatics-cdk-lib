---
status: accepted
---

# Compose steps as separate demand executions, grouped into phases

Running several containerized steps against one set of staged data could have been modelled as
an ordered `steps` list *inside* a single `DemandExecution`, moving `execution_image`,
`command` and `resource_requirements` per-step. We instead make **a step a whole
`DemandExecution`**, and introduce a Demand Execution Group that holds an ordered list of
phases, each phase a set of steps that may run concurrently. A lone execution is a group of one.

The reason is that the capabilities actually being asked for are already fields on
`DemandExecution`. Per-step image, per-step resource requirements and per-step job queue need no
new model at all, and because each step becomes its own child state-machine execution, "which
step failed" is answerable from the console without anything new. A steps-inside-one-execution
design would have had to re-invent each of those as per-step overrides.

## Considered options

**Steps inside one execution** is the cheaper runtime change, and it was recommended first. The
execution already brackets staging and cleanup, and that bracket is exactly what steps want to
share — so nothing about the group lifecycle would have needed inventing. It was rejected
because it buys less: per-step queues were an explicit non-goal under it, and each per-step
capability is new model surface.

**Several executions sharing one working directory**, distinguished only by a group identity,
would have avoided all data movement between steps. It was rejected because two hazards are live
and unguarded today: `write_env_file` is `Path.write_text`, so a second execution silently
truncates the first's `.demand.env` with no lock and no merge; and `cleanup_working_dir` defaults
true, so one execution's cleanup deletes the shared directory out from under another.

**Cross-references between independent executions**, where B names A's output S3 URI, already
work today with no platform change whatsoever, because output paths are pure concatenation of
`output_s3_prefix` and the param value, with no hashing. Rejected only because it keeps the S3
round-trip the feature exists to remove. It remains the correct answer for anyone who does not
care about that round-trip.

## Consequences

**Infrastructure job count roughly doubles.** Each step now pays the full bracket — scaffolding,
create-job-definition, per-input sync, per-output sync, cleanup. A three-step group runs on the
order of twenty infrastructure jobs around three science jobs, against roughly nine for a
comparable single execution. Every S3 round-trip for intermediates disappears, so this is a trade
whose sign depends on intermediate size versus Batch startup overhead. That overhead has never
been measured, and measuring it is a prerequisite to trusting the trade.

**The identical-`job_name` trap dissolves.** Two steps with the same image and command would have
collided, because `job_name` comes from the strict execution hash. That hash includes
`execution_id`, and ids are now derived per step, so they differ by construction. No step index
needs to enter the job name.

**Execution ids are minted programmatically, against a system that validates none.** There is no
registry, no uniqueness check, and `setup_file_system` does not even `mkdir`, so nothing would
notice a collision — while `{scratch}/{execution_id}` means two colliding executions share a
working directory and one's cleanup destroys the other's data. The hazard predates this decision;
this decision increases exposure to it. Ids are derived as `{group_id}-{step_name}`, which is
unique within a group by construction and pushes cross-group uniqueness onto `group_id`.

**Derived ids must be truncated.** The id flows into job names, job definition names, the ECS
task family and finally the docker volume name, where `ECS_VOLUME_COMPONENT_BUDGET = 139` exists
because `amazon-efs-utils` caps the derived path at 246 characters. The converter truncates with
a hash suffix, which costs readability in exactly the cases where names are longest.

**Group members must co-locate on one file system.** `select_file_system` seeds on
`f"{execution_id}#scratch"`, so members would otherwise scatter across candidates at random and
every edge would become a cross-filesystem copy. `DemandExecution` gains an optional seed field,
falling back to `execution_id` so standalone behavior is unchanged.

**A partial failure strands several working directories, not one.** Fail-fast aborts in-flight
siblings, and an aborted execution's cleanup does not run. A group-level `Catch` routing to a
cleanup chain is therefore a goal of the first version; the new outer state machine is the only
component that knows all of a group's working directories.
