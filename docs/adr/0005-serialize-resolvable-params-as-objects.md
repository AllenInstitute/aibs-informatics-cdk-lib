---
status: accepted
---

# Serialize resolvable params as objects, not two-position strings

`DemandExecutionParameters.sanitize_serialized_params` is a `@field_serializer("params")` that
collapses every `Resolvable` value to `to_str()` — a two-position string, `"local @ remote"`.
`ResolvableBase.to_str` builds from exactly `local` and `remote`, and `from_str` reconstructs
from exactly those two. **Any other field on a resolvable is therefore silently discarded on the
way out.** We will emit `to_dict()` instead, so a resolvable survives serialization intact.

The failure mode is the reason this needs recording: the loss is invisible to the obvious test.
A new field serializes correctly when the resolvable is dumped on its own, and vanishes only when
it is dumped inside `params` — which is the form that actually travels to the state machine.
Demonstrated against `main`:

```text
1) Resolvable serialized ALONE : {'local': 'clean-bam', 'remote': 's3://…',
                                  'extra_remotes': ['efs://…'], 'action': 'DELOCALIZE'}
2) same object INSIDE params   : {'clean-bam': 'clean-bam @ s3://out/run1/clean.bam'}
3) extra_remotes survived?     : False
```

This is a general blocker, not a multi-step one. It is what OCSDV-453 is stuck against while
trying to add include/exclude filters to `ResolvableBase`, and it is what would prevent an output
from naming more than one destination. Both want the same fix, so the fix is being done first, on
its own ticket, rather than arriving as part of a feature.

## Considered options

**A sibling field outside `params`** — for example `output_destinations` on
`DemandExecutionParameters`, keyed by param name — survives by construction, because the
flattening serializer is scoped to `params` and never sees it. It is additive, defaults to empty
so every existing payload is byte-identical, and it is the lower-risk answer. It was not chosen
because it leaves the trap in place for the next person, and it puts data about an output
somewhere other than on that output. It remains the fallback if the change below proves more
disruptive than measured.

**Extending the string to N positions**, `"local @ remote1 @ remote2"`, keeps everything on the
resolvable with the smallest conceptual change. Rejected because it means editing a shipped
string grammar concurrently with OCSDV-453, which is editing the same grammar for unrelated
reasons, and every producer and parser of that form would have to agree at once.

## Consequences

**The blast radius is smaller than it looks, and was measured rather than estimated.**
`sanitize_serialized_params` has exactly two call sites, both in `core`: the serializer itself
and `get_execution_hash`. There are zero references in `aws-utils`, `aws-lambda` or `cdk-lib`.

**Both directions already work.** Dict-valued params are accepted inbound today, resolve
correctly, and re-serialize as dicts — verified by running it. The flattening only ever fires for
values that are actual `Resolvable` instances, which is what
`update_demand_execution_parameter_outputs` puts into `params` after path rewriting. So this is a
change to one branch of one method, and the resulting form is one the model already round-trips.

**`job_definition_name` does not change.** The non-strict execution hash is
`execution_type + execution_image + command` only; params are not in it. Only the strict hash
shifts, which changes `job_name` — and Batch job names are ephemeral and already permit
duplicates, so nothing persistent moves.

**Test fallout is a handful of fixtures.** One assertion in `test_parameters.py` plus the
serialization fixtures in `test_model.py`. The dozen or so `" @ "` assertions in
`test_resolvables.py` exercise `to_str` and `from_str` directly, which are not being changed.

**Sequencing matters more than the change does.** OCSDV-453 is touching this same boundary.
Whichever lands first shapes the other, so they should not be developed in parallel.
