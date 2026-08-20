---
status: accepted
---

# Move data between steps by redirecting stage-out, not by adding transfer steps

A step's output reaches the next step by **changing the destination of the transfer that already
happens**: the producing execution's post-execution sync writes to an `efs://` path inside the
consuming execution's working directory instead of to `s3://`. An edge is not a new operation and
adds no new state machine states.

This works because a working directory is computable before its execution runs —
`container_working_path` is `{scratch}/{execution_id}`, `execution_id` is caller-supplied, and the
scratch mount point is fixed at synth time. So whatever lowers a group knows exactly where the
consumer will work while it is still building the producer's request.

## Considered options

**A distinct transfer operation between executions** is the obvious shape and reads most clearly:
workloads stay pristine and data movement is a sibling concern. It was rejected because it adds a
Batch job per edge, and the per-job overhead is precisely the cost this feature exists to avoid.
Redirected stage-out replaces a transfer that is already paid for.

**Routing intermediates through S3** needs no change at all, since a caller can compute an output's
S3 URI in advance. Rejected for the round-trip.

**Hardlinking into the consumer's directory** would move zero bytes, since producer output and
consumer working directory sit on the same file system, and refcounting would give independent
lifetimes. Not chosen because no repo uses links of any kind, so there is no in-house precedent,
and a step modifying a linked file in place would corrupt the other view. Recorded as unexplored
rather than rejected on merit.

## Consequences

**Cleanup needs no deferral anywhere.** The copy happens during the producer's post-execution
phase, before the producer's own cleanup. This is what makes several-executions viable at all: the
alternative shapes all required making cleanup deferrable, and `ContextManagerConfiguration`
reaches the handler as a synth-time constructor argument that the production stack never passes,
so a caller cannot defer cleanup today without a CDK change.

**One coercion in the handler is the whole blocker.** The layers below already support it:
`DataSyncOperations.sync` dispatches on `isinstance(path, S3Path)` and routes everything else to
`sync_local_to_local`; `DataSyncTask` types both sides as `S3Path | EFSPath | Path` with no
cross-field validator; and `data-sync-v2` imposes no scheme constraint. Only
`post_execution_data_sync_requests` narrows it, by hardcoding `destination_path=S3Path(...)`.

**The combination is untested.** `sync_local_to_local` has tests, all against `tmp_path`. No test
anywhere drives an `efs://` URI through `sync()`, and nothing in production emits an EFS-to-EFS
request. This should be verified end to end before the design is trusted. Note also that
`sync_local_to_local` honors none of `force`, `size_only` or `require_lock` — it is an
unconditional full copy or move, with no locking.

**Steps stay independently submittable.** The edge is expressed as a destination on the producer,
not as a reference on the consumer, so a consumer submitted on its own is a valid execution that
happens to expect a file already present. Preserving that is what keeps a single step debuggable
by hand.

**Outputs need more than one destination.** An output feeding the next step *and* being kept
requires a plural destination, which the current model cannot express. See ADR 0005.
