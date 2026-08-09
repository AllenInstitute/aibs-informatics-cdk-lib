---
status: accepted
---

# Pull published images directly from GHCR, with ECR mirroring opt-in

`aibs-informatics-aws-lambda` now publishes to `ghcr.io/alleninstitute/aibs-informatics-aws-lambda`,
so CDK no longer has to clone and build that repo. We consume those images **directly by
URI** by default, and offer ECR mirroring as an opt-in per `DockerAsset`. Direct pull needs
no new dependency and no ECR lifecycle; mirroring exists for the cases that genuinely need
it — in-region pull cost, independence from GitHub availability, and Lambda container
images, which AWS requires to be in ECR.

## Considered options

Mirroring-always was rejected on dependency grounds rather than architectural ones.
`cdk-ecr-deployment` is proven in-house (`gcs-infra` runs three `ECRDeployment`s today) but
`gcs`, `ocs`, and `ocs-deploy` all pin `~=3.1.13` while upstream supports only v4. A hard
dependency on either major forces a coordinated multi-repo change; declaring it as an
optional `ecr-mirror` extra with a lazy import and a `>=3.1,<5` range avoids the resolver
conflict entirely and matches the fact that mirroring is already opt-in.

Registry throttling was raised as an argument for mirroring and does not hold up: GHCR
publishes no pull rate limit for public packages and public package usage is free, unlike
Docker Hub's 100-pulls-per-6h anonymous cap. The real cost of direct pull is NAT gateway
data processing — roughly $0.045/GB, paid on each fresh Batch instance, since
`ECS_IMAGE_PULL_BEHAVIOR=default` caches per instance rather than per job. That is the
number to weigh when deciding whether to mirror.

## Consequences

**Mirror sources must be digest-pinned.** CloudFormation invokes a custom resource only
when its properties change, so mirroring `:latest` → `:latest` would run once and then
freeze the ECR copy forever while `latest` moved on. Enabling a mirror therefore resolves
the source tag to its immutable digest at synth time, via an anonymous OCI token plus a
manifest `HEAD` using stdlib `urllib` — no new dependency. Synth then requires network
access to the registry when mirroring is enabled; supplying a digest explicitly skips the
lookup and keeps synth offline-capable. Resolution failure is a hard error naming that fix,
never a silent fallback to the mutable tag.

**Mirrored images carry two tags.** A sanitized digest tag (`sha256-<hex>`, since `:` is
illegal in an ECR tag) for immutable reference, plus the original source tag as a moving
alias. `ECRDeployment` takes one source/destination pair, so this is two constructs — but
the second is nearly free, as the layers already exist in the destination and the copy
skips blobs already present. `mirror_tags` overrides the default when that cost matters.

**Private packages are documented, not solved.** The package is public today. If it goes
private, both synth-time digest resolution and the Batch pull need credentials — and
`RegisterJobDefinition` has no `repositoryCredentials` parameter, so the only lever for the
pull is ECS agent auth on the Batch launch template. The digest resolver keeps its auth
header pluggable so a token can be added cheaply, but no credential plumbing is built.
