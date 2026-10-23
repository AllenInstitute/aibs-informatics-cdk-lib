---
status: accepted
---

# A Docker Asset is its own construct, not a property of the assets construct

Each AWS service consumes a container image differently — Batch wants a URI string, ECS
wants an `ecs.ContainerImage`, Lambda wants a `lambda_.DockerImageCode`. The obvious place
to expose those is as properties on `AIBSInformaticsDockerAssets` (`lambda_code`,
`ecs_image`, …), but that construct is a *namespace*: five downstream repos subclass it and
add their own `cached_property` members for their own images. A per-representation property
on the namespace can only ever describe one of the assets in it. So we introduce a
`DockerAsset` construct representing a single image — built locally from a Git Source or
resolved from a Container Image Source — and hang the per-service representations off that.

## Consequences

`AIBSInformaticsDockerAssets.AIBS_INFORMATICS_AWS_LAMBDA` now returns a `DockerAsset`
rather than a raw `aws_ecr_assets.DockerImageAsset`. This is source-compatible in practice:
all eight downstream call sites pass it into a parameter that does
`isinstance(x, str) → x, else x.image_uri`, and `DockerAsset` exposes `.image_uri`. The
Step Functions fragment signatures are widened via a `DockerAssetLike` alias and a shared
`resolve_image_uri()` helper, which also removes eight copies of that same ternary.

`DockerAsset` is a `constructs.Construct` rather than a plain value object specifically so
it can own child constructs — the ECR repository and `ECRDeployment` used by
[ADR-0002](./0002-direct-ghcr-pull-with-opt-in-ecr-mirroring.md).

`as_lambda_code()` raises on an unmirrored Container Image Source. This is an AWS
constraint, not a gap in our implementation: Lambda container images must live in ECR, and
`DockerImageCode` offers only `from_ecr()` and `from_image_asset()`. Mirroring is the fix,
and we require it to be requested explicitly rather than provisioning an ECR repository as
a side effect of a property access.
