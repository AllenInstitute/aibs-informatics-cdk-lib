# AIBS Informatics CDK Library

A CDK construct library for AIBS Informatics services. Its distinctive concern is
**asset provisioning**: turning a versioned reference to one of our Python packages into
something AWS can run — a Lambda zip, a container image for Batch, ECS, or Lambda.

## Language

### Sources

**Source**:
A reference to a specific version of one of our packages, before anything has been built
or resolved. Every Asset is produced from exactly one Source.
_Avoid_: repo, repo URL (a Source is not always a repository)

**Git Source**:
A Source naming a git repository and, optionally, a Ref within it.

**Container Image Source**:
A Source naming a pre-built image already published to a registry.
_Avoid_: GHCR source, image URL, registry URL

**Ref**:
The point in a Git Source's history to use — a branch, a tag, or a commit. Branch and
tag are interchangeable everywhere we resolve them; commit is not, and is the only
distinction that changes behaviour.

**Version ID**:
The deterministic identifier for the exact version a Source names. Used to tag a Mirror
so the same upstream version always lands under the same name.

### Assets

**Asset**:
The deployable artifact produced or resolved from a Source. An Asset knows how to present
itself to each AWS service that can consume it.
_Avoid_: artifact, bundle, package (when the Asset is meant)

**Code Asset**:
An Asset that is a Lambda deployment package (a zip). Only ever produced from a Git
Source, because it requires a checkout to build.

**Docker Asset**:
An Asset that is a container image, whether built locally from a Git Source or resolved
from a Container Image Source. The unit that carries per-service representations.
_Avoid_: docker image asset (that is the CDK type it may wrap, not our concept)

**Assets Construct**:
A named collection of Assets belonging to one project. Downstream repos subclass ours and
add their own Assets to it. It is a namespace, never a single Asset.

### Registries

**Direct Pull**:
Consuming a Container Image Source at its original registry — the workload pulls straight
from where it was published.

**Mirror**:
A copy of a Container Image Source replicated into an ECR repository at deploy time, so
workloads pull in-region. Opt-in.
_Avoid_: sync, replication, cache

**Digest Pinning**:
Resolving a mutable tag to its immutable content digest, so that a reference names one
exact image forever.
