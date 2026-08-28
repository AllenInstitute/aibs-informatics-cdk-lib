# Scripts

One-off maintenance scripts for `aibs-informatics-cdk-lib`.

## regen_instance_types.py

Regenerates [src/aibs_informatics_cdk_lib/constructs_/batch/instance_types.py](../src/aibs_informatics_cdk_lib/constructs_/batch/instance_types.py)
by querying AWS for instance type descriptions, on-demand/spot pricing, and Spot Advisor
interruption rates, then bucketing the results via the filters declared in the script.

### Setup

Run from the package root ([aibs-informatics-cdk-lib/](../)):

```bash
cd aibs-informatics-cdk-lib

# install dev/lint deps into .venv via uv (see ../Makefile)
make install

# activate the venv so `python` resolves to .venv/bin/python
source .venv/bin/activate
```

`make install` runs `uv sync --frozen --group dev --group lint`, which creates
`.venv/` and installs `aibs-informatics-aws-utils` (used inside the script) along
with the rest of the package's dev dependencies.

### AWS credentials

The script calls EC2 and Pricing APIs, so you need credentials with read access
to both. Pass an AWS profile with `--profile`, or export `AWS_PROFILE` /
`AWS_REGION` before invoking.

### Usage

```bash
# from aibs-informatics-cdk-lib/, with the venv activated
python scripts/regen_instance_types.py \
    --region us-west-2 \
    --profile sandbox
```

Flags:

- `--region` — AWS region to query (default `us-west-2`).
- `--profile` — AWS profile to use; sets `AWS_PROFILE` for the run.
- `--arch` — `x86_64`, `arm64`, or `both` (default). `both` produces the base
  lists plus `*_ARM` variants.
- `--output` — destination `.py` file. Defaults to
  `src/aibs_informatics_cdk_lib/constructs_/batch/instance_types.py`.

### Tuning the presets

Filters and preset definitions live in `regen_instance_types.py` itself —
edit `BASE_FILTERS` or the `Preset(...)` entries in `build_presets()` to change
what each list contains, then re-run. Diff the output against the previous
committed version before committing.

## backfill-docs-versions.sh

Best-effort backfill of versioned documentation for release tags that predate mike-based doc versioning. Each tag's docs are built in a throwaway git worktree and deployed to `gh-pages` under its `X.Y` version; versions already published are skipped, and nothing is pushed without `--push`.

See [Documentation](../CONTRIBUTING.md#documentation) for how the versioned site is laid out and published.

```bash
# from aibs-informatics-cdk-lib/
scripts/backfill-docs-versions.sh            # build locally, review first
make docs-serve-versions
scripts/backfill-docs-versions.sh --push     # publish
```

Flags:

- `--push` — push the resulting `gh-pages` commits to `origin`.
- `--force` — rebuild versions that are already published.
- `TAG ...` — backfill only these tags instead of every `v*` tag.
