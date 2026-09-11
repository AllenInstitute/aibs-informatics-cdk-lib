# Contributing to `aibs-informatics-cdk-lib`

Contributions are welcome and appreciated!

## Types of Contributions

### Reporting Bugs

Report bugs to our [issues page](https://github.com/aibs-informatics-cdk-lib/issues).

If you are reporting a bug, please include:

- Your operating system name and version.
- Any details about your local setup that might be helpful in troubleshooting.
- Detailed steps to reproduce the bug, in the form of a [minimal reproducible example](https://stackoverflow.com/help/minimal-reproducible-example).

### Making Changes

Look through the GitHub issues for bugs, features, and other requests. Most issues will have a label that can help you identify the type of issue.  

### Submitting Feedback

The best way to send feedback is to [create an issue](https://github.com/aibs-informatics-cdk-lib/issues/new) on GitHub.

If you are proposing a feature:

- Explain in detail how it would work.
- Keep the scope as narrow as possible, to make it easier to implement.
- Remember that while contributions are welcome, developer/maintainer time is limited.

## Documentation

### Building the site

```bash
# Serve documentation locally
make docs-serve

# Build documentation
make docs-build
```

### Versioned documentation

The published site keeps one entry per minor release alongside a `dev` build, using [mike](https://github.com/jimporter/mike). The version selector in the site header switches between them.

| URL | Contents |
| --- | --- |
| `/` | Redirect to `latest/` |
| `latest/` | The newest release (an alias for the highest `X.Y`) |
| `X.Y/` | Built from that minor version's most recent release tag |
| `dev/` | Built from the tip of `main` |

Publishing is automatic:

- Pushes to `main` redeploy `dev` (`.github/workflows/publish_docs.yml`).
- A release redeploys its `X.Y` version and moves the `latest` alias (the `publish-docs` job in `.github/workflows/release.yml`). It builds from the release tag, so published docs match the released code. Patch releases refresh their minor version rather than adding an entry.

To preview or publish by hand:

```bash
# Serve every published version, with the version selector
make docs-serve-versions

# List published versions
make docs-versions

# Commit to gh-pages without pushing; add DOCS_PUSH=true to publish
make docs-deploy-dev
make docs-deploy-release DOCS_VERSION=1.1
```

Versions released before doc versioning existed can be backfilled best-effort. Versions already published are skipped, and nothing is pushed without `--push`:

```bash
scripts/backfill-docs-versions.sh
scripts/backfill-docs-versions.sh --push
```

#### One-time migration from the unversioned site

The `gh-pages` branch predates versioning: it holds one flat site at the branch root, and mike publishes into per-version subdirectories instead. Rather than deleting the leftover root files, rebuild the branch — the old workflow force-pushed an orphan commit on every deploy, so there is no history to keep.

Run this once, after these changes land on `main`:

```bash
S=docs-versioned-migration

# Build every past release, plus dev and the latest alias, on a scratch
# branch. Nothing is pushed, and gh-pages is left untouched.
DOCS_BRANCH=$S scripts/backfill-docs-versions.sh
make docs-deploy-dev DOCS_BRANCH=$S
make docs-deploy-release DOCS_BRANCH=$S DOCS_VERSION=<newest X.Y>

# Review it, then replace gh-pages with it.
git ls-tree --name-only $S
make docs-serve-versions DOCS_BRANCH=$S
git push --force origin $S:gh-pages
```

From then on the two workflows keep the branch current, and the scratch branch can be deleted.
