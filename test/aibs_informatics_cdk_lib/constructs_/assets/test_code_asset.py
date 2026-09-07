import aws_cdk as cdk
from aws_cdk import aws_s3_assets

from aibs_informatics_cdk_lib.constructs_.assets import code_asset
from aibs_informatics_cdk_lib.constructs_.assets.code_asset import (
    CDK_OUT_GLOB_EXCLUDES,
    PYTHON_GLOB_EXCLUDES,
)
from test.aibs_informatics_cdk_lib.base import BaseTest

EXCLUDES = [*PYTHON_GLOB_EXCLUDES, *CDK_OUT_GLOB_EXCLUDES]

# Discovered rather than listed, so a new *_GLOB_EXCLUDES constant is covered by
# the invariant tests below without anyone remembering to add it here.
GLOB_EXCLUDE_LISTS = {
    name: value for name, value in vars(code_asset).items() if name.endswith("_GLOB_EXCLUDES")
}


class GlobExcludeTests(BaseTest):
    """Exercises the exclude patterns through CDK itself.

    The asset hash is a fingerprint of everything the exclude list did not
    filter out, so "does this pattern work" reduces to "does touching a file
    move the hash". No bundling, so no container is needed.
    """

    def setUp(self) -> None:
        super().setUp()
        self.asset_path = self.tmp_path()
        (self.asset_path / "src").mkdir()
        (self.asset_path / "src" / "a.py").write_text("a = 1")

    def asset_hash(self, exclude: list[str] | None = None) -> str:
        # A fresh App per call: AssetStaging memoizes by source path within one
        # cloud assembly, so reusing the App would return the first hash.
        stack = cdk.Stack(cdk.App(), "S")
        return aws_s3_assets.Asset(
            stack,
            "A",
            path=str(self.asset_path),
            exclude=EXCLUDES if exclude is None else exclude,
        ).asset_hash

    def write(self, *parts: str, content: str = "x") -> None:
        target = self.asset_path.joinpath(*parts)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)

    def test__cdk_out_is_excluded(self):
        """A trailing slash ("**/cdk.out/") matched nothing, so cdk.out was hashed."""
        self.write("cdk.out", "tree.json", content="{}")
        before = self.asset_hash()
        self.write("cdk.out", "tree.json", content='{"changed": true}')
        self.write("cdk.out", "asset.abc", "nested.py", content="nested = 1")
        assert before == self.asset_hash()

    def test__python_artifacts_are_excluded(self):
        for parts in [
            ("__pycache__", "a.cpython-311.pyc"),
            (".venv", "lib", "site.py"),
            ("build", "lib", "a.py"),
            ("dist", "pkg-1.0.tar"),
            (".eggs", "thing", "x.py"),
            ("src", "b.pyc"),
            ("src", "pkg.egg-info", "PKG-INFO"),
        ]:
            with self.subTest(path="/".join(parts)):
                before = self.asset_hash()
                self.write(*parts)
                assert before == self.asset_hash()

    def test__source_changes_still_move_the_hash(self):
        """Guard against the excludes being so broad that nothing is hashed."""
        before = self.asset_hash()
        self.write("src", "a.py", content="a = 2")
        assert before != self.asset_hash()

    def test__glob_exclude_lists_were_discovered(self):
        """Guards the discovery itself -- an empty mapping would vacuously pass."""
        assert set(GLOB_EXCLUDE_LISTS) >= {
            "PYTHON_GLOB_EXCLUDES",
            "GLOBAL_GLOB_EXCLUDES",
            "CDK_OUT_GLOB_EXCLUDES",
        }

    def test__glob_excludes_contain_bare_and_contents_forms(self):
        """CDK only prunes a directory when the directory path itself matches.

        Without the bare form, CDK walks all of `.venv` and rejects each file
        individually -- correct, but it reads every entry to do it.
        """
        for name, patterns in GLOB_EXCLUDE_LISTS.items():
            for pattern in patterns:
                if not pattern.endswith("/**"):
                    continue
                with self.subTest(constant=name, pattern=pattern):
                    assert pattern[: -len("/**")] in patterns

    def test__glob_excludes_avoid_brace_expansion(self):
        """These lists are also used with IgnoreMode.DOCKER, which has no braces."""
        for name, patterns in GLOB_EXCLUDE_LISTS.items():
            for pattern in patterns:
                with self.subTest(constant=name, pattern=pattern):
                    assert "{" not in pattern and "}" not in pattern
