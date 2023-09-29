__all__ = ["get_package_root"]

import pathlib


def get_package_root() -> str:
    """Find the root package

    ASSUMPTION: the infrastructure package name is "aibs-informatics-cdk-lib"

    Returns:
        str: Absolute root path
    """
    return str(
        next(
            filter(
                lambda p: (p / "aibs-informatics-cdk-lib").is_dir(),  # type: ignore
                pathlib.Path(__file__).absolute().parents,
            )
        )
    )
