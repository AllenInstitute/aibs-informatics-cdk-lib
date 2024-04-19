from aibs_informatics_core.utils.modules import load_all_modules_from_pkg


def test_imports():
    import aibs_informatics_cdk_lib.constructs_.assets
    import aibs_informatics_cdk_lib.constructs_.base
    import aibs_informatics_cdk_lib.constructs_.batch
    import aibs_informatics_cdk_lib.constructs_.cw
    import aibs_informatics_cdk_lib.constructs_.efs
    import aibs_informatics_cdk_lib.constructs_.monitoring
    import aibs_informatics_cdk_lib.constructs_.s3
    import aibs_informatics_cdk_lib.constructs_.sfn
    import aibs_informatics_cdk_lib.constructs_.ssm

    load_all_modules_from_pkg(aibs_informatics_cdk_lib.constructs_.assets)
    load_all_modules_from_pkg(aibs_informatics_cdk_lib.constructs_.s3)
    load_all_modules_from_pkg(aibs_informatics_cdk_lib.constructs_.batch)
    load_all_modules_from_pkg(aibs_informatics_cdk_lib.constructs_)

    import aibs_informatics_cdk_lib

    load_all_modules_from_pkg(aibs_informatics_cdk_lib)
