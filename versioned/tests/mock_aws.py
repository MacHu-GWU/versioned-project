# -*- coding: utf-8 -*-

import typing as T
import dataclasses

import moto
import boto3
import botocore.exceptions
from s3pathlib import S3Path, context
from boto_session_manager import BotoSesManager

if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3.client import S3Client


@dataclasses.dataclass(frozen=True)
class MockAwsTestConfig:
    use_mock: bool = dataclasses.field()
    aws_region: str = dataclasses.field()
    prefix: str = dataclasses.field()
    aws_profile: T.Optional[str] = dataclasses.field(default=None)


class _BaseMockAwsTest:
    @classmethod
    def setup_mock(cls, mock_aws_test_config: MockAwsTestConfig):
        cls.mock_aws_test_config = mock_aws_test_config
        if mock_aws_test_config.use_mock:
            cls.mock_aws = moto.mock_aws()
            cls.mock_aws.start()

        if mock_aws_test_config.use_mock:
            cls.bsm: "BotoSesManager" = BotoSesManager(
                region_name=mock_aws_test_config.aws_region
            )
        else:
            cls.bsm: "BotoSesManager" = BotoSesManager(
                profile_name=mock_aws_test_config.aws_profile,
                region_name=mock_aws_test_config.aws_region,
            )

        cls.bucket: str = (
            f"{cls.bsm.aws_account_id}-{mock_aws_test_config.aws_region}-data"
        )
        cls.s3dir_root: "S3Path" = (
            S3Path(f"s3://{cls.bucket}").joinpath(mock_aws_test_config.prefix).to_dir()
        )

        cls.boto_ses: "boto3.Session" = cls.bsm.boto_ses
        context.attach_boto_session(cls.boto_ses)
        cls.s3_client: "S3Client" = cls.boto_ses.client("s3")

        try:
            cls.s3_client.create_bucket(Bucket=cls.bucket)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "BucketAlreadyExists":
                pass
            else:
                raise e

    @classmethod
    def teardown_class(cls):
        if cls.mock_aws_test_config.use_mock:
            cls.mock_aws.stop()


class BaseMockAwsTest(_BaseMockAwsTest):
    use_mock: bool = True

    @classmethod
    def setup_class(cls):
        mock_aws_test_config = MockAwsTestConfig(
            use_mock=cls.use_mock,
            aws_region="us-east-1",
            prefix="project/versioned/tests/",
            aws_profile="bmt_app_dev_us_east_1",  # Use default profile
        )
        cls.setup_mock(mock_aws_test_config)
        cls.setup_mock_post_hook()

    @classmethod
    def setup_mock_post_hook(cls):
        pass