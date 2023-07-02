# -*- coding: utf-8 -*-

import typing as T
import boto3
from boto_session_manager import BotoSesManager

from pynamodb.models import PAY_PER_REQUEST_BILLING_MODE, WRITE_CAPACITY_UNITS
from pynamodb.connection import Connection
from . import constants
from . import dynamodb


def bootstrap(
    bsm: BotoSesManager,
    dynamodb_write_capacity_units: T.Optional[int] = None,
    dynamodb_read_capacity_units: T.Optional[int] = None,
    skip_s3: bool = False,
    skip_dynamodb: bool = False,
):
    # validate input arguments
    if sum(
        [
            dynamodb_write_capacity_units is None,
            dynamodb_read_capacity_units is None,
        ]
    ) not in [
        0,
        2,
    ]:  # pragma: no cover
        raise ValueError

    aws_account_id = bsm.sts_client.get_caller_identity()["Account"]
    aws_region = bsm.aws_region

    # create s3 bucket
    if skip_s3 is False:
        bucket = f"{aws_account_id}-{aws_region}-{constants.BUCKET_NAME}"
        bsm.s3_client.create_bucket(Bucket=bucket)

    # create dynamodb table
    if skip_dynamodb is False:
        if (
            dynamodb_write_capacity_units is None
            and dynamodb_read_capacity_units is None
        ):

            class Base(dynamodb.Base):
                class Meta:
                    table_name = constants.DYNAMODB_TABLE_NAME
                    region = aws_region
                    billing_mode = PAY_PER_REQUEST_BILLING_MODE

        else:  # pragma: no cover

            class Base(dynamodb.Base):
                class Meta:
                    table_name = constants.DYNAMODB_TABLE_NAME
                    region = aws_region
                    write_capacity_units = dynamodb_write_capacity_units
                    read_capacity_units = dynamodb_read_capacity_units

        with bsm.awscli():
            Connection()
            Base.create_table(wait=True)
