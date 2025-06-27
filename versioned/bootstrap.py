# -*- coding: utf-8 -*-

"""
Todo: docstring
"""

import typing as T
from boto_session_manager import BotoSesManager
from s3pathlib import context

from pynamodb.constants import PAY_PER_REQUEST_BILLING_MODE
from pynamodb.connection import Connection
from pynamodb_session_manager.api import use_boto_session

from . import dynamodb


def bootstrap(
    bsm: BotoSesManager,
    aws_region: str,
    bucket_name: str,
    dynamodb_table_name: str,
    dynamodb_write_capacity_units: T.Optional[int] = None,
    dynamodb_read_capacity_units: T.Optional[int] = None,
):
    """
    Bootstrap the associated AWS account and region in the boto session manager.
    Create the S3 bucket and DynamoDB table if not exist.
    """
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

    # create s3 bucket
    try:
        bsm.s3_client.head_bucket(Bucket=bucket_name)
    except Exception as e:  # pragma: no cover
        if "Not Found" in str(e):
            bsm.s3_client.create_bucket(Bucket=bucket_name)
        else:
            raise e

    # create dynamodb table
    if dynamodb_write_capacity_units is None and dynamodb_read_capacity_units is None:

        class Base(dynamodb.Base):
            class Meta:
                table_name = dynamodb_table_name
                region = aws_region
                billing_mode = PAY_PER_REQUEST_BILLING_MODE

    else:  # pragma: no cover

        class Base(dynamodb.Base):
            class Meta:
                table_name = dynamodb_table_name
                region = aws_region
                write_capacity_units = dynamodb_write_capacity_units
                read_capacity_units = dynamodb_read_capacity_units

    # bind s3pathlib to use the given boto session
    context.attach_boto_session(bsm.boto_ses)
    # bind pynamodb to use the given boto session
    with use_boto_session(
        Base,
        bsm,
        restore_on_exit=False,
    ):
        Base.create_table(wait=True)
