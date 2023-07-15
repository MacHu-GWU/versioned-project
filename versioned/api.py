# -*- coding: utf-8 -*-

"""
Public API.
"""

from . import exc
from .constants import DYNAMODB_TABLE_NAME
from .constants import S3_PREFIX
from .constants import LATEST_VERSION
from .constants import VERSION_ZFILL
from .constants import METADATA_KEY_ARTIFACT_SHA256
from .s3_and_dynamodb_backend import Artifact # will be removed in 1.X.Y
from .s3_and_dynamodb_backend import Alias # will be removed in 1.X.Y
from .s3_and_dynamodb_backend import Repository # will be removed in 1.X.Y
from . import s3_and_dynamodb_backend
from . import s3_only_backend
