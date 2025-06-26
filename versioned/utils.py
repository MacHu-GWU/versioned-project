# -*- coding: utf-8 -*-

import typing as T
from datetime import datetime, timezone

from .constants import LATEST_VERSION


def get_utc_now() -> datetime:  # pragma: no cover
    return datetime.now(timezone.utc)


def encode_version(version: T.Optional[T.Union[int, str]]) -> str:
    """
    Normalize version input into standardized string format.

    Converts various version inputs into a consistent string representation
    by removing leading zeros from numeric versions while preserving the
    special LATEST version identifier.

    :param version: Version input - None, "LATEST", integer, or zero-padded string

    :returns: Normalized version string ("LATEST" or numeric without leading zeros)

    Examples::

        encode_version(None)       # → "LATEST"
        encode_version("LATEST")   # → "LATEST"
        encode_version(1)          # → "1"
        encode_version("000001")   # → "1"
        encode_version(42)         # → "42"
    """
    if version is None:
        return LATEST_VERSION
    else:
        return str(version).lstrip("0")
