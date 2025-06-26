# -*- coding: utf-8 -*-

"""
S3-Only Backend for Versioned Artifact Management

This module provides a comprehensive S3-based backend for storing and managing
versioned artifacts with alias support, enabling sophisticated deployment patterns
like blue/green deployments, canary releases, and rollbacks.


What is S3-Only Backend?
------------------------------------------------------------------------------
The S3-Only Backend is a storage system that uses Amazon S3 as the sole backend
for artifact versioning and alias management. Unlike hybrid approaches that combine
S3 with DynamoDB, this backend stores all metadata and version information directly
in S3 using a structured folder hierarchy and JSON files.


S3 Folder Structure
------------------------------------------------------------------------------
The backend organizes artifacts in S3 using the following hierarchical structure:

.. code-block:: javascript

    s3://{bucket}/{s3_prefix}/      # <--- this is a Repository
    ├── {artifact_name_1}/          # <--- this is an Artifact
    │   ├── versions/               # <--- contains all versions of the artifact
    │   │   ├── 000000_LATEST{suffix}          # Latest version (always first)
    │   │   ├── 999999_000001{suffix}          # Version 1 (reverse sorted)
    │   │   ├── 999998_000002{suffix}          # Version 2
    │   │   ├── 999997_000003{suffix}          # Version 3
    │   │   └── ...
    │   └── aliases/                # <--- contains alias JSON files
    │       ├── prod.json                      # Production alias
    │       ├── staging.json                   # Staging alias
    │       └── ...
    ├── {artifact_name_2}/
    │   ├── versions/
    │   └── aliases/
    └── ...


Key Structure Components:
------------------------------------------------------------------------------
- **Root Directory**: ``s3://{bucket}/{s3_prefix}/`` (default: "versioned-artifacts")
- **Artifact Directories**: Each artifact gets its own folder named after the artifact
- **Versions Subdirectory**: Contains all versions of the artifact binary files
- **Aliases Subdirectory**: Contains JSON files mapping aliases to specific versions


Filename Encoding System
------------------------------------------------------------------------------
The backend uses a sophisticated filename encoding system that leverages S3's
alphabetical sorting to ensure proper version ordering:


Version Filename Format
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
- **LATEST version**: ``000000_LATEST{suffix}`` (always appears first in listings)
- **Numeric versions**: ``{reverse_sort_key}_{zero_padded_version}{suffix}``


Examples
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
- Version ``LATEST`` → ``000000_LATEST.zip``
- Version ``1`` → ``999999_000001.zip``
- Version ``2`` → ``999998_000002.zip``
- Version ``999999`` → ``000001_999999.zip``

The encoding ensures that:

1. LATEST always appears first in S3 listings
2. Newer versions appear before older versions
3. Efficient pagination and sorting without requiring metadata queries


Alias Implementation
------------------------------------------------------------------------------
Aliases are implemented as JSON files stored in the ``aliases/`` subdirectory of
each artifact. Each alias file contains comprehensive metadata about version
mappings and supports advanced deployment patterns.


Alias Structure
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Each alias JSON file contains::

    {
        "name": "artifact_name",
        "alias": "prod", 
        "version": "5",
        "secondary_version": "4",
        "secondary_version_weight": 20,
        "version_s3uri": "s3://bucket/path/to/version/5",
        "secondary_version_s3uri": "s3://bucket/path/to/version/4",
        "update_at": "2024-01-01T12:00:00Z"
    }


Alias Features:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
1. **Primary Version**: The main version that the alias points to
2. **Secondary Version** (optional): Enables traffic splitting for gradual rollouts
3. **Weight-based Routing**: ``secondary_version_weight`` determines percentage of 
   traffic routed to the secondary version (0-100)
4. **Immutable References**: Aliases contain full S3 URIs to specific artifact versions
5. **Update Tracking**: Timestamps track when aliases were last modified


Deployment Patterns Enabled
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
- **Blue/Green Deployments**: Switch aliases between versions instantly
- **Canary Releases**: Gradually increase traffic to new versions using weights
- **Rollbacks**: Instantly revert to previous versions by updating alias
- **A/B Testing**: Route percentage of traffic to different versions


Core Classes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The module provides several key classes:

- :class:`Repository`: Main interface for artifact and alias operations
- :class:`Artifact`: Represents a versioned artifact with metadata
- :class:`Alias`: Represents an alias mapping with traffic splitting support


Usage Examples
------------------------------------------------------------------------------
**Repository setup**::

    repo = Repository(
        aws_region="us-east-1",
        s3_bucket="my-artifacts-bucket",
        suffix=".zip"
    )
    repo.bootstrap(bsm)

**Artifact management**::

    # Upload and publish versions
    repo.put_artifact(bsm, name="myapp", content=binary_data)
    repo.publish_artifact_version(bsm, name="myapp")  # Creates version 1
    
    # Create alias for production
    repo.put_alias(bsm, name="myapp", alias="prod", version=1)
    
    # Canary deployment with traffic splitting
    repo.put_alias(
        bsm, name="myapp", alias="prod", 
        version=1, secondary_version=2, secondary_version_weight=10
    )

The S3-Only Backend provides a robust, scalable solution for artifact management
with native support for modern deployment practices.
"""

import typing as T

import json
import random
import dataclasses
from datetime import datetime, timedelta
from functools import cached_property

from boto_session_manager import BotoSesManager
from s3pathlib import S3Path
from func_args.api import OPT

from . import exc
from .constants import (
    S3_PREFIX,
    LATEST_VERSION,
    VERSION_ZFILL,
    METADATA_KEY_ARTIFACT_SHA256,
)
from .utils import get_utc_now, encode_version
from .vendor.hashes import hashes


def encode_filename(version: T.Optional[T.Union[int, str]]) -> str:
    """
    Encode version into S3-compatible filename for proper chronological sorting.

    Creates filenames that leverage S3's alphabetical object listing to ensure
    versions appear in reverse chronological order (newest first). Uses a clever
    reverse-sorting algorithm where higher version numbers get lower alphabetical
    prefixes, ensuring LATEST always appears first.

    The encoding scheme uses the formula:
    ``{(10^6 - version_number):06d}_{version_number:06d}``

    :param version: Version number to encode into filename

    :returns: Encoded filename suitable for S3 storage

    Examples:
        Filename encoding for chronological sorting::

            encode_filename(None)      # "000000_LATEST"
            encode_filename("LATEST")  # "000000_LATEST"
            encode_filename(1)         # "999999_000001"
            encode_filename(2)         # "999998_000002"
            encode_filename(999999)    # "000001_999999"

        When listed alphabetically in S3, files appear as:
        - 000000_LATEST (newest development)
        - 000001_999999 (version 999999)
        - 999998_000002 (version 2)
        - 999999_000001 (version 1, oldest)
    """
    # First normalize the version input
    version = encode_version(version)

    # Special case: LATEST gets prefix of all zeros to appear first
    if version == LATEST_VERSION:
        return "{}_{}".format(
            VERSION_ZFILL * "0",  # "000000"
            LATEST_VERSION,  # "LATEST"
        )
    else:
        # Numeric versions: reverse sort using (10^6 - version_num) as prefix
        # This ensures higher version numbers get lower prefixes for reverse chronological order
        return "{}_{}".format(
            str(10**VERSION_ZFILL - int(version)).zfill(
                VERSION_ZFILL
            ),  # Reverse sort prefix
            version.zfill(VERSION_ZFILL),  # Zero-padded version number
        )


def decode_filename(version: str) -> str:
    """
    Decode S3 filename back to original version string.

    Reverses the encoding performed by :func:`encode_filename` to extract
    the original version number from the S3-compatible filename format.

    :param version: Encoded filename to decode

    :returns: Original version string

    Examples:
        Filename decoding::

            decode_filename("000000_LATEST")  # "LATEST"
            decode_filename("000001_999999")  # "999999"
            decode_filename("999998_000002")  # "2"
            decode_filename("999999_000001")  # "1"

    .. seealso::
        :func:`encode_filename` for the encoding process
    """
    # Check if filename starts with all zeros (indicates LATEST version)
    if version.startswith(VERSION_ZFILL * "0"):  # Starts with "000000"
        return LATEST_VERSION
    else:
        # Extract version number from second part after underscore and remove padding
        return str(int(version.split("_")[1]))


def validate_alias_name(alias: str):
    """
    Validate alias name according to naming convention rules.

    Ensures alias names follow the required naming patterns to prevent
    conflicts with internal version identifiers and maintain consistency
    across the artifact management system.

    :param alias: Alias name to validate

    :raises ValueError: If alias name violates naming conventions

    Validation Rules:
        - Cannot be "LATEST" (reserved for version identifier)
        - Cannot contain hyphens (conflicts with internal encoding)
        - Must start with alphabetic character

    Examples:
        Valid alias names::

            validate_alias_name("prod")     # Valid
            validate_alias_name("staging")  # Valid
            validate_alias_name("v1_0")     # Valid

        Invalid alias names::

            validate_alias_name("LATEST")     # Raises ValueError
            validate_alias_name("prod-v1")    # Raises ValueError
            validate_alias_name("1_prod")     # Raises ValueError
    """
    if alias == LATEST_VERSION:
        raise ValueError(f"alias name cannot be {LATEST_VERSION!r}.")
    if "-" in alias:
        raise ValueError("alias name cannot contain '-'.")
    if alias[0].isalpha() is False:
        raise ValueError("alias name must start with a alpha letter.")


@dataclasses.dataclass
class Base:
    """
    Base class providing common serialization functionality for data classes.

    Provides standard dictionary conversion methods used by artifact and alias
    data classes for JSON serialization and deserialization operations.
    """

    @classmethod
    def from_dict(cls, dct: dict):
        """
        Create instance from dictionary representation.

        :param dct: Dictionary containing field values for the data class

        :returns: New instance of the data class
        """
        return cls(**dct)

    def to_dict(self) -> dict:
        """
        Convert instance to dictionary representation.

        :returns: Dictionary containing all field values
        """
        return dataclasses.asdict(self)


@dataclasses.dataclass
class Artifact(Base):
    """
    Immutable artifact version with metadata and content access.

    Represents a specific version of an artifact stored in S3, containing
    all necessary metadata for version identification, content integrity
    verification, and direct access to the stored binary data.

    :param name: Artifact identifier unique within the repository
    :param version: Version string ("LATEST" or numeric like "1", "2", etc.)
    :param update_at: ISO format UTC timestamp when artifact was last modified
    :param s3uri: Complete S3 URI pointing to the artifact binary
    :param sha256: SHA256 hash for content integrity verification

    **Examples**:
        Artifact representation::

            {
                "name": "myapp",
                "version": "3",
                "update_at": "2024-01-01T12:00:00Z",
                "s3uri": "s3://bucket/artifacts/myapp/versions/999997_000003.zip",
                "sha256": "abc123...def789"
            }
    """

    name: str
    version: str
    update_at: str
    s3uri: str
    sha256: str

    @property
    def update_datetime(self) -> datetime:
        """
        Parse update timestamp into timezone-aware datetime object.

        :returns: Parsed datetime with UTC timezone information
        """
        return datetime.fromisoformat(self.update_at)

    @property
    def s3path(self) -> S3Path:
        """
        Create S3Path object for direct S3 operations.

        :returns: S3Path instance for reading, copying, or inspecting the artifact
        """
        return S3Path(self.s3uri)

    def get_content(self, bsm: BotoSesManager) -> bytes:
        """
        Download and return the complete artifact binary content.

        :param bsm: AWS session manager for S3 access

        :returns: Complete binary content of the artifact

        :raises ClientError: If artifact no longer exists in S3
        """
        return self.s3path.read_bytes(bsm=bsm)


@dataclasses.dataclass
class Alias(Base):
    """
    Named reference to artifact versions with traffic splitting support.

    Provides stable references to specific artifact versions that enable
    sophisticated deployment patterns including blue/green deployments,
    canary releases, and gradual rollouts with weighted traffic distribution.

    :param name: Artifact identifier that this alias references
    :param alias: Human-readable alias name (cannot contain hyphens)
    :param update_at: ISO format UTC timestamp when alias was last modified
    :param version: Primary artifact version this alias points to
    :param secondary_version: Optional secondary version for traffic splitting
    :param secondary_version_weight: Percentage (0-99) of traffic routed to secondary version
    :param version_s3uri: Complete S3 URI of the primary artifact version
    :param secondary_version_s3uri: Complete S3 URI of the secondary artifact version

    **Examples**:
        Simple alias pointing to single version::

            {
                "name": "myapp",
                "alias": "prod",
                "version": "5",
                "secondary_version": null,
                "secondary_version_weight": null,
                "version_s3uri": "s3://bucket/artifacts/myapp/versions/999995_000005.zip",
                "secondary_version_s3uri": null,
                "update_at": "2024-01-01T12:00:00Z"
            }

        Canary deployment with 20% traffic to new version::

            {
                "name": "myapp",
                "alias": "prod",
                "version": "5",
                "secondary_version": "6",
                "secondary_version_weight": 20,
                "version_s3uri": "s3://bucket/artifacts/myapp/versions/999995_000005.zip",
                "secondary_version_s3uri": "s3://bucket/artifacts/myapp/versions/999994_000006.zip",
                "update_at": "2024-01-01T15:30:00Z"
            }
    """

    name: str
    alias: str
    update_at: str
    version: str
    secondary_version: T.Optional[str]
    secondary_version_weight: T.Optional[int]
    version_s3uri: str
    secondary_version_s3uri: T.Optional[str]

    @property
    def update_datetime(self) -> datetime:
        """
        Parse update timestamp into timezone-aware datetime object.

        :returns: Parsed datetime with UTC timezone information
        """
        return datetime.fromisoformat(self.update_at)

    @property
    def s3path_version(self) -> S3Path:
        """
        Create S3Path object for the primary artifact version.

        :returns: S3Path instance for the main version referenced by this alias
        """
        return S3Path(self.version_s3uri)

    def get_version_content(self, bsm: BotoSesManager) -> bytes:
        """
        Download and return the primary artifact version content.

        :param bsm: AWS session manager for S3 access

        :returns: Complete binary content of the primary artifact version

        :raises ClientError: If primary artifact no longer exists in S3
        """
        return self.s3path_version.read_bytes(bsm=bsm)

    @property
    def s3path_secondary_version(self) -> S3Path:
        """
        Create S3Path object for the secondary artifact version.

        :returns: S3Path instance for the secondary version used in traffic splitting

        :raises AttributeError: If secondary_version_s3uri is None
        """
        return S3Path(self.secondary_version_s3uri)

    def get_secondary_version_content(self, bsm: BotoSesManager) -> bytes:
        """
        Download and return the secondary artifact version content.

        :param bsm: AWS session manager for S3 access

        :returns: Complete binary content of the secondary artifact version

        :raises ClientError: If secondary artifact no longer exists in S3
        :raises AttributeError: If secondary_version_s3uri is None
        """
        return self.s3path_secondary_version.read_bytes(bsm=bsm)

    @cached_property
    def _version_weight(self) -> int:
        """
        Calculate primary version weight for traffic distribution.

        :returns: Percentage weight for primary version (0-100)
        """
        if self.secondary_version_weight is None:
            return 100
        else:
            return 100 - self.secondary_version_weight

    def random_artifact(self) -> str:
        """
        Select artifact version based on weighted random distribution.

        Implements traffic splitting by randomly selecting between primary
        and secondary versions based on the configured weight distribution.
        Used for canary deployments and gradual rollouts.

        :returns: S3 URI of the selected artifact version

        Examples:
            Single version alias always returns primary::

                alias.random_artifact()  # Always returns version_s3uri

            Weighted distribution (80% primary, 20% secondary)::

                # Returns version_s3uri ~80% of the time
                # Returns secondary_version_s3uri ~20% of the time
                selected_uri = alias.random_artifact()
        """
        # Generate random number between 1-100 for traffic distribution
        # _version_weight is the percentage that should go to primary version
        if random.randint(1, 100) <= self._version_weight:
            # Random number falls within primary version's allocation
            return self.version_s3uri
        else:
            # Random number falls within secondary version's allocation
            return self.secondary_version_s3uri


@dataclasses.dataclass
class Repository:
    """
    S3-based artifact repository with comprehensive version and alias management.

    Provides a complete artifact management system using S3 as the storage backend.
    Supports versioned artifacts, stable aliases, traffic splitting for deployments,
    and automated lifecycle management with intelligent filename encoding for
    optimal performance.

    The repository organizes artifacts in a hierarchical S3 structure that enables
    efficient listing, versioning, and alias management without requiring external
    databases or metadata stores.

    :param aws_region: AWS region where the S3 bucket resides
    :param s3_bucket: S3 bucket name for artifact storage
    :param s3_prefix: S3 key prefix (folder path) for organizing artifacts
    :param suffix: File extension to append to all artifact files

    **Examples**:
        Repository initialization::

            repo = Repository(
                aws_region="us-east-1",
                s3_bucket="company-artifacts",
                s3_prefix="applications",
                suffix=".zip"
            )

        Complete workflow::

            # Setup and upload
            repo.bootstrap(bsm)
            repo.put_artifact(bsm, "myapp", binary_data)

            # Version management
            v1 = repo.publish_artifact_version(bsm, "myapp")  # Creates version 1
            repo.put_artifact(bsm, "myapp", new_binary_data)
            v2 = repo.publish_artifact_version(bsm, "myapp")  # Creates version 2

            # Alias and deployment patterns
            repo.put_alias(bsm, "myapp", "prod", version=1)  # Blue/green
            repo.put_alias(                                  # Canary
                bsm, "myapp", "prod", version=1,
                secondary_version=2, secondary_version_weight=10
            )

    .. note::

        All artifacts are stored with SHA256 content hashing for integrity
        verification and automatic deduplication during publishing.
    """

    aws_region: str = dataclasses.field()
    s3_bucket: str = dataclasses.field()
    s3_prefix: str = dataclasses.field(default=S3_PREFIX)
    suffix: str = dataclasses.field(default="")

    @property
    def s3dir_artifact_store(self) -> S3Path:
        """
        Get the root S3 directory for the entire artifact repository.

        :returns: S3Path pointing to the repository root directory

        Example:
            Repository root structure::

                s3://my-bucket/artifacts/  # s3dir_artifact_store
                ├── app1/
                ├── app2/
                └── service-x/
        """
        return S3Path(self.s3_bucket).joinpath(self.s3_prefix).to_dir()

    def _get_artifact_s3dir(self, name: str) -> S3Path:
        """
        Get the S3 directory for a specific artifact.

        :param name: Artifact name

        :returns: S3Path pointing to the artifact's directory

        Example:
            Artifact directory structure::

                s3://my-bucket/artifacts/myapp/  # _get_artifact_s3dir("myapp")
                ├── versions/
                └── aliases/
        """
        return self.s3dir_artifact_store.joinpath(name).to_dir()

    def _encode_basename(self, version: T.Optional[T.Union[int, str]] = None) -> str:
        return f"{encode_filename(version)}{self.suffix}"

    def _decode_basename(self, basename: str) -> str:
        """
        Remove file suffix from basename to get encoded version string.

        :param basename: Complete filename including suffix

        :returns: Encoded version string without suffix
        """
        basename = basename[::-1]
        suffix = self.suffix[::-1]
        basename = basename.replace(suffix, "", 1)
        return basename[::-1]

    def _get_artifact_s3path(
        self,
        name: str,
        version: T.Optional[T.Union[int, str]] = None,
    ) -> S3Path:
        """
        Get the complete S3 path for a specific artifact version.

        :param name: Artifact name
        :param version: Version number (defaults to LATEST)

        :returns: S3Path pointing to the specific artifact version file

        Example:
            Version file paths::

                s3://bucket/artifacts/myapp/versions/000000_LATEST.zip
                s3://bucket/artifacts/myapp/versions/999999_000001.zip
                s3://bucket/artifacts/myapp/versions/999998_000002.zip
        """
        return self._get_artifact_s3dir(name=name).joinpath(
            "versions",
            self._encode_basename(version),
        )

    def _get_alias_s3path(self, name: str, alias: str) -> S3Path:
        """
        Get the S3 path for a specific alias JSON file.

        :param name: Artifact name
        :param alias: Alias name

        :returns: S3Path pointing to the alias JSON configuration file

        Example:
            Alias file paths::

                s3://bucket/artifacts/myapp/aliases/prod.json
                s3://bucket/artifacts/myapp/aliases/staging.json
                s3://bucket/artifacts/myapp/aliases/canary.json
        """
        return self._get_artifact_s3dir(name=name).joinpath(
            "aliases",
            f"{alias}.json",
        )

    def bootstrap(
        self,
        bsm: BotoSesManager,
    ):
        """
        Initialize the S3 backend by creating the bucket if it doesn't exist.

        Ensures the specified S3 bucket exists and is accessible for artifact
        storage. Creates the bucket with appropriate regional configuration
        if it doesn't already exist.

        :param bsm: AWS session manager for S3 operations

        :raises ClientError: If bucket creation fails due to permissions or conflicts

        .. note::
            This method is idempotent - safe to call multiple times.
        """
        try:
            bsm.s3_client.head_bucket(Bucket=self.s3_bucket)
        except Exception as e:  # pragma: no cover
            if "Not Found" in str(e):
                kwargs = dict(Bucket=self.s3_bucket)
                if self.aws_region != "us-east-1":
                    kwargs["CreateBucketConfiguration"] = dict(
                        LocationConstraint=self.aws_region
                    )
                bsm.s3_client.create_bucket(**kwargs)
            else:
                raise e

    def _get_artifact_object(
        self,
        bsm: BotoSesManager,
        name: str,
        version: T.Optional[T.Union[int, str]] = None,
    ) -> Artifact:
        try:
            s3path = self._get_artifact_s3path(name=name, version=version)
            s3path.head_object(bsm=bsm)
            return Artifact(
                name=name,
                version=encode_version(version),
                update_at=s3path.last_modified_at.isoformat(),
                s3uri=s3path.uri,
                sha256=s3path.metadata[METADATA_KEY_ARTIFACT_SHA256],
            )
        except Exception as e:  # pragma: no cover
            error_msg = str(e)
            if "Not Found" in error_msg or "does not exist" in error_msg:
                raise exc.ArtifactNotFoundError(
                    f"Cannot find artifact: artifact name = {name!r}, version = {version!r}"
                )
            else:
                raise e

    def _list_artifact_versions_s3path(
        self,
        bsm: BotoSesManager,
        name: str,
        limit: T.Optional[int] = None,
    ) -> T.List[S3Path]:  # pragma: no cover
        """
        List the s3path of artifact versions of this artifact. Perform additional
        check to make sure the s3 dir structure are not contaminated.
        """
        s3dir_artifact = self._get_artifact_s3dir(name=name).joinpath("versions")
        if limit is None:
            s3path_list = s3dir_artifact.iter_objects(bsm=bsm).all()
        else:
            s3path_list = s3dir_artifact.iter_objects(bsm=bsm, limit=limit).all()
        n = len(s3path_list)
        if n >= 1:
            if (
                decode_filename(self._decode_basename(s3path_list[0].basename))
                != LATEST_VERSION
            ):
                raise exc.ArtifactS3BackendError(
                    f"S3 folder {s3dir_artifact.uri} for artifact {name!r} is contaminated! "
                    f"The first s3 object is not the LATEST version {s3path_list[0].uri}"
                )
        if n >= 2:
            for s3path in s3path_list[1:]:
                if (
                    decode_filename(self._decode_basename(s3path.basename)).isdigit()
                    is False
                ):
                    raise exc.ArtifactS3BackendError(
                        f"S3 folder {s3dir_artifact.uri} for artifact {name!r} is contaminated! "
                        f"Found non-numeric version {s3path.uri!r}"
                    )
        return s3path_list

    def get_latest_published_artifact_version_number(
        self,
        bsm: BotoSesManager,
        name: str,
    ) -> int:
        """
        Get the highest published version number for an artifact.

        Returns the numeric version of the most recently published artifact.
        Returns 0 if only the LATEST version exists (no published versions yet).

        :param bsm: AWS session manager for S3 operations
        :param name: Artifact name to check

        :returns: Highest published version number, or 0 if none published

        Examples:
            Version progression::

                repo.put_artifact(bsm, "myapp", data)  # Creates LATEST
                repo.get_latest_published_version(bsm, "myapp")  # Returns 0

                repo.publish_artifact_version(bsm, "myapp")  # Creates version 1
                repo.get_latest_published_version(bsm, "myapp")  # Returns 1

                repo.put_artifact(bsm, "myapp", new_data)  # Updates LATEST
                repo.publish_artifact_version(bsm, "myapp")  # Creates version 2
                repo.get_latest_published_version(bsm, "myapp")  # Returns 2
        """
        s3path_list = self._list_artifact_versions_s3path(
            bsm=bsm,
            name=name,
            limit=2,
        )
        if len(s3path_list) in [0, 1]:
            return 0
        else:
            return int(decode_filename(self._decode_basename(s3path_list[1].basename)))

    def _get_alias_object(
        self,
        bsm: BotoSesManager,
        name: str,
        alias: str,
    ) -> Alias:
        try:
            s3path = self._get_alias_s3path(name=name, alias=alias)
            s3path.head_object(bsm=bsm)
            alias = Alias.from_dict(json.loads(s3path.read_text(bsm=bsm)))
            alias.update_at = s3path.last_modified_at.isoformat()
            return alias
        except Exception as e:
            error_msg = str(e)
            if "Not Found" in error_msg or "does not exist" in error_msg:
                raise exc.AliasNotFoundError(
                    f"Cannot find alias: artifact name = {name!r}, alias = {alias!r}"
                )
            else:  # pragma: no cover
                raise e

    # --------------------------------------------------------------------------
    # Artifact
    # --------------------------------------------------------------------------
    def put_artifact(
        self,
        bsm: BotoSesManager,
        name: str,
        content: bytes,
        content_type: str = OPT,
        metadata: T.Dict[str, str] = OPT,
        tags: T.Dict[str, str] = OPT,
    ) -> Artifact:
        """
        Upload or update the LATEST version of an artifact.

        Creates or updates the mutable LATEST version with new content. If the
        content is identical to the existing LATEST version (based on SHA256),
        no upload occurs and the existing artifact metadata is returned.

        This is typically the first step in the artifact workflow, followed by
        :meth:`publish_artifact_version` to create immutable numbered versions.

        :param bsm: AWS session manager for S3 operations
        :param name: Unique artifact identifier within the repository
        :param content: Binary artifact data to store
        :param content_type: MIME type for S3 object (auto-detected if not provided)
        :param metadata: Additional S3 object metadata key-value pairs
        :param tags: S3 object tags for categorization and billing

        :returns: Artifact object with metadata for the LATEST version

        Examples:
            Basic artifact upload::

                with open("app.zip", "rb") as f:
                    artifact = repo.put_artifact(
                        bsm=bsm,
                        name="myapp",
                        content=f.read()
                    )

            With metadata and content type::

                artifact = repo.put_artifact(
                    bsm=bsm,
                    name="myapp",
                    content=binary_data,
                    content_type="application/zip",
                    metadata={"build_id": "12345", "commit": "abc123"},
                    tags={"environment": "production", "team": "backend"}
                )

        .. note::
            Content deduplication is automatic - identical content won't be
            re-uploaded, making this operation safe for CI/CD pipelines.
        """
        # Calculate SHA256 hash for content integrity checking and deduplication
        artifact_sha256 = hashes.of_bytes(content)
        # Get S3 path for LATEST version (always encoded as 000000_LATEST{suffix})
        s3path = self._get_artifact_s3path(name=name, version=LATEST_VERSION)

        # Optimization: Skip upload if content hasn't changed (deduplication)
        if s3path.exists(bsm=bsm):
            # Compare SHA256 hash stored in S3 metadata with new content hash
            if s3path.metadata[METADATA_KEY_ARTIFACT_SHA256] == artifact_sha256:
                # Content is identical, return existing artifact info without uploading
                return Artifact(
                    name=name,
                    version=LATEST_VERSION,
                    update_at=s3path.last_modified_at.isoformat(),
                    s3uri=s3path.uri,
                    sha256=artifact_sha256,
                )

        # Content has changed or this is a new artifact, prepare for upload
        # Build metadata dictionary with required fields
        final_metadata = dict(
            artifact_name=name,  # Store artifact name for reference
            artifact_sha256=artifact_sha256,  # Store hash for future deduplication
        )
        # Merge user-provided metadata if any
        if metadata is not OPT:
            final_metadata.update(metadata)

        # Upload new content to S3 LATEST version with metadata and tags
        s3path.write_bytes(
            content,
            metadata=final_metadata,
            content_type=content_type,
            tags=tags,
            bsm=bsm,
        )
        # Refresh object metadata to get accurate post-upload timestamp
        s3path.head_object(bsm=bsm)

        # Return artifact object with updated information
        return Artifact(
            name=name,
            version=LATEST_VERSION,
            update_at=s3path.last_modified_at.isoformat(),
            s3uri=s3path.uri,
            sha256=artifact_sha256,
        )

    def get_artifact_version(
        self,
        bsm: BotoSesManager,
        name: str,
        version: T.Optional[T.Union[int, str]] = None,
    ) -> Artifact:
        """
        Retrieve metadata for a specific artifact version.

        Returns comprehensive information about an artifact version including
        S3 location, content hash, and modification timestamps without downloading
        the actual binary content.

        :param bsm: AWS session manager for S3 operations
        :param name: Artifact name to retrieve
        :param version: Specific version to get (defaults to LATEST)

        :returns: Artifact object with complete metadata

        :raises ArtifactNotFoundError: If the specified artifact or version doesn't exist

        Examples:
            Get latest version::

                artifact = repo.get_artifact_version(bsm, "myapp")
                print(f"Latest SHA256: {artifact.sha256}")

            Get specific version::

                v2 = repo.get_artifact_version(bsm, "myapp", version=2)
                print(f"Version 2 S3 URI: {v2.s3uri}")
                print(f"Updated: {v2.update_datetime}")
        """
        return self._get_artifact_object(
            bsm=bsm,
            name=name,
            version=version,
        )

    def list_artifact_versions(
        self,
        bsm: BotoSesManager,
        name: str,
    ) -> T.List[Artifact]:
        """
        List all versions of an artifact in reverse chronological order.

        Returns all available versions including the mutable LATEST version
        and all published immutable versions. The list is ordered with LATEST
        first, followed by numbered versions from newest to oldest.

        :param bsm: AWS session manager for S3 operations
        :param name: Artifact name to list versions for

        :returns: List of Artifact objects sorted by version (newest first)

        :raises ArtifactNotFoundError: If the artifact doesn't exist

        Examples:
            List all versions::

                versions = repo.list_artifact_versions(bsm, "myapp")
                for artifact in versions:
                    print(f"Version {artifact.version}: {artifact.update_at}")

            Output example::

                Version LATEST: 2024-01-01T15:30:00Z
                Version 3: 2024-01-01T14:00:00Z
                Version 2: 2024-01-01T12:00:00Z
                Version 1: 2024-01-01T10:00:00Z
        """
        artifact_list = list()
        for s3path in self._list_artifact_versions_s3path(
            bsm=bsm,
            name=name,
        ):
            s3path.head_object(bsm=bsm)
            artifact = Artifact(
                name=name,
                version=decode_filename(self._decode_basename(s3path.basename)),
                update_at=s3path.last_modified_at.isoformat(),
                s3uri=s3path.uri,
                sha256=s3path.metadata[METADATA_KEY_ARTIFACT_SHA256],
            )
            artifact_list.append(artifact)
        return artifact_list

    def publish_artifact_version(
        self,
        bsm: BotoSesManager,
        name: str,
    ) -> Artifact:
        """
        Create an immutable numbered version from the current LATEST artifact.

        Copies the current LATEST version to create a new numbered version that
        cannot be modified. If the LATEST content is identical to the most recent
        published version (based on SHA256), no new version is created and the
        existing version is returned.

        This enables immutable deployments while maintaining a mutable development
        version in LATEST. Version numbers are automatically incremented.

        :param bsm: AWS session manager for S3 operations
        :param name: Artifact name to publish a version for

        :returns: Artifact object representing the published version

        :raises ArtifactNotFoundError: If no LATEST version exists

        Examples:
            Publishing workflow::

                # Upload new content
                repo.put_artifact(bsm, "myapp", new_content)

                # Create immutable version
                v1 = repo.publish_artifact_version(bsm, "myapp")
                print(f"Published version {v1.version}")  # "Published version 1"

                # Update and publish again
                repo.put_artifact(bsm, "myapp", newer_content)
                v2 = repo.publish_artifact_version(bsm, "myapp")
                print(f"Published version {v2.version}")  # "Published version 2"

            No-op when content unchanged::

                # If LATEST hasn't changed since last publish
                same_version = repo.publish_artifact_version(bsm, "myapp")
                # Returns existing version, no new version created

        .. note::
            Published versions are immutable and cannot be modified. Use
            :meth:`delete_artifact_version` to remove unwanted versions.
        """
        # Get up to 2 most recent artifact versions to check state
        # Returns list sorted by S3 alphabetical order: [LATEST, most_recent_version]
        s3path_list = self._list_artifact_versions_s3path(
            bsm=bsm,
            name=name,
            limit=2,  # Only need LATEST + 1 previous version for comparison
        )
        n = len(s3path_list)

        # Ensure LATEST version exists before we can publish
        if n == 0:
            raise exc.ArtifactNotFoundError(
                f"artifact {name!r} not found! you must put artifact first!"
            )

        # First item is always LATEST due to filename encoding (000000_LATEST)
        s3path_latest = s3path_list[0]

        # Case 1: Only LATEST exists, create first numbered version (version 1)
        if n == 1:
            new_version = "1"
            # Generate S3 path for new version 1 (will be encoded as 999999_000001)
            s3path_new = self._get_artifact_s3path(name=name, version=new_version)
            # Copy LATEST content to version 1 with all metadata intact
            s3path_latest.copy_to(s3path_new, bsm=bsm)
            # Refresh metadata after copy to get accurate timestamps
            s3path_new.head_object(bsm=bsm)
            return Artifact(
                name=name,
                version=new_version,
                update_at=s3path_new.last_modified_at.isoformat(),
                s3uri=s3path_new.uri,
                sha256=s3path_new.metadata[METADATA_KEY_ARTIFACT_SHA256],
            )

        # Case 2: Both LATEST and previous version exist, need to compare content
        else:
            # Second item is the most recent numbered version
            s3path_previous = s3path_list[1]
            # Extract version number from encoded filename (e.g., 999998_000002 -> "2")
            previous_version = decode_filename(
                self._decode_basename(s3path_previous.basename)
            )
            # Calculate next version number by incrementing
            new_version = str(int(previous_version) + 1)

            # Content deduplication: check if LATEST is identical to previous version
            # ETags are MD5 hashes that change when content changes
            if s3path_previous.etag == s3path_latest.etag:
                # Content is identical, return existing version instead of creating duplicate
                s3path_previous.head_object(bsm=bsm)
                return Artifact(
                    name=name,
                    version=previous_version,  # Return existing version, not new one
                    update_at=s3path_previous.last_modified_at.isoformat(),
                    s3uri=s3path_previous.uri,
                    sha256=s3path_previous.metadata[METADATA_KEY_ARTIFACT_SHA256],
                )
            else:
                # Content has changed, create new numbered version
                s3path_new = self._get_artifact_s3path(name=name, version=new_version)
                # Copy LATEST to new version with incremented number
                s3path_latest.copy_to(s3path_new, bsm=bsm)
                # Refresh metadata to get post-copy timestamps
                s3path_new.head_object(bsm=bsm)
                return Artifact(
                    name=name,
                    version=new_version,
                    update_at=s3path_new.last_modified_at.isoformat(),
                    s3uri=s3path_new.uri,
                    sha256=s3path_new.metadata[METADATA_KEY_ARTIFACT_SHA256],
                )

    def delete_artifact_version(
        self,
        bsm: BotoSesManager,
        name: str,
        version: T.Union[int, str],
    ):
        """
        Permanently delete a specific artifact version from S3.

        Removes the specified version file from S3 storage. The LATEST version
        cannot be deleted using this method - use :meth:`purge_artifact` to
        remove all versions including LATEST.

        This operation is irreversible and should be used carefully, especially
        if the version is referenced by active aliases.

        :param bsm: AWS session manager for S3 operations
        :param name: Artifact name
        :param version: Specific version number to delete (cannot be "LATEST")

        :raises ArtifactS3BackendError: If attempting to delete the LATEST version

        Examples:
            Delete specific versions::

                # Delete old version no longer needed
                repo.delete_artifact_version(bsm, "myapp", version=1)

                # This will raise an error
                repo.delete_artifact_version(bsm, "myapp", version="LATEST")

        .. warning::
            Deleting a version that is actively referenced by aliases may cause
            deployment issues. Check alias references before deletion.
        """
        if encode_version(version) == LATEST_VERSION:
            raise exc.ArtifactS3BackendError(
                "You cannot delete the LATEST artifact version!"
            )
        self._get_artifact_s3path(name=name, version=version).delete(bsm=bsm)

    def list_artifact_names(
        self,
        bsm: BotoSesManager,
    ) -> T.List[str]:
        """
        List all artifact names stored in this repository.
        
        Scans the repository root directory to find all artifacts that have
        been created. Returns just the artifact names without version information.
        
        :param bsm: AWS session manager for S3 operations
        
        :returns: Sorted list of unique artifact names
        
        Examples:
            Discover all artifacts::\
            
                names = repo.list_artifact_names(bsm)
                print(f"Found artifacts: {names}")
                # Output: ["api-service", "frontend", "worker-service"]
                
            Use with other operations::\
            
                for name in repo.list_artifact_names(bsm):
                    latest = repo.get_artifact_version(bsm, name)
                    print(f"{name}: {latest.version} ({latest.update_at})")
        """
        names = list()
        for p in self.s3dir_artifact_store.iterdir(bsm=bsm):
            if p.is_dir():
                names.append(p.basename)
        return names

    # ------------------------------------------------------------------------------
    # Alias
    # ------------------------------------------------------------------------------
    def put_alias(
        self,
        bsm: BotoSesManager,
        name: str,
        alias: str,
        version: T.Optional[T.Union[int, str]] = None,
        secondary_version: T.Optional[T.Union[int, str]] = None,
        secondary_version_weight: T.Optional[int] = None,
    ) -> Alias:
        """
        Create or update an alias pointing to artifact versions with traffic splitting.

        Creates stable references to specific artifact versions that enable sophisticated
        deployment patterns. Supports optional traffic splitting between a primary and
        secondary version for gradual rollouts and canary deployments.

        :param bsm: AWS session manager for S3 operations
        :param name: Artifact name to create alias for
        :param alias: Human-readable alias name (cannot contain hyphens)
        :param version: Primary version to point to (defaults to LATEST)
        :param secondary_version: Optional secondary version for traffic splitting
        :param secondary_version_weight: Percentage (0-99) of traffic routed to secondary

        :returns: Alias object with complete configuration

        :raises ValueError: If alias name contains hyphens or weight is invalid
        :raises TypeError: If secondary_version_weight is not an integer
        :raises ArtifactNotFoundError: If specified versions don't exist

        Examples:
            Simple alias (blue/green deployment)::

                # Point prod to version 5
                alias = repo.put_alias(bsm, "myapp", "prod", version=5)

            Canary deployment with traffic splitting::

                # 80% traffic to v5, 20% to v6
                alias = repo.put_alias(
                    bsm, "myapp", "prod",
                    version=5,
                    secondary_version=6,
                    secondary_version_weight=20
                )

            Point to latest development::

                # Alias points to mutable LATEST version
                alias = repo.put_alias(bsm, "myapp", "dev")

        .. note::
            Aliases are immediately active after creation. Ensure versions
            are thoroughly tested before updating production aliases.
        """
        # Step 1: Validate alias naming conventions
        # Hyphens conflict with our internal encoding scheme
        if "-" in alias:  # pragma: no cover
            raise ValueError("alias cannot have hyphen")

        # Step 2: Normalize version inputs to standardized format
        # Converts None/"LATEST"/1/"000001" to consistent strings ("LATEST"/"1")
        version = encode_version(version)

        # Step 3: Validate traffic splitting configuration if secondary version provided
        if secondary_version is not None:
            # Normalize secondary version the same way
            secondary_version = encode_version(secondary_version)

            # Ensure weight is proper integer type for traffic percentage
            if not isinstance(secondary_version_weight, int):
                raise TypeError("secondary_version_weight must be int")

            # Weight must be 0-99 (percentage to route to secondary, 100-weight goes to primary)
            if not (0 <= secondary_version_weight < 100):
                raise ValueError("secondary_version_weight must be 0 <= x < 100")

            # Prevent same version for primary and secondary (would be meaningless)
            if version == secondary_version:
                raise ValueError(
                    f"version {version!r} and secondary_version {secondary_version!r} "
                    f"cannot be the same!"
                )

        # Step 4: Verify primary version exists in S3 before creating alias
        version_s3path = self._get_artifact_s3path(name=name, version=version)
        version_s3uri = version_s3path.uri  # Store URI for alias JSON
        if version_s3path.exists(bsm=bsm) is False:
            raise exc.ArtifactNotFoundError(
                f"Cannot put alias to artifact name = {name!r}, version = {version}"
            )

        # Step 5: Verify secondary version exists if traffic splitting is configured
        if secondary_version is not None:
            secondary_version_s3path = self._get_artifact_s3path(
                name=name,
                version=secondary_version,
            )
            secondary_version_s3uri = (
                secondary_version_s3path.uri
            )  # Store URI for alias JSON
            if secondary_version_s3path.exists(bsm=bsm) is False:
                raise exc.ArtifactNotFoundError(
                    f"Cannot put alias to artifact name = {name!r}, version = {secondary_version}"
                )
        else:
            # No traffic splitting, secondary URI remains None
            secondary_version_s3uri = None

        # Step 6: Create alias data structure with all configuration
        alias_obj = Alias(
            name=name,
            alias=alias,
            version=version,
            update_at="unknown",  # Will be updated after S3 write with actual timestamp
            secondary_version=secondary_version,
            secondary_version_weight=secondary_version_weight,
            version_s3uri=version_s3uri,  # Full S3 URI to primary version
            secondary_version_s3uri=secondary_version_s3uri,  # Full S3 URI to secondary (or None)
        )

        # Step 7: Write alias configuration to S3 as JSON file
        # Path: s3://bucket/prefix/artifact_name/aliases/alias_name.json
        alias_s3path = self._get_alias_s3path(name=name, alias=alias)
        # Convert alias object to JSON and write to S3
        alias_s3path.write_text(json.dumps(alias_obj.to_dict()), bsm=bsm)

        # Step 8: Get accurate timestamp from S3 after write operation
        alias_s3path.head_object(bsm=bsm)
        # Update alias object with real S3 modification timestamp
        alias_obj.update_at = alias_s3path.last_modified_at.isoformat()

        return alias_obj

    def get_alias(
        self,
        bsm: BotoSesManager,
        name: str,
        alias: str,
    ) -> Alias:
        """
        Retrieve detailed information about a specific alias.

        Returns complete alias configuration including version references,
        traffic splitting settings, and update timestamps.

        :param bsm: AWS session manager for S3 operations
        :param name: Artifact name
        :param alias: Alias name to retrieve

        :returns: Alias object with complete configuration

        :raises AliasNotFoundError: If the specified alias doesn't exist

        Examples:
            Get alias configuration::

                alias = repo.get_alias(bsm, "myapp", "prod")
                print(f"Primary version: {alias.version}")
                if alias.secondary_version:
                    print(f"Secondary: {alias.secondary_version} ({alias.secondary_version_weight}%)")

            Use for deployment decisions::

                alias = repo.get_alias(bsm, "myapp", "prod")
                selected_uri = alias.random_artifact()  # Traffic splitting
                artifact_data = S3Path(selected_uri).read_bytes(bsm)
        """
        return self._get_alias_object(bsm=bsm, name=name, alias=alias)

    def list_aliases(
        self,
        bsm: BotoSesManager,
        name: str,
    ) -> T.List[Alias]:
        """
        List all aliases configured for a specific artifact.

        Returns all alias configurations for the given artifact, including
        their version references and traffic splitting settings.

        :param bsm: AWS session manager for S3 operations
        :param name: Artifact name to list aliases for

        :returns: List of Alias objects for the artifact

        Examples:
            List all aliases::

                aliases = repo.list_aliases(bsm, "myapp")
                for alias in aliases:
                    print(f"{alias.alias}: v{alias.version}")
                    if alias.secondary_version:
                        print(f"  └─ Canary: v{alias.secondary_version} ({alias.secondary_version_weight}%)")

            Output example::

                prod: v5
                  └─ Canary: v6 (20%)
                staging: vLATEST
                dev: vLATEST
        """
        s3dir = self._get_artifact_s3dir(name=name).joinpath("aliases")
        alias_list = list()
        for s3path in s3dir.iter_objects(bsm=bsm):
            alias = Alias.from_dict(json.loads(s3path.read_text(bsm=bsm)))
            alias.update_at = s3path.last_modified_at.isoformat()
            alias_list.append(alias)
        return alias_list

    def delete_alias(
        self,
        bsm: BotoSesManager,
        name: str,
        alias: str,
    ):
        """
        Permanently delete an alias from the repository.

        Removes the alias configuration file from S3. This operation is
        irreversible and will immediately stop routing traffic to the
        versions referenced by this alias.

        :param bsm: AWS session manager for S3 operations
        :param name: Artifact name
        :param alias: Alias name to delete

        Examples:
            Remove obsolete aliases::

                # Remove old staging alias
                repo.delete_alias(bsm, "myapp", "staging_v2")

            Cleanup after blue/green deployment::

                # After successful deployment, remove canary
                repo.put_alias(bsm, "myapp", "prod", version=6)  # Full traffic to v6
                repo.delete_alias(bsm, "myapp", "canary")        # Remove canary alias

        .. warning::
            Deleting an alias that is actively used by applications will cause
            immediate failures. Ensure no systems depend on the alias before deletion.
        """
        self._get_alias_s3path(name=name, alias=alias).delete(bsm=bsm)

    def purge_artifact_versions(
        self,
        bsm: BotoSesManager,
        name: str,
        keep_last_n: int = 10,
        purge_older_than_secs: int = 90 * 24 * 60 * 60,
    ) -> T.Tuple[datetime, T.List[Artifact]]:
        """
        Automatically clean up old artifact versions based on retention policies.

        Removes artifact versions that don't meet the retention criteria, helping
        manage storage costs and maintain repository hygiene. The LATEST version
        is always preserved regardless of age or count.

        A version is kept if it meets ANY of these criteria:
        - Is within the last ``keep_last_n`` versions (by creation time)
        - Is newer than ``purge_older_than_secs`` seconds
        - Is the LATEST version (always preserved)

        :param bsm: AWS session manager for S3 operations
        :param name: Artifact name to clean up
        :param keep_last_n: Minimum number of recent versions to retain
        :param purge_older_than_secs: Maximum age in seconds (default: 90 days)

        :returns: Tuple of (purge_timestamp, list_of_deleted_artifacts)

        Examples:
            Conservative cleanup (keep 10 versions, 90 days)::

                purge_time, deleted = repo.purge_artifact_versions(bsm, "myapp")
                print(f"Deleted {len(deleted)} old versions")

            Aggressive cleanup (keep 3 versions, 7 days)::

                purge_time, deleted = repo.purge_artifact_versions(
                    bsm, "myapp",
                    keep_last_n=3,
                    purge_older_than_secs=7*24*60*60
                )

            Dry run analysis::

                _, would_delete = repo.purge_artifact_versions(
                    bsm, "myapp", keep_last_n=5, purge_older_than_secs=30*24*60*60
                )
                print(f"Would delete: {[v.version for v in would_delete]}")

        .. note::
            This operation cannot be undone. Consider running with restrictive
            parameters first to verify which versions would be deleted.
        """
        # Get all versions sorted chronologically (LATEST first, then newest to oldest)
        artifact_list = self.list_artifact_versions(bsm=bsm, name=name)

        # Record purge operation timestamp for consistent time reference
        purge_time = get_utc_now()
        # Calculate cutoff datetime: versions older than this will be eligible for deletion
        expire = purge_time - timedelta(seconds=purge_older_than_secs)

        deleted_artifact_list = list()

        # Skip first keep_last_n+1 versions (LATEST + keep_last_n numbered versions)
        # This ensures we always keep the minimum required versions
        for artifact in artifact_list[keep_last_n + 1 :]:
            # Only delete if version is older than the age threshold
            # This implements the "AND" logic: must be both outside count AND older than age limit
            if artifact.update_datetime < expire:
                # Delete the artifact version from S3
                self.delete_artifact_version(
                    bsm=bsm,
                    name=name,
                    version=artifact.version,
                )
                # Track what was deleted for reporting
                deleted_artifact_list.append(artifact)

        return purge_time, deleted_artifact_list

    def purge_artifact(
        self,
        bsm: BotoSesManager,
        name: str,
    ):
        """
        Completely remove an artifact and all its associated data.

        Permanently deletes all versions, aliases, and metadata for the specified
        artifact. This is equivalent to removing the entire artifact directory
        from the repository.

        :param bsm: AWS session manager for S3 operations
        :param name: Artifact name to completely remove

        Examples:
            Remove obsolete artifact::

                # Remove an artifact that's no longer maintained
                repo.purge_artifact(bsm, "legacy-service")

            Clean up test artifacts::

                # Remove temporary test artifacts
                for test_name in ["test-app-1", "test-app-2"]:
                    repo.purge_artifact(bsm, test_name)

        .. danger::

            This operation is IRREVERSIBLE. All versions, aliases, and metadata
            for this artifact will be permanently lost. Ensure no systems depend
            on this artifact before deletion.
        """
        self._get_artifact_s3dir(name=name).delete(bsm=bsm)

    def purge_all(self, bsm: BotoSesManager):
        """
        Completely destroy the entire artifact repository.

        Removes all artifacts, versions, aliases, and metadata from the repository.
        This effectively resets the repository to an empty state while preserving
        the S3 bucket itself.

        :param bsm: AWS session manager for S3 operations

        Examples:
            Reset repository for testing::

                # Clean slate for integration tests
                repo.purge_all(bsm)
                repo.bootstrap(bsm)  # Reinitialize

            Decommission repository::

                # Remove all data before deleting repository
                repo.purge_all(bsm)

        .. danger::

            This operation is IRREVERSIBLE and will destroy ALL artifacts,
            versions, and aliases in the repository. Only use for testing
            or when completely decommissioning the repository.
        """
        self.s3dir_artifact_store.delete(bsm=bsm)
