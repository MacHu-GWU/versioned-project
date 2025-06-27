# -*- coding: utf-8 -*-

"""
DynamoDB Backend for Versioned Artifact Metadata Management

This module provides a DynamoDB-based backend for storing and managing metadata
of versioned artifacts and aliases. It implements a single-table design pattern
optimized for efficient querying and consistent performance in artifact version
management systems.


DynamoDB Table Schema Design
------------------------------------------------------------------------------
The backend uses a single DynamoDB table with a composite primary key design
that efficiently stores both artifact versions and aliases using strategic
key patterns and sort key encoding.


Table Structure
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Primary Key Components:**

- **Partition Key (pk)**: Groups related items for efficient querying
- **Sort Key (sk)**: Enables range queries and proper sorting within partitions

**Composite Key Patterns:**

1. **Artifact Versions**: ``pk=artifact_name, sk=zero_padded_version``
2. **Aliases**: ``pk=__artifact_name-alias, sk=alias_name``


Artifact Version Storage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Artifact versions are stored using the artifact name as the partition key
and zero-padded version numbers as the sort key::

    # Example artifact entries
    pk="my-app", sk="LATEST"   -> Version: LATEST
    pk="my-app", sk="000001"   -> Version: 1
    pk="my-app", sk="000002"   -> Version: 2
    pk="my-app", sk="000999"   -> Version: 999

**Benefits:**

- All versions of an artifact are co-located in the same partition
- Sort key ordering ensures proper version sequence (LATEST, then 1, 2, 3...)
- Efficient range queries for listing versions
- Zero-padding ensures lexicographic sorting matches numeric ordering


Alias Storage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Aliases are stored in separate partitions using a special naming convention::

    # Example alias entries
    pk="__my-app-alias", sk="prod"     -> Points to version 5
    pk="__my-app-alias", sk="staging"  -> Points to version 3
    pk="__my-app-alias", sk="dev"      -> Points to LATEST

**Benefits:**

- Aliases are isolated from artifact versions for independent scaling
- All aliases for an artifact are grouped in a single partition
- Efficient alias lookup and listing operations
- Special prefix ``__`` prevents naming conflicts with artifact names


Sort Key Encoding Strategy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The module implements a sophisticated version encoding system that ensures
proper sorting and efficient queries:

**Version Encoding Rules:**

1. **LATEST Version**: Always stored as ``"LATEST"`` (appears first in sort order)
2. **Numeric Versions**: Zero-padded to 6 digits for consistent lexicographic sorting
3. **Version Normalization**: Leading zeros are stripped for display (``000001`` → ``1``)

**Examples:**

.. code-block:: python

    encode_version_sk(None)       # → "LATEST" 
    encode_version_sk("LATEST")   # → "LATEST"
    encode_version_sk(1)          # → "000001"
    encode_version_sk("000001")   # → "000001"
    encode_version_sk(999999)     # → "999999"


Advantages Over S3-Only Backend
------------------------------------------------------------------------------
The DynamoDB backend provides several advantages over pure S3-based storage:

**Performance:**

- Sub-millisecond query latency for metadata operations
- Native support for conditional updates and transactions
- Efficient pagination and filtering capabilities

**Consistency:**

- Strong consistency for read-after-write operations
- ACID transactions for complex operations
- Built-in optimistic locking with version attributes

**Querying:**

- Rich query patterns using partition and sort keys
- Support for complex filter expressions
- Global Secondary Indexes for additional access patterns

**Scalability:**

- Automatic scaling based on traffic patterns
- No file system limitations or directory traversal overhead
- Predictable performance at any scale


Integration with S3+DynamoDB Backend
------------------------------------------------------------------------------
This module serves as the metadata layer for the hybrid S3+DynamoDB backend
architecture:

- **DynamoDB**: Stores artifact metadata, version tracking, and alias mappings
- **S3**: Stores actual artifact binary content and files
- **Hybrid Benefits**: Fast metadata queries + cost-effective binary storage

The combination provides both the performance of DynamoDB for metadata operations
and the cost-effectiveness of S3 for large binary artifact storage.
"""

import typing as T
from datetime import datetime

from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute
from pynamodb.attributes import NumberAttribute
from pynamodb.attributes import BooleanAttribute
from pynamodb.attributes import UTCDateTimeAttribute

from .constants import (
    LATEST_VERSION,
    VERSION_ZFILL,
)
from .utils import get_utc_now, encode_version


def encode_version_sk(version: T.Optional[T.Union[int, str]]) -> str:
    """
    Generate DynamoDB sort key for version with proper lexicographic ordering.

    Creates zero-padded sort keys that ensure correct version ordering in DynamoDB
    queries. LATEST version remains as-is to appear first in lexicographic sort,
    while numeric versions are zero-padded to maintain proper sequence.

    :param version: Version input - None, "LATEST", integer, or string

    :returns: DynamoDB sort key - "LATEST" or zero-padded numeric string

    Sort Key Examples::

        encode_version_sk(None)       # → "LATEST" (sorts first)
        encode_version_sk("LATEST")   # → "LATEST" (sorts first)
        encode_version_sk(1)          # → "000001"
        encode_version_sk("000001")   # → "000001"
        encode_version_sk(999999)     # → "999999"

    DynamoDB Query Order::

        sk="LATEST"   <- Always first
        sk="000001"   <- Version 1
        sk="000002"   <- Version 2
        sk="000003"   <- Version 3
        ...
    """
    return encode_version(version).zfill(VERSION_ZFILL)


def encode_alias_pk(name: str) -> str:
    """
    Generate DynamoDB partition key for alias storage.

    Creates a special partition key format that isolates alias records from
    artifact version records while maintaining the relationship to the parent
    artifact. The double underscore prefix prevents naming conflicts.

    :param name: Artifact name that the aliases belong to

    :returns: DynamoDB partition key for alias records

    Partition Key Examples::

        encode_alias_pk("my-app")     # → "__my-app-alias"
        encode_alias_pk("frontend")   # → "__frontend-alias"
        encode_alias_pk("api-v2")     # → "__api-v2-alias"

    Key Design Benefits:

    - Aliases are stored in separate partitions from artifact versions
    - All aliases for an artifact are co-located in one partition
    - Double underscore prefix avoids conflicts with artifact names
    - "-alias" suffix clearly identifies the record type
    """
    return f"__{name}-alias"


class Base(Model):
    """
    Base DynamoDB model implementing the composite primary key pattern.

    Provides the foundational structure for all DynamoDB items in the versioned
    artifact system. Uses a composite primary key with partition key (pk) and
    sort key (sk) to enable efficient querying patterns.

    Key Design:

    - **pk (Partition Key)**: Groups related items for co-location and efficient access
    - **sk (Sort Key)**: Enables range queries and sorting within each partition

    The composite key pattern allows storing different types of records
    (artifacts, aliases) in the same table while maintaining query efficiency.

    .. important::

        This class has not defined the table name or region in it's meta,
        so it must be subclassed to define the table name and region.
    """

    pk: T.Union[str, UnicodeAttribute] = UnicodeAttribute(hash_key=True)
    sk: T.Union[str, UnicodeAttribute] = UnicodeAttribute(
        range_key=True,
        default=LATEST_VERSION,
    )


class Artifact(Base):
    """
    DynamoDB model for artifact version metadata storage.

    Stores metadata about each version of an artifact including timestamps,
    content hashes, and soft deletion status. Each artifact version is stored
    as a separate DynamoDB item with a composite key pattern.

    Storage Pattern:

    - **pk**: Artifact name (groups all versions together)
    - **sk**: Zero-padded version number ("LATEST", "000001", "000002", etc.)
    - **Attributes**: Metadata about the specific version

    :param update_at: UTC timestamp when this version was last modified
    :param is_deleted: Soft deletion flag (allows hiding without data loss)
    :param sha256: Content hash for deduplication and integrity verification

    Examples::

        # LATEST version entry
        pk="my-app", sk="LATEST", sha256="abc123...", is_deleted=False

        # Numbered version entries
        pk="my-app", sk="000001", sha256="def456...", is_deleted=False
        pk="my-app", sk="000002", sha256="ghi789...", is_deleted=False

    Query Patterns:

    - Get specific version: ``pk="my-app" AND sk="000001"``
    - List all versions: ``pk="my-app"`` (returns sorted by sk)
    - Get latest: ``pk="my-app" AND sk="LATEST"``
    """

    update_at: T.Union[datetime, UTCDateTimeAttribute] = UTCDateTimeAttribute()
    is_deleted: T.Union[bool, BooleanAttribute] = BooleanAttribute(
        default=False,
    )
    sha256: T.Union[str, UnicodeAttribute] = UnicodeAttribute()

    @classmethod
    def new(
        cls,
        name: str,
        version: T.Optional[T.Union[int, str]] = None,
    ) -> "Artifact":
        """
        Create new Artifact instance with proper key encoding.

        Factory method that handles DynamoDB key generation and version encoding
        automatically. Ensures consistent key format across all artifact records.

        :param name: Artifact name (used as partition key)
        :param version: Version identifier - None defaults to LATEST

        :returns: Configured Artifact instance ready for DynamoDB operations

        Examples::

            # Create LATEST version artifact
            artifact = Artifact.new("my-app")
            # pk="my-app", sk="LATEST"

            # Create specific version artifact
            artifact = Artifact.new("my-app", version=5)
            # pk="my-app", sk="000005"
        """
        if version is None:
            return cls(pk=name)
        else:
            return cls(pk=name, sk=encode_version_sk(version))

    @property
    def name(self) -> str:
        """
        Get the artifact name from the partition key.

        :returns: Artifact name extracted from DynamoDB partition key
        """
        return self.pk

    @property
    def version(self) -> str:
        """
        Get the human-readable version from the sort key.

        Converts the zero-padded DynamoDB sort key back to the original
        version format by removing leading zeros from numeric versions.

        :returns: Human-readable version ("LATEST", "1", "42", etc.)

        Examples::

            # DynamoDB sk="LATEST" → version="LATEST"
            # DynamoDB sk="000001" → version="1"
            # DynamoDB sk="000042" → version="42"
        """
        return self.sk.lstrip("0")

    def to_dict(self) -> dict:
        """
        Convert Artifact instance to dictionary representation.

        Creates a clean dictionary with human-readable field names and values,
        suitable for API responses and data interchange.

        :returns: Dictionary with artifact metadata

        Dictionary Structure::

            {
                "name": "my-app",
                "version": "5",
                "update_at": datetime(2024, 1, 1, 12, 0, 0),
                "sha256": "abc123def456..."
            }
        """
        return {
            "name": self.name,
            "version": self.version,
            "update_at": self.update_at,
            "sha256": self.sha256,
        }


class Alias(Base):
    """
    DynamoDB model for artifact alias management with traffic splitting support.

    Stores alias mappings that point to specific artifact versions, enabling
    deployment patterns like blue/green deployments, canary releases, and A/B testing.
    Supports both single-version aliases and dual-version traffic splitting.

    Storage Pattern:

    - **pk**: Encoded alias partition key ("__artifact_name-alias")
    - **sk**: Alias name ("prod", "staging", "dev", etc.)
    - **Attributes**: Version mapping and traffic routing configuration

    :param update_at: UTC timestamp when this alias was last modified
    :param version: Primary version this alias points to (zero-padded)
    :param secondary_version: Optional secondary version for traffic splitting
    :param secondary_version_weight: Percentage of traffic routed to secondary version (0-99)

    Traffic Splitting:

    When secondary_version is specified, traffic is split between two versions:

    - **Primary Version**: Receives (100 - secondary_version_weight)% of traffic
    - **Secondary Version**: Receives secondary_version_weight% of traffic

    Examples::

        # Simple alias pointing to single version
        pk="__my-app-alias", sk="prod", version="000005"

        # Canary deployment with 20% traffic to new version
        pk="__my-app-alias", sk="prod",
        version="000005", secondary_version="000006",
        secondary_version_weight=20

    Query Patterns:

    - Get specific alias: ``pk="__my-app-alias" AND sk="prod"``
    - List all aliases: ``pk="__my-app-alias"`` (returns all aliases for artifact)
    """

    update_at: T.Union[datetime, UTCDateTimeAttribute] = UTCDateTimeAttribute(
        default=get_utc_now,
    )
    version: T.Union[str, UnicodeAttribute] = UnicodeAttribute()
    secondary_version: T.Optional[T.Union[str, UnicodeAttribute]] = UnicodeAttribute(
        null=True,
    )
    secondary_version_weight: T.Optional[T.Union[int, NumberAttribute]] = (
        NumberAttribute(
            null=True,
        )
    )

    @classmethod
    def new(
        cls,
        name: str,
        alias: str,
        version: T.Optional[T.Union[int, str]] = None,
        secondary_version: T.Optional[T.Union[int, str]] = None,
        secondary_version_weight: T.Optional[int] = None,
    ):
        """
        Create new Alias instance with proper key encoding and validation.

        Factory method that handles DynamoDB key generation, version encoding,
        and validates traffic splitting configuration. Ensures consistent
        data format and prevents invalid alias configurations.

        :param name: Artifact name that this alias belongs to
        :param alias: Alias name ("prod", "staging", etc.)
        :param version: Primary version - None defaults to LATEST
        :param secondary_version: Optional secondary version for traffic splitting
        :param secondary_version_weight: Traffic percentage for secondary version (0-99)

        :returns: Configured Alias instance ready for DynamoDB operations

        :raises ValueError: If primary and secondary versions are identical

        Examples::

            # Simple alias
            alias = Alias.new("my-app", "prod", version=5)

            # Canary deployment with traffic splitting
            alias = Alias.new(
                name="my-app",
                alias="prod",
                version=5,
                secondary_version=6,
                secondary_version_weight=20
            )
        """
        if version is None:
            version = LATEST_VERSION
        version = encode_version_sk(version)
        if secondary_version is not None:
            secondary_version = encode_version_sk(secondary_version)
            if version == secondary_version:
                raise ValueError
        return cls(
            pk=encode_alias_pk(name),
            sk=alias,
            version=version,
            secondary_version=secondary_version,
            secondary_version_weight=secondary_version_weight,
        )

    @property
    def name(self) -> str:
        """
        Extract artifact name from the encoded partition key.

        Decodes the partition key format "__artifact_name-alias" back to
        the original artifact name by removing the encoding prefix and suffix.

        :returns: Original artifact name

        Examples::

            # pk="__my-app-alias" → name="my-app"
            # pk="__frontend-service-alias" → name="frontend-service"
        """
        return "-".join(self.pk.split("-")[:-1])[2:]

    @property
    def alias(self) -> str:
        """
        Get the alias name from the sort key.

        :returns: Alias name extracted from DynamoDB sort key
        """
        return self.sk

    def to_dict(self) -> dict:
        """
        Convert Alias instance to dictionary representation.

        Creates a clean dictionary with human-readable field names and values,
        including proper version format conversion. Secondary version fields
        are included when traffic splitting is configured.

        :returns: Dictionary with alias configuration and metadata

        Dictionary Structure::

            # Simple alias
            {
                "name": "my-app",
                "alias": "prod",
                "update_at": datetime(2024, 1, 1, 12, 0, 0),
                "version": "5",
                "secondary_version": None,
                "secondary_version_weight": None
            }

            # Traffic splitting alias
            {
                "name": "my-app",
                "alias": "prod",
                "update_at": datetime(2024, 1, 1, 12, 0, 0),
                "version": "5",
                "secondary_version": "6",
                "secondary_version_weight": 20
            }
        """
        if self.secondary_version is None:
            secondary_version = None
        else:
            secondary_version = self.secondary_version.lstrip("0")
        return {
            "name": self.name,
            "alias": self.alias,
            "update_at": self.update_at,
            "version": self.version.lstrip("0"),
            "secondary_version": secondary_version,
            "secondary_version_weight": self.secondary_version_weight,
        }
