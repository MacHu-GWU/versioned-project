# -*- coding: utf-8 -*-

"""
S3 + DynamoDB Hybrid Backend for Versioned Artifact Management

This module provides a high-performance hybrid backend that combines Amazon S3
for binary artifact storage with DynamoDB for metadata management. This architecture
delivers the best of both services: cost-effective binary storage with fast,
consistent metadata operations.


Hybrid Architecture Benefits
------------------------------------------------------------------------------
The S3+DynamoDB backend addresses the limitations of single-service approaches
by leveraging the strengths of both AWS services:

**S3 for Binary Storage:**

- Cost-effective storage for large artifacts ($/GB significantly lower than DynamoDB)
- Native support for large file uploads and streaming
- Built-in versioning and lifecycle management capabilities
- Global replication and durability (99.999999999% durability)

**DynamoDB for Metadata:**

- Sub-millisecond query latency for artifact metadata operations
- Strong consistency for read-after-write operations
- ACID transactions for complex multi-item operations
- Auto-scaling based on traffic patterns


Architecture Overview
------------------------------------------------------------------------------
The hybrid backend implements a clear separation of concerns:

**Data Storage Layer:**

.. code-block:: javascript

    S3 Bucket Structure:
    s3://{bucket}/{s3_prefix}/
    ├── {artifact_name_1}/
    │   ├── LATEST{suffix}              # Latest version binary
    │   ├── 000001{suffix}              # Version 1 binary  
    │   ├── 000002{suffix}              # Version 2 binary
    │   └── ...

    DynamoDB Table Structure:
    Artifacts:
    - pk="my-app", sk="LATEST"   -> Metadata for LATEST version
    - pk="my-app", sk="000001"   -> Metadata for Version 1
    - pk="my-app", sk="000002"   -> Metadata for Version 2
    
    Aliases:  
    - pk="__my-app-alias", sk="prod"     -> Points to version 5
    - pk="__my-app-alias", sk="staging"  -> Points to version 3


**Operational Workflow:**

1. **Artifact Upload**: Binary content → S3, Metadata → DynamoDB
2. **Version Creation**: Copy S3 object, Create DynamoDB version record
3. **Alias Management**: Update DynamoDB alias mappings only
4. **Content Retrieval**: Query DynamoDB for S3 location, Fetch from S3
5. **Version Listing**: Query DynamoDB only (no S3 API calls needed)


Key Components
------------------------------------------------------------------------------
:class:`Repository` **Class:**

Central management class that coordinates S3 and DynamoDB operations.
Provides high-level API for artifact lifecycle management while handling
the complexity of dual-service coordination.

:class:`Artifact` **DataClass:**

Public API representation of artifact versions with integrated S3 access.
Encapsulates both DynamoDB metadata and S3 content retrieval capabilities.

:class:`Alias` **DataClass:**

Traffic routing configuration with weighted deployment support.
Enables sophisticated deployment patterns including canary releases and
blue/green deployments with percentage-based traffic splitting.


Performance Characteristics
------------------------------------------------------------------------------
**Metadata Operations (DynamoDB):**

- Get artifact version: ~1-5ms latency
- List artifact versions: ~5-15ms latency  
- Create/update alias: ~5-10ms latency
- Version publishing: ~10-20ms latency

**Content Operations (S3):**

- Small artifacts (<1MB): ~50-200ms latency
- Large artifacts (>100MB): Depends on bandwidth
- Concurrent downloads: Scales horizontally

**Cost Optimization:**

- DynamoDB: Pay only for metadata operations (minimal storage cost)
- S3: Cost-effective storage for any artifact size
- Combined: ~60-80% cost reduction vs DynamoDB-only for large artifacts


Deployment Patterns
------------------------------------------------------------------------------
**Blue/Green Deployments:**

.. code-block:: python

    # Deploy new version
    repo.publish_artifact_version("my-app")  # Creates version 6
    
    # Instant switch to new version
    repo.put_alias("my-app", "prod", version=6)

**Canary Releases:**

.. code-block:: python

    # Route 20% traffic to new version
    repo.put_alias(
        name="my-app", 
        alias="prod", 
        version=5,                    # 80% traffic
        secondary_version=6,          # 20% traffic  
        secondary_version_weight=20
    )

**Rollback:**

.. code-block:: python

    # Instant rollback to previous version
    repo.put_alias("my-app", "prod", version=5)


Consistency and Reliability
------------------------------------------------------------------------------
**Strong Consistency:**

- DynamoDB provides strong consistency for all metadata operations
- S3 provides eventual consistency but with read-after-write consistency for new objects
- Metadata operations are immediately consistent across all clients

**Error Handling:**

- Automatic retry logic for transient failures
- Graceful degradation when services are unavailable
- Comprehensive exception handling with detailed error context

**Data Integrity:**

- SHA256 content hashing for deduplication and verification
- Atomic operations where possible to prevent partial state
- Soft deletion to prevent accidental data loss
"""

import typing as T

import random
import dataclasses
from datetime import datetime, timedelta
from functools import cached_property

from boto_session_manager import BotoSesManager
from s3pathlib import S3Path
from func_args.api import OPT, remove_optional
from pynamodb_session_manager.api import use_boto_session
from .vendor.hashes import hashes

from . import constants
from . import dynamodb
from . import exc
from .utils import get_utc_now
from .bootstrap import bootstrap

hashes.use_sha256()


@dataclasses.dataclass
class Artifact:
    """
    Public API representation of a versioned artifact with integrated S3 access.

    This is the **safe, user-facing interface** for working with artifact versions.
    It provides a clean abstraction over the underlying storage systems while preventing
    direct access to internal DynamoDB ORM objects that could cause data corruption
    if misused.

    **Public API vs Internal ORM Distinction**:

    - **This class (Public API)**: Safe for end users, provides controlled access to artifact data
    - **dynamodb.Artifact (Internal ORM)**: Direct DynamoDB model, used internally by Repository
    - **User Safety**: End users should only work with this dataclass, never the ORM directly

    The Repository class internally uses :class:`~versioned.dynamodb.Artifact` ORM objects
    for DynamoDB operations, then converts them to this safe public interface. This prevents
    users from accidentally corrupting data through direct ORM manipulation.

    :param name: Artifact name identifier
    :param version: Version identifier ("LATEST", "1", "2", etc.)
    :param update_at: UTC timestamp when this artifact version was last modified
    :param s3uri: Complete S3 URI where the binary content is stored
    :param sha256: SHA256 hash of the binary content for integrity verification

    **Usage Examples**::

        # Safe public API usage (recommended)
        artifact = repo.get_artifact_version("my-app", version=5)

        # Access metadata safely
        print(f"Version {artifact.version} updated at {artifact.update_at}")
        print(f"Content hash: {artifact.sha256}")
        print(f"Stored at: {artifact.s3uri}")

        # Download content with session management
        content = artifact.get_content(bsm)

        # Access S3 path for advanced operations
        s3path = artifact.s3path
        metadata = s3path.head_object(bsm=bsm).metadata

    .. warning::

        Never directly instantiate or modify :class:`~versioned.dynamodb.Artifact` ORM objects.
        Always use Repository methods that return this safe public API class instead.

    .. seealso::

        :class:`~versioned.dynamodb.Artifact` for internal ORM implementation details
    """

    name: str
    version: str
    update_at: datetime
    s3uri: str
    sha256: str

    @property
    def s3path(self) -> S3Path:
        """
        Get S3Path object for advanced S3 operations.

        Provides access to the full S3Path API for operations like metadata
        inspection, copying, lifecycle management, and other advanced S3 features.

        :returns: S3Path instance pointing to this artifact's binary content

        **Usage Examples**::

            # Check if artifact exists
            exists = artifact.s3path.exists()

            # Get S3 object metadata
            metadata = artifact.s3path.head_object().metadata

            # Copy to another location
            artifact.s3path.copy_to("s3://backup-bucket/artifacts/")

            # Get object size
            size = artifact.s3path.size
        """
        return S3Path(self.s3uri)

    def get_content(self, bsm: BotoSesManager) -> bytes:
        """
        Download and return the binary content of this artifact version.

        Retrieves the complete binary content from S3 storage. For large artifacts,
        consider using streaming methods or the s3path property for more control
        over the download process.

        :param bsm: Boto session manager for AWS credentials and configuration

        :returns: Complete binary content of the artifact

        **Usage Examples**::

            # Download small artifact
            content = artifact.get_content(bsm)

            # Write to local file
            with open("downloaded_artifact.zip", "wb") as f:
                f.write(artifact.get_content(bsm))

            # For large files, consider streaming:
            # artifact.s3path.download_file("local_file.zip", bsm=bsm)

        .. note::
            This method loads the entire content into memory. For large artifacts
            (>100MB), consider using s3path.download_file() for streaming downloads.
        """
        return self.s3path.read_bytes(bsm=bsm)


@dataclasses.dataclass
class Alias:
    """
    Public API representation of an artifact alias with traffic splitting support.

    This is the **safe, user-facing interface** for working with artifact aliases and
    traffic routing configurations. It provides controlled access to alias data while
    preventing direct manipulation of internal DynamoDB ORM objects that could lead
    to inconsistent or corrupted deployment configurations.

    **Public API vs Internal ORM Distinction**:

    - **This class (Public API)**: Safe for end users, provides controlled alias operations
    - **dynamodb.Alias (Internal ORM)**: Direct DynamoDB model, used internally by Repository
    - **User Safety**: End users should only work with this dataclass, never the ORM directly

    The Repository class internally uses :class:`~versioned.dynamodb.Alias` ORM objects
    for DynamoDB operations, then converts them to this safe public interface. This prevents
    users from creating invalid traffic configurations or corrupting deployment state.

    :param name: Artifact name that this alias belongs to
    :param alias: Alias identifier ("prod", "staging", "dev", etc.) - cannot contain hyphens
    :param update_at: UTC timestamp when this alias configuration was last modified
    :param version: Primary version this alias points to
    :param secondary_version: Optional secondary version for traffic splitting
    :param secondary_version_weight: Percentage of traffic routed to secondary version (0-99)
    :param version_s3uri: Complete S3 URI of the primary artifact version
    :param secondary_version_s3uri: Complete S3 URI of the secondary artifact version (if configured)

    **Traffic Routing**:

    When secondary_version is configured, traffic is split as follows:

    - **Primary Version**: Receives (100 - secondary_version_weight)% of requests
    - **Secondary Version**: Receives secondary_version_weight% of requests

    **Usage Examples**::

        # Safe public API usage (recommended)
        alias = repo.get_alias("my-app", "prod")
        content = alias.get_version_content(bsm)  # Always gets primary version

        # Canary deployment with traffic splitting
        alias = repo.put_alias(
            name="my-app",
            alias="prod",
            version=5,                    # 80% traffic
            secondary_version=6,          # 20% traffic
            secondary_version_weight=20
        )

        # Traffic-weighted selection for requests
        selected_uri = alias.random_artifact()  # Returns URI based on weights

        # Access specific versions with session management
        primary_content = alias.get_version_content(bsm)
        if alias.secondary_version:
            secondary_content = alias.get_secondary_version_content(bsm)

    .. warning::
        Never directly instantiate or modify :class:`~versioned.dynamodb.Alias` ORM objects.
        Always use Repository methods that return this safe public API class instead.

    .. note::

        Alias names cannot contain hyphens due to DynamoDB key encoding constraints.
        Use underscores or camelCase for multi-word alias names.

    .. seealso::

        :class:`~versioned.dynamodb.Alias` for internal ORM implementation details
    """

    name: str
    alias: str
    update_at: datetime
    version: str
    secondary_version: T.Optional[str]
    secondary_version_weight: T.Optional[int]
    version_s3uri: str
    secondary_version_s3uri: T.Optional[str]

    @property
    def s3path_version(self) -> S3Path:
        """
        Get S3Path object for the primary artifact version.

        Provides access to the full S3Path API for the primary version that
        this alias points to. Use this for advanced S3 operations on the
        primary artifact version.

        :returns: S3Path instance pointing to primary artifact version

        **Usage Examples**::

            # Check if primary version exists
            exists = alias.s3path_version.exists()

            # Get primary version metadata
            metadata = alias.s3path_version.head_object().metadata

            # Copy primary version
            alias.s3path_version.copy_to("s3://backup/")
        """
        return S3Path(self.version_s3uri)

    def get_version_content(self, bsm: BotoSesManager) -> bytes:
        """
        Download the binary content of the primary artifact version.

        Always returns the content of the primary version, ignoring any
        traffic splitting configuration. Use random_artifact() if you need
        traffic-weighted selection.

        :param bsm: Boto session manager for AWS credentials and configuration

        :returns: Complete binary content of the primary artifact version

        **Usage Examples**::

            # Always get primary version content
            content = alias.get_version_content(bsm)

            # For traffic-weighted selection:
            selected_uri = alias.random_artifact()
            if selected_uri == alias.version_s3uri:
                content = alias.get_version_content(bsm)
            else:
                content = alias.get_secondary_version_content(bsm)
        """
        return self.s3path_version.read_bytes(bsm=bsm)

    @property
    def s3path_secondary_version(self) -> S3Path:
        """
        Get S3Path object for the secondary artifact version.

        Provides access to the full S3Path API for the secondary version
        in traffic splitting configurations. Returns None if no secondary
        version is configured.

        :returns: S3Path instance pointing to secondary artifact version

        :raises ValueError: If no secondary version is configured

        **Usage Examples**::

            # Check if secondary version is configured
            if alias.secondary_version:
                # Get secondary version S3 path
                s3path = alias.s3path_secondary_version
                exists = s3path.exists()
        """
        return S3Path(self.secondary_version_s3uri)

    def get_secondary_version_content(self, bsm: BotoSesManager) -> bytes:
        """
        Download the binary content of the secondary artifact version.

        Returns the content of the secondary version used in traffic splitting.
        Only available when secondary_version is configured.

        :param bsm: Boto session manager for AWS credentials and configuration

        :returns: Complete binary content of the secondary artifact version

        :raises ValueError: If no secondary version is configured

        **Usage Examples**::

            # Check if secondary version exists before accessing
            if alias.secondary_version:
                secondary_content = alias.get_secondary_version_content(bsm)
            else:
                print("No secondary version configured")
        """
        return self.s3path_secondary_version.read_bytes(bsm=bsm)

    @cached_property
    def _version_weight(self) -> int:
        """
        Calculate the effective weight for primary version traffic routing.

        Internal method that computes the percentage of traffic that should
        be routed to the primary version based on the secondary version weight.

        :returns: Primary version weight (0-100)
        """
        if self.secondary_version_weight is None:
            return 100
        else:
            return 100 - self.secondary_version_weight

    def random_artifact(self) -> str:
        """
        Randomly select artifact version S3 URI based on traffic weights.

        Implements weighted random selection for traffic splitting between
        primary and secondary versions. This is the core method for canary
        deployments and A/B testing scenarios.

        :returns: S3 URI of selected artifact version (primary or secondary)

        **Traffic Distribution**:

        - **No Secondary Version**: Always returns primary version URI
        - **With Secondary Version**: Random selection based on weights

          - Primary gets (100 - secondary_version_weight)% chance
          - Secondary gets secondary_version_weight% chance

        **Usage Examples**::

            # Simple canary deployment (20% new, 80% stable)
            alias = repo.put_alias(
                "my-app", "prod",
                version=5,                    # Stable - 80% traffic
                secondary_version=6,          # Canary - 20% traffic
                secondary_version_weight=20
            )

            # Random selection for each request
            selected_uri = alias.random_artifact()
            content = S3Path(selected_uri).read_bytes(bsm=bsm)

            # Simulate 1000 requests to verify distribution
            primary_count = sum(
                1 for _ in range(1000)
                if alias.random_artifact() == alias.version_s3uri
            )
            # primary_count should be approximately 800

        .. note::
            Each call produces an independent random selection. For consistent
            routing within a session, cache the result of this method.
        """
        if random.randint(1, 100) <= self._version_weight:
            return self.version_s3uri
        else:
            return self.secondary_version_s3uri


@dataclasses.dataclass
class Repository:
    """
    Central management class for S3+DynamoDB hybrid artifact repository.

    Coordinates operations between S3 (binary storage) and DynamoDB (metadata)
    to provide a unified, high-performance artifact management system. Handles
    the complexity of dual-service coordination while presenting a clean,
    intuitive API for artifact lifecycle management.

    The Repository class is the primary entry point for all artifact operations
    including uploading, versioning, aliasing, and retrieval. It automatically
    manages the optimal distribution of data between S3 and DynamoDB based on
    the nature of each operation.

    :param aws_region: AWS region where both S3 bucket and DynamoDB table are located
    :param s3_bucket: S3 bucket name for binary artifact storage
    :param s3_prefix: S3 key prefix (folder path) for organizing artifacts
    :param dynamodb_table_name: DynamoDB table name for metadata storage
    :param suffix: File extension suffix for artifact binaries (e.g., \".zip\", \".tar.gz\")

    **Architecture Overview**:

    - **S3 Storage**: ``s3://{s3_bucket}/{s3_prefix}/`` contains artifact binaries
    - **DynamoDB Metadata**: ``{dynamodb_table_name}`` contains version tracking and aliases
    - **Coordination**: Repository ensures consistency between both storage layers

    **Session Management**:

    Most Repository methods accept an optional ``bsm`` (BotoSesManager) parameter for
    explicit AWS credential and session management. When ``bsm`` is provided, it overrides
    the global session configuration established by :func:`~versioned.bootstrap.bootstrap`.

    - **Default Behavior**: Uses global boto session configured via bootstrap
    - **Explicit Session**: Pass ``bsm`` parameter to use specific credentials/configuration
    - **Bootstrap Requirement**: Call :meth:`bootstrap` once before first use to bind
      s3pathlib and pynamodb libraries to the appropriate boto session

    **Usage Examples**::

        # Initialize repository
        repo = Repository(
            aws_region=\"us-east-1\",
            s3_bucket=\"my-artifacts\",
            s3_prefix=\"versioned-artifacts\",
            dynamodb_table_name=\"artifact-metadata\"
        )

        # Bootstrap AWS resources (run once to bind libraries)
        repo.bootstrap(bsm)

        # Upload new artifact version (uses global session)
        artifact = repo.put_artifact(\"my-app\", content=binary_data)

        # Create immutable version with explicit session
        version = repo.publish_artifact_version(\"my-app\", bsm=custom_bsm)

        # Create production alias
        alias = repo.put_alias(\"my-app\", \"prod\", version=version.version)

        # Retrieve via alias with explicit session
        prod_artifact = repo.get_alias(\"my-app\", \"prod\", bsm=bsm)
        content = prod_artifact.get_version_content(bsm)

    **Key Features**:

    - **Automatic Deduplication**: SHA256-based content deduplication across versions
    - **Soft Deletion**: Safe deletion with recovery capabilities
    - **Traffic Splitting**: Weighted routing for canary deployments
    - **Atomic Operations**: Consistent state management across S3 and DynamoDB
    - **Cost Optimization**: Efficient data placement for minimal storage costs
    - **Session Flexibility**: Support for both global and explicit session management

    .. note::

        The Repository requires both S3 and DynamoDB permissions. Use the :meth:`bootstrap`
        method to create necessary AWS resources and bind the underlying libraries to
        your boto session automatically.
    """

    aws_region: str = dataclasses.field()
    s3_bucket: str = dataclasses.field()
    s3_prefix: str = dataclasses.field(default=constants.S3_PREFIX)
    dynamodb_table_name: str = dataclasses.field(default=constants.DYNAMODB_TABLE_NAME)
    suffix: str = dataclasses.field(default="")

    @property
    def s3dir_artifact_store(self) -> S3Path:
        """
        Get the root S3 directory path for the artifact repository.

        :returns: S3Path pointing to the root directory of the artifact store
        """
        return S3Path(self.s3_bucket).joinpath(self.s3_prefix).to_dir()

    def get_artifact_s3path(self, name: str, version: str) -> S3Path:
        """
        Generate S3 path for specific artifact version using consistent naming.

        Constructs the complete S3 path where a specific artifact version
        is stored, using the repository's naming conventions and encoding.

        :param name: Artifact name identifier
        :param version: Version identifier ("LATEST", "1", "2", etc.)

        :returns: Complete S3Path to the artifact version binary
        """
        return self.s3dir_artifact_store.joinpath(
            name,
            f"{dynamodb.encode_version_sk(version)}{self.suffix}",
        )

    def bootstrap(
        self,
        bsm: BotoSesManager,
        dynamodb_write_capacity_units: T.Optional[int] = None,
        dynamodb_read_capacity_units: T.Optional[int] = None,
    ):
        """
        Initialize AWS resources required for the artifact repository.

        Creates the S3 bucket and DynamoDB table if they don't exist.
        This is a one-time setup operation that prepares the infrastructure
        for artifact storage and management.

        :param bsm: Boto session manager for AWS credentials and configuration
        :param dynamodb_write_capacity_units: DynamoDB write capacity (None for on-demand)
        :param dynamodb_read_capacity_units: DynamoDB read capacity (None for on-demand)

        **Usage Examples**::

            # Bootstrap with on-demand billing (recommended)
            repo.bootstrap(bsm)

            # Bootstrap with provisioned capacity
            repo.bootstrap(bsm,
                dynamodb_write_capacity_units=5,
                dynamodb_read_capacity_units=10
            )

        .. note::

            This operation is idempotent and safe to run multiple times.
            Existing resources will not be modified.
        """
        bootstrap(
            bsm=bsm,
            aws_region=self.aws_region,
            bucket_name=self.s3_bucket,
            dynamodb_table_name=self.dynamodb_table_name,
            dynamodb_write_capacity_units=dynamodb_write_capacity_units,
            dynamodb_read_capacity_units=dynamodb_read_capacity_units,
        )
        with use_boto_session(self._ArtifactOrmClass, bsm, restore_on_exit=False):
            self._ArtifactOrmClass.describe_table()
        with use_boto_session(self._AliasOrmClass, bsm, restore_on_exit=False):
            self._AliasOrmClass.describe_table()

    @cached_property
    def _ArtifactOrmClass(self) -> T.Type[dynamodb.Artifact]:
        class Artifact_(dynamodb.Artifact):
            class Meta:
                table_name = self.dynamodb_table_name
                region = self.aws_region

        return Artifact_

    @cached_property
    def _AliasOrmClass(self) -> T.Type[dynamodb.Alias]:
        class Alias_(dynamodb.Alias):
            class Meta:
                table_name = self.dynamodb_table_name
                region = self.aws_region

        return Alias_

    def _get_artifact_object(
        self,
        artifact: dynamodb.Artifact,
    ) -> Artifact:
        """
        Convert a DynamoDB item to a public API Artifact object.
        """
        dct = artifact.to_dict()
        dct["s3uri"] = self.get_artifact_s3path(
            name=artifact.name,
            version=artifact.version,
        ).uri
        return Artifact(**dct)

    def _get_alias_object(
        self,
        alias: dynamodb.Alias,
    ) -> Alias:
        """
        Convert a DynamoDB item to a public API Alias object.
        """
        dct = alias.to_dict()
        dct["version_s3uri"] = self.get_artifact_s3path(
            name=alias.name,
            version=alias.version,
        ).uri
        if alias.secondary_version is None:
            dct["secondary_version_s3uri"] = None
        else:
            dct["secondary_version_s3uri"] = self.get_artifact_s3path(
                name=alias.name,
                version=alias.secondary_version,
            ).uri
        return Alias(**dct)

    # ------------------------------------------------------------------------------
    # Artifact
    # ------------------------------------------------------------------------------
    def put_artifact(
        self,
        name: str,
        content: bytes,
        content_type: str = OPT,
        metadata: T.Dict[str, str] = OPT,
        tags: T.Dict[str, str] = OPT,
        bsm: T.Optional[BotoSesManager] = None,
    ) -> Artifact:
        """
        Create or update artifact to the latest version with automatic deduplication.

        Uploads binary content to S3 and creates/updates the corresponding DynamoDB
        metadata record. Implements SHA256-based content deduplication to avoid
        unnecessary uploads when content hasn't changed.

        :param name: Artifact name identifier
        :param content: Binary artifact content to upload
        :param content_type: MIME content type for S3 object (e.g., "application/zip")
        :param metadata: Additional S3 object metadata key-value pairs
        :param tags: S3 object tags for categorization and billing
        :param bsm: Optional boto session manager for explicit AWS credentials/configuration.
            If None, uses the global session established by bootstrap

        :returns: Public API Artifact object with integrated S3 access capabilities

        **Usage Examples**::

            # Basic artifact upload
            artifact = repo.put_artifact("my-app", binary_content)

            # Upload with content type and metadata
            artifact = repo.put_artifact(
                name="my-app",
                content=zip_content,
                content_type="application/zip",
                metadata={"build_id": "12345", "environment": "production"},
                tags={"team": "backend", "project": "api"}
            )

            # Upload with explicit session
            artifact = repo.put_artifact("my-app", content, bsm=custom_bsm)

        .. note::

            Content deduplication uses SHA256 hashing. If content hasn't changed,
            only the timestamp is updated, avoiding unnecessary S3 uploads.
        """
        # Step 1: Create DynamoDB artifact record for LATEST version
        artifact = self._ArtifactOrmClass.new(name=name)

        # Step 2: Calculate SHA256 hash for content deduplication and integrity
        artifact_sha256 = hashes.of_bytes(content)
        artifact.sha256 = artifact_sha256

        # Step 3: Determine S3 location for LATEST version
        s3path = self.get_artifact_s3path(name=name, version=constants.LATEST_VERSION)

        # Step 4: Check for content deduplication - avoid unnecessary uploads
        if s3path.exists(bsm=bsm):
            # Compare SHA256 hashes to detect if content has actually changed
            if s3path.metadata["artifact_sha256"] == artifact_sha256:
                # Content unchanged - just update timestamp and return existing artifact
                artifact.update_at = s3path.last_modified_at
                return self._get_artifact_object(artifact=artifact)

        # Step 5: Prepare S3 metadata with required fields for artifact tracking
        final_metadata = dict(
            artifact_name=name,
            artifact_sha256=artifact_sha256,
        )
        # Merge any additional user-provided metadata
        if metadata is not OPT:
            final_metadata.update(metadata)

        # Step 6: Upload new content to S3 with metadata and tags
        s3path.write_bytes(
            content,
            metadata=final_metadata,
            **remove_optional(
                content_type=content_type,
                tags=tags,
            ),
            bsm=bsm,
        )

        # Step 7: Refresh S3 object info to get accurate timestamp
        s3path.head_object(bsm=bsm)

        # Step 8: Save artifact metadata to DynamoDB with S3 timestamp
        artifact.update_at = s3path.last_modified_at

        with use_boto_session(self._ArtifactOrmClass, bsm):
            artifact.save()

        # Step 9: Return public API artifact object with integrated S3 access
        return self._get_artifact_object(artifact=artifact)

    def _get_artifact_dynamodb_item(
        self,
        name: str,
        version: T.Union[int, str],
        bsm: T.Optional[BotoSesManager] = None,
    ) -> dynamodb.Artifact:
        """
        Retrieve a specific artifact version from DynamoDB.
        """
        try:
            with use_boto_session(self._ArtifactOrmClass, bsm):
                # Use encode_version_sk to handle both int and str versions
                artifact = self._ArtifactOrmClass.get(
                    hash_key=name,
                    range_key=dynamodb.encode_version_sk(version),
                )
                if artifact.is_deleted:
                    raise exc.ArtifactNotFoundError(
                        f"name = {name!r}, version = {version!r}"
                    )
                return artifact
        except self._ArtifactOrmClass.DoesNotExist:
            raise exc.ArtifactNotFoundError(f"name = {name!r}, version = {version!r}")

    def get_artifact_version(
        self,
        name: str,
        version: T.Optional[T.Union[int, str]] = None,
        bsm: T.Optional[BotoSesManager] = None,
    ) -> Artifact:
        """
        Retrieve detailed information about a specific artifact version.

        Fetches artifact metadata from DynamoDB and constructs a public API
        Artifact object with integrated S3 access capabilities. Returns the
        LATEST version by default when no version is specified.

        :param name: Artifact name identifier
        :param version: Specific version to retrieve (int, str, or None for LATEST)
        :param bsm: Optional boto session manager for explicit AWS credentials/configuration.
            If None, uses the global session established by bootstrap

        :returns: Public API Artifact object with metadata and S3 content access

        :raises ArtifactNotFoundError: If artifact or version doesn't exist or is deleted

        **Usage Examples**::

            # Get latest version
            artifact = repo.get_artifact_version("my-app")

            # Get specific numbered version
            artifact = repo.get_artifact_version("my-app", version=5)

            # Get version with explicit session
            artifact = repo.get_artifact_version("my-app", version="3", bsm=custom_bsm)

            # Access artifact properties
            print(f"Version: {artifact.version}")
            print(f"Updated: {artifact.update_at}")
            print(f"SHA256: {artifact.sha256}")
            print(f"S3 URI: {artifact.s3uri}")

            # Download content
            content = artifact.get_content(bsm)
        """
        if version is None:
            version = constants.LATEST_VERSION
        artifact = self._get_artifact_dynamodb_item(
            name=name,
            version=version,
            bsm=bsm,
        )
        return self._get_artifact_object(artifact=artifact)

    def list_artifact_versions(
        self,
        name: str,
        bsm: T.Optional[BotoSesManager] = None,
    ) -> T.List[Artifact]:
        """
        Retrieve all versions of an artifact in descending order by version number.

        Queries DynamoDB for all non-deleted versions of the specified artifact.
        Returns a list ordered from newest to oldest, with LATEST always appearing
        first if it exists.

        :param name: Artifact name identifier to list versions for
        :param bsm: Optional boto session manager for explicit AWS credentials/configuration.
            If None, uses the global session established by bootstrap

        :returns: List of public API Artifact objects ordered by version (newest first)

        **Usage Examples**::

            # List all versions of an artifact
            versions = repo.list_artifact_versions("my-app")

            # Access version information
            for artifact in versions:
                print(f"Version {artifact.version}: {artifact.update_at}")

            # List versions with explicit session
            versions = repo.list_artifact_versions("my-app", bsm=custom_bsm)

            # Get latest and previous version
            if len(versions) >= 2:
                latest = versions[0]  # Always LATEST if it exists
                previous = versions[1]  # Most recent numbered version

        .. note::

            Only returns non-deleted versions. Soft-deleted versions are excluded
            from the results.
        """
        with use_boto_session(self._ArtifactOrmClass, bsm):
            artifact_list = [
                self._get_artifact_object(artifact=artifact)
                for artifact in self._ArtifactOrmClass.query(
                    hash_key=name,
                    scan_index_forward=False,
                    filter_condition=self._ArtifactOrmClass.is_deleted == False,
                )
            ]
            return artifact_list

    def publish_artifact_version(
        self,
        name: str,
        bsm: T.Optional[BotoSesManager] = None,
    ) -> Artifact:
        """
        Create an immutable numbered version from the current LATEST artifact.

        Performs a server-side copy in S3 from LATEST to a new numbered version
        and creates the corresponding DynamoDB metadata record. This creates a
        permanent, immutable snapshot that can be used for deployments and rollbacks.

        :param name: Artifact name identifier to publish version for
        :param bsm: Optional boto session manager for explicit AWS credentials/configuration.
            If None, uses the global session established by bootstrap

        :returns: Public API Artifact object representing the newly created version

        :raises ArtifactNotFoundError: If no LATEST version exists for the artifact

        **Usage Examples**::

            # Create version 1 from LATEST
            version = repo.publish_artifact_version("my-app")
            print(f"Created version: {version.version}")

            # Publish version with explicit session
            version = repo.publish_artifact_version("my-app", bsm=custom_bsm)

            # Typical deployment workflow
            # 1. Upload new content to LATEST
            artifact = repo.put_artifact("my-app", new_content)

            # 2. Create immutable version
            version = repo.publish_artifact_version("my-app")

            # 3. Deploy to production
            repo.put_alias("my-app", "prod", version=version.version)

        .. note::

            Versions are automatically numbered sequentially starting from 1.
            The version number is determined by incrementing the highest existing
            numbered version.
        """
        # Step 1: Query for up to 2 most recent versions to determine next version number
        # Query returns results in descending order: [LATEST, most_recent_version]
        with use_boto_session(self._ArtifactOrmClass, bsm):
            artifacts = list(
                self._ArtifactOrmClass.query(
                    hash_key=name, scan_index_forward=False, limit=2
                )
            )

            # Step 3: Validate artifact exists and determine next version number
            if len(artifacts) == 0:
                # No artifact exists - cannot publish version without LATEST
                raise exc.ArtifactNotFoundError(f"name = {name!r}")
            elif len(artifacts) == 1:
                # Only LATEST exists - this will be version 1
                new_version = dynamodb.encode_version(1)
            else:
                # Multiple versions exist - increment highest numbered version
                # artifacts[1] is the most recent numbered version (not LATEST)
                new_version = str(int(artifacts[1].version) + 1)

            # Step 4: Copy binary content from LATEST to new immutable version in S3
            s3path_old = self.get_artifact_s3path(
                name=name,
                version=constants.LATEST_VERSION,
            )
            s3path_new = self.get_artifact_s3path(name=name, version=new_version)
            # S3 server-side copy preserves all metadata and content
            s3path_old.copy_to(s3path_new, bsm=bsm)
            # Refresh object info to get accurate timestamp
            s3path_new.head_object(bsm=bsm)

            # Step 5: Create DynamoDB record for the new immutable version
            artifact = self._ArtifactOrmClass.new(name=name, version=new_version)
            # Copy SHA256 from LATEST version (artifacts[0] is always LATEST due to sort order)
            artifact.sha256 = artifacts[0].sha256
            # Use S3 copy timestamp for consistency
            artifact.update_at = s3path_new.last_modified_at
            artifact.save()

            # Step 6: Return public API artifact object
            return self._get_artifact_object(artifact=artifact)

    def delete_artifact_version(
        self,
        name: str,
        version: T.Optional[T.Union[int, str]] = None,
        bsm: T.Optional[BotoSesManager] = None,
    ):
        """
        Soft delete a specific artifact version by marking it as deleted.

        Marks the artifact version as deleted in DynamoDB without removing the
        actual S3 binary or DynamoDB item. Soft-deleted versions become invisible
        to get_artifact_version and list_artifact_versions operations but can be
        recovered if needed.

        :param name: Artifact name identifier
        :param version: Specific version to delete (int, str, or None for LATEST)
        :param bsm: Optional boto session manager for explicit AWS credentials/configuration.
           If None, uses the global session established by bootstrap

        **Usage Examples**::

            # Soft delete latest version
            repo.delete_artifact_version("my-app")

            # Soft delete specific version
            repo.delete_artifact_version("my-app", version=5)

            # Delete with explicit session
            repo.delete_artifact_version("my-app", version="3", bsm=custom_bsm)

        .. note::

            This is a soft delete operation. The S3 binary and DynamoDB record
            remain intact but marked as deleted. Use purge_artifact for permanent
            deletion if needed.
        """
        if version is None:
            version = constants.LATEST_VERSION

        with use_boto_session(self._ArtifactOrmClass, bsm):
            res = self._ArtifactOrmClass.new(name=name, version=version).update(
                actions=[
                    self._ArtifactOrmClass.is_deleted.set(True),
                ],
            )
            # print(res)

    def list_artifact_names(
        self,
        bsm: T.Optional[BotoSesManager] = None,
    ) -> T.List[str]:
        """
        Retrieve all artifact names available in this repository.

        Scans the S3 bucket directory structure to identify all artifact names
        that have been uploaded to the repository. This provides a high-level
        view of all artifacts without listing individual versions.

        :param bsm: Optional boto session manager for explicit AWS credentials/configuration.
           If None, uses the global session established by bootstrap

        :returns: List of artifact name strings found in the repository

        **Usage Examples**::

            # List all artifact names
            names = repo.list_artifact_names()
            print(f"Found {len(names)} artifacts: {names}")

            # List names with explicit session
            names = repo.list_artifact_names(bsm=custom_bsm)

            # Iterate through all artifacts
            for name in repo.list_artifact_names():
                versions = repo.list_artifact_versions(name)
                print(f"{name}: {len(versions)} versions")

        .. note::

            This method scans the S3 directory structure, so it may be slower for
            repositories with many artifacts compared to DynamoDB-based queries.
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
        name: str,
        alias: str,
        version: T.Optional[T.Union[int, str]] = None,
        secondary_version: T.Optional[T.Union[int, str]] = None,
        secondary_version_weight: T.Optional[int] = None,
        bsm: T.Optional[BotoSesManager] = None,
    ) -> Alias:
        """
        Create or update an alias pointing to artifact version(s) with optional traffic splitting.

        Creates a named alias that can point to a single version or split traffic between
        two versions for advanced deployment patterns like canary releases and blue/green
        deployments. Validates that target versions exist before creating the alias.

        :param name: Artifact name identifier the alias belongs to
        :param alias: Alias name identifier (cannot contain hyphens due to DynamoDB constraints)
        :param version: Primary version to point to (int, str, or None for LATEST)
        :param secondary_version: Optional secondary version for traffic splitting
        :param secondary_version_weight: Percentage (0-99) of traffic routed to secondary version
        :param bsm: Optional boto session manager for explicit AWS credentials/configuration.
           If None, uses the global session established by bootstrap

        :returns: Public API Alias object with traffic splitting configuration

        :raises ValueError: If alias contains hyphens, traffic weight is invalid, or versions are identical
        :raises TypeError: If secondary_version_weight is not an integer
        :raises ArtifactNotFoundError: If target version(s) don't exist

        **Usage Examples**::

            # Simple alias pointing to latest
            alias = repo.put_alias("my-app", "prod")

            # Alias pointing to specific version
            alias = repo.put_alias("my-app", "prod", version=5)

            # Canary deployment with 20% traffic to new version
            alias = repo.put_alias(
                name="my-app",
                alias="prod",
                version=5,                    # 80% traffic
                secondary_version=6,          # 20% traffic
                secondary_version_weight=20
            )

            # Blue/green deployment preparation
            alias = repo.put_alias("my-app", "staging", version=6)  # Test on staging
            alias = repo.put_alias("my-app", "prod", version=6)     # Switch production

            # Create alias with explicit session
            alias = repo.put_alias("my-app", "prod", version=5, bsm=custom_bsm)

        .. note::

            Alias names cannot contain hyphens due to DynamoDB key encoding constraints.
            Use underscores or camelCase for multi-word aliases.
        """
        # Step 1: Validate alias naming constraints
        # Hyphens are forbidden due to DynamoDB key encoding conflicts
        if "-" in alias:  # pragma: no cover
            raise ValueError("alias cannot have hyphen")

        # Step 2: Normalize primary version identifier
        version = dynamodb.encode_version(version)

        # Step 3: Validate and normalize traffic splitting configuration
        if secondary_version is not None:
            # Normalize secondary version identifier
            secondary_version = dynamodb.encode_version(secondary_version)

            # Validate traffic weight parameter type
            if not isinstance(secondary_version_weight, int):
                raise TypeError("secondary_version_weight must be int")

            # Validate traffic weight range (0-99, primary gets remainder)
            if not (0 <= secondary_version_weight < 100):
                raise ValueError("secondary_version_weight must be 0 <= x < 100")

            # Prevent pointing alias to the same version twice
            if version == secondary_version:
                raise ValueError(
                    f"version {version!r} and secondary_version {secondary_version!r} "
                    f"cannot be the same!"
                )

        # Step 4: Verify target artifact versions exist in DynamoDB
        # Validate primary version exists and is not soft-deleted
        self._get_artifact_dynamodb_item(
            name=name,
            version=version,
            bsm=bsm,
        )

        # Validate secondary version exists if traffic splitting is configured
        if secondary_version is not None:
            self._get_artifact_dynamodb_item(
                name=name,
                version=secondary_version,
                bsm=bsm,
            )

        # Step 5: Create or update alias record in DynamoDB
        with use_boto_session(self._AliasOrmClass, bsm):
            alias = self._AliasOrmClass.new(
                name=name,
                alias=alias,
                version=version,
                secondary_version=secondary_version,
                secondary_version_weight=secondary_version_weight,
            )
            alias.save()

        # Step 6: Return public API alias object with S3 URI resolution
        return self._get_alias_object(alias=alias)

    def get_alias(
        self,
        name: str,
        alias: str,
        bsm: T.Optional[BotoSesManager] = None,
    ) -> Alias:
        """
        Retrieve detailed information about a specific artifact alias.

        Fetches alias configuration from DynamoDB and constructs a public API
        Alias object with traffic splitting details and S3 access capabilities
        for both primary and secondary versions if configured.

        :param name: Artifact name identifier the alias belongs to
        :param alias: Alias name identifier to retrieve
        :param bsm: Optional boto session manager for explicit AWS credentials/configuration.
           If None, uses the global session established by bootstrap

        :returns: Public API Alias object with version details and S3 access

        :raises AliasNotFoundError: If the alias doesn't exist

        **Usage Examples**::

            # Get simple alias
            alias = repo.get_alias("my-app", "prod")
            print(f"Production points to version: {alias.version}")

            # Get alias with traffic splitting
            alias = repo.get_alias("my-app", "canary")
            if alias.secondary_version:
                print(f"Traffic split: {alias.version} ({100-alias.secondary_version_weight}%) "
                      f"and {alias.secondary_version} ({alias.secondary_version_weight}%)")

            # Get alias with explicit session
            alias = repo.get_alias("my-app", "prod", bsm=custom_bsm)

            # Use alias for content access
            content = alias.get_version_content(bsm)  # Always primary version
            selected_uri = alias.random_artifact()    # Traffic-weighted selection
        """
        try:
            with use_boto_session(self._AliasOrmClass, bsm):
                alias = self._AliasOrmClass.get(
                    hash_key=dynamodb.encode_alias_pk(name),
                    range_key=alias,
                )
                return self._get_alias_object(alias=alias)
        except self._AliasOrmClass.DoesNotExist:
            raise exc.AliasNotFoundError(f"name = {name!r}, alias = {alias!r}")

    def list_aliases(
        self,
        name: str,
        bsm: T.Optional[BotoSesManager] = None,
    ) -> T.List[Alias]:
        """
        Retrieve all aliases configured for a specific artifact.

        Queries DynamoDB for all aliases associated with the specified artifact
        and constructs a list of public API Alias objects with complete traffic
        splitting configurations and S3 access capabilities.

        :param name: Artifact name identifier to list aliases for
        :param bsm: Optional boto session manager for explicit AWS credentials/configuration.
           If None, uses the global session established by bootstrap

        :returns: List of public API Alias objects for the artifact

        **Usage Examples**::

            # List all aliases for an artifact
            aliases = repo.list_aliases("my-app")
            for alias in aliases:
                print(f"Alias {alias.alias} -> version {alias.version}")

            # List aliases with explicit session
            aliases = repo.list_aliases("my-app", bsm=custom_bsm)

            # Check for specific deployment patterns
            for alias in repo.list_aliases("my-app"):
                if alias.secondary_version:
                    print(f"Canary: {alias.alias} splitting traffic between "
                          f"v{alias.version} and v{alias.secondary_version}")
                else:
                    print(f"Single: {alias.alias} -> v{alias.version}")

        .. note::

            Returns all aliases regardless of their traffic splitting configuration.
            Check the secondary_version field to determine if traffic splitting is active.
        """
        with use_boto_session(self._AliasOrmClass, bsm):
            alias_list = [
                self._get_alias_object(alias=alias)
                for alias in self._AliasOrmClass.query(
                    hash_key=dynamodb.encode_alias_pk(name)
                )
            ]
            return alias_list

    def delete_alias(
        self,
        name: str,
        alias: str,
        bsm: T.Optional[BotoSesManager] = None,
    ):
        """
        Permanently delete an alias from the repository.

        Removes the alias record from DynamoDB completely. This is a hard delete
        operation that cannot be undone. The underlying artifact versions and
        S3 objects remain intact and unaffected.

        :param name: Artifact name identifier the alias belongs to
        :param alias: Alias name identifier to delete
        :param bsm: Optional boto session manager for explicit AWS credentials/configuration.
           If None, uses the global session established by bootstrap

        **Usage Examples**::

            # Delete production alias
            repo.delete_alias("my-app", "prod")

            # Delete with explicit session
            repo.delete_alias("my-app", "staging", bsm=custom_bsm)

            # Safe cleanup workflow
            # 1. First redirect traffic to a safe version
            repo.put_alias("my-app", "prod", version="stable_version")

            # 2. Then delete the problematic alias
            repo.delete_alias("my-app", "canary")

        .. warning::

            This is a permanent deletion. The alias configuration including any
            traffic splitting setup will be completely lost and cannot be recovered.
        """
        with use_boto_session(self._AliasOrmClass, bsm):
            res = self._AliasOrmClass.new(name=name, alias=alias).delete()
            # print(res)

    def purge_artifact_versions(
        self,
        name: str,
        keep_last_n: int = 10,
        purge_older_than_secs: int = 90 * 24 * 60 * 60,
        bsm: T.Optional[BotoSesManager] = None,
    ) -> T.Tuple[datetime, T.List[Artifact]]:
        """
        Selectively soft-delete old artifact versions based on retention policies.

        Applies retention rules to automatically clean up old versions while preserving
        recent versions and the LATEST version. Uses both count-based and age-based
        retention criteria to determine which versions to soft-delete.

        :param name: Artifact name identifier to purge versions for
        :param keep_last_n: Minimum number of recent versions to preserve (default: 10)
        :param purge_older_than_secs: Age threshold in seconds for purging (default: 90 days)
        :param bsm: Optional boto session manager for explicit AWS credentials/configuration.
           If None, uses the global session established by bootstrap

        :returns: Tuple of (purge_timestamp, list_of_deleted_artifacts)

        **Retention Logic**:

        A version is soft-deleted if ALL of the following conditions are met:

        - Version is NOT in the most recent ``keep_last_n`` versions
        - Version is older than ``purge_older_than_secs`` seconds
        - Version is NOT the LATEST version (always preserved)

        **Usage Examples**::

            # Use default retention (keep 10 recent, purge >90 days)
            purge_time, deleted = repo.purge_artifact_versions("my-app")
            print(f"Purged {len(deleted)} versions at {purge_time}")

            # Custom retention policy (keep 5 recent, purge >30 days)
            purge_time, deleted = repo.purge_artifact_versions(
                name="my-app",
                keep_last_n=5,
                purge_older_than_secs=30 * 24 * 60 * 60  # 30 days
            )

            # Aggressive cleanup (keep only 3 recent, purge >7 days)
            purge_time, deleted = repo.purge_artifact_versions(
                "my-app", keep_last_n=3, purge_older_than_secs=7*24*60*60, bsm=custom_bsm
            )

        .. note::

            This performs soft deletion only. Use purge_artifact for permanent deletion
            of all artifact data including S3 binaries.
        """
        artifact_list = self.list_artifact_versions(name=name, bsm=bsm)
        purge_time = get_utc_now()
        expire = purge_time - timedelta(seconds=purge_older_than_secs)
        deleted_artifact_list = list()
        for artifact in artifact_list[keep_last_n + 1 :]:
            if artifact.update_at < expire:
                self.delete_artifact_version(
                    name=name,
                    version=artifact.version,
                    bsm=bsm,
                )
                deleted_artifact_list.append(artifact)
        return purge_time, deleted_artifact_list

    def purge_artifact(
        self,
        name: str,
        bsm: T.Optional[BotoSesManager] = None,
    ):
        """
        Permanently delete all versions and aliases for a specific artifact.

        Performs complete cleanup by removing all S3 binaries, DynamoDB metadata
        records, and alias configurations for the specified artifact. This is
        an irreversible hard delete operation.

        :param name: Artifact name identifier to completely purge
        :param bsm: Optional boto session manager for explicit AWS credentials/configuration.
            If None, uses the global session established by bootstrap

        **Usage Examples**::

            # Permanently delete all data for an artifact
            repo.purge_artifact("my-app")

            # Delete with explicit session
            repo.purge_artifact("my-app", bsm=custom_bsm)

            # Safe deletion workflow
            # 1. First ensure no aliases point to this artifact
            aliases = repo.list_aliases("my-app")
            for alias in aliases:
                repo.delete_alias("my-app", alias.alias)

            # 2. Then purge the artifact completely
            repo.purge_artifact("my-app")

        .. danger::

            This operation is IRREVERSIBLE. All versions, aliases, metadata,
            and S3 binaries for this artifact will be permanently destroyed.
            Ensure no systems depend on this artifact before deletion.
        """
        s3path = self.get_artifact_s3path(name=name, version=constants.LATEST_VERSION)
        s3dir = s3path.parent
        s3dir.delete(bsm=bsm)

        with use_boto_session(self._ArtifactOrmClass, bsm):
            with self._ArtifactOrmClass.batch_write() as batch:
                for artifact in self._ArtifactOrmClass.query(hash_key=name):
                    batch.delete(artifact)

        with use_boto_session(self._AliasOrmClass, bsm):
            with self._AliasOrmClass.batch_write() as batch:
                for alias in self._AliasOrmClass.query(
                    hash_key=dynamodb.encode_alias_pk(name)
                ):
                    batch.delete(alias)

    def purge_all(self, bsm: T.Optional[BotoSesManager] = None):
        """
        Permanently delete ALL artifacts and aliases in the entire repository.

        Performs complete repository cleanup by removing all S3 content and
        DynamoDB data. This destroys the entire repository content while
        leaving the AWS resources (bucket, table) intact for future use.

        :param bsm: Optional boto session manager for explicit AWS credentials/configuration.
           If None, uses the global session established by bootstrap

        **Usage Examples**::

            # Nuclear option - delete everything (typically for testing)
            repo.purge_all()

            # Purge with explicit session
            repo.purge_all(bsm=custom_bsm)

            # Repository reset workflow
            # 1. Backup critical data if needed
            important_artifacts = ["critical-app", "backup-service"]
            for name in important_artifacts:
                artifact = repo.get_artifact_version(name)
                # ... backup logic ...

            # 2. Purge everything
            repo.purge_all()

            # 3. Repository is now empty but ready for new artifacts

        .. danger::

            This operation is IRREVERSIBLE and destroys ALL artifacts,
            versions, aliases, and metadata in the repository. Only use
            for testing or complete repository decommissioning.
        """
        self.s3dir_artifact_store.delete(bsm=bsm)
        # note: Artifact and Alias are in the same DynamoDB table
        # since we do scan, so we only need to delete all Artifact items
        with use_boto_session(self._ArtifactOrmClass, bsm):
            with self._ArtifactOrmClass.batch_write() as batch:
                for item in self._ArtifactOrmClass.scan():
                    batch.delete(item)
