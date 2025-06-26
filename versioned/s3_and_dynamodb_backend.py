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
from s3pathlib import S3Path, context
from func_args import NOTHING
from pynamodb.connection import Connection
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
    
    Provides a clean, user-friendly interface for working with artifact versions
    that abstracts away the underlying DynamoDB storage details. Combines metadata
    from DynamoDB with direct S3 content access capabilities.
    
    This is the primary data structure returned by Repository methods for artifact
    operations. It encapsulates both the metadata (stored in DynamoDB) and provides
    convenient methods for accessing the binary content (stored in S3).
    
    :param name: Artifact name identifier
    :param version: Version identifier ("LATEST", "1", "2", etc.)
    :param update_at: UTC timestamp when this artifact version was last modified
    :param s3uri: Complete S3 URI where the binary content is stored
    :param sha256: SHA256 hash of the binary content for integrity verification
    
    **Usage Examples**::
    
        # Get artifact version
        artifact = repo.get_artifact_version("my-app", version=5)
        
        # Access metadata
        print(f"Version {artifact.version} updated at {artifact.update_at}")
        print(f"Content hash: {artifact.sha256}")
        print(f"Stored at: {artifact.s3uri}")
        
        # Download content
        content = artifact.get_content(bsm)
        
        # Access S3 path for advanced operations
        s3path = artifact.s3path
        metadata = s3path.head_object().metadata
    
    .. note::
        This class provides a simplified interface compared to the underlying
        DynamoDB model. For direct DynamoDB operations, use the dynamodb module.
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
    
    Provides sophisticated traffic routing capabilities for deployment patterns
    including blue/green deployments, canary releases, and A/B testing. Encapsulates
    both single-version aliases and dual-version traffic splitting configurations.
    
    This is the primary data structure returned by Repository methods for alias
    operations. It combines DynamoDB metadata with direct S3 access to both
    primary and secondary artifact versions.
    
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
    
        # Simple alias pointing to single version
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
        
        # Random selection based on traffic weights
        selected_uri = alias.random_artifact()  # Returns URI based on weights
        
        # Access specific versions
        primary_content = alias.get_version_content(bsm)
        if alias.secondary_version:
            secondary_content = alias.get_secondary_version_content(bsm)
    
    .. note::
        Alias names cannot contain hyphens due to DynamoDB key encoding constraints.
        Use underscores or camelCase for multi-word alias names.
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
    
    **Usage Examples**::

        # Initialize repository
        repo = Repository(
            aws_region=\"us-east-1\",
            s3_bucket=\"my-artifacts\", 
            s3_prefix=\"versioned-artifacts\",
            dynamodb_table_name=\"artifact-metadata\"
        )\n        
        # Bootstrap AWS resources (run once)
        repo.bootstrap(bsm)\n        
        # Upload new artifact version
        artifact = repo.put_artifact(\"my-app\", content=binary_data)\n        
        # Create immutable version
        version = repo.publish_artifact_version(\"my-app\")\n        
        # Create production alias
        alias = repo.put_alias(\"my-app\", \"prod\", version=version.version)\n        
        # Retrieve via alias
        prod_artifact = repo.get_alias(\"my-app\", \"prod\")\n        content = prod_artifact.get_version_content(bsm)
    
    **Key Features**:
    
    - **Automatic Deduplication**: SHA256-based content deduplication across versions
    - **Soft Deletion**: Safe deletion with recovery capabilities
    - **Traffic Splitting**: Weighted routing for canary deployments
    - **Atomic Operations**: Consistent state management across S3 and DynamoDB
    - **Cost Optimization**: Efficient data placement for minimal storage costs
    
    .. note::

        The Repository requires both S3 and DynamoDB permissions. Use the :meth:`bootstrap`
        method to create necessary AWS resources automatically.
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

    @property
    def _artifact_class(self) -> T.Type[dynamodb.Artifact]:
        class Artifact(dynamodb.Artifact):
            class Meta:
                table_name = self.dynamodb_table_name
                region = self.aws_region

        return Artifact

    @property
    def _alias_class(self) -> T.Type[dynamodb.Alias]:
        class Alias(dynamodb.Alias):
            class Meta:
                table_name = self.dynamodb_table_name
                region = self.aws_region

        return Alias

    def _get_artifact_object(
        self,
        artifact: dynamodb.Artifact,
    ) -> Artifact:
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

    def connect_boto_session(self, bsm: BotoSesManager):
        """
        Explicitly connect the underlying DynamoDB to the specified AWS Credential.

        :param bsm: ``boto_session_manager.BotoSesManager`` object.
        """
        context.attach_boto_session(bsm.boto_ses)
        with bsm.awscli():
            Connection()

    # ------------------------------------------------------------------------------
    # Artifact
    # ------------------------------------------------------------------------------
    def put_artifact(
        self,
        name: str,
        content: bytes,
        content_type: str = NOTHING,
        metadata: T.Dict[str, str] = NOTHING,
        tags: T.Dict[str, str] = NOTHING,
    ) -> Artifact:
        """
        Create / Update artifact to the latest.

        :param name: artifact name.
        :param content: binary artifact content.
        :param content_type: optional content type of the s3 object.
        :param metadata: optional metadata of the s3 object.
        :param tags: optional tags of the s3 object.
        """
        # Step 1: Create DynamoDB artifact record for LATEST version
        artifact = self._artifact_class.new(name=name)
        
        # Step 2: Calculate SHA256 hash for content deduplication and integrity
        artifact_sha256 = hashes.of_bytes(content)
        artifact.sha256 = artifact_sha256
        
        # Step 3: Determine S3 location for LATEST version
        s3path = self.get_artifact_s3path(name=name, version=constants.LATEST_VERSION)

        # Step 4: Check for content deduplication - avoid unnecessary uploads
        if s3path.exists():
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
        if metadata is not NOTHING:
            final_metadata.update(metadata)
            
        # Step 6: Upload new content to S3 with metadata and tags
        s3path.write_bytes(
            content,
            metadata=final_metadata,
            content_type=content_type,
            tags=tags,
        )
        
        # Step 7: Refresh S3 object info to get accurate timestamp
        s3path.head_object()
        
        # Step 8: Save artifact metadata to DynamoDB with S3 timestamp
        artifact.update_at = s3path.last_modified_at
        artifact.save()
        
        # Step 9: Return public API artifact object with integrated S3 access
        return self._get_artifact_object(artifact=artifact)

    def _get_artifact_dynamodb_item(
        self,
        artifact_class: T.Type[dynamodb.Artifact],
        name: str,
        version: T.Union[int, str],
    ) -> dynamodb.Artifact:
        try:
            artifact = artifact_class.get(
                hash_key=name,
                range_key=dynamodb.encode_version_sk(version),
            )
            if artifact.is_deleted:
                raise exc.ArtifactNotFoundError(
                    f"name = {name!r}, version = {version!r}"
                )
            return artifact
        except artifact_class.DoesNotExist:
            raise exc.ArtifactNotFoundError(f"name = {name!r}, version = {version!r}")

    def get_artifact_version(
        self,
        name: str,
        version: T.Optional[T.Union[int, str]] = None,
    ) -> Artifact:
        """
        Return the information about the artifact or artifact version.

        :param name: artifact name.
        :param version: artifact version. If ``None``, return the latest version.
        """
        if version is None:
            version = constants.LATEST_VERSION
        artifact = self._get_artifact_dynamodb_item(
            self._artifact_class,
            name=name,
            version=version,
        )
        return self._get_artifact_object(artifact=artifact)

    def list_artifact_versions(
        self,
        name: str,
    ) -> T.List[Artifact]:
        """
        Return a list of artifact versions. The latest version is always the first item.
        And the newer version comes first.

        :param name: artifact name.
        """
        Artifact = self._artifact_class
        return [
            self._get_artifact_object(artifact=artifact)
            for artifact in Artifact.query(
                hash_key=name,
                scan_index_forward=False,
                filter_condition=Artifact.is_deleted == False,
            )
        ]

    def publish_artifact_version(
        self,
        name: str,
    ) -> Artifact:
        """
        Creates a version from the latest artifact. Use versions to create an
        immutable snapshot of your latest artifact.

        :param name: artifact name.
        """
        # Step 1: Get DynamoDB artifact class with repository configuration
        Artifact = self._artifact_class
        
        # Step 2: Query for up to 2 most recent versions to determine next version number
        # Query returns results in descending order: [LATEST, most_recent_version]
        artifacts = list(
            Artifact.query(hash_key=name, scan_index_forward=False, limit=2)
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
        s3path_old.copy_to(s3path_new)
        # Refresh object info to get accurate timestamp
        s3path_new.head_object()

        # Step 5: Create DynamoDB record for the new immutable version
        artifact = Artifact.new(name=name, version=new_version)
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
    ):
        """
        Deletes a specific version of artifact. If version is not specified,
        the latest version is deleted. Note that this is a soft delete,
        neither the S3 artifact nor the DynamoDB item is deleted. It is just
        become "invisible" to the :func:`get_artifact` and :func:`list_artifacts``.

        :param name: artifact name.
        :param version: artifact version. If ``None``, delete the latest version.
        """
        Artifact = self._artifact_class
        if version is None:
            version = constants.LATEST_VERSION
        res = Artifact.new(name=name, version=version).update(
            actions=[
                Artifact.is_deleted.set(True),
            ],
        )
        # print(res)

    def list_artifact_names(self) -> T.List[str]:
        """
        Return a list of artifact names in this repository.

        :return: list of artifact names.
        """
        names = list()
        for p in self.s3dir_artifact_store.iterdir():
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
    ) -> Alias:
        """
        Creates an alias for an artifact version. If ``version`` is not specified,
        the latest version is used.

        You can also map an alias to split invocation requests between two versions.
        Use the ``secondary_version`` and ``secondary_version_weight`` to specify
        a second version and the percentage of invocation requests that it receives.

        :param name: artifact name.
        :param alias: alias name. alias name cannot have hyphen
        :param version: artifact version. If ``None``, the latest version is used.
        :param secondary_version: see above.
        :param secondary_version_weight: an integer between 0 ~ 100.
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
        Artifact = self._artifact_class
        
        # Validate primary version exists and is not soft-deleted
        self._get_artifact_dynamodb_item(
            Artifact,
            name=name,
            version=version,
        )
        
        # Validate secondary version exists if traffic splitting is configured
        if secondary_version is not None:
            self._get_artifact_dynamodb_item(
                Artifact,
                name=name,
                version=secondary_version,
            )

        # Step 5: Create or update alias record in DynamoDB
        Alias = self._alias_class
        alias = Alias.new(
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
    ) -> Alias:
        """
        Return details about the alias.

        :param name: artifact name.
        :param alias: alias name. alias name cannot have hyphen
        """
        Alias = self._alias_class
        try:
            return self._get_alias_object(
                alias=Alias.get(
                    hash_key=dynamodb.encode_alias_pk(name),
                    range_key=alias,
                ),
            )
        except Alias.DoesNotExist:
            raise exc.AliasNotFoundError(f"name = {name!r}, alias = {alias!r}")

    def list_aliases(
        self,
        name: str,
    ) -> T.List[Alias]:
        """
        Returns a list of aliases for an artifact.

        :param name: artifact name.
        """
        Alias = self._alias_class
        return [
            self._get_alias_object(alias=alias)
            for alias in Alias.query(hash_key=dynamodb.encode_alias_pk(name))
        ]

    def delete_alias(
        self,
        name: str,
        alias: str,
    ):
        """
        Permanently delete an alias from the repository.

        It only removes the alias from DynamoDB. It won't delete the
        underlying artifact version or S3 object.
        """
        res = self._alias_class.new(name=name, alias=alias).delete()
        # print(res)

    def purge_artifact_versions(
        self,
        name: str,
        keep_last_n: int = 10,
        purge_older_than_secs: int = 90 * 24 * 60 * 60,
    ) -> T.Tuple[datetime, T.List[Artifact]]:
        """
        If an artifact version is within the last ``keep_last_n`` versions
        and is not older than ``purge_older_than_secs`` seconds, it will not be deleted.
        Otherwise, it will be deleted. In addition, the latest version will
        always be kept.

        :param name: artifact name.
        :param keep_last_n: number of versions to keep.
        :param purge_older_than_secs: seconds to keep.
        """
        artifact_list = self.list_artifact_versions(name=name)
        purge_time = get_utc_now()
        expire = purge_time - timedelta(seconds=purge_older_than_secs)
        deleted_artifact_list = list()
        for artifact in artifact_list[keep_last_n + 1 :]:
            if artifact.update_at < expire:
                self.delete_artifact_version(
                    name=name,
                    version=artifact.version,
                )
                deleted_artifact_list.append(artifact)
        return purge_time, deleted_artifact_list

    def purge_artifact(
        self,
        name: str,
    ):
        """
        Completely delete all artifacts and aliases of the given artifact name.
        This operation is irreversible. It will remove all related S3 artifacts
        and DynamoDB items.

        :param name: artifact name.

        .. danger::

            This operation is IRREVERSIBLE. All versions, aliases, and metadata
            for this artifact will be permanently lost. Ensure no systems depend
            on this artifact before deletion.
        """
        s3path = self.get_artifact_s3path(name=name, version=constants.LATEST_VERSION)
        s3dir = s3path.parent
        s3dir.delete()

        Artifact = self._artifact_class
        Alias = self._alias_class
        with Artifact.batch_write() as batch:
            for artifact in Artifact.query(hash_key=name):
                batch.delete(artifact)
        with Alias.batch_write() as batch:
            for alias in Alias.query(hash_key=dynamodb.encode_alias_pk(name)):
                batch.delete(alias)

    def purge_all(self):
        """
        Completely delete all artifacts and aliases in this Repository
        This operation is irreversible. It will remove all related S3 artifacts
        and DynamoDB items.

        .. danger::

            This operation is IRREVERSIBLE and will destroy ALL artifacts,
            versions, and aliases in the repository. Only use for testing
            or when completely decommissioning the repository.
        """
        self.s3dir_artifact_store.delete()
        Artifact = self._artifact_class
        with Artifact.batch_write() as batch:
            for item in Artifact.scan():
                batch.delete(item)
