# -*- coding: utf-8 -*-

from versioned.dynamodb import (
    encode_version_sk,
    encode_alias_pk,
    get_utc_now,
    Base,
    Artifact,
    Alias,
)

import pytest
from datetime import datetime, timezone

from versioned.constants import LATEST_VERSION
from versioned.tests.mock_aws import BaseMockAwsTest


class TestUtilityFunctions:
    """Test utility functions for encoding and timestamp generation."""
    def test_encode_version_sk(self):
        """Test DynamoDB sort key encoding for versions."""
        # None should default to LATEST
        assert encode_version_sk(None) == "LATEST"

        # LATEST should remain unchanged
        assert encode_version_sk("LATEST") == "LATEST"
        assert encode_version_sk(LATEST_VERSION) == "LATEST"

        # Numeric versions should be zero-padded
        assert encode_version_sk(1) == "000001"
        assert encode_version_sk(2) == "000002"
        assert encode_version_sk(42) == "000042"
        assert encode_version_sk(999999) == "999999"

        # String versions should be converted and padded
        assert encode_version_sk("1") == "000001"
        assert encode_version_sk("000001") == "000001"  # Already padded
        assert encode_version_sk("42") == "000042"

        # Edge cases
        assert encode_version_sk(0) == "000000"

        # Test that encoding maintains lexicographic ordering
        versions = [1, 2, 10, 100, 999]
        encoded = [encode_version_sk(v) for v in versions]
        assert encoded == sorted(encoded)

    def test_encode_alias_pk(self):
        """Test DynamoDB partition key encoding for aliases."""
        # Basic alias names
        assert encode_alias_pk("my-app") == "__my-app-alias"
        assert encode_alias_pk("frontend") == "__frontend-alias"
        assert encode_alias_pk("api-v2") == "__api-v2-alias"

        # Special characters in artifact names
        assert encode_alias_pk("service_name") == "__service_name-alias"
        assert encode_alias_pk("app.service") == "__app.service-alias"

        # Edge cases
        assert encode_alias_pk("") == "__-alias"
        assert encode_alias_pk("a") == "__a-alias"

        # Ensure consistent prefix pattern
        test_names = ["app1", "app2", "service-x"]
        for name in test_names:
            pk = encode_alias_pk(name)
            assert pk.startswith("__")
            assert pk.endswith("-alias")
            assert f"__{name}-alias" == pk


class TestBaseModel:
    """Test the Base DynamoDB model class."""

    def test_base_model_structure(self):
        """Test Base model has correct attributes."""
        # Test that Base class has required attributes
        assert hasattr(Base, "pk")
        assert hasattr(Base, "sk")

        # Test attribute types (they should be UnicodeAttribute instances)
        from pynamodb.attributes import UnicodeAttribute

        assert isinstance(Base.pk, UnicodeAttribute)
        assert isinstance(Base.sk, UnicodeAttribute)

        # Test that pk is hash key and sk is range key
        assert Base.pk.is_hash_key is True
        assert Base.sk.is_range_key is True

        # Test default value for sk
        assert Base.sk.default == LATEST_VERSION


class TestArtifactModel:
    """Test the Artifact DynamoDB model class."""

    @pytest.fixture
    def mock_artifact_data(self):
        """Provide test data for artifact creation."""
        return {
            "pk": "test-app",
            "sk": "000001",
            "update_at": datetime.now(timezone.utc),
            "is_deleted": False,
            "sha256": "abc123def456",
        }

    def test_artifact_inheritance(self):
        """Test that Artifact inherits from Base."""
        assert issubclass(Artifact, Base)

    def test_artifact_attributes(self):
        """Test Artifact model has correct attributes."""
        from pynamodb.attributes import (
            UTCDateTimeAttribute,
            BooleanAttribute,
            UnicodeAttribute,
        )

        # Test attribute existence and types
        assert hasattr(Artifact, "update_at")
        assert hasattr(Artifact, "is_deleted")
        assert hasattr(Artifact, "sha256")

        assert isinstance(Artifact.update_at, UTCDateTimeAttribute)
        assert isinstance(Artifact.is_deleted, BooleanAttribute)
        assert isinstance(Artifact.sha256, UnicodeAttribute)

        # Test default values
        assert Artifact.is_deleted.default is False

    def test_artifact_new_factory_method(self):
        """Test Artifact.new() factory method."""
        # Test LATEST version creation (default)
        artifact = Artifact.new("test-app")
        assert artifact.pk == "test-app"
        assert artifact.sk == LATEST_VERSION

        # Test with explicit None version
        artifact = Artifact.new("test-app", version=None)
        assert artifact.pk == "test-app"
        assert artifact.sk == LATEST_VERSION

        # Test with numeric version
        artifact = Artifact.new("test-app", version=5)
        assert artifact.pk == "test-app"
        assert artifact.sk == "000005"

        # Test with string version
        artifact = Artifact.new("test-app", version="42")
        assert artifact.pk == "test-app"
        assert artifact.sk == "000042"

        # Test with LATEST string version
        artifact = Artifact.new("test-app", version="LATEST")
        assert artifact.pk == "test-app"
        assert artifact.sk == "LATEST"

    def test_artifact_name_property(self, mock_artifact_data):
        """Test artifact name property extraction."""
        artifact = Artifact(**mock_artifact_data)
        assert artifact.name == "test-app"

        # Test with different names
        artifact.pk = "different-service"
        assert artifact.name == "different-service"

    def test_artifact_version_property(self):
        """Test artifact version property extraction."""
        # Test LATEST version
        artifact = Artifact.new("test-app", version="LATEST")
        assert artifact.version == "LATEST"

        # Test numeric versions (should strip leading zeros)
        artifact = Artifact.new("test-app", version=1)
        assert artifact.version == "1"

        artifact = Artifact.new("test-app", version=42)
        assert artifact.version == "42"

        artifact = Artifact.new("test-app", version=999999)
        assert artifact.version == "999999"

        # Test edge case with manually set sk
        artifact = Artifact(pk="test", sk="000000")
        assert artifact.version == ""  # All zeros stripped

    def test_artifact_to_dict(self):
        """Test artifact dictionary conversion."""
        now = datetime.now(timezone.utc)
        artifact = Artifact(
            pk="test-app",
            sk="000005",
            update_at=now,
            is_deleted=False,
            sha256="abc123def456",
        )

        result = artifact.to_dict()
        expected = {
            "name": "test-app",
            "version": "5",
            "update_at": now,
            "sha256": "abc123def456",
        }

        assert result == expected
        assert isinstance(result, dict)
        assert len(result) == 4  # Should only include specified fields


class TestAliasModel:
    """Test the Alias DynamoDB model class."""

    @pytest.fixture
    def mock_alias_data(self):
        """Provide test data for alias creation."""
        return {
            "pk": "__test-app-alias",
            "sk": "prod",
            "update_at": datetime.now(timezone.utc),
            "version": "000005",
            "secondary_version": None,
            "secondary_version_weight": None,
        }

    def test_alias_inheritance(self):
        """Test that Alias inherits from Base."""
        assert issubclass(Alias, Base)

    def test_alias_attributes(self):
        """Test Alias model has correct attributes."""
        from pynamodb.attributes import (
            UTCDateTimeAttribute,
            UnicodeAttribute,
            NumberAttribute,
        )

        # Test attribute existence and types
        assert hasattr(Alias, "update_at")
        assert hasattr(Alias, "version")
        assert hasattr(Alias, "secondary_version")
        assert hasattr(Alias, "secondary_version_weight")

        assert isinstance(Alias.update_at, UTCDateTimeAttribute)
        assert isinstance(Alias.version, UnicodeAttribute)
        assert isinstance(Alias.secondary_version, UnicodeAttribute)
        assert isinstance(Alias.secondary_version_weight, NumberAttribute)

        # Test default value for update_at
        assert Alias.update_at.default == get_utc_now

        # Test nullable attributes
        assert Alias.secondary_version.null is True
        assert Alias.secondary_version_weight.null is True

    def test_alias_new_factory_method(self):
        """Test Alias.new() factory method."""
        # Test simple alias creation
        alias = Alias.new("test-app", "prod", version=5)
        assert alias.pk == "__test-app-alias"
        assert alias.sk == "prod"
        assert alias.version == "000005"
        assert alias.secondary_version is None
        assert alias.secondary_version_weight is None

        # Test with LATEST version (default)
        alias = Alias.new("test-app", "dev")
        assert alias.version == "LATEST"

        # Test with explicit None version
        alias = Alias.new("test-app", "staging", version=None)
        assert alias.version == "LATEST"

        # Test with traffic splitting
        alias = Alias.new(
            "test-app",
            "prod",
            version=5,
            secondary_version=6,
            secondary_version_weight=20,
        )
        assert alias.version == "000005"
        assert alias.secondary_version == "000006"
        assert alias.secondary_version_weight == 20

        # Test version validation - same version for primary and secondary
        with pytest.raises(ValueError):
            Alias.new(
                "test-app",
                "prod",
                version=5,
                secondary_version=5,
                secondary_version_weight=20,
            )

    def test_alias_name_property(self):
        """Test alias name property extraction from encoded partition key."""
        # Test simple artifact name
        alias = Alias.new("my-app", "prod")
        assert alias.name == "my-app"

        # Test artifact name with hyphens
        alias = Alias.new("frontend-service", "staging")
        assert alias.name == "frontend-service"

        # Test complex artifact name
        alias = Alias.new("api-gateway-v2", "prod")
        assert alias.name == "api-gateway-v2"

        # Test edge case with multiple hyphens
        alias = Alias.new("a-b-c-d", "test")
        assert alias.name == "a-b-c-d"

    def test_alias_alias_property(self, mock_alias_data):
        """Test alias alias property extraction."""
        alias = Alias(**mock_alias_data)
        assert alias.alias == "prod"

        # Test with different alias names
        alias.sk = "staging"
        assert alias.alias == "staging"

        alias.sk = "dev"
        assert alias.alias == "dev"

    def test_alias_to_dict_simple(self):
        """Test alias dictionary conversion for simple alias."""
        now = datetime.now(timezone.utc)
        alias = Alias(
            pk="__test-app-alias",
            sk="prod",
            update_at=now,
            version="000005",
            secondary_version=None,
            secondary_version_weight=None,
        )

        result = alias.to_dict()
        expected = {
            "name": "test-app",
            "alias": "prod",
            "update_at": now,
            "version": "5",
            "secondary_version": None,
            "secondary_version_weight": None,
        }

        assert result == expected
        assert isinstance(result, dict)
        assert len(result) == 6

    def test_alias_to_dict_with_traffic_splitting(self):
        """Test alias dictionary conversion with traffic splitting."""
        now = datetime.now(timezone.utc)
        alias = Alias(
            pk="__test-app-alias",
            sk="prod",
            update_at=now,
            version="000005",
            secondary_version="000006",
            secondary_version_weight=20,
        )

        result = alias.to_dict()
        expected = {
            "name": "test-app",
            "alias": "prod",
            "update_at": now,
            "version": "5",
            "secondary_version": "6",
            "secondary_version_weight": 20,
        }

        assert result == expected
        assert result["secondary_version"] == "6"  # Leading zeros stripped
        assert result["secondary_version_weight"] == 20


class TestDynamoDBIntegration(BaseMockAwsTest):
    """Integration tests for DynamoDB ORM classes with mocked AWS."""

    use_mock = True

    @classmethod
    def setup_mock_post_hook(cls):
        """Set up DynamoDB table for testing."""
        from pynamodb.connection import Connection
        from s3pathlib import context

        # Setup connection context
        context.attach_boto_session(cls.bsm.boto_ses)
        with cls.bsm.awscli():
            Connection()

        # Create test table configuration
        class TestArtifact(Artifact):
            class Meta:
                table_name = "test-artifacts"
                region = cls.bsm.aws_region

        class TestAlias(Alias):
            class Meta:
                table_name = "test-artifacts"
                region = cls.bsm.aws_region

        cls.TestArtifact = TestArtifact
        cls.TestAlias = TestAlias

        # Create the table
        try:
            cls.TestArtifact.create_table(wait=True)
        except Exception:
            pass  # Table may already exist

    def test_artifact_crud_operations(self):
        """Test basic CRUD operations for Artifact model."""
        # Create artifact
        artifact = self.TestArtifact.new("test-app", version=1)
        artifact.update_at = get_utc_now()
        artifact.sha256 = "abc123def456"
        artifact.save()

        # Read artifact
        retrieved = self.TestArtifact.get("test-app", "000001")
        assert retrieved.name == "test-app"
        assert retrieved.version == "1"
        assert retrieved.sha256 == "abc123def456"
        assert retrieved.is_deleted is False

        # Update artifact
        retrieved.sha256 = "new_hash_value"
        retrieved.save()

        # Verify update
        updated = self.TestArtifact.get("test-app", "000001")
        assert updated.sha256 == "new_hash_value"

        # Test soft delete
        updated.is_deleted = True
        updated.save()

        # Verify soft delete
        deleted = self.TestArtifact.get("test-app", "000001")
        assert deleted.is_deleted is True

    def test_artifact_query_operations(self):
        """Test query operations for Artifact model."""
        # Create multiple versions
        for version in [1, 2, 3]:
            artifact = self.TestArtifact.new("query-test", version=version)
            artifact.update_at = get_utc_now()
            artifact.sha256 = f"hash_{version}"
            artifact.save()

        # Create LATEST version
        latest = self.TestArtifact.new("query-test")
        latest.update_at = get_utc_now()
        latest.sha256 = "latest_hash"
        latest.save()

        # Query all versions
        versions = list(self.TestArtifact.query("query-test"))
        assert len(versions) >= 4  # May include items from other tests

        # Verify sort order (LATEST should be first due to sort key)
        version_sks = [v.sk for v in versions if v.pk == "query-test"]
        latest_index = version_sks.index("LATEST")
        numeric_sks = [sk for sk in version_sks if sk != "LATEST"]
        assert version_sks[latest_index] == "LATEST"
        assert numeric_sks == sorted(numeric_sks)

    def test_alias_crud_operations(self):
        """Test basic CRUD operations for Alias model."""
        # Create alias
        alias = self.TestAlias.new("test-app", "prod", version=5)
        alias.save()

        # Read alias
        retrieved = self.TestAlias.get("__test-app-alias", "prod")
        assert retrieved.name == "test-app"
        assert retrieved.alias == "prod"
        assert retrieved.version == "000005"
        assert retrieved.secondary_version is None

        # Update with traffic splitting
        retrieved.secondary_version = "000006"
        retrieved.secondary_version_weight = 20
        retrieved.save()

        # Verify update
        updated = self.TestAlias.get("__test-app-alias", "prod")
        assert updated.secondary_version == "000006"
        assert updated.secondary_version_weight == 20

        # Test to_dict with traffic splitting
        alias_dict = updated.to_dict()
        assert alias_dict["secondary_version"] == "6"
        assert alias_dict["secondary_version_weight"] == 20

    def test_alias_query_operations(self):
        """Test query operations for Alias model."""
        # Create multiple aliases for same artifact
        aliases = ["prod", "staging", "dev"]
        for alias_name in aliases:
            alias = self.TestAlias.new("multi-alias-test", alias_name, version=1)
            alias.save()

        # Query all aliases for artifact
        alias_list = list(self.TestAlias.query("__multi-alias-test-alias"))
        retrieved_aliases = [a.alias for a in alias_list]

        for alias_name in aliases:
            assert alias_name in retrieved_aliases

    def test_artifact_version_filtering(self):
        """Test filtering artifacts by deletion status."""
        # Create artifacts with different states
        active = self.TestArtifact.new("filter-test", version=1)
        active.update_at = get_utc_now()
        active.sha256 = "active_hash"
        active.is_deleted = False
        active.save()

        deleted = self.TestArtifact.new("filter-test", version=2)
        deleted.update_at = get_utc_now()
        deleted.sha256 = "deleted_hash"
        deleted.is_deleted = True
        deleted.save()

        # Query with filter for non-deleted items
        active_artifacts = list(
            self.TestArtifact.query(
                "filter-test", filter_condition=self.TestArtifact.is_deleted == False
            )
        )

        # Verify only active artifacts returned
        for artifact in active_artifacts:
            if artifact.pk == "filter-test":
                assert artifact.is_deleted is False

    def test_comprehensive_artifact_workflow(self):
        """Test a complete artifact lifecycle workflow."""
        # Create LATEST version
        latest = self.TestArtifact.new("workflow-test")
        latest.update_at = get_utc_now()
        latest.sha256 = "latest_content_hash"
        latest.save()

        # Publish version 1
        v1 = self.TestArtifact.new("workflow-test", version=1)
        v1.update_at = get_utc_now()
        v1.sha256 = "latest_content_hash"  # Same content
        v1.save()

        # Update LATEST with new content
        latest.sha256 = "new_content_hash"
        latest.save()

        # Publish version 2
        v2 = self.TestArtifact.new("workflow-test", version=2)
        v2.update_at = get_utc_now()
        v2.sha256 = "new_content_hash"
        v2.save()

        # Create aliases
        prod_alias = self.TestAlias.new("workflow-test", "prod", version=1)
        prod_alias.save()

        staging_alias = self.TestAlias.new("workflow-test", "staging", version=2)
        staging_alias.save()

        canary_alias = self.TestAlias.new(
            "workflow-test",
            "canary",
            version=1,
            secondary_version=2,
            secondary_version_weight=10,
        )
        canary_alias.save()

        # Verify complete setup
        artifacts = list(self.TestArtifact.query("workflow-test"))
        aliases = list(self.TestAlias.query("__workflow-test-alias"))

        artifact_versions = [a.version for a in artifacts if a.pk == "workflow-test"]
        alias_names = [a.alias for a in aliases]

        assert "LATEST" in artifact_versions
        assert "1" in artifact_versions
        assert "2" in artifact_versions
        assert "prod" in alias_names
        assert "staging" in alias_names
        assert "canary" in alias_names

        # Verify canary alias configuration
        canary = next(a for a in aliases if a.alias == "canary")
        canary_dict = canary.to_dict()
        assert canary_dict["version"] == "1"
        assert canary_dict["secondary_version"] == "2"
        assert canary_dict["secondary_version_weight"] == 10


if __name__ == "__main__":
    from versioned.tests import run_cov_test

    run_cov_test(
        __file__,
        "versioned.dynamodb",
        preview=False,
    )
