# -*- coding: utf-8 -*-

import moto
import pytest

import time
from datetime import datetime
from s3pathlib import S3Path, context

from versioned import exc
from versioned import constants
from versioned.dynamodb import encode_version_sk
from versioned.tests.mock_aws import BaseMockTest
from versioned.s3_and_dynamodb_backend import (
    Alias,
    Repository,
)

from rich import print as rprint


class TestAlias:
    def test_random_artifact(self):
        ali = Alias(
            name="deploy",
            alias="LIVE",
            update_at=datetime.utcnow(),
            version="1",
            secondary_version="2",
            secondary_version_weight=50,
            version_s3uri="s3uri1",
            secondary_version_s3uri="s3uri2",
        )
        for _ in range(10):
            assert ali.random_artifact() in ["s3uri1", "s3uri2"]

        ali = Alias(
            name="deploy",
            alias="LIVE",
            update_at=datetime.utcnow(),
            version="1",
            secondary_version=None,
            secondary_version_weight=None,
            version_s3uri="s3uri1",
            secondary_version_s3uri=None,
        )
        for _ in range(10):
            artifact_s3_uri = ali.random_artifact()
            assert artifact_s3_uri == "s3uri1"


class Test(BaseMockTest):
    use_mock = True

    mock_list = [
        moto.mock_sts,
        moto.mock_s3,
        moto.mock_dynamodb,
    ]

    _mock_list = []

    repo: Repository = None

    @classmethod
    def setup_class_post_hook(cls):
        context.attach_boto_session(cls.bsm.boto_ses)
        cls.repo = Repository(
            aws_region=cls.bsm.aws_region,
            s3_bucket=f"{cls.bsm.aws_account_id}-{cls.bsm.aws_region}-artifacts",
            suffix=".txt",
        )
        cls.repo.bootstrap(cls.bsm)

    def _test_error(self):
        name = "deploy"

        self.repo.purge_all()

        # --- test Artifact error ---
        # at this moment, no artifact exists
        with pytest.raises(exc.ArtifactNotFoundError):
            self.repo.publish_artifact_version(name=name)

        with pytest.raises(exc.ArtifactNotFoundError):
            self.repo.get_artifact_version(name=name)

        artifact_list = self.repo.list_artifact_versions(name=name)
        assert len(artifact_list) == 0

        # delete an non existing artifact version should always success for idempotency
        self.repo.delete_artifact_version(name=name)
        self.repo.delete_artifact_version(name=name, version=1)

        # --- test Alias error ---
        alias = "LIVE"

        # try to put alias on non-exist artifact
        with pytest.raises(exc.ArtifactNotFoundError):
            self.repo.put_alias(name=name, alias=alias, version=999)

        # secondary_version_weight type is wrong
        with pytest.raises(TypeError):
            self.repo.put_alias(name=name, alias=alias, secondary_version=999)

        # secondary_version_weight type is wrong
        with pytest.raises(TypeError):
            self.repo.put_alias(
                name=name,
                alias=alias,
                secondary_version=999,
                secondary_version_weight=0.5,
            )

        # secondary_version_weight value range is wrong
        with pytest.raises(ValueError):
            self.repo.put_alias(
                name=name,
                alias=alias,
                secondary_version=999,
                secondary_version_weight=-100,
            )

        # secondary_version_weight value range is wrong
        with pytest.raises(ValueError):
            self.repo.put_alias(
                name=name,
                alias=alias,
                secondary_version=999,
                secondary_version_weight=999,
            )

        # try to put alias on non-exist artifact
        with pytest.raises(exc.ArtifactNotFoundError):
            self.repo.put_alias(
                name=name,
                alias=alias,
                secondary_version=999,
                secondary_version_weight=20,
            )

        # version and secondary_version is the same
        with pytest.raises(ValueError):
            self.repo.put_alias(
                name=name,
                alias=alias,
                version=1,
                secondary_version=1,
                secondary_version_weight=20,
            )

        # alias not exists
        with pytest.raises(exc.AliasNotFoundError):
            self.repo.get_alias(name=name, alias="Invalid")

        alias_list = self.repo.list_aliases(name=name)
        assert len(alias_list) == 0

    def _test_artifact_and_alias(self):
        name = "deploy"
        alias = "LIVE"

        self.repo.purge_all()

        # ======================================================================
        # Artifact
        # ======================================================================
        # put artifact
        artifact = self.repo.put_artifact(
            name=name,
            content=b"v1",
            content_type="text/plain",
            metadata={"foo": "bar"},
        )
        # rprint(artifact)
        expected_update_at = artifact.update_at

        # put artifact with the same content, S3 and Dynamodb should not changed
        artifact = self.repo.put_artifact(name=name, content=b"v1")
        # rprint(artifact)

        def _assert_artifact(artifact):
            assert artifact.name == name
            assert artifact.version == constants.LATEST_VERSION
            assert artifact.update_at == expected_update_at
            assert artifact.s3uri.endswith(constants.LATEST_VERSION + ".txt")
            assert artifact.get_content(bsm=self.bsm) == b"v1"

        _assert_artifact(artifact)

        artifact = self.repo.get_artifact_version(name=name)
        # rprint(artifact)
        _assert_artifact(artifact)

        artifact_list = self.repo.list_artifact_versions(name=name)
        # rprint(artifact_list)
        assert len(artifact_list) == 1
        _assert_artifact(artifact_list[0])

        artifact = self.repo.publish_artifact_version(name=name)
        # rprint(artifact)
        assert artifact.version == "1"
        assert artifact.s3uri.endswith("1".zfill(constants.VERSION_ZFILL) + ".txt")
        assert (
            artifact.s3path.basename == str("1").zfill(constants.VERSION_ZFILL) + ".txt"
        )
        assert artifact.s3path.metadata["foo"] == "bar"
        assert artifact.get_content(bsm=self.bsm) == b"v1"

        # put artifact again
        artifact = self.repo.put_artifact(name=name, content=b"v2")
        # rprint(artifact)
        assert artifact.version == constants.LATEST_VERSION
        assert S3Path(artifact.s3uri).read_text(bsm=self.bsm) == "v2"

        artifact = self.repo.publish_artifact_version(name=name)
        # rprint(artifact)
        assert artifact.version == "2"
        s3path = S3Path(artifact.s3uri)
        assert artifact.s3uri.endswith("2".zfill(constants.VERSION_ZFILL) + ".txt")
        assert (
            artifact.s3path.basename == str("2").zfill(constants.VERSION_ZFILL) + ".txt"
        )
        assert artifact.get_content(bsm=self.bsm) == b"v2"

        artifact_list = self.repo.list_artifact_versions(name=name)
        assert len(artifact_list) == 3

        # ======================================================================
        # Alias
        # ======================================================================
        # put alias
        ali = self.repo.put_alias(name=name, alias=alias)
        # rprint(ali)
        expected_update_at = ali.update_at

        def _assert_alias(ali):
            assert ali.name == name
            assert ali.alias == alias
            assert ali.update_at == expected_update_at
            assert ali.version == constants.LATEST_VERSION
            assert ali.secondary_version is None
            assert ali.secondary_version_weight is None
            assert ali.version_s3uri.endswith(constants.LATEST_VERSION + ".txt")
            assert ali.secondary_version_s3uri is None
            assert ali.get_version_content(bsm=self.bsm) == b"v2"

        _assert_alias(ali)

        ali = self.repo.get_alias(name=name, alias=alias)
        # rprint(ali)
        _assert_alias(ali)

        ali_list = self.repo.list_aliases(name=name)
        assert len(ali_list) == 1
        _assert_alias(ali_list[0])

        # put alias again
        ali = self.repo.put_alias(
            name=name,
            alias=alias,
            version=1,
            secondary_version=2,
            secondary_version_weight=20,
        )
        # rprint(ali)

        def _assert_alias(ali):
            assert ali.name == name
            assert ali.alias == alias
            assert ali.update_at > expected_update_at
            assert ali.version == "1"
            assert ali.secondary_version == "2"
            assert ali.secondary_version_weight == 20
            assert ali.version_s3uri.endswith(encode_version_sk(1) + ".txt")
            assert ali.secondary_version_s3uri.endswith(encode_version_sk(2) + ".txt")
            assert ali.get_version_content(bsm=self.bsm) == b"v1"
            assert ali.get_secondary_version_content(bsm=self.bsm) == b"v2"

        _assert_alias(ali)

        ali = self.repo.get_alias(name=name, alias=alias)
        # rprint(ali)
        _assert_alias(ali)

        ali_list = self.repo.list_aliases(name=name)
        assert len(ali_list) == 1
        _assert_alias(ali_list[0])

        # --- test delete methods
        self.repo.delete_alias(name=name, alias=alias)
        with pytest.raises(exc.AliasNotFoundError):
            self.repo.get_alias(name=name, alias=alias)
        ali_list = self.repo.list_aliases(name=name)
        assert len(ali_list) == 0

        self.repo.delete_artifact_version(name=name)
        with pytest.raises(exc.ArtifactNotFoundError):
            self.repo.get_artifact_version(name=name)
        artifact_list = self.repo.list_artifact_versions(name=name)
        assert len(artifact_list) == 2

        self.repo.delete_artifact_version(name=name, version=1)
        with pytest.raises(exc.ArtifactNotFoundError):
            self.repo.get_artifact_version(name=name, version=1)
        artifact_list = self.repo.list_artifact_versions(name=name)
        assert len(artifact_list) == 1

        # it is a soft delete, so S3 artifact is not deleted
        assert s3path.parent.count_objects(bsm=self.bsm) == 3

        # --- purge
        with pytest.raises(exc.ArtifactNotFoundError):
            self.repo.put_alias(name=name, alias="DEV", version=1)

        self.repo.put_alias(name=name, alias="DEV", version=2)
        ali_list = self.repo.list_aliases(name=name)
        assert len(ali_list) == 1

        self.repo.purge_artifact(name=name)
        assert s3path.parent.count_objects(bsm=self.bsm) == 0
        artifact_list = self.repo.list_artifact_versions(name=name)
        assert len(artifact_list) == 0
        ali_list = self.repo.list_aliases(name=name)
        assert len(ali_list) == 0

    def _test_purge_artifact_versions(self):
        name = "life_cycle"

        def reset():
            self.repo.purge_artifact(name=name)
            for i in range(1, 1 + 5):
                self.repo.put_artifact(
                    name=name,
                    content=f"v{i}".encode("utf-8"),
                )
                self.repo.publish_artifact_version(name=name)
                time.sleep(1)

        reset()
        _, deleted_artifact_list = self.repo.purge_artifact_versions(
            name=name,
            keep_last_n=3,
            purge_older_than_secs=0,
        )
        artifact_list = self.repo.list_artifact_versions(name=name)
        assert len(artifact_list) == 4
        assert len(deleted_artifact_list) == 2
        assert [artifact.version for artifact in artifact_list] == [
            "LATEST",
            "5",
            "4",
            "3",
        ]
        assert [artifact.version for artifact in deleted_artifact_list] == ["2", "1"]

        reset()
        purge_time, deleted_artifact_list = self.repo.purge_artifact_versions(
            name=name,
            keep_last_n=0,
            purge_older_than_secs=3,
        )
        artifact_list = self.repo.list_artifact_versions(name=name)
        for artifact in artifact_list:
            assert (purge_time - artifact.update_at).total_seconds() <= 3
        for artifact in deleted_artifact_list:
            assert (purge_time - artifact.update_at).total_seconds() > 3

    def _test_list_artifact_names(self):
        self.repo.purge_all()

        self.repo.put_artifact(name="a", content=b"v1")
        names = self.repo.list_artifact_names()
        assert names == ["a"]

        self.repo.put_artifact(name="b", content=b"v1")
        names = self.repo.list_artifact_names()
        assert names == ["a", "b"]

    def test(self):
        self.repo.connect_boto_session(self.bsm)
        self._test_error()
        self._test_artifact_and_alias()
        self._test_purge_artifact_versions()
        self._test_list_artifact_names()


if __name__ == "__main__":
    from versioned.tests import run_cov_test

    run_cov_test(__file__, "versioned.s3_and_dynamodb_backend", preview=False)
