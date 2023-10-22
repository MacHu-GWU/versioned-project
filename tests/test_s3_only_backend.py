# -*- coding: utf-8 -*-

import moto
import pytest
import time

from s3pathlib import S3Path

from versioned import exc
from versioned import constants
from versioned.tests.mock_aws import BaseMockTest
from versioned.s3_only_backend import (
    hashes,
    encode_version,
    encode_filename,
    decode_filename,
    validate_alias_name,
    Artifact,
    Alias,
    Repository,
)

from rich import print as rprint


def test_encode_version():
    assert encode_version(None) == "LATEST"
    assert encode_version("LATEST") == "LATEST"
    assert encode_version(1) == "1"
    assert encode_version(999999) == "999999"
    assert encode_version("1") == "1"
    assert encode_version("000001") == "1"


def test_encode_filename():
    assert encode_filename(None) == "000000_LATEST"
    assert encode_filename("LATEST") == "000000_LATEST"
    assert encode_filename("999999") == "000001_999999"
    assert encode_filename("2") == "999998_000002"
    assert encode_filename("1") == "999999_000001"
    assert encode_filename(999999) == "000001_999999"
    assert encode_filename(2) == "999998_000002"
    assert encode_filename(1) == "999999_000001"


def test_decode_filename():
    assert decode_filename("000000_LATEST") == "LATEST"
    assert decode_filename("000001_999999") == "999999"
    assert decode_filename("999998_000002") == "2"
    assert decode_filename("999999_000001") == "1"


def test_validate_alias_name():
    for alias in [
        "LATEST",
        "hello-world",
        "123abc",
    ]:
        with pytest.raises(ValueError):
            validate_alias_name(alias)


class TestAlias:
    def test_random_artifact(self):
        ali = Alias(
            name="deploy",
            alias="LIVE",
            update_at="",
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
            update_at="",
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
    ]

    _mock_list = []

    repo: Repository = None

    @classmethod
    def setup_class_post_hook(cls):
        cls.repo = Repository(
            aws_region=cls.bsm.aws_region,
            s3_bucket=f"{cls.bsm.aws_account_id}-{cls.bsm.aws_region}-artifacts",
            suffix=".txt",
        )
        cls.repo.bootstrap(cls.bsm)

    def _test_error(self):
        name = "deploy"
        LIVE = "LIVE"
        self.repo.purge_all(bsm=self.bsm)

        # --- test Artifact error ---
        # at this moment, no artifact exists
        with pytest.raises(exc.ArtifactNotFoundError):
            self.repo.publish_artifact_version(bsm=self.bsm, name=name)

        with pytest.raises(exc.ArtifactNotFoundError):
            self.repo.get_artifact_version(bsm=self.bsm, name=name)

        artifact_list = self.repo.list_artifact_versions(bsm=self.bsm, name=name)
        assert len(artifact_list) == 0

        with pytest.raises(exc.ArtifactS3BackendError):
            self.repo.delete_artifact_version(
                bsm=self.bsm, name=name, version=constants.LATEST_VERSION
            )

        # delete an non existing artifact version should always success for idempotency
        self.repo.delete_artifact_version(bsm=self.bsm, name=name, version=1)

        # --- test Alias error ---
        alias = "LIVE"

        # try to put alias on non-exist artifact
        with pytest.raises(exc.ArtifactNotFoundError):
            self.repo.put_alias(
                bsm=self.bsm,
                name=name,
                alias=alias,
                version=999,
            )

        # secondary_version_weight value type is wrong
        with pytest.raises(TypeError):
            self.repo.put_alias(
                bsm=self.bsm,
                name=name,
                alias=alias,
                secondary_version=999,
                secondary_version_weight=None,
            )

        # secondary_version_weight value type is wrong
        with pytest.raises(TypeError):
            self.repo.put_alias(
                bsm=self.bsm,
                name=name,
                alias=alias,
                secondary_version=999,
                secondary_version_weight=0.5,
            )

        # secondary_version_weight value range is wrong
        with pytest.raises(ValueError):
            self.repo.put_alias(
                bsm=self.bsm,
                name=name,
                alias=alias,
                secondary_version=999,
                secondary_version_weight=-100,
            )

        # secondary_version_weight value range is wrong
        with pytest.raises(ValueError):
            self.repo.put_alias(
                bsm=self.bsm,
                name=name,
                alias=alias,
                secondary_version=999,
                secondary_version_weight=999,
            )

        # try to put alias on non-exist artifact
        with pytest.raises(exc.ArtifactNotFoundError):
            self.repo.put_alias(
                bsm=self.bsm,
                name=name,
                alias=alias,
                secondary_version=999,
                secondary_version_weight=20,
            )

        # version and secondary_version is the same
        with pytest.raises(ValueError):
            self.repo.put_alias(
                bsm=self.bsm,
                name=name,
                alias=alias,
                version=1,
                secondary_version=1,
                secondary_version_weight=20,
            )

        # alias not exists
        with pytest.raises(exc.AliasNotFoundError):
            self.repo.get_alias(bsm=self.bsm, name=name, alias="Invalid")

        alias_list = self.repo.list_aliases(bsm=self.bsm, name=name)
        assert len(alias_list) == 0

        # delete an non existing alias should always success for idempotency
        self.repo.delete_alias(bsm=self.bsm, name=name, alias=LIVE)

    def _test_artifact_and_alias(self):
        name = "deploy"
        alias = "LIVE"

        self.repo.purge_all(bsm=self.bsm)

        # ======================================================================
        # Artifact
        # ======================================================================
        assert (
            self.repo.get_latest_published_artifact_version_number(
                bsm=self.bsm, name=name
            )
            == 0
        )

        # put artifact
        artifact = self.repo.put_artifact(
            bsm=self.bsm,
            name=name,
            content=b"v1",
            content_type="text/plain",
            metadata={"foo": "bar"},
        )
        # rprint(artifact)
        expected_update_at = artifact.update_at

        def _assert_artifact(artifact):
            assert artifact.name == name
            assert artifact.version == constants.LATEST_VERSION
            assert artifact.update_at == expected_update_at
            _ = artifact.update_datetime
            assert artifact.s3uri.endswith(constants.LATEST_VERSION + ".txt")
            assert artifact.sha256 == hashes.of_bytes(b"v1")
            assert artifact.get_content(bsm=self.bsm) == b"v1"

        _assert_artifact(artifact)

        assert (
            self.repo.get_latest_published_artifact_version_number(
                bsm=self.bsm, name=name
            )
            == 0
        )

        # put artifact with the same content, S3 and Dynamodb should not changed
        # update time should not changed too
        artifact = self.repo.put_artifact(bsm=self.bsm, name=name, content=b"v1")
        # rprint(artifact)
        _assert_artifact(artifact)

        # get artifact by name and version
        artifact = self.repo.get_artifact_version(bsm=self.bsm, name=name)
        # rprint(artifact)
        _assert_artifact(artifact)

        # list artifact versions
        artifact_list = self.repo.list_artifact_versions(bsm=self.bsm, name=name)
        # rprint(artifact_list)
        assert len(artifact_list) == 1
        _assert_artifact(artifact_list[0])

        # publish a new version, v1 should be created
        artifact = self.repo.publish_artifact_version(bsm=self.bsm, name=name)
        # rprint(artifact)

        def _assert_artifact(artifact):
            assert artifact.name == name
            assert artifact.version == "1"
            _ = artifact.update_datetime
            assert artifact.s3uri.endswith("1.txt")
            assert artifact.sha256 == hashes.of_bytes(b"v1")
            assert artifact.get_content(bsm=self.bsm) == b"v1"
            artifact.s3path.head_object(bsm=self.bsm)
            assert artifact.s3path.metadata["foo"] == "bar"

        _assert_artifact(artifact)
        assert (
            self.repo.get_latest_published_artifact_version_number(
                bsm=self.bsm, name=name
            )
            == 1
        )

        # publish a new version again, since no change between latest and v1
        # so no new version should be created
        artifact = self.repo.publish_artifact_version(bsm=self.bsm, name=name)
        # rprint(artifact)
        _assert_artifact(artifact)

        # put artifact again
        artifact = self.repo.put_artifact(bsm=self.bsm, name=name, content=b"v2")
        # rprint(artifact)
        assert artifact.version == constants.LATEST_VERSION
        assert S3Path(artifact.s3uri).read_text(bsm=self.bsm) == "v2"

        artifact = self.repo.publish_artifact_version(bsm=self.bsm, name=name)
        # rprint(artifact)
        assert artifact.version == "2"
        assert artifact.s3uri.endswith("2".zfill(constants.VERSION_ZFILL) + ".txt")
        assert artifact.get_content(bsm=self.bsm) == b"v2"

        # when you list a bunch of versions, the first version is always the LATEST
        # and then newer version comes first
        artifact_list = self.repo.list_artifact_versions(bsm=self.bsm, name=name)
        # rprint(artifact_list)
        assert len(artifact_list) == 3
        assert artifact_list[0].version == constants.LATEST_VERSION
        assert artifact_list[1].version == "2"
        assert artifact_list[2].version == "1"

        # ======================================================================
        # Alias
        # ======================================================================
        # put alias the first time
        ali = self.repo.put_alias(bsm=self.bsm, name=name, alias=alias)
        # rprint(ali)
        expected_update_at = ali.update_at

        def _assert_alias(ali):
            assert ali.name == name
            assert ali.alias == alias
            assert ali.update_at == expected_update_at
            _ = ali.update_datetime
            assert ali.version == constants.LATEST_VERSION
            assert ali.secondary_version is None
            assert ali.secondary_version_weight is None
            assert ali.version_s3uri.endswith(constants.LATEST_VERSION + ".txt")
            assert ali.secondary_version_s3uri is None
            assert ali.get_version_content(bsm=self.bsm) == b"v2"

        _assert_alias(ali)

        # get the alias immediately
        ali = self.repo.get_alias(bsm=self.bsm, name=name, alias=alias)
        # rprint(ali)
        _assert_alias(ali)

        # list alias
        ali_list = self.repo.list_aliases(bsm=self.bsm, name=name)
        assert len(ali_list) == 1
        _assert_alias(ali_list[0])

        # put alias again
        ali = self.repo.put_alias(
            bsm=self.bsm,
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
            assert ali.version == "1"
            assert ali.secondary_version == "2"
            assert ali.secondary_version_weight == 20
            assert ali.version_s3uri.endswith(encode_filename("1") + ".txt")
            assert ali.secondary_version_s3uri.endswith(encode_filename("2") + ".txt")
            assert ali.get_version_content(bsm=self.bsm) == b"v1"
            assert ali.get_secondary_version_content(bsm=self.bsm) == b"v2"

        _assert_alias(ali)

        # get the updated alias
        ali = self.repo.get_alias(bsm=self.bsm, name=name, alias=alias)
        # rprint(ali)
        _assert_alias(ali)

        ali_list = self.repo.list_aliases(bsm=self.bsm, name=name)
        assert len(ali_list) == 1
        _assert_alias(ali_list[0])

        # the second version doesn't exists, should raise error
        with pytest.raises(exc.ArtifactNotFoundError):
            self.repo.put_alias(
                bsm=self.bsm,
                name=name,
                alias=alias,
                secondary_version=999,
                secondary_version_weight=50,
            )

    def _test_delete_and_purge(self):
        name = "deploy"
        LIVE = "LIVE"
        DEV = "DEV"

        self.repo.purge_all(bsm=self.bsm)

        # prepare
        self.repo.put_artifact(bsm=self.bsm, name=name, content=b"v1")
        self.repo.publish_artifact_version(bsm=self.bsm, name=name)
        self.repo.put_artifact(bsm=self.bsm, name=name, content=b"v2")
        self.repo.publish_artifact_version(bsm=self.bsm, name=name)
        self.repo.put_artifact(bsm=self.bsm, name=name, content=b"v3")
        self.repo.publish_artifact_version(bsm=self.bsm, name=name)

        self.repo.put_alias(
            bsm=self.bsm,
            name=name,
            alias=LIVE,
            version=2,
            secondary_version=3,
            secondary_version_weight=10,
        )
        self.repo.put_alias(bsm=self.bsm, name=name, alias=DEV)

        ali_list = self.repo.list_aliases(bsm=self.bsm, name=name)
        assert len(ali_list) == 2

        # delete alias then get it back, should raise error
        self.repo.delete_alias(bsm=self.bsm, name=name, alias=DEV)
        with pytest.raises(exc.AliasNotFoundError):
            self.repo.get_alias(bsm=self.bsm, name=name, alias=DEV)
        ali_list = self.repo.list_aliases(bsm=self.bsm, name=name)
        assert len(ali_list) == 1

        # delete artifact version then get it back, should raise error
        artifact_list = self.repo.list_artifact_versions(bsm=self.bsm, name=name)
        assert len(artifact_list) == 4
        self.repo.delete_artifact_version(bsm=self.bsm, name=name, version=3)
        with pytest.raises(exc.ArtifactNotFoundError):
            self.repo.get_artifact_version(bsm=self.bsm, name=name, version=3)
        artifact_list = self.repo.list_artifact_versions(bsm=self.bsm, name=name)
        assert len(artifact_list) == 3

        # --- purge
        self.repo.purge_artifact(bsm=self.bsm, name=name)
        artifact_list = self.repo.list_artifact_versions(bsm=self.bsm, name=name)
        assert len(artifact_list) == 0
        ali_list = self.repo.list_aliases(bsm=self.bsm, name=name)
        assert len(ali_list) == 0

    def _test_purge_artifact_versions(self):
        name = "life_cycle"

        def reset():
            self.repo.purge_artifact(bsm=self.bsm, name=name)
            for i in range(1, 1 + 5):
                self.repo.put_artifact(
                    bsm=self.bsm,
                    name=name,
                    content=f"v{i}".encode("utf-8"),
                )
                self.repo.publish_artifact_version(bsm=self.bsm, name=name)
                time.sleep(1)

        reset()
        _, deleted_artifact_list = self.repo.purge_artifact_versions(
            bsm=self.bsm,
            name=name,
            keep_last_n=3,
            purge_older_than_secs=0,
        )
        artifact_list = self.repo.list_artifact_versions(bsm=self.bsm, name=name)
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
            bsm=self.bsm,
            name=name,
            keep_last_n=0,
            purge_older_than_secs=3,
        )
        artifact_list = self.repo.list_artifact_versions(bsm=self.bsm, name=name)
        for artifact in artifact_list:
            assert (purge_time - artifact.update_datetime).total_seconds() <= 3
        for artifact in deleted_artifact_list:
            assert (purge_time - artifact.update_datetime).total_seconds() > 3

    def _test_list_artifact_names(self):
        self.repo.purge_all(bsm=self.bsm)

        self.repo.put_artifact(bsm=self.bsm, name="a", content=b"v1")
        names = self.repo.list_artifact_names(bsm=self.bsm)
        assert names == ["a"]

        self.repo.put_artifact(bsm=self.bsm, name="b", content=b"v1")
        names = self.repo.list_artifact_names(bsm=self.bsm)
        assert names == ["a", "b"]

    def test(self):
        self._test_error()
        self._test_artifact_and_alias()
        self._test_delete_and_purge()
        self._test_purge_artifact_versions()
        self._test_list_artifact_names()


if __name__ == "__main__":
    from versioned.tests import run_cov_test

    run_cov_test(__file__, "versioned.s3_only_backend", preview=False)
