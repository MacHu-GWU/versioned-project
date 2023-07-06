# -*- coding: utf-8 -*-

import typing as T

import dataclasses
from datetime import datetime

from boto_session_manager import BotoSesManager
from s3pathlib import S3Path
from func_args import NOTHING

from . import constants
from . import dynamodb
from . import exc
from .bootstrap import bootstrap
from .vendor.hashes import hashes

hashes.use_sha256()


@dataclasses.dataclass
class Artifact:
    """
    Data class for artifact.

    It is not the same as the underlying DynamodbItem, it is a public facing
    API data class.

    :param name: artifact name.
    :param version: artifact version.
    :param update_at: a utc datetime object, when this artifact was updated.
    :param s3uri: s3uri of the artifact version.
    :param sha256: sha256 of the content of the artifact version.
    """

    name: str
    version: str
    update_at: datetime
    s3uri: str
    sha256: str

    @property
    def s3path(self) -> S3Path:
        """
        Return the s3path of this artifact version.
        """
        return S3Path(self.s3uri)

    def get_content(self, bsm: BotoSesManager) -> bytes:
        """
        Get the content of this artifact version.
        """
        return self.s3path.read_bytes(bsm=bsm)


@dataclasses.dataclass
class Alias:
    """
    Data class for alias.

    It is not the same as the underlying DynamodbItem, it is a public facing
    API data class.

    :param name: artifact name.
    :param alias: alias name. alias name cannot have hyphen
    :param version: artifact version. If ``None``, the latest version is used.
    :param secondary_version: see above.
    :param secondary_version_weight: an integer between 0 ~ 100.
    :param version_s3uri: s3uri of the primary artifact version of this alias.
    :param secondary_version_s3uri: s3uri of the secondary artifact version of this alias.
    """

    name: str
    alias: str
    version: str
    secondary_version: T.Optional[str]
    secondary_version_weight: T.Optional[int]
    version_s3uri: str
    secondary_version_s3uri: T.Optional[str]

    @property
    def s3path_version(self) -> S3Path:
        """
        Return the s3path of the primary artifact version of this alias.
        """
        return S3Path(self.version_s3uri)

    def get_version_content(self, bsm: BotoSesManager) -> bytes:
        """
        Get the content of the primary artifact version of this alias.
        """
        return self.s3path_version.read_bytes(bsm=bsm)

    @property
    def s3path_secondary_version(self) -> S3Path:
        """
        Return the s3path of the secondary artifact version of this alias.
        """
        return S3Path(self.secondary_version_s3uri)

    def get_secondary_version_content(self, bsm: BotoSesManager) -> bytes:
        """
        Get the content of the secondary artifact version of this alias.
        """
        return self.s3path_secondary_version.read_bytes(bsm=bsm)


@dataclasses.dataclass
class Repository:
    s3_bucket: T.Optional[str] = dataclasses.field(default=None)
    s3_prefix: T.Optional[str] = dataclasses.field(default=constants.S3_PREFIX)
    dynamodb_table_name: T.Optional[str] = dataclasses.field(
        default=constants.DYNAMODB_TABLE_NAME
    )

    def _get_s3_bucket(self, bsm: BotoSesManager) -> str:
        if self.s3_bucket is None:
            return f"{bsm.aws_account_id}-{bsm.aws_region}-{constants.BUCKET_NAME}"
        else:
            return self.s3_bucket

    def _get_s3path(self, bsm: BotoSesManager, name: str, version: str) -> S3Path:
        return S3Path(self._get_s3_bucket(bsm=bsm)).joinpath(
            self.s3_prefix,
            name,
            dynamodb.encode_version(version),
        )

    def bootstrap(
        self,
        bsm: BotoSesManager,
        dynamodb_write_capacity_units: T.Optional[int] = None,
        dynamodb_read_capacity_units: T.Optional[int] = None,
    ):
        bootstrap(
            bsm=bsm,
            bucket_name=self._get_s3_bucket(bsm=bsm),
            dynamodb_table_name=self.dynamodb_table_name,
            dynamodb_write_capacity_units=dynamodb_write_capacity_units,
            dynamodb_read_capacity_units=dynamodb_read_capacity_units,
        )

    def _get_artifact_class(self, bsm: BotoSesManager) -> T.Type[dynamodb.Artifact]:
        class Artifact(dynamodb.Artifact):
            class Meta:
                table_name = self.dynamodb_table_name
                region = bsm.aws_region

        return Artifact

    def _get_alias_class(self, bsm: BotoSesManager) -> T.Type[dynamodb.Alias]:
        class Alias(dynamodb.Alias):
            class Meta:
                table_name = self.dynamodb_table_name
                region = bsm.aws_region

        return Alias

    def _get_artifact_object(
        self,
        bsm: BotoSesManager,
        artifact: dynamodb.Artifact,
    ) -> Artifact:
        dct = artifact.to_dict()
        dct["s3uri"] = self._get_s3path(
            bsm=bsm,
            name=artifact.name,
            version=artifact.version,
        ).uri
        return Artifact(**dct)

    def _get_alias_object(
        self,
        bsm: BotoSesManager,
        alias: dynamodb.Alias,
    ) -> Alias:
        dct = alias.to_dict()
        dct["version_s3uri"] = self._get_s3path(
            bsm=bsm,
            name=alias.name,
            version=alias.version,
        ).uri
        if alias.secondary_version is None:
            dct["secondary_version_s3uri"] = None
        else:
            dct["secondary_version_s3uri"] = self._get_s3path(
                bsm=bsm,
                name=alias.name,
                version=alias.secondary_version,
            ).uri
        return Alias(**dct)

    # ------------------------------------------------------------------------------
    # Artifact
    # ------------------------------------------------------------------------------
    def put_artifact(
        self,
        bsm: BotoSesManager,
        name: str,
        content: bytes,
        content_type: str = NOTHING,
        metadata: T.Dict[str, str] = NOTHING,
        tags: T.Dict[str, str] = NOTHING,
    ) -> Artifact:
        """
        Create / Update artifact to the latest.

        :param bsm: ``boto_session_manager.BotoSesManager`` object.
        :param name: artifact name.
        :param content: binary artifact content.
        :param metadata: optional metadata of the s3 object.
        :param tags: optional tags of the s3 object.
        """
        Artifact = self._get_artifact_class(bsm)
        artifact = Artifact.new(name=name)
        artifact_sha256 = hashes.of_bytes(content)
        artifact.sha256 = artifact_sha256
        s3path = self._get_s3path(bsm=bsm, name=name, version=constants.LATEST_VERSION)

        # do nothing if the content is not changed
        if s3path.exists(bsm=bsm):
            if s3path.metadata["artifact_sha256"] == artifact_sha256:
                return self._get_artifact_object(bsm=bsm, artifact=artifact)

        final_metadata = dict(
            artifact_name=name,
            artifact_sha256=artifact_sha256,
        )
        if metadata is not NOTHING:
            final_metadata.update(metadata)
        s3path.write_bytes(
            content,
            metadata=final_metadata,
            content_type=content_type,
            tags=tags,
            bsm=bsm,
        )
        artifact.save()
        return self._get_artifact_object(bsm=bsm, artifact=artifact)

    def _get_artifact_dynamodb_item(
        self,
        artifact_class: T.Type[dynamodb.Artifact],
        name: str,
        version: T.Union[int, str],
    ) -> dynamodb.Artifact:
        try:
            artifact = artifact_class.get(
                hash_key=name,
                range_key=dynamodb.encode_version(version),
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
        bsm: BotoSesManager,
        name: str,
        version: T.Optional[T.Union[int, str]] = None,
    ) -> Artifact:
        """
        Return the information about the artifact or artifact version.

        :param bsm: ``boto_session_manager.BotoSesManager`` object.
        :param name: artifact name.
        :param version: artifact version. If ``None``, return the latest version.
        """
        Artifact = self._get_artifact_class(bsm)
        if version is None:
            version = constants.LATEST_VERSION
        artifact = self._get_artifact_dynamodb_item(
            Artifact, name=name, version=version
        )
        return self._get_artifact_object(bsm=bsm, artifact=artifact)

    def list_artifact_versions(
        self,
        bsm: BotoSesManager,
        name: str,
    ) -> T.List[Artifact]:
        """
        Return a list of artifact versions. The latest version is always the first item.
        And the newer version comes first.

        :param bsm: ``boto_session_manager.BotoSesManager`` object.
        :param name: artifact name.
        """
        Artifact = self._get_artifact_class(bsm)
        return [
            self._get_artifact_object(bsm=bsm, artifact=artifact)
            for artifact in Artifact.query(
                hash_key=name,
                scan_index_forward=False,
                filter_condition=Artifact.is_deleted == False,
            )
        ]

    def publish_artifact_version(
        self,
        bsm: BotoSesManager,
        name: str,
    ) -> Artifact:
        """
        Creates a version from the latest artifact. Use versions to create an
        immutable snapshot of your latest artifact.

        :param bsm: ``boto_session_manager.BotoSesManager`` object.
        :param name: artifact name.
        """
        Artifact = self._get_artifact_class(bsm)
        artifacts = list(
            Artifact.query(hash_key=name, scan_index_forward=False, limit=2)
        )
        if len(artifacts) == 0:
            raise exc.ArtifactNotFoundError(f"name = {name!r}")
        elif len(artifacts) == 1:
            new_version = "1"
        else:
            new_version = str(int(artifacts[1].version) + 1)
        artifact = Artifact.new(name=name, version=new_version)
        artifact.sha256 = artifacts[0].sha256
        artifact.save()
        s3path_old = self._get_s3path(
            bsm=bsm, name=name, version=constants.LATEST_VERSION
        )
        s3path_new = self._get_s3path(bsm=bsm, name=name, version=new_version)
        s3path_old.copy_to(s3path_new, bsm=bsm)
        return self._get_artifact_object(bsm=bsm, artifact=artifact)

    def delete_artifact_version(
        self,
        bsm: BotoSesManager,
        name: str,
        version: T.Optional[T.Union[int, str]] = None,
    ):
        """
        Deletes a specific version of artifact. If version is not specified,
        the latest version is deleted. Note that this is a soft delete,
        neither the S3 artifact nor the DynamoDB item is deleted. It is just
        become "invisible" to the :func:`get_artifact` and :func:`list_artifacts``.

        :param bsm: ``boto_session_manager.BotoSesManager`` object.
        :param name: artifact name.
        :param version: artifact version. If ``None``, delete the latest version.
        """
        Artifact = self._get_artifact_class(bsm)
        if version is None:
            version = constants.LATEST_VERSION
        res = Artifact.new(name=name, version=version).update(
            actions=[
                Artifact.is_deleted.set(True),
            ],
        )
        # print(res)

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
        Creates an alias for an artifact version. If ``version`` is not specified,
        the latest version is used.

        You can also map an alias to split invocation requests between two versions.
        Use the ``secondary_version`` and ``secondary_version_weight`` to specify
        a second version and the percentage of invocation requests that it receives.

        :param bsm: ``boto_session_manager.BotoSesManager`` object.
        :param name: artifact name.
        :param alias: alias name. alias name cannot have hyphen
        :param version: artifact version. If ``None``, the latest version is used.
        :param secondary_version: see above.
        :param secondary_version_weight: an integer between 0 ~ 100.
        """
        # validate argument
        if "-" in alias:  # pragma: no cover
            raise ValueError("alias cannot have hyphen")

        if secondary_version is not None:
            if not isinstance(secondary_version_weight, int):
                raise TypeError("secondary_version_weight must be int")
            if not (0 <= secondary_version_weight < 100):
                raise ValueError("secondary_version_weight must be 0 <= x < 100")

        # ensure the artifact exists
        Artifact = self._get_artifact_class(bsm)
        if version is None:
            version = constants.LATEST_VERSION
        self._get_artifact_dynamodb_item(Artifact, name=name, version=version)
        if secondary_version is not None:
            self._get_artifact_dynamodb_item(
                Artifact, name=name, version=secondary_version
            )

        Alias = self._get_alias_class(bsm)
        alias = Alias.new(
            name=name,
            alias=alias,
            version=version,
            secondary_version=secondary_version,
            secondary_version_weight=secondary_version_weight,
        )

        alias.save()
        return self._get_alias_object(bsm=bsm, alias=alias)

    def get_alias(
        self,
        bsm: BotoSesManager,
        name: str,
        alias: str,
    ) -> Alias:
        """
        Return details about the alias.

        :param bsm: ``boto_session_manager.BotoSesManager`` object.
        :param name: artifact name.
        :param alias: alias name. alias name cannot have hyphen
        """
        Alias = self._get_alias_class(bsm)
        try:
            return self._get_alias_object(
                bsm=bsm,
                alias=Alias.get(
                    hash_key=dynamodb.encode_alias_key(name),
                    range_key=alias,
                ),
            )
        except Alias.DoesNotExist:
            raise exc.AliasNotFoundError(f"name = {name!r}, alias = {alias!r}")

    def list_aliases(
        self,
        bsm: BotoSesManager,
        name: str,
    ) -> T.List[Alias]:
        """
        Returns a list of aliases for an artifact.

        :param bsm: ``boto_session_manager.BotoSesManager`` object.
        :param name: artifact name.
        """
        Alias = self._get_alias_class(bsm)
        return [
            self._get_alias_object(bsm=bsm, alias=alias)
            for alias in Alias.query(hash_key=dynamodb.encode_alias_key(name))
        ]

    def delete_alias(
        self,
        bsm: BotoSesManager,
        name: str,
        alias: str,
    ):
        """
        Deletes an alias.
        """
        Alias = self._get_alias_class(bsm)
        res = Alias.new(name=name, alias=alias).delete()
        # print(res)

    def purge_artifact(
        self,
        bsm: BotoSesManager,
        name: str,
    ):
        """
        Completely delete all artifacts and aliases of the given artifact name.
        This operation is irreversible. It will remove all related S3 artifacts
        and DynamoDB items.

        :param bsm: ``boto_session_manager.BotoSesManager`` object.
        :param name: artifact name.
        """
        s3path = self._get_s3path(bsm=bsm, name=name, version=constants.LATEST_VERSION)
        s3dir = s3path.parent
        s3dir.delete(bsm=bsm)

        Artifact = self._get_artifact_class(bsm)
        Alias = self._get_alias_class(bsm)
        with Artifact.batch_write() as batch:
            for artifact in Artifact.query(hash_key=name):
                batch.delete(artifact)
        with Alias.batch_write() as batch:
            for alias in Alias.query(hash_key=dynamodb.encode_alias_key(name)):
                batch.delete(alias)
