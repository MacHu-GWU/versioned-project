# -*- coding: utf-8 -*-

import typing as T

import random
import dataclasses
from datetime import datetime, timezone, timedelta

from boto_session_manager import BotoSesManager
from s3pathlib import S3Path, context
from func_args import NOTHING
from pynamodb.connection import Connection

from . import constants
from . import dynamodb
from . import exc
from .compat import cached_property
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
    :param update_at: an utc datetime object when this artifact was updated
    :param version: artifact version. If ``None``, the latest version is used.
    :param secondary_version: see above.
    :param secondary_version_weight: an integer between 0 ~ 100.
    :param version_s3uri: s3uri of the primary artifact version of this alias.
    :param secondary_version_s3uri: s3uri of the secondary artifact version of this alias.
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

    @cached_property
    def _version_weight(self) -> int:
        if self.secondary_version_weight is None:
            return 100
        else:
            return 100 - self.secondary_version_weight

    def random_artifact(self) -> str:
        """
        Randomly return either the primary or secondary artifact version s3uri
        based on the weight.
        """
        if random.randint(1, 100) <= self._version_weight:
            return self.version_s3uri
        else:
            return self.secondary_version_s3uri


@dataclasses.dataclass
class Repository:
    """
    Repository class for artifact store.

    :param aws_region: the aws region of where the artifact store is.
    :param s3_bucket: the s3 bucket name of the artifact store.
    :param s3_prefix: the s3 prefix (folder path) of the artifact store.
    :param dynamodb_table_name: the dynamodb table name of the artifact metadata store.
    :param suffix: the file extension suffix of the artifact binary.
    """

    aws_region: str = dataclasses.field()
    s3_bucket: str = dataclasses.field()
    s3_prefix: str = dataclasses.field(default=constants.S3_PREFIX)
    dynamodb_table_name: str = dataclasses.field(default=constants.DYNAMODB_TABLE_NAME)
    suffix: str = dataclasses.field(default="")

    @property
    def s3dir_artifact_store(self) -> S3Path:
        """
        Return the s3dir of the artifact store folder.
        """
        return S3Path(self.s3_bucket).joinpath(self.s3_prefix).to_dir()

    def get_artifact_s3path(self, name: str, version: str) -> S3Path:
        """
        Return the s3path of the artifact s3 object.

        :param name: artifact name.
        :param version: artifact version. If ``None``, return the latest version.
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
        Create necessary backend resources for the artifact store.
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
        :param metadata: optional metadata of the s3 object.
        :param tags: optional tags of the s3 object.
        """
        artifact = self._artifact_class.new(name=name)
        artifact_sha256 = hashes.of_bytes(content)
        artifact.sha256 = artifact_sha256
        s3path = self.get_artifact_s3path(name=name, version=constants.LATEST_VERSION)

        # do nothing if the content is not changed
        if s3path.exists():
            if s3path.metadata["artifact_sha256"] == artifact_sha256:
                artifact.update_at = s3path.last_modified_at
                return self._get_artifact_object(artifact=artifact)

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
        )
        s3path.head_object()
        artifact.update_at = s3path.last_modified_at
        artifact.save()
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
        Artifact = self._artifact_class
        artifacts = list(
            Artifact.query(hash_key=name, scan_index_forward=False, limit=2)
        )
        if len(artifacts) == 0:
            raise exc.ArtifactNotFoundError(f"name = {name!r}")
        elif len(artifacts) == 1:
            new_version = dynamodb.encode_version(1)
        else:
            new_version = str(int(artifacts[1].version) + 1)

        # copy artifact from latest to the new version
        s3path_old = self.get_artifact_s3path(
            name=name,
            version=constants.LATEST_VERSION,
        )
        s3path_new = self.get_artifact_s3path(name=name, version=new_version)
        s3path_old.copy_to(s3path_new)
        s3path_new.head_object()

        # create artifact object
        artifact = Artifact.new(name=name, version=new_version)
        artifact.sha256 = artifacts[0].sha256
        artifact.update_at = s3path_new.last_modified_at
        artifact.save()
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
        # validate argument
        # todo: add more alias naming convention rules
        if "-" in alias:  # pragma: no cover
            raise ValueError("alias cannot have hyphen")

        version = dynamodb.encode_version(version)

        if secondary_version is not None:
            secondary_version = dynamodb.encode_version(secondary_version)
            if not isinstance(secondary_version_weight, int):
                raise TypeError("secondary_version_weight must be int")
            if not (0 <= secondary_version_weight < 100):
                raise ValueError("secondary_version_weight must be 0 <= x < 100")
            if version == secondary_version:
                raise ValueError(
                    f"version {version!r} and secondary_version {secondary_version!r} "
                    f"cannot be the same!"
                )

        # ensure the artifact exists
        Artifact = self._artifact_class
        self._get_artifact_dynamodb_item(
            Artifact,
            name=name,
            version=version,
        )
        if secondary_version is not None:
            self._get_artifact_dynamodb_item(
                Artifact,
                name=name,
                version=secondary_version,
            )

        Alias = self._alias_class
        alias = Alias.new(
            name=name,
            alias=alias,
            version=version,
            secondary_version=secondary_version,
            secondary_version_weight=secondary_version_weight,
        )
        alias.save()
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
        Deletes an alias.
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
        purge_time = datetime.utcnow().replace(tzinfo=timezone.utc)
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
        """
        self.s3dir_artifact_store.delete()
        Artifact = self._artifact_class
        with Artifact.batch_write() as batch:
            for item in Artifact.scan():
                batch.delete(item)
