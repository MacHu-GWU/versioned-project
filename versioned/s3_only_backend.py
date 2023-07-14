# -*- coding: utf-8 -*-

import typing as T

import json
import random
import dataclasses
from datetime import datetime

from boto_session_manager import BotoSesManager
from s3pathlib import S3Path
from func_args import NOTHING

from . import exc
from .compat import cached_property
from .constants import (
    S3_PREFIX,
    LATEST_VERSION,
    VERSION_ZFILL,
    METADATA_KEY_ARTIFACT_SHA256,
)
from .vendor.hashes import hashes


def encode_version(version: T.Optional[T.Union[int, str]]) -> str:
    """
    Encode human readable "version" into the data class field "version".
    """
    if version is None:
        return LATEST_VERSION
    else:
        return str(version)


def encode_filename(version: T.Optional[T.Union[int, str]]) -> str:
    """
    Encode version into file name so that we can leverage the feature that
    S3 list objects always return objects in alphabetical order.

    Example::

        LATEST -> 000000_LATEST
        LATEST -> 000000_LATEST
        999999 -> 000001_999999
        ...
        2      -> 999998_000002
        1      -> 999999_000001
    """
    version = encode_version(version)
    if version == LATEST_VERSION:
        return "{}_{}".format(
            VERSION_ZFILL * "0",
            LATEST_VERSION,
        )
    else:
        return "{}_{}".format(
            str(10**VERSION_ZFILL - int(version)).zfill(VERSION_ZFILL),
            version.zfill(VERSION_ZFILL),
        )


def decode_filename(version: str) -> str:
    """
    Inverse of :func:`encode_filename`.

    Example::

        000000_LATEST -> LATEST
        000001_999999 -> 999999
        ...
        999998_000002 -> 2
        999999_000001 -> 1
    """
    if version.startswith(VERSION_ZFILL * "0"):
        return LATEST_VERSION
    else:
        return str(int(version.split("_")[1]))


def validate_alias_name(alias: str):
    """
    Validate the alias name.
    """
    if alias == LATEST_VERSION:
        raise ValueError(f"alias name cannot be {LATEST_VERSION!r}.")
    if "-" in alias:
        raise ValueError("alias name cannot contain '-'.")
    if alias[0].isalpha() is False:
        raise ValueError("alias name must start with a alpha letter.")


@dataclasses.dataclass
class Base:
    @classmethod
    def from_dict(cls, dct: dict):
        return cls(**dct)

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class Artifact(Base):
    """
    Data class for artifact.

    :param name: artifact name.
    :param version: artifact version.
    :param update_at: an utc datetime object when this artifact was updated,
        in string format
    :param s3uri: s3uri of the artifact version.
    :param sha256: sha256 of the content of the artifact version.
    """

    name: str
    version: str
    update_at: str
    s3uri: str
    sha256: str

    # def __post_init__(self):
    #     if self.version is None:
    #         self.version = LATEST_VERSION
    #     self.version = str(self.version)

    @property
    def update_datetime(self) -> datetime:
        """
        Return the datetime format of the update_at field.
        """
        return datetime.fromisoformat(self.update_at)

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

    # @classmethod
    # def _from_s3uri(
    #     cls,
    #     bsm: BotoSesManager,
    #     s3uri: str,
    # ):
    #     s3path = S3Path.from_s3_uri(s3uri)
    #     s3path.head_object(bsm=bsm)
    #     return cls(
    #         name=s3path.parent.parent.basename,
    #         version=decode_version(s3path.fname),
    #         update_at=s3path.last_modified_at,
    #         s3uri=s3uri,
    #         sha256=s3path.metadata[METADATA_KEY_ARTIFACT_SHA256],
    #     )


@dataclasses.dataclass
class Alias(Base):
    """
    Data class for alias.

    :param name: artifact name.
    :param alias: alias name. alias name cannot have hyphen
    :param update_at: an utc datetime object when this artifact was updated,
        in string format
    :param version: artifact version. If ``None``, the latest version is used.
    :param secondary_version: see above.
    :param secondary_version_weight: an integer between 0 ~ 100.
    :param version_s3uri: s3uri of the primary artifact version of this alias.
    :param secondary_version_s3uri: s3uri of the secondary artifact version of this alias.
    """

    name: str
    alias: str
    update_at: str
    version: str
    secondary_version: T.Optional[str]
    secondary_version_weight: T.Optional[int]
    version_s3uri: str
    secondary_version_s3uri: T.Optional[str]

    # def __post_init__(self):
    #     if self.version is None:
    #         self.version = LATEST_VERSION
    #     self.version = str(self.version)
    #     if self.secondary_version is not None:
    #         self.secondary_version = str(LATEST_VERSION)
    #         if self.version == self.secondary_version:
    #             raise

    @property
    def update_datetime(self) -> datetime:
        """
        Return the datetime format of the update_at field.
        """
        return datetime.fromisoformat(self.update_at)

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
    :param aws_region: the aws region of where the artifact store is.
    :param s3_bucket: the s3 bucket name of the artifact store.
    :param s3_prefix: the s3 prefix (folder path) of the artifact store.
    :param suffix: the file extension suffix of the artifact binary.
    """

    aws_region: str = dataclasses.field()
    s3_bucket: str = dataclasses.field()
    s3_prefix: str = dataclasses.field(default=S3_PREFIX)
    suffix: str = dataclasses.field(default="")

    @property
    def s3dir_artifact_store(self) -> S3Path:
        """
        Return the s3dir of the artifact store folder.

        Example: ``s3://${s3_bucket}/${s3_prefix}/``
        """
        return S3Path(self.s3_bucket).joinpath(self.s3_prefix).to_dir()

    def _get_artifact_s3dir(self, name: str) -> S3Path:
        """
        Example: ``s3://${s3_bucket}/${s3_prefix}/${name}/``
        """
        return self.s3dir_artifact_store.joinpath(name).to_dir()

    def _get_artifact_s3path(
        self,
        name: str,
        version: T.Optional[T.Union[int, str]] = None,
    ) -> S3Path:
        """
        Example: ``s3://${s3_bucket}/${s3_prefix}/${name}/versions/${encoded_version}${suffix}``
        """
        return self._get_artifact_s3dir(name=name).joinpath(
            "versions",
            f"{encode_filename(version)}{self.suffix}",
        )

    def _get_alias_s3path(self, name: str, alias: str) -> S3Path:
        """
        Example: ``s3://${s3_bucket}/${s3_prefix}/${name}/aliases/${alias}.json``
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
        Create necessary backend resources for the artifact store.

        :param bsm: ``boto_session_manager.BotoSesManager`` object.
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

    def get_latest_published_artifact_version_number(
        self,
        bsm: BotoSesManager,
        name: str,
    ) -> int:
        """
        Return the latest published artifact version number,
        if no version has been published yet, return 0.

        :param bsm: ``boto_session_manager.BotoSesManager`` object.
        :param name: artifact name.
        """
        s3dir_artifact = self._get_artifact_s3dir(name=name)
        s3path_list = s3dir_artifact.iter_objects(bsm=bsm, limit=2).all()
        if len(s3path_list) in [0, 1]:
            return 0
        else:
            return int(decode_filename(s3path_list[1].fname))

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
        content_type: str = NOTHING,
        metadata: T.Dict[str, str] = NOTHING,
        tags: T.Dict[str, str] = NOTHING,
    ) -> Artifact:
        """
        Create / Update artifact to the latest.

        :param bsm: ``boto_session_manager.BotoSesManager`` object.
        :param name: artifact name.
        :param content: binary artifact content.
        :param content_type: customize s3 content type.
        :param metadata: optional metadata of the s3 object.
        :param tags: optional tags of the s3 object.
        """
        artifact_sha256 = hashes.of_bytes(content)
        s3path = self._get_artifact_s3path(name=name, version=LATEST_VERSION)

        # do nothing if the content is not changed
        if s3path.exists(bsm=bsm):
            if s3path.metadata[METADATA_KEY_ARTIFACT_SHA256] == artifact_sha256:
                return Artifact(
                    name=name,
                    version=LATEST_VERSION,
                    update_at=s3path.last_modified_at.isoformat(),
                    s3uri=s3path.uri,
                    sha256=artifact_sha256,
                )

        # prepare metadata
        final_metadata = dict(
            artifact_name=name,
            artifact_sha256=artifact_sha256,
        )
        if metadata is not NOTHING:
            final_metadata.update(metadata)

        # write artifact to S3
        s3path.write_bytes(
            content,
            metadata=final_metadata,
            content_type=content_type,
            tags=tags,
            bsm=bsm,
        )
        s3path.head_object(bsm=bsm)

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
        Return the information about the artifact or artifact version.

        :param bsm: ``boto_session_manager.BotoSesManager`` object.
        :param name: artifact name.
        :param version: artifact version. If ``None``, return the latest version.
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
        Return a list of artifact versions. The latest version is always the first item.
        And the newer version comes first.

        :param bsm: ``boto_session_manager.BotoSesManager`` object.
        :param name: artifact name.
        """
        s3dir_artifact = self._get_artifact_s3dir(name=name)
        return [
            Artifact(
                name=name,
                version=decode_filename(s3path.fname),
                update_at=s3path.last_modified_at.isoformat(),
                s3uri=s3path.uri,
                sha256=s3path.metadata[METADATA_KEY_ARTIFACT_SHA256],
            )
            for s3path in s3dir_artifact.iter_objects(bsm=bsm)
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
        s3dir_artifact = self._get_artifact_s3dir(name=name)
        s3path_list = s3dir_artifact.iter_objects(bsm=bsm, limit=2).all()
        n = len(s3path_list)
        s3path_latest = self._get_artifact_s3path(
            name=name,
            version=LATEST_VERSION,
        )
        if n == 0:
            raise exc.ArtifactNotFoundError(
                f"artifact {name!r} not found! you must put artifact first!"
            )
        elif n == 1:
            new_version = "1"
            s3path_new = self._get_artifact_s3path(name=name, version=new_version)
            s3path_latest.copy_to(s3path_new, bsm=bsm)
            return Artifact(
                name=name,
                version=new_version,
                update_at=s3path_new.last_modified_at.isoformat(),
                s3uri=s3path_new.uri,
                sha256=s3path_new.metadata[METADATA_KEY_ARTIFACT_SHA256],
            )
        else:
            previous_version = decode_filename(s3path_list[1].fname)
            new_version = str(int(previous_version) + 1)
            s3path_new = self._get_artifact_s3path(name=name, version=new_version)
            s3path_previous = s3path_list[1]
            if s3path_previous.etag == s3path_latest.etag:
                return Artifact(
                    name=name,
                    version=previous_version,
                    update_at=s3path_previous.last_modified_at.isoformat(),
                    s3uri=s3path_previous.uri,
                    sha256=s3path_previous.metadata[METADATA_KEY_ARTIFACT_SHA256],
                )
            else:
                s3path_latest.copy_to(s3path_new, bsm=bsm)
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
        self._get_artifact_s3path(name=name, version=version).delete(bsm=bsm)

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
        :param alias: alias name. alias name cannot have hyphen.
        :param version: artifact version. If ``None``, the latest version is used.
        :param secondary_version: see above.
        :param secondary_version_weight: an integer between 0 ~ 100.
        """
        # validate input argument
        # todo: add more alias naming convention rules
        if "-" in alias:  # pragma: no cover
            raise ValueError("alias cannot have hyphen")

        version = encode_version(version)

        if secondary_version is not None:
            secondary_version = encode_version(secondary_version)
            if not isinstance(secondary_version_weight, int):
                raise TypeError("secondary_version_weight must be int")
            if not (0 <= secondary_version_weight < 100):
                raise ValueError("secondary_version_weight must be 0 <= x < 100")
            if version == secondary_version:
                raise ValueError(
                    f"version {version!r} and secondary_version {secondary_version!r} "
                    f"cannot be the same!"
                )

        # verify the artifact exists
        version_s3path = self._get_artifact_s3path(name=name, version=version)
        version_s3uri = version_s3path.uri
        if version_s3path.exists(bsm=bsm) is False:
            raise exc.ArtifactNotFoundError(
                f"Cannot put alias to artifact name = {name!r}, version = {version}"
            )

        if secondary_version is not None:
            secondary_version_s3path = self._get_artifact_s3path(
                name=name,
                version=secondary_version,
            )
            secondary_version_s3uri = secondary_version_s3path.uri
            if secondary_version_s3path.exists(bsm=bsm) is False:
                raise exc.ArtifactNotFoundError(
                    f"Cannot put alias to artifact name = {name!r}, version = {version}"
                )
        else:
            secondary_version_s3uri = None

        # create alias object
        alias_obj = Alias(
            name=name,
            alias=alias,
            version=version,
            update_at="unknown",
            secondary_version=secondary_version,
            secondary_version_weight=secondary_version_weight,
            version_s3uri=version_s3uri,
            secondary_version_s3uri=secondary_version_s3uri,
        )
        alias_s3path = self._get_alias_s3path(name=name, alias=alias)
        alias_s3path.write_text(json.dumps(alias_obj.to_dict()), bsm=bsm)
        alias_s3path.head_object(bsm=bsm)
        alias_obj.update_at = alias_s3path.last_modified_at.isoformat()
        return alias_obj

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
        :param alias: alias name. alias name cannot have hyphen.
        """
        return self._get_alias_object(bsm=bsm, name=name, alias=alias)

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
        s3dir = self._get_artifact_s3dir(name=name).joinpath("aliases")
        return [
            self._get_alias_object(
                bsm=bsm,
                name=s3path.parent.parent.basename,
                alias=s3path.fname,
            )
            for s3path in s3dir.iter_objects(bsm=bsm)
        ]

    def delete_alias(
        self,
        bsm: BotoSesManager,
        name: str,
        alias: str,
    ):
        """
        Deletes an alias.

        :param bsm: ``boto_session_manager.BotoSesManager`` object.
        :param name: artifact name.
        :param alias: alias name. alias name cannot have hyphen.
        """
        self._get_alias_s3path(name=name, alias=alias).delete(bsm=bsm)

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
        self._get_artifact_s3dir(name=name).delete(bsm=bsm)

    def purge_all(self, bsm: BotoSesManager):
        """
        Completely delete all artifacts and aliases in this Repository
        This operation is irreversible. It will remove all related S3 artifacts.

        :param bsm: ``boto_session_manager.BotoSesManager`` object.
        """
        self.s3dir_artifact_store.delete(bsm=bsm)