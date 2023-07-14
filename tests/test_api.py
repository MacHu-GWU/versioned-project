# -*- coding: utf-8 -*-

from versioned import api


def test():
    _ = api
    _ = api.exc
    _ = api.DYNAMODB_TABLE_NAME
    _ = api.S3_PREFIX
    _ = api.LATEST_VERSION
    _ = api.VERSION_ZFILL
    _ = api.METADATA_KEY_ARTIFACT_SHA256
    _ = api.Artifact
    _ = api.Alias
    _ = api.Repository
    _ = api.Repository.get_artifact_s3path
    _ = api.Repository.put_artifact
    _ = api.Repository.get_artifact_version
    _ = api.Repository.list_artifact_versions
    _ = api.Repository.publish_artifact_version
    _ = api.Repository.delete_artifact_version
    _ = api.Repository.put_alias
    _ = api.Repository.get_alias
    _ = api.Repository.list_aliases
    _ = api.Repository.delete_alias
    _ = api.Repository.purge_artifact
    _ = api.Repository.purge_all
    _ = api.Artifact.s3path
    _ = api.Artifact.get_content
    _ = api.Alias.s3path_version
    _ = api.Alias.get_version_content
    _ = api.Alias.s3path_secondary_version
    _ = api.Alias.get_secondary_version_content
    _ = api.s3_and_dynamodb_backend
    _ = api.s3_only_backend


if __name__ == "__main__":
    from versioned.tests import run_cov_test

    run_cov_test(__file__, "versioned.api", preview=False)
