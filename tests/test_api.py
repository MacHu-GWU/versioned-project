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
    _ = api.s3_only_backend
    _ = api.s3_only_backend.Artifact
    _ = api.s3_only_backend.Artifact.update_datetime
    _ = api.s3_only_backend.Artifact.s3path
    _ = api.s3_only_backend.Artifact.get_content
    _ = api.s3_only_backend.Alias.update_datetime
    _ = api.s3_only_backend.Alias.s3path_version
    _ = api.s3_only_backend.Alias.get_version_content
    _ = api.s3_only_backend.Alias.s3path_secondary_version
    _ = api.s3_only_backend.Alias.random_artifact
    _ = api.s3_only_backend.Alias.get_secondary_version_content
    _ = api.s3_only_backend.Repository
    _ = api.s3_only_backend.Repository.s3dir_artifact_store
    _ = api.s3_only_backend.Repository.bootstrap
    _ = api.s3_only_backend.Repository.get_latest_published_artifact_version_number
    _ = api.s3_only_backend.Repository.put_artifact
    _ = api.s3_only_backend.Repository.get_artifact_version
    _ = api.s3_only_backend.Repository.list_artifact_versions
    _ = api.s3_only_backend.Repository.publish_artifact_version
    _ = api.s3_only_backend.Repository.delete_artifact_version
    _ = api.s3_only_backend.Repository.list_artifact_names
    _ = api.s3_only_backend.Repository.put_alias
    _ = api.s3_only_backend.Repository.get_alias
    _ = api.s3_only_backend.Repository.list_aliases
    _ = api.s3_only_backend.Repository.delete_alias
    _ = api.s3_only_backend.Repository.purge_artifact_versions
    _ = api.s3_only_backend.Repository.purge_artifact
    _ = api.s3_only_backend.Repository.purge_all
    _ = api.s3_and_dynamodb_backend
    _ = api.s3_and_dynamodb_backend.Artifact
    _ = api.s3_and_dynamodb_backend.Artifact.s3path
    _ = api.s3_and_dynamodb_backend.Artifact.get_content
    _ = api.s3_and_dynamodb_backend.Alias
    _ = api.s3_and_dynamodb_backend.Alias.s3path_version
    _ = api.s3_and_dynamodb_backend.Alias.get_version_content
    _ = api.s3_and_dynamodb_backend.Alias.s3path_secondary_version
    _ = api.s3_and_dynamodb_backend.Alias.get_secondary_version_content
    _ = api.s3_and_dynamodb_backend.Alias.random_artifact
    _ = api.s3_and_dynamodb_backend.Repository
    _ = api.s3_and_dynamodb_backend.Repository.s3dir_artifact_store
    _ = api.s3_and_dynamodb_backend.Repository.get_artifact_s3path
    _ = api.s3_and_dynamodb_backend.Repository.bootstrap
    _ = api.s3_and_dynamodb_backend.Repository.put_artifact
    _ = api.s3_and_dynamodb_backend.Repository.get_artifact_version
    _ = api.s3_and_dynamodb_backend.Repository.list_artifact_versions
    _ = api.s3_and_dynamodb_backend.Repository.publish_artifact_version
    _ = api.s3_and_dynamodb_backend.Repository.delete_artifact_version
    _ = api.s3_and_dynamodb_backend.Repository.list_artifact_names
    _ = api.s3_and_dynamodb_backend.Repository.put_alias
    _ = api.s3_and_dynamodb_backend.Repository.get_alias
    _ = api.s3_and_dynamodb_backend.Repository.list_aliases
    _ = api.s3_and_dynamodb_backend.Repository.delete_alias
    _ = api.s3_and_dynamodb_backend.Repository.purge_artifact_versions
    _ = api.s3_and_dynamodb_backend.Repository.purge_artifact
    _ = api.s3_and_dynamodb_backend.Repository.purge_all


if __name__ == "__main__":
    from versioned.tests import run_cov_test

    run_cov_test(
        __file__,
        "versioned.api",
        preview=False,
    )
