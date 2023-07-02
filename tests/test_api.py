# -*- coding: utf-8 -*-

from versioned import api


def test():
    _ = api
    _ = api.exc
    _ = api.DYNAMODB_TABLE_NAME
    _ = api.BUCKET_NAME
    _ = api.S3_PREFIX
    _ = api.LATEST_VERSION
    _ = api.VERSION_ZFILL
    _ = api.bootstrap
    _ = api.Artifact
    _ = api.Alias
    _ = api.put_artifact
    _ = api.get_artifact
    _ = api.list_artifacts
    _ = api.publish_version
    _ = api.delete_artifact
    _ = api.put_alias
    _ = api.get_alias
    _ = api.list_aliases
    _ = api.delete_alias
    _ = api.purge
    _ = api.Artifact.s3path
    _ = api.Artifact.get_content
    _ = api.Alias.s3path_version
    _ = api.Alias.get_version_content
    _ = api.Alias.s3path_additional_version
    _ = api.Alias.get_additional_version_content

if __name__ == "__main__":
    from versioned.tests import run_cov_test

    run_cov_test(__file__, "versioned.api", preview=False)
