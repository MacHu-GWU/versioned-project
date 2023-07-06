.. _release_history:

Release and Version History
==============================================================================


x.y.z (Backlog)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

**Minor Improvements**

**Bugfixes**

**Miscellaneous**


0.3.2 (2023-07-06)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Minor Improvements**

- now the ``versioned.api.Repository`` takes explicit ``aws_region``, ``s3_bucket`` arguments in constructor.
- add ``versioned.api.Repository.get_artifact_s3path`` to public API.


0.3.1 (2023-07-06)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- Created a new public API class ``versioned.api.Repository``, allow developer to customize the S3 bucket and DynamoDB table name backend. So old API are renamed to:
    - ``versioned.api.Repository.bootstrap``
    - ``versioned.api.Repository.put_artifact``
    - ``versioned.api.Repository.get_artifact_version``
    - ``versioned.api.Repository.list_artifact_versions``
    - ``versioned.api.Repository.publish_artifact_version``
    - ``versioned.api.Repository.delete_artifact_version``
    - ``versioned.api.Repository.put_alias``
    - ``versioned.api.Repository.get_alias``
    - ``versioned.api.Repository.list_aliases``
    - ``versioned.api.Repository.delete_alias``
    - ``versioned.api.Repository.purge_artifact``


0.2.1 (2023-07-05)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- add ``content_type``, ``metadata``, ``tags`` arguments to ``versioned.put_artifact``.
- ``versioned.put_artifact`` now will skip uploading to s3 if ``content`` is not changed.


0.1.2 (2023-07-01)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Miscellaneous**

- rename ``versioned.api.get_artifact`` to ``versioned.api.get_artifact_version``.
- rename ``versioned.api.list_artifacts`` to ``versioned.api.list_artifact_versions``.
- rename ``versioned.api.delete_artifact`` to ``versioned.api.delete_artifact_version``.
- rename ``versioned.api.purge`` to ``versioned.api.purge_artifact``.
- rename ``additional_version`` to ``secondary_version``.


0.1.1 (2023-07-01)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- First release.
- Add the following public api:
    - ``versioned.api.exc``
    - ``versioned.api.DYNAMODB_TABLE_NAME``
    - ``versioned.api.BUCKET_NAME``
    - ``versioned.api.S3_PREFIX``
    - ``versioned.api.LATEST_VERSION``
    - ``versioned.api.VERSION_ZFILL``
    - ``versioned.api.bootstrap``
    - ``versioned.api.Artifact``
    - ``versioned.api.Alias``
    - ``versioned.api.put_artifact``
    - ``versioned.api.get_artifact``
    - ``versioned.api.list_artifacts``
    - ``versioned.api.publish_version``
    - ``versioned.api.delete_artifact``
    - ``versioned.api.put_alias``
    - ``versioned.api.get_alias``
    - ``versioned.api.list_aliases``
    - ``versioned.api.delete_alias``
    - ``versioned.api.purge``
