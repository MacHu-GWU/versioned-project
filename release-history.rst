.. _release_history:

Release and Version History
==============================================================================


x.y.z (Backlog)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

**Minor Improvements**

**Bugfixes**

**Miscellaneous**


0.1.2 (2023-07-01)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Miscellaneous**

- rename ``versioned.api.get_artifact`` to ``versioned.api.get_artifact_version``
- rename ``versioned.api.list_artifacts`` to ``versioned.api.list_artifact_versions``
- rename ``versioned.api.delete_artifact`` to ``versioned.api.delete_artifact_version``
- rename ``versioned.api.purge`` to ``versioned.api.purge_artifact``


0.1.1 (2023-07-01)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- First release
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
