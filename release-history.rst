.. _release_history:

Release and Version History
==============================================================================


x.y.z (Backlog)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

**Minor Improvements**

**Bugfixes**

**Miscellaneous**


0.6.1 (2023-10-22)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- Add ``Repository.list_artifact_names()`` and ``Repository.purge_artifact_versions()`` methods to both ``s3_and_dynamodb_backend`` and ``s3_only_backend``


0.5.4 (2023-10-20)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- Fix a bug that the ``s3_only_backend`` is not able to handle suffix like ``.tar.gz``, ``.json.gz``.


0.5.3 (2023-07-14)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Minor Improvements**

- add validation to detect if human manually contaminate the artifact S3 dir.
- add the missing ``Alias.random_artifact()`` method.

**Bugfixes**

- fix bug that sometimes it uses the wrong boto session to get the S3 metadata and S3 last modified time.

**Miscellaneous**

- Update the s3 and dynamodb backend example jupyter notebook.
- Add s3 only backend example jupyter notebook.


0.5.2 (2023-07-14)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Bugfixes**

- fix a bug that accidentally deleted the ``get_artifact_s3path`` API.


0.5.1 (2023-07-14)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- add S3 only backend in ``s3_only_backend.py`` module. It doesn't require DynamoDB table.
- the old ``core.py`` module is renamed to ``s3_and_dynamodb_backend.py`` module. The old API is kept intentionally for backward compatibility. However, it is scheduled to be removed in 1.X.Y release.
- suggest to use ``from versioned import api as versioned``, then use ``versioned.s3_and_dynamodb_backend`` or ``versioned.s3_only_backend`` to access the backend.

**Minor Improvements**

- improve test for ``s3_and_dynamodb_backend``.


0.4.2 (2023-07-07)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Bugfixes**

- fix a bug that the ``purge_all`` method cannot delete DynamoDB items correctly when S3 doesn't have the artifacts.


0.4.1 (2023-07-07)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Minor Improvements**

- add ``versioned.api.Repository.connect_boto_session`` to public API. it can explicitly connect the S3 and DynamoDB API to the given boto session.
- removed useless argument ``bsm`` in many APIs.
- add ``versioned.api.Repository.purge_all`` to public API.


0.3.3 (2023-07-06)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Minor Improvements**

- allow user to set a file name extension for the artifact repository via ``versioned.api.Repository(..., suffix=".tar.gz")``.


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
