
.. image:: https://readthedocs.org/projects/versioned/badge/?version=latest
    :target: https://versioned.readthedocs.io/en/latest/
    :alt: Documentation Status

.. image:: https://github.com/MacHu-GWU/versioned-project/actions/workflows/main.yml/badge.svg
    :target: https://github.com/MacHu-GWU/versioned-project/actions?query=workflow:CI

.. image:: https://codecov.io/gh/MacHu-GWU/versioned-project/branch/main/graph/badge.svg
    :target: https://codecov.io/gh/MacHu-GWU/versioned-project

.. image:: https://img.shields.io/pypi/v/versioned.svg
    :target: https://pypi.python.org/pypi/versioned

.. image:: https://img.shields.io/pypi/l/versioned.svg
    :target: https://pypi.python.org/pypi/versioned

.. image:: https://img.shields.io/pypi/pyversions/versioned.svg
    :target: https://pypi.python.org/pypi/versioned

.. image:: https://img.shields.io/badge/✍️_Release_History!--None.svg?style=social&logo=github
    :target: https://github.com/MacHu-GWU/versioned-project/blob/main/release-history.rst

.. image:: https://img.shields.io/badge/⭐_Star_me_on_GitHub!--None.svg?style=social&logo=github
    :target: https://github.com/MacHu-GWU/versioned-project

------

.. image:: https://img.shields.io/badge/Link-API-blue.svg
    :target: https://versioned.readthedocs.io/en/latest/py-modindex.html

.. image:: https://img.shields.io/badge/Link-Install-blue.svg
    :target: `install`_

.. image:: https://img.shields.io/badge/Link-GitHub-blue.svg
    :target: https://github.com/MacHu-GWU/versioned-project

.. image:: https://img.shields.io/badge/Link-Submit_Issue-blue.svg
    :target: https://github.com/MacHu-GWU/versioned-project/issues

.. image:: https://img.shields.io/badge/Link-Request_Feature-blue.svg
    :target: https://github.com/MacHu-GWU/versioned-project/issues

.. image:: https://img.shields.io/badge/Link-Download-blue.svg
    :target: https://pypi.org/pypi/versioned#files


Welcome to ``versioned`` Documentation
==============================================================================
.. image:: https://versioned.readthedocs.io/en/latest/_static/versioned-logo.png
    :target: https://versioned.readthedocs.io/en/latest/

**Versioned** provides enterprise-grade artifact version management and deployment patterns for AWS environments. It combines the performance of DynamoDB metadata storage with cost-effective S3 binary storage to enable sophisticated deployment strategies including blue/green deployments, canary releases, and instant rollbacks.

**Key Features**

🚀 **Advanced Deployment Patterns**
   - Blue/Green deployments with instant switching
   - Canary releases with weighted traffic splitting
   - One-click rollbacks to any previous version

⚡ **High Performance Architecture**
   - DynamoDB for sub-millisecond metadata queries
   - S3 for cost-effective binary artifact storage
   - Automatic content deduplication with SHA256 hashing

🔒 **Enterprise Ready**
   - Immutable version snapshots
   - Soft deletion with recovery capabilities
   - Comprehensive audit trails and metadata tracking

🎯 **Simple API**
   - Intuitive Python interface
   - Safe public API that prevents data corruption
   - Flexible session management for multi-account scenarios

.. image:: https://github.com/MacHu-GWU/versioned-project/assets/6800411/57f7970e-3821-45a0-9deb-64890e04c129


Related Projects
------------------------------------------------------------------------------
- `aws_glue_artifact <https://github.com/MacHu-GWU/aws_glue_artifact-project>`_ - Versioned deployment for AWS Glue ETL jobs
- `airflow_dag_artifact <https://github.com/MacHu-GWU/airflow_dag_artifact-project>`_ - Versioned deployment for Airflow DAGs


.. _install:

Installation
------------------------------------------------------------------------------

Install from PyPI:

.. code-block:: console

    $ pip install versioned

Upgrade to latest version:

.. code-block:: console

    $ pip install --upgrade versioned