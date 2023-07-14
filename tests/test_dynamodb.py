# -*- coding: utf-8 -*-

from versioned.dynamodb import encode_version, encode_version_sk


def test_encode_version():
    assert encode_version(None) == "LATEST"
    assert encode_version("LATEST") == "LATEST"
    assert encode_version(1) == "1"
    assert encode_version(999999) == "999999"
    assert encode_version("1") == "1"
    assert encode_version("000001") == "1"


def test_encode_version_sk():
    assert encode_version_sk(None) == "LATEST"
    assert encode_version_sk("LATEST") == "LATEST"
    assert encode_version_sk("999999") == "999999"
    assert encode_version_sk("2") == "000002"
    assert encode_version_sk("1") == "000001"
    assert encode_version_sk(999999) == "999999"
    assert encode_version_sk(2) == "000002"
    assert encode_version_sk(1) == "000001"


if __name__ == "__main__":
    from versioned.tests import run_cov_test

    run_cov_test(__file__, "versioned.dynamodb", preview=False)
