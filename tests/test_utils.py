# -*- coding: utf-8 -*-

from versioned.utils import encode_version


def test_encode_version():
    assert encode_version(None) == "LATEST"
    assert encode_version("LATEST") == "LATEST"
    assert encode_version(1) == "1"
    assert encode_version(999999) == "999999"
    assert encode_version("1") == "1"
    assert encode_version("000001") == "1"


if __name__ == "__main__":
    from versioned.tests import run_cov_test

    run_cov_test(
        __file__,
        "versioned.utils",
        preview=False,
    )
