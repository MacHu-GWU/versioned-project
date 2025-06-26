# -*- coding: utf-8 -*-

if __name__ == "__main__":
    from versioned.tests import run_cov_test

    run_cov_test(
        __file__,
        "versioned",
        is_folder=True,
        preview=False,
    )
