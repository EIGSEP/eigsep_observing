"""Producer / fixture contract conformance tests.

This subpackage ships inside the installed wheel so that
``eigsep-field verify`` can run it on nodes that do not have the
eigsep_observing test tree checked out (e.g. the Pi). Invoke via::

    pytest --pyargs eigsep_observing.contract_tests

In the eigsep_observing development checkout the same tests are
discovered by plain ``pytest`` thanks to the entry in
``tool.pytest.ini_options.testpaths``.
"""
