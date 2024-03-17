# Unit tests for `tests/util/script_as_test.py`
import os
from unittest import mock

import pytest
from util.script_as_test import script_only, test_only, test_only_if


def test_test_only_if_True_testing():
    _flag = [False]

    @test_only_if(True)
    def toggle():
        _flag[0] = not _flag[0]

    with mock.patch.dict(os.environ, {"PYTEST_CURRENT_TEST": "some_test_name"}):
        assert "PYTEST_CURRENT_TEST" in os.environ
        toggle()
        assert _flag[0]


def test_test_only_if_True_not_testing():
    _flag = [False]

    @test_only_if(True)
    def toggle():
        _flag[0] = not _flag[0]

    with mock.patch.dict(os.environ):
        if "PYTEST_CURRENT_TEST" in os.environ:
            del os.environ["PYTEST_CURRENT_TEST"]
        assert "PYTEST_CURRENT_TEST" not in os.environ
        toggle()
        assert not _flag[0]


def test_test_only_if_False_testing():
    _flag = [False]

    @test_only_if(False)
    def toggle():
        _flag[0] = not _flag[0]

    with mock.patch.dict(os.environ, {"PYTEST_CURRENT_TEST": "some_test_name"}):
        assert "PYTEST_CURRENT_TEST" in os.environ
        toggle()
        assert not _flag[0]


def test_test_only_if_False_not_testing():
    _flag = [False]

    @test_only_if(False)
    def toggle():
        _flag[0] = not _flag[0]

    with mock.patch.dict(os.environ):
        if "PYTEST_CURRENT_TEST" in os.environ:
            del os.environ["PYTEST_CURRENT_TEST"]
        assert "PYTEST_CURRENT_TEST" not in os.environ
        toggle()
        assert _flag[0]


def test_test_only_testing():
    _flag = [False]

    @test_only
    def toggle():
        _flag[0] = not _flag[0]

    with mock.patch.dict(os.environ, {"PYTEST_CURRENT_TEST": "some_test_name"}):
        assert "PYTEST_CURRENT_TEST" in os.environ
        toggle()
        assert _flag[0]


def test_test_only_not_testing():
    _flag = [False]

    @test_only
    def toggle():
        _flag[0] = not _flag[0]

    with mock.patch.dict(os.environ):
        if "PYTEST_CURRENT_TEST" in os.environ:
            del os.environ["PYTEST_CURRENT_TEST"]
        assert "PYTEST_CURRENT_TEST" not in os.environ
        toggle()
        assert not _flag[0]


def test_script_only_testing():
    _flag = [False]

    @script_only
    def toggle():
        _flag[0] = not _flag[0]

    with mock.patch.dict(os.environ, {"PYTEST_CURRENT_TEST": "some_test_name"}):
        assert "PYTEST_CURRENT_TEST" in os.environ
        toggle()
        assert not _flag[0]


def test_script_only_not_testing():
    _flag = [False]

    @script_only
    def toggle():
        _flag[0] = not _flag[0]

    with mock.patch.dict(os.environ):
        if "PYTEST_CURRENT_TEST" in os.environ:
            del os.environ["PYTEST_CURRENT_TEST"]
        assert "PYTEST_CURRENT_TEST" not in os.environ
        toggle()
        assert _flag[0]


def test_env_state_at_application_testing():
    # WARNING: "test_only..." decorators must not depend on `os.environ` state
    #   at the time of definition of the target function for the decorator.
    # In most cases, 'PYTEST_CURRENT_TEST' will _not_ be defined in the `os.environ`
    #   at time of definition or import.
    assert "PYTEST_CURRENT_TEST" in os.environ
    test_test_only_if_True_testing()
    test_test_only_if_True_not_testing()
    test_test_only_if_False_testing()
    test_test_only_if_False_not_testing()
    test_test_only_testing()
    test_test_only_not_testing()
    test_script_only_testing()
    test_script_only_not_testing()


def test_env_state_at_application_not_testing():
    # See comment at: `test_env_state_at_import_testing`.
    with mock.patch.dict(os.environ):
        del os.environ["PYTEST_CURRENT_TEST"]
        assert "PYTEST_CURRENT_TEST" not in os.environ
        test_test_only_if_True_testing()
        test_test_only_if_True_not_testing()
        test_test_only_if_False_testing()
        test_test_only_if_False_not_testing()
        test_test_only_testing()
        test_test_only_not_testing()
        test_script_only_testing()
        test_script_only_not_testing()
