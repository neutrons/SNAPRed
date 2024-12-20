import os

import pytest
import unittest.mock as mock

from snapred.meta.decorators import Resettable
from snapred.meta.decorators.Singleton import reset_Singletons

# import sys
# sys.path.append('.')
# os.environ['PYTHONPATH'] = './src'

# Allow override: e.g. `env=dev pytest ...`
if not os.environ.get("env"):
    os.environ["env"] = "test"

from mantid.kernel import ConfigService  # noqa: E402
from snapred.meta.Config import (  # noqa: E402
    Config,  # noqa: E402
    Resource,  # noqa: E402
)


# PATCH the `unittest.mock.Mock` class: BANNED FUNCTIONS
def banned_function(function_name: str):
  _error_message: str = f"`Mock.{function_name}` is a mock, it always evaluates to True. Use `Mock.assert_{function_name}` instead."

  def _banned_function(self, *args, **kwargs):
      nonlocal _error_message # this line should not be necessary!

      # Ensure that the complete message is in the pytest-captured output stream:
      print(_error_message)

      raise RuntimeError(_error_message)

  return _banned_function

# `mock.Mock.called` is OK: it exists as a boolean attribute
mock.Mock.called_once = banned_function("called_once")
mock.Mock.called_once_with = banned_function("called_once_with")
mock.Mock.called_with = banned_function("called_with")
mock.Mock.not_called = banned_function("not_called")


def mock_decorator(orig_cls):
    return orig_cls

###### PATCH THE DECORATORS HERE ######

mockResettable = mock.Mock()
mockResettable.Resettable = mock_decorator
mock.patch.dict("sys.modules", {"snapred.meta.decorators.Resettable": mockResettable}).start()
mock.patch.dict("sys.modules", {"snapred.meta.decorators._Resettable": Resettable}).start()

mantidConfig = config = ConfigService.Instance()
mantidConfig["CheckMantidVersion.OnStartup"] = "0"
mantidConfig["UpdateInstrumentDefinitions.OnStartup"] = "0"
mantidConfig["usagereports.enabled"] = "0"

#######################################

# this at teardown removes the loggers, eliminating logger-related error printouts
#   see https://github.com/pytest-dev/pytest/issues/5502#issuecomment-647157873
@pytest.fixture(autouse=True, scope="session")
def clear_loggers():  # noqa: PT004
    """Remove handlers from all loggers"""
    import logging

    yield  # ... teardown follows:
    loggers = [logging.getLogger()] + list(logging.Logger.manager.loggerDict.values())
    for logger in loggers:
        handlers = getattr(logger, "handlers", [])
        for handler in handlers:
            logger.removeHandler(handler)

########################################################################################################################
# In combination, the following autouse fixtures allow unit tests and integration tests
#   to be run successfully without mocking out the `@Singleton` decorator.
#
#   * The main objective is to allow the `@Singleton` classes to function as singletons, for the
#     duration of the single-test scope.  This functionality is necessary, for example, in order for
#     the `Indexer` class to function correctly during the state-initialization sequence.
#
#   * There are some fine points involved with using `@Singleton` classes during testing at class and module scope.
#     Such usage should probably be avoided whenever possible.  It's a bit tricky to get this to work correctly
#     within the test framework.
#
#   * TODO: Regardless of these fixtures, at the moment the `@Singleton` decorator must be completely turned ON during
#     integration tests, without any modification (e.g. or "reset").  There is something going on at "session" scope
#     with specific singletons not being deleted between tests, which results in multiple singleton instances when the
#     fixtures are used.  This behavior does not seem to be an issue for the unit tests.
#     We can track this down by turning on the garbage collector `gc`, but this work has not yet been completed.
#
# Implementation notes:
#
#   * Right now, there are > 36 `@Singleton` decorated classes.  Probably, there should be far fewer.
#     Almost none of these classes are compute-intensive to initialize, or retain any cached data.
#     These would be the normal justifications for the use of this pattern.
#
#   * Applying the `@Singleton` decorator changes the behavior of the classes,
#     so we don't want to mock the decorator out during testing. At present, the key class where this is important
#     is the `Indexer` class, which is not itself a singleton, but which is owned and cached
#     by the `LocalDataService` singleton.  `Indexer` instances retain local data about indexing events
#     that have occurred since their initialization.
#

@pytest.fixture(autouse=True)
def _reset_Singletons(request):
    if not "integration" in request.keywords:
        reset_Singletons()
    yield

@pytest.fixture(scope="class", autouse=True)
def _reset_class_scope_Singletons(request):
    if not "integration" in request.keywords:
        reset_Singletons()
    yield

@pytest.fixture(scope="module", autouse=True)
def _reset_module_scope_Singletons(request):
    if not "integration" in request.keywords:
        reset_Singletons()
    yield

########################################################################################################################


## Import various `pytest.fixture` defined in separate `tests/util` modules:
#  -------------------------------------------------------------------------
# *** IMPORTANT WARNING: these must be included _after_ the `Singleton` decorator is patched ! ***
#   * Otherwise, the modules imported by these will not have the patched decorator applied to them.

from util.golden_data import goldenData, goldenDataFilePath
from util.state_helpers import state_root_fixture
from util.IPTS_override import IPTS_override_fixture
from util.Config_helpers import Config_override_fixture
from util.pytest_helpers import (
    calibration_home_from_mirror,
    cleanup_workspace_at_exit,
    cleanup_class_workspace_at_exit,
    get_unique_timestamp,
    reduction_home_from_mirror
)
