import os

import pytest
import unittest.mock as mock

from snapred.meta.decorators import Resettable

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


# PATCH THE DECORATOR HERE
mockSingleton = mock.Mock()
mockSingleton.Singleton = mock_decorator
mock.patch.dict("sys.modules", {"snapred.meta.decorators.Singleton": mockSingleton}).start()

mockResettable = mock.Mock()
mockResettable.Resettable = mock_decorator
mock.patch.dict("sys.modules", {"snapred.meta.decorators.Resettable": mockResettable}).start()
mock.patch.dict("sys.modules", {"snapred.meta.decorators._Resettable": Resettable}).start()

mantidConfig = config = ConfigService.Instance()
mantidConfig["CheckMantidVersion.OnStartup"] = "0"
mantidConfig["UpdateInstrumentDefinitions.OnStartup"] = "0"
mantidConfig["usagereports.enabled"] = "0"


# this at teardown removes the loggers, eliminating logger error printouts
# see https://github.com/pytest-dev/pytest/issues/5502#issuecomment-647157873
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


## Import various `pytest.fixture` defined in separate `tests/util` modules:
#  -------------------------------------------------------------------------
# *** IMPORTANT WARNING: these must be included _after_ the `Singleton` decorator is patched ! ***
#   * Otherwise, the modules imported by these will not have the patched decorator applied to them.

from util.golden_data import goldenData, goldenDataFilePath
from util.state_helpers import state_root_fixture
from util.IPTS_override import IPTS_override_fixture
from util.Config_helpers import Config_override_fixture
from util.pytest_helpers import (
    cleanup_workspace_at_exit,
    cleanup_class_workspace_at_exit,
)
