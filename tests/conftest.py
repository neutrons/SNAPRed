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


## Import `pytest.fixture` defined in separate `tests/util` modules:
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
