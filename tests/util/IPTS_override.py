from contextlib import ExitStack, contextmanager
from pathlib import Path

import pytest
from mantid.kernel import amend_config
from util.Config_helpers import Config_override

from snapred.meta.Config import Config, datasearch_directories

# In order to allow convenient usage within CIS-test scripts,
# `IPTS_override` is deliberately _not_ implemented as a test fixture.

# IMPLEMENTATION NOTES:
# * This context manager is designed to be used to override Mantid's `GetIPTS` search behavior _explicitly_,
# within any running script;
# * In addition, this context manager modifies `Config["IPTS.root"]` if required;
# * In order to allow convenient usage within CIS-test scripts,
# `IPTS_override` is deliberately _not_ implemented as a test fixture.
# * A fine point: if an appropriate IPTS directory is found by `GetIPTS` for a specified run,
# a query will _not_ be sent to ONCAT;
#   conversely, if no directory is found, a query _will_ be sent to ONCAT:
# in this case if there actually _is_ an `/SNS` mount, and a file at the `/SNS` path IPTS-directory actually exists,
# that directory will then be selected by `GetIPTS` and returned as the IPTS directory.

# IMPORTANT: in addition to the use of this context manager:
# `IPTS.root` in `application.yml` may be directly modified --
# by itself, this is sufficient for GUI applications, and for running in Mantid workbench.
# However, for stand-alone scripts, this context manager will need to be used in addition.


@contextmanager
def IPTS_override(basePath: str = Config["IPTS.root"], instrumentName: str = Config["instrument.name"]):
    # Context manager to safely override the IPTS search-directory locations:
    # * In intended usage, this will modify what `GetIPTS` returns,
    # allowing the use of a locally-mounted data partition, containing a useful subset of the data from `/SNS/SNAP`.
    # * The `__enter__` method returns the IPTS-root directory path (as `str`)
    # -- normally, this would be the parent directory to `<instrument.home>` (e.g. "/SNS").
    # * This context manager does _nothing_ when both of the following are true:
    # -- 'basePath' is the default IPTS root, and
    # -- 'application.yml' contains the default 'IPTS.root' setting.

    # __enter__
    stack = ExitStack()
    IPTS_root = Path(basePath)
    if IPTS_root != Config["IPTS.default"]:
        instrumentHome = IPTS_root / instrumentName

        dataSearchDirectories = datasearch_directories(instrumentHome)
        stack.enter_context(amend_config(data_dir=dataSearchDirectories, prepend_datadir=True))
    if Config["IPTS.root"] != basePath:
        stack.enter_context(Config_override("IPTS.root", basePath))
    yield IPTS_root

    # __exit__
    stack.close()


@pytest.fixture
def IPTS_override_fixture():
    _stack = ExitStack()

    def _IPTS_override_fixture(basePath: str = Config["IPTS.root"], instrumentName: str = Config["instrument.name"]):
        return _stack.enter_context(IPTS_override(basePath, instrumentName))

    yield _IPTS_override_fixture

    # teardown => __exit__
    _stack.close()
