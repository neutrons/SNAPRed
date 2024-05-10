from collections import namedtuple
from contextlib import ExitStack, contextmanager
from typing import Any, Dict, Tuple

import pytest
from snapred.meta.Config import Config

Node = namedtuple("Node", "dict key")

# Implementation notes:
# * In order to allow convenient usage within CIS-test scripts,
# `Config_override` is deliberately _not_ implemented as a test fixture.
# * Multi-level substitution is not implemented: in general this can _effectively_ result in a
# period-delimited key corresponding to values from _multiple_ `Config` nodes.
# It's assumed that this does not pose much limitation, and that it should be possible
# to accomplish any required "override" usage with (possibly multiple) single-node subsitutions.


@contextmanager
def Config_override(key: str, value: Any):
    # Context manager to safely override a `Config` entry:
    # * `__enter__` returns the `Config` instance.

    # Find the _primary_ node associated with a period-delimited key.
    def lookupNode(dict_: Dict[str, Any], key: str) -> Tuple[Dict[str, Any], str]:
        # key_1.key_2. ... key_(n-1) lookup
        ks = key.split(".")
        val = dict_
        for k in ks[0:-1]:
            val = val.get(k)
            if not isinstance(val, dict):
                # Anything else may not correspond to a _single_ `Config` node
                raise RuntimeError(
                    f"not implemented: probable multilevel substitution with key: '{key}' for value: {value}"
                )

        return Node(val, ks[-1])

    # __enter__
    _savedNode: Tuple[Dict[str, Any], str] = lookupNode(Config._config, key)
    _savedValue: Any = _savedNode.dict[_savedNode.key]
    _savedNode.dict[_savedNode.key] = value
    yield Config

    # __exit__
    del _savedNode.dict[_savedNode.key]
    if _savedValue is not None:
        _savedNode.dict[_savedNode.key] = _savedValue


@pytest.fixture()
def Config_override_fixture():
    _stack = ExitStack()

    def _Config_override_fixture(key: str, value: Any):
        return _stack.enter_context(Config_override(key, value))

    yield _Config_override_fixture

    # teardown => __exit__
    _stack.close()
