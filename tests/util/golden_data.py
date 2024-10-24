import copy
import json
import logging
from collections import namedtuple
from collections.abc import Mapping, Sequence
from numbers import Number
from pathlib import Path
from typing import Any, Dict

import numpy as np
import pytest
from util.script_as_test import test_only

logger = logging.getLogger(__name__)


def _isPrimitive(obj):
    # if obj.__class__.__module__ == "builtins":
    #     return not (isinstance(obj, dict) or isinstance(obj, list) or isinstance(obj, tuple))
    # return False
    return isinstance(obj, str) or isinstance(obj, Number) or obj is None


class ApproxAnyNested:
    # ** Based on the implementation of `pytest.approx` **:
    # * Perform approximate comparisons where the expected value is any nested composite class,
    # comprised of numeric, and non-numeric primitive values;
    # * any non-numeric values must compare verbatim;
    # * keys and field names are required to match.

    def __init__(self, expected: Any, reltol: float = None, abstol: float = None, nan_ok=False):
        self.expected = expected
        self.reltol = reltol
        self.abstol = abstol
        self.nan_ok = nan_ok

    @classmethod
    def _flattenObject(cls, obj: Any, _memo: Dict[int, Any] = {}):
        # Flatten an object into a list of primitive types
        primitives = []
        if _isPrimitive(obj):
            primitives = [obj]
        else:
            # don't follow circular references
            _id = id(obj)
            if not (_id in _memo and _memo[_id] == obj):
                _memo[_id] = obj
                if not isinstance(obj, Sequence):
                    if not isinstance(obj, Mapping):
                        obj = obj.dict()
                    obj = obj.items()
                for o in obj:
                    primitives.extend(cls._flattenObject(o, _memo))
        return primitives

    def __eq__(self, other) -> bool:
        expected_ = self._flattenObject(self.expected)
        actual_ = self._flattenObject(other)
        if len(expected_) != len(actual_):
            raise RuntimeError(
                f"Object {other.__class__} structure doesn't match the original: " + "golden data must be regenerated."
            )
        status = True
        for e, a in zip(expected_, actual_):
            if not isinstance(e, Number):
                if e != a:
                    status = False
                    break
                continue
            elif not pytest.approx(e, rel=self.reltol, abs=self.abstol, nan_ok=self.nan_ok) == a:
                logger.debug(
                    f"failed comparison: rel: {2.0 * np.abs(e - a) / (np.abs(e) + np.abs(a))}; abs: {np.abs(e - a)}"
                )
                status = False
                if not logger.isEnabledFor(logging.DEBUG):
                    break
        return status


@pytest.fixture
def goldenDataFilePath(request) -> Path:
    # Generate a golden-data file path from test marker information:
    # * as "<golden_data_root> / <short_name>_<golden_data_date>.json".
    _kwargs = request.node.get_closest_marker("golden_data").kwargs
    basePath = request.node.get_closest_marker("golden_data").kwargs["path"]
    shortName = request.node.get_closest_marker("golden_data").kwargs["short_name"]
    goldenDataDate = request.node.get_closest_marker("golden_data").kwargs["date"]
    if not basePath or not shortName or not goldenDataDate:
        raise RuntimeError(
            "'golden_data' marker requires all of 'path', 'short_name', and 'date' "
            + "keyword arguments to be fully specified"
        )
    return Path(basePath) / f"{shortName}_{goldenDataDate}.json"


@pytest.fixture
def goldenData(goldenDataFilePath):
    # Obtain the golden-data corresponding to test marker information:
    # * returns a namedtuple: (<data: Dict[int, Any]>, <key: int>, <filePath: Path>)
    # * the returned key keeps track of the number of times `goldenData()` is called within a test:
    # this allows storing and retrieving _multiple_ golden-data objects for comparison;
    # * in case no golden data exists: it can be collected and saved to disk at test teardown:
    #

    # Example of usage:
    # # Record new golden data (note that `assertMatchToGoldenData` does this internally):
    # data, key1, path = goldenData(goldenDataPath)
    #
    # data[key1] = anyObject1
    # ...
    # _, key2, _ = goldenData(goldenDataPath)
    # data[key2] = anyObject2
    # ...
    # # Collected objects will be written to disk at teardown.
    # # Compare to existing golden data (note that `assertMatchToGoldenData` does this internally):
    # data, key1, path = goldenData(goldenDataPath)
    # expected = data[key1]
    # assert ApproxAnyNested(expected, 1.0e-6) == anyObject1
    # ...
    # _, key2, _ = goldenData(goldenDataPath)
    # expected = data[key2]
    # assert ApproxAnyNested(expected, 1.0e-6) == anyObject2

    DataInfo = namedtuple("DataInfo", "data, key")
    _newData = True
    _data: Dict[int, Any] = {}
    if goldenDataFilePath.exists():
        with open(goldenDataFilePath, "r") as data:
            _data = json.loads(data.read())
            _newData = False
    _info = {"data": _data, "keyCount": 0, "filePath": goldenDataFilePath}
    __data = copy.deepcopy(_data)

    def _goldenData():
        _info["keyCount"] += 1
        # Object instances for later comparison are saved to `_info["data"][str(_keyCount)]`:
        # * note that json dict require string keys, not integer.
        return DataInfo(_info["data"], str(_info["keyCount"]))

    yield _goldenData

    # teardown => if required, save any new golden data
    if _newData:
        with open(goldenDataFilePath, "w") as goldenDataFile:
            goldenDataFile.write(json.dumps(_info["data"]))
    else:
        # verify that it hasn't been corrupted
        assert _data == __data


@test_only
def assertMatchToGoldenData(actual: Any, goldenData, reltol: float = None, abstol: float = None, nan_ok=False):
    # Approximate comparison of any object to existing golden data, or automatic recording of new golden data.
    # (See previous discussion at `goldenData` pytest.fixture.)

    data, key = goldenData()
    expected = data.get(key)
    if expected:
        expected_ = ApproxAnyNested(expected, reltol=reltol, abstol=abstol, nan_ok=nan_ok)
        assert expected_ == actual
    else:
        # Save _new_ golden data (while making sure that it's json-serializable)
        if not (_isPrimitive(actual) or isinstance(actual, Sequence) or isinstance(actual, Mapping)):
            actual = actual.dict()
        data[key] = actual
