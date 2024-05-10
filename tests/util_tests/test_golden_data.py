# Unit tests for `tests/util/golden_data.py`
import tempfile
from contextlib import ExitStack
from datetime import date
from pathlib import Path

import pytest
from snapred.meta.Config import Resource
from util.golden_data import ApproxAnyNested, _isPrimitive, assertMatchToGoldenData

# Generate a temporary golden-data directory, that will be automatically deleted at the end of the module's execution.
# * Note that pytest collection occurs at import: all _marker_ definitions must be resolved at that time.
_stack = ExitStack()
_golden_data_base = _stack.enter_context(tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")))


@pytest.fixture(autouse=True, scope="module")
def _cleanup_temporary_directory():
    yield
    # teardown
    _stack.close()


@pytest.mark.golden_data(
    path=Resource.getPath("inputs/util_test"), short_name="test_gdp", date=date.today().isoformat()
)
def test_goldenDataFilePath(goldenDataFilePath):
    assert (
        goldenDataFilePath == Path(Resource.getPath("inputs/util_test")) / f"test_gdp_{date.today().isoformat()}.json"
    )


def test_isPrimitive():
    assert not _isPrimitive(ApproxAnyNested)  # arbitrary compound object
    assert not _isPrimitive((1, 2, 3))
    assert not _isPrimitive([4, 5, 6])
    assert not _isPrimitive({1: "one", 2: "two", 3: "three"})
    assert _isPrimitive(1)
    assert _isPrimitive(1.2345)
    assert _isPrimitive("string")
    assert _isPrimitive(True)
    assert _isPrimitive(None)


def test_flattenObject():
    cls = ApproxAnyNested
    assert cls._flattenObject({1: "one", 2: "two", 3: "three"}) == [1, "one", 2, "two", 3, "three"]
    assert cls._flattenObject(("45", {1: "one", 2: "two", 3: "three"})) == ["45", 1, "one", 2, "two", 3, "three"]
    assert cls._flattenObject({1: [1, 2, 3], 2: "two"}) == [1, 1, 2, 3, 2, "two"]


def test_flattenObject_circular_ref():
    cls = ApproxAnyNested
    dict_with_circ = {1: "one", 2: "two", 3: "three"}
    dict_with_circ[4] = dict_with_circ
    assert cls._flattenObject(dict_with_circ) == [1, "one", 2, "two", 3, "three", 4]


@pytest.mark.golden_data(path=_golden_data_base, short_name="test_gd_keys", date=date.today().isoformat())
def test_goldenData_keys(goldenData):
    _, k = goldenData()
    assert k == "1"
    for n in range(10):
        _, k = goldenData()
    assert k == "11"


@pytest.mark.golden_data(path=Resource.getPath("inputs/util_test"), short_name="test_gd_IO", date="2024-04-17")
def test_goldenData_save(goldenData):  # noqa: ARG001
    # TODO: I'm not sure how to test this step: the write of data occurs at teardown,
    # which occurs after the test execution completes.

    # Generate data for the remaining tests:

    # dict_, k = goldenData()
    # obj1 = ["45", {"1": 1.0, "2": 2.0, "3": "three"}]
    # dict_[k] = obj1
    # obj2 = ["90", {"6": 6.0, "7": 7.0, "8": "eight"}]
    # dict_, k = goldenData()
    # dict_[k] = obj2

    pass


@pytest.mark.golden_data(path=Resource.getPath("inputs/util_test"), short_name="test_gd_IO", date="2024-04-17")
def test_goldenData_load(goldenData):
    obj1 = ["45", {"1": 1.0, "2": 2.0, "3": "three"}]
    obj2 = ["90", {"6": 6.0, "7": 7.0, "8": "eight"}]
    dict_, k = goldenData()
    assert dict_[k] == obj1
    dict_, k = goldenData()
    assert dict_[k] == obj2


@pytest.mark.golden_data(path=Resource.getPath("inputs/util_test"), short_name="test_gd_IO", date="2024-04-17")
def test_assertMatchToGoldenData_matches(goldenData):
    obj1 = ["45", {"1": 1.000001, "2": 2.000001, "3": "three"}]
    obj2 = ["90", {"6": 6.001, "7": 7.001, "8": "eight"}]
    assertMatchToGoldenData(obj1, goldenData, 1.0e-6)
    assertMatchToGoldenData(obj2, goldenData, 1.0e-3)


@pytest.mark.golden_data(path=Resource.getPath("inputs/util_test"), short_name="test_gd_IO", date="2024-04-17")
def test_assertMatchToGoldenData_fails(goldenData):
    obj1 = ["45", {"1": 1.000001, "2": 2.000001, "3": "three"}]
    obj2 = ["90", {"6": 6.001, "7": 7.001, "8": "eight"}]  # noqa: F841
    with pytest.raises(AssertionError):
        assertMatchToGoldenData(obj1, goldenData, 1.0e-7)
