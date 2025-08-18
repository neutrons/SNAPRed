import pytest

from snapred.backend.dao.indexing.IndexEntry import IndexEntry
from snapred.meta.Time import isoFromTimestamp

testData = {
    "runNumber": "1234",
    "useLiteMode": False,
    "version": 1,
    "appliesTo": "1234",
    "comments": "test",
    "author": "test",
    "timestamp": 1234.1234,
}

testDataWithIsoTimestamp = {
    "runNumber": "1234",
    "useLiteMode": False,
    "version": 1,
    "appliesTo": "1234",
    "comments": "test",
    "author": "test",
    "timestamp": isoFromTimestamp(1234.1234),
}


def test_persistTimestampAsIsoFormat():
    """
    Test that the timestamp is persisted as an ISO format string.
    """
    # Arrange
    entry = IndexEntry(**testData)
    entryDict = entry.model_dump()
    assert isinstance(entryDict["timestamp"], str)
    assert entryDict["timestamp"] == isoFromTimestamp(1234.1234)


def test_parseTimestampAsFloat():
    """
    Test that the timestamp is parsed as a float.
    """
    # Arrange
    entry = IndexEntry(**testDataWithIsoTimestamp)
    assert isinstance(entry.timestamp, float)
    assert round(entry.timestamp, ndigits=4) == 1234.1234


def test_correctData():
    """
    Test that the correct data is returned when the input is correct.
    """
    # Arrange
    entry = IndexEntry(**testData)
    assert entry.appliesTo == testData["appliesTo"]


def test_appliesTo():
    """
    Test that the appliesTo property is correctly parsed.
    """
    symbols = [">=", "<=", "<", ">"]
    for symbol in symbols:
        # Arrange
        testDataSymbol = testData.copy()
        testDataSymbol["appliesTo"] = f"{symbol}1234"
        entry = IndexEntry(**testDataSymbol)
        assert symbol in entry.appliesTo


def test_appliesToMultiple():
    """
    Test that the appliesTo property is correctly parsed.
    """
    # Arrange
    testDataSymbol = testData.copy()
    testDataSymbol["appliesTo"] = ">=1234,<=4321"
    entry = IndexEntry(**testDataSymbol)
    assert "1234" in entry.appliesTo
    assert "4321" in entry.appliesTo


def test_appliesToFailsValidation():
    """
    Test that the appliesTo property fails validation when the input is incorrect.
    """
    # Arrange
    testDataSymbol = testData.copy()
    testDataSymbol["appliesTo"] = "1234a"

    with pytest.raises(ValueError, match="appliesTo must be in the format of"):
        IndexEntry(**testDataSymbol)


def test_appliesToMultipleFailsValidation():
    """
    Test that the appliesTo property fails validation when the input is incorrect.
    """
    # Arrange
    testDataSymbol = testData.copy()
    testDataSymbol["appliesTo"] = ">=1234,<=4321,1234a"

    with pytest.raises(ValueError, match="appliesTo must be in the format of"):
        IndexEntry(**testDataSymbol)


def test_appliesToInvalidSymbol():
    """
    Test that the appliesTo property fails validation when the symbol is invalid.
    """
    # Arrange
    testDataSymbol = testData.copy()
    testDataSymbol["appliesTo"] = "*1234"

    with pytest.raises(ValueError, match="appliesTo must be in the format of"):
        IndexEntry(**testDataSymbol)
