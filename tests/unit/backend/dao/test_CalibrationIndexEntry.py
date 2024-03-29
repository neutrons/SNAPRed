import pytest
from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry

testData = {
    "runNumber": "1234",
    "version": "1.0",
    "appliesTo": "1234",
    "comments": "test",
    "author": "test",
    "timestamp": 1234,
}


def test_correctData():
    """
    Test that the correct data is returned when the input is correct.
    """
    # Arrange
    entry = CalibrationIndexEntry(**testData)
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
        entry = CalibrationIndexEntry(**testDataSymbol)
        assert symbol in entry.appliesTo


def test_appliesToFailsValidation():
    """
    Test that the appliesTo property fails validation when the input is incorrect.
    """
    # Arrange
    testDataSymbol = testData.copy()
    testDataSymbol["appliesTo"] = "1234a"

    with pytest.raises(ValueError, match="appliesTo must be in the format of"):
        CalibrationIndexEntry(**testDataSymbol)


def test_appliesToInvalidSymbol():
    """
    Test that the appliesTo property fails validation when the symbol is invalid.
    """
    # Arrange
    testDataSymbol = testData.copy()
    testDataSymbol["appliesTo"] = "*1234"

    with pytest.raises(ValueError, match="appliesTo must be in the format of"):
        CalibrationIndexEntry(**testDataSymbol)
