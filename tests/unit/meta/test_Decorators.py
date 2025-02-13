import json
from typing import List
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel
from qtpy.QtWidgets import QWidget

from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.backend.error.RecoverableException import RecoverableException
from snapred.backend.error.StateValidationException import StateValidationException
from snapred.meta.decorators._Resettable import Resettable
from snapred.meta.decorators.Builder import Builder
from snapred.meta.decorators.EntryExitLogger import EntryExitLogger
from snapred.meta.decorators.ExceptionHandler import ExceptionHandler
from snapred.meta.decorators.FromString import FromString


class Tester:
    @FromString
    def assertIsModel(self, model: SNAPRequest):
        type(model)
        assert issubclass(type(model), BaseModel)

    @FromString
    def assertIsListOfModel(self, listOfModel: List[SNAPRequest]):
        assert isinstance(listOfModel, List)
        self.assertIsModel(listOfModel[0])


@Builder
class Apple(BaseModel):
    color: str
    size: int


def test_FromStringOnBaseModel():
    tester = Tester()
    tester.assertIsModel(SNAPRequest(path="test"))


def test_FromStringOnString():
    tester = Tester()
    tester.assertIsModel(SNAPRequest(path="test").json())


def test_FromStringOnListOfString():
    tester = Tester()
    tester.assertIsListOfModel([SNAPRequest(path="test")])


def test_FromStringOnListOfBaseModel():
    tester = Tester()
    tester.assertIsListOfModel(json.dumps([SNAPRequest(path="test").dict()]))


def generateMockExceptionWithTraceback(exceptionType: type, msg: str = "Test error message"):
    try:
        raise exceptionType(msg)
    except exceptionType as e:  # noqa: BLE001
        return e


def test_stateValidationExceptionNoWritePermissions():  # noqa: ARG001
    # Create an exception with a real traceback
    mock_exception = generateMockExceptionWithTraceback(PermissionError)

    with pytest.raises(
        StateValidationException, match=r"The following error occurred:.*\n\nPlease contact your IS or CIS\."
    ):
        raise StateValidationException(mock_exception)


def test_stateValidationExceptionFileDoesNotExist():  # noqa: ARG001
    # Create an exception with a real traceback
    mock_exception = generateMockExceptionWithTraceback(FileNotFoundError)

    with pytest.raises(
        StateValidationException, match=r"The following error occurred:.*\n\nPlease contact your IS or CIS\."
    ):
        raise StateValidationException(mock_exception)


@patch("snapred.backend.error.StateValidationException.logger")
def test_stateValidationExceptionWithInvalidState(mockLogger):  # noqa: ARG001
    # Create an exception with a real traceback
    testMessage = "Here I will tell you, the end user, the reason why the state is invalid."
    mock_exception = generateMockExceptionWithTraceback(RuntimeError, testMessage)

    with pytest.raises(
        StateValidationException, match=r"Instrument State for given Run Number is invalid! \(See logs for details\.\)"
    ):
        raise StateValidationException(mock_exception)

    mockLogger.error.assert_called_once_with(testMessage)


@ExceptionHandler(StateValidationException)
def throwsStateException():
    raise RuntimeError("I love exceptions!!! Ah ha ha!")


def test_stateExceptionHandler():
    with pytest.raises(StateValidationException):
        throwsStateException()


def test_recoverableExceptionKwargs():
    exceptionString = "State uninitialized"
    with pytest.raises(RecoverableException, match=exceptionString):
        raise RecoverableException.stateUninitialized("57514", True)


def throwsContinueWarning():
    raise ContinueWarning("This is a warning.  Heed it.", ContinueWarning.Type.MISSING_DIFFRACTION_CALIBRATION)


def test_continueWarningHandler():
    with pytest.raises(ContinueWarning):
        throwsContinueWarning()


def test_builder():
    apple = Apple.builder().color("red").size(5).build()
    assert apple.color == "red"
    assert apple.size == 5


def test_memberIncorrect():
    with pytest.raises(RuntimeError):
        Apple.builder().color("red").size(5).seeds(2).build()


@Resettable
class BasicWidget(QWidget):
    def __init__(self, text, parent=None):
        super(BasicWidget, self).__init__(parent)
        self.text = text

    def getText(self):
        return self.text

    def setText(self, text):
        self.text = text


@pytest.mark.ui
def test_resettable(qtbot):
    parent = QWidget()
    qtbot.addWidget(parent)
    widget = BasicWidget("hello", parent=parent)
    widget.setText("goodbye")
    assert widget.getText() == "goodbye"
    widget.reset()
    assert widget.getText() == "hello"


def test_entryExitLogger():
    mockLogger = MagicMock()

    @EntryExitLogger(mockLogger)
    def testFunc():
        print("in testFunc")

    testFunc()
    assert mockLogger.debug.call_count == 2
    assert mockLogger.debug.call_args_list[0][0][0] == "Entering testFunc"
    assert mockLogger.debug.call_args_list[1][0][0] == "Exiting testFunc"
