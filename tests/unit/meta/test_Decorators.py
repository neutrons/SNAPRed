import json
import traceback
from typing import List
from unittest import mock
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


def generateMockExceptionWithTraceback():
    try:
        raise Exception("Test error message")
    except Exception as e:  # noqa: BLE001
        return e


@mock.patch("pathlib.Path.exists", return_value=True)
@mock.patch("os.access", return_value=False)
def test_stateValidationExceptionNoWritePermissions(mockExists, mockAccess):  # noqa: ARG001
    # Create an exception with a real traceback
    mock_exception = generateMockExceptionWithTraceback()

    # Mock the traceback to simulate a valid file path
    mockTb = [traceback.FrameSummary("nonExistentFile.txt", 42, "testFunction")]
    with patch("traceback.extract_tb", return_value=mockTb):
        try:
            raise StateValidationException(mock_exception)
        except StateValidationException as e:
            # Assert the error message is as expected
            expectedMessage = "You do not have write permissions: nonExistentFile.txt"
            assert e.message == expectedMessage  # noqa: PT017


@mock.patch("pathlib.Path.exists", return_value=False)
def test_stateValidationExceptionFileDoesNotExist(mockExists):  # noqa: ARG001
    # Create an exception with a real traceback
    mockException = generateMockExceptionWithTraceback()

    # Mock the traceback to simulate a valid file path
    mockTb = [traceback.FrameSummary("non_existent_file.txt", 42, "test_function")]
    with patch("traceback.extract_tb", return_value=mockTb):
        try:
            raise StateValidationException(mockException)
        except StateValidationException as e:
            # Assert the error message is as expected
            expectedMessage = "The file does not exist: non_existent_file.txt"
            assert e.message == expectedMessage  # noqa: PT017


@patch("snapred.backend.log.logger.snapredLogger")
@patch("snapred.meta.Config.Config", {"instrument.home": "/expected/path"})
def test_stateValidationExceptionWithPermissionIssue(mockLogger):  # noqa: ARG001
    exception_msg = "Instrument State for given Run Number is invalid! (see logs for details.)"
    try:
        raise StateValidationException(RuntimeError("Error accessing SNS/SNAP/expected/path/somefile.txt"))
    except StateValidationException as e:
        assert str(e) == exception_msg  # noqa: PT017


@patch("snapred.backend.log.logger.snapredLogger")
def test_stateValidationExceptionWithInvalidState(mockLogger):  # noqa: ARG001
    exception_msg = "Instrument State for given Run Number is invalid! (see logs for details.)"
    try:
        raise StateValidationException(RuntimeError("Random error message"))
    except StateValidationException as e:
        assert str(e) == exception_msg  # noqa: PT017


def test_stateValidationExceptionWritePerms():
    exception = Exception("Test Exception")

    # Mocking the checkFileAndPermissions method to simulate file existence and permission
    with mock.patch.object(StateValidationException, "_checkFileAndPermissions", return_value=(True, True)):
        # Creating a fake traceback
        try:
            raise exception
        except Exception as e:  # noqa: BLE001
            tb = e.__traceback__  # noqa: F841

        # Raising the exception with the mocked traceback
        with pytest.raises(StateValidationException) as excinfo:
            raise StateValidationException(exception)

        # Asserting that the error message is as expected
        assert "The following error occurred:Test Exception\n\nPlease contact your CIS." in str(excinfo.value)


@ExceptionHandler(StateValidationException)
def throwsStateException():
    raise RuntimeError("I love exceptions!!! Ah ha ha!")


def test_stateExceptionHandler():
    with pytest.raises(StateValidationException):
        throwsStateException()


@ExceptionHandler(RecoverableException, "state")
def throwsRecoverableException():
    raise RuntimeError("'NoneType' object has no attribute 'instrumentState'")


def test_recoverableExceptionHandler():
    with pytest.raises(RecoverableException):
        throwsRecoverableException()


def test_recoverableExceptionKwargs():
    exceptionPath = "SNS/SNAP/expected/path/somefile.txt"
    exceptionString = f"Error accessing {exceptionPath}"
    with pytest.raises(RecoverableException, match=exceptionString):
        raise RecoverableException(RuntimeError(exceptionString), exceptionString, extraInfo="some extra info")


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
