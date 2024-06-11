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


@patch("snapred.backend.error.StateValidationException.Config")
def test_stateValidationExceptionWritePerms(mockConfig):
    exceptionPath = "SNS/SNAP/expected/path/somefile.txt"
    exceptionString = f"Error accessing {exceptionPath}"
    mockConfig.__getitem__.return_value = exceptionPath
    with pytest.raises(StateValidationException, match="You don't have permission to write to analysis directory: "):
        raise StateValidationException(RuntimeError(exceptionString))


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
