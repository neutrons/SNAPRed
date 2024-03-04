import json
from typing import List
from unittest.mock import patch

import pytest
from pydantic import BaseModel
from qtpy.QtWidgets import QWidget
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.error.RecoverableException import RecoverableException
from snapred.backend.error.StateValidationException import StateValidationException
from snapred.meta.decorators._Resettable import Resettable
from snapred.meta.decorators.Builder import Builder
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


@ExceptionHandler(StateValidationException)
def throwsStateException():
    raise RuntimeError("I love exceptions!!! Ah ha ha!")


@ExceptionHandler(StateValidationException)
def throwsStateMissingException():
    raise PermissionError("[Errno 13] Permission denied: /SNS/SNAP/shared/Calibration/Powder/6bbb60d63372c34b/")


def test_stateExceptionHandler():
    try:
        throwsStateException()
        pytest.fail("should have thrown an exception")
    except StateValidationException:
        assert True


def test_stateMissingExceptionHandler():
    try:
        throwsStateMissingException()
        pytest.fail("state missing")
    except StateValidationException:
        assert True


@ExceptionHandler(RecoverableException, "state")
def throwsRecoverableException():
    raise RuntimeError("'NoneType' object has no attribute 'instrumentState'")


def test_recoverableExceptionHandler():
    try:
        throwsRecoverableException()
        pytest.fail("should have thrown an exception")
    except RecoverableException:
        assert True


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
