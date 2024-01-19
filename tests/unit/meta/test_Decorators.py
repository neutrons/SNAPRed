import json
from typing import List

import pytest
from pydantic import BaseModel
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.error.StateValidationException import StateValidationException
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


def test_stateExceptionHandler():
    try:
        throwsStateException()
        pytest.fail("should have thrown an exception")
    except StateValidationException:
        assert True


def test_builder():
    apple = Apple.builder().color("red").size(5).build()
    assert apple.color == "red"
    assert apple.size == 5


def test_memberIncorrect():
    with pytest.raises(RuntimeError):
        Apple.builder().color("red").size(5).seeds(2).build()
