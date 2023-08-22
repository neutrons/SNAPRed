import os
from typing import List
from unittest import TestCase

from pydantic import BaseModel
from snapred.meta.Config import Resource
from snapred.meta.redantic import (
    list_to_raw,
    list_to_raw_pretty,
    write_model,
    write_model_list,
    write_model_list_pretty,
    write_model_pretty,
)


class ModelTest(BaseModel):
    id: int  # noqa: A003
    name: str
    tickets: List[int] = []


class TestRedantic(TestCase):
    files = []
    modelList = [
        ModelTest(id=1, name="name1", tickets=[1, 232, 342]),
        ModelTest(id=2, name="name2", tickets=[2, 233, 343]),
    ]
    model = ModelTest(id=1, name="name1", tickets=[1, 232, 342])

    @classmethod
    def tearDownClass(cls):
        # create unique set of files from cls.files
        files = set(cls.files)
        # remove files
        for file in files:
            os.remove(file)
        cls.files = []

    def test_list_to_raw(self):
        assert list_to_raw(self.modelList) == Resource.read("outputs/meta/redantic/list.json")

    def test_to_raw_pretty(self):
        assert list_to_raw_pretty(self.modelList) == Resource.read("outputs/meta/redantic/pretty_list.json")

    def test_write_model(self):
        path = Resource.getPath("test.json")
        self.files.append(path)
        write_model(self.model, path)
        assert Resource.read("test.json") == Resource.read("outputs/meta/redantic/model.json")

    def test_write_model_pretty(self):
        path = Resource.getPath("test.json")
        self.files.append(path)
        write_model_pretty(self.model, path)
        assert Resource.read("test.json") == Resource.read("outputs/meta/redantic/pretty_model.json")

    def test_write_model_list(self):
        path = Resource.getPath("test.json")
        self.files.append(path)
        write_model_list(self.modelList, path)
        assert Resource.read("test.json") == Resource.read("outputs/meta/redantic/list.json")

    def test_write_model_list_pretty(self):
        path = Resource.getPath("test.json")
        self.files.append(path)
        write_model_list_pretty(self.modelList, path)
        assert Resource.read("test.json") == Resource.read("outputs/meta/redantic/pretty_list.json")
