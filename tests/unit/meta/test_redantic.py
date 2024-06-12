import os
from typing import List
from unittest import TestCase

import pytest
from pydantic import BaseModel
from snapred.meta.Config import Resource
from snapred.meta.redantic import (
    list_from_raw,
    list_to_raw,
    list_to_raw_pretty,
    parse_file_as,
    parse_obj_as,
    parse_raw_as,
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

    def test_parse_raw_as(self):
        assert parse_raw_as(ModelTest, self.model.model_dump_json()) == self.model

    def test_parse_raw_as_list(self):
        assert parse_raw_as(List[ModelTest], list_to_raw(self.modelList)) == self.modelList

    def test_parse_obj_as(self):
        assert parse_obj_as(ModelTest, self.model.model_dump()) == self.model

    def test_parse_obj_as_list(self):
        assert parse_obj_as(List[ModelTest], self.modelList) == self.modelList

    def test_parse_file_as(self):
        assert parse_file_as(List[ModelTest], Resource.getPath("outputs/meta/redantic/list.json")) == self.modelList

    def test_list_to_raw(self):
        assert list_to_raw(self.modelList) == Resource.read("outputs/meta/redantic/list.json").strip()

    def test_to_raw_pretty(self):
        assert list_to_raw_pretty(self.modelList) == Resource.read("outputs/meta/redantic/pretty_list.json").strip()

    def test_list_from_raw(self):
        src = Resource.read("outputs/meta/redantic/list.json").strip()
        expected = self.modelList
        actual = list_from_raw(List[ModelTest], src)
        assert actual == expected

    def test_list_from_raw_bad_type(self):
        src = Resource.read("outputs/meta/redantic/list.json").strip()
        with pytest.raises(TypeError, match=r"target type must derive from \'List\[BaseModel\]\'"):
            list_from_raw(List[int], src)
        with pytest.raises(TypeError, match=r"target type must derive from \'List\[BaseModel\]\'"):
            list_from_raw(BaseModel, src)

    def test_write_model(self):
        path = Resource.getPath("test.json")
        self.files.append(path)
        write_model(self.model, path)
        assert Resource.read("test.json") == Resource.read("outputs/meta/redantic/model.json").strip()

    def test_write_model_pretty(self):
        path = Resource.getPath("test.json")
        self.files.append(path)
        write_model_pretty(self.model, path)
        assert Resource.read("test.json") == Resource.read("outputs/meta/redantic/pretty_model.json").strip()

    def test_write_model_list(self):
        path = Resource.getPath("test.json")
        self.files.append(path)
        write_model_list(self.modelList, path)
        assert Resource.read("test.json") == Resource.read("outputs/meta/redantic/list.json").strip()

    def test_write_model_list_pretty(self):
        path = Resource.getPath("test.json")
        self.files.append(path)
        write_model_list_pretty(self.modelList, path)
        assert Resource.read("test.json") == Resource.read("outputs/meta/redantic/pretty_list.json").strip()
