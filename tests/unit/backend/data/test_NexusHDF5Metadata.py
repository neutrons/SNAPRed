import tempfile
from datetime import datetime
from enum import Enum
from pathlib import Path

import h5py
from util.script_as_test import not_a_test

from snapred.backend.data.NexusHDF5Metadata import NexusHDF5Metadata as n5m
from snapred.meta.Config import Resource

# IMPLEMENTATION NOTES:
# * The following reference was very useful during the design of this implementation
#    (and it provides the first test case):
#     see: https://stackoverflow.com/questions/12507206/how-to-completely-traverse-a-complex-dictionary-of-unknown-depth

dict_of_mixed = {
    "body": [
        {
            "declarations": [
                {
                    "id": {"name": "i", "type": "Identifier"},
                    "init": {"type": "Literal", "value": 2},
                    "type": "VariableDeclarator",
                }
            ],
            "kind": "var",
            "type": "VariableDeclaration",
        },
        {
            "declarations": [
                {
                    "id": {"name": "j", "type": "Identifier"},
                    "init": {"type": "Literal", "value": 4},
                    "type": "VariableDeclarator",
                }
            ],
            "kind": "var",
            "type": "VariableDeclaration",
        },
        {
            "declarations": [
                {
                    "id": {"name": "answer", "type": "Identifier"},
                    "init": {
                        "left": {"name": "i", "type": "Identifier"},
                        "operator": "*",
                        "right": {"name": "j", "type": "Identifier"},
                        "type": "BinaryExpression",
                    },
                    "type": "VariableDeclarator",
                }
            ],
            "kind": "var",
            "type": "VariableDeclaration",
        },
    ],
    "type": "Program",
}
dict_of_mixed_inputs = (dict_of_mixed, dict_of_mixed)

# dict of list, and list of dict:
dict_of_list = {"a": [1, 2, 3], "b": [{"one": 1, "two": 2}, {"three": 3, "four": 4}]}
dict_of_list_inputs = (dict_of_list, dict_of_list)

dict_of_list_of_list = {
    "a": [[1, 2, 3], [1, 2, 3], [1, 2, 3]],
    "b": [
        [{"one": 1, "two": 2}, {"three": 3, "four": 4}],
        [{"five": 5, "six": 6}, {"seven": 7, "eight": 8}],
    ],
}
dict_of_list_of_list_inputs = (dict_of_list_of_list, dict_of_list_of_list)

dict_of_dict = {"one": {"one": 1, "two": 2}, "two": {"three": 3, "four": 4}}
dict_of_dict_inputs = (dict_of_dict, dict_of_dict)

# terminal None are encoded as 'None':
dict_of_None = {"one": {"one": None, "two": 2}, "two": {"three": 3, "four": None}}
dict_of_None_inputs = (dict_of_None, dict_of_None)


# terminal _opaque_ (but derived from str) are encoded as str:
class StringDerived(str):
    def __str__(self):
        return self


dict_of_StringDerived = {
    "one": {"one": StringDerived("s_one"), "two": 2},
    "two": {"three": 3, "four": StringDerived("s_four")},
}
dict_of_StringDerived_reconstruct = {"one": {"one": "s_one", "two": 2}, "two": {"three": 3, "four": "s_four"}}
dict_of_StringDerived_inputs = (dict_of_StringDerived, dict_of_StringDerived_reconstruct)


class tags(str, Enum):
    ONE = "one"
    TWO = "two"
    THREE = "three"
    FOUR = "four"


# branch enum are encoded as <enum>.value, not str(<enum>)
#   * note that Pydantic will initialize <enum> from <enum>.value
dict_of_branch_enum = {
    "one": {tags.ONE: "one", tags.TWO: "two"},
    "two": [1, 2, 3],
    "three": [4, 5, 6],
    "four": {tags.THREE: "three"},
}
dict_of_branch_enum_reconstruct = {
    "one": {tags.ONE.value: "one", tags.TWO.value: "two"},
    "two": [1, 2, 3],
    "three": [4, 5, 6],
    "four": {tags.THREE.value: "three"},
}
dict_of_branch_enum_inputs = (dict_of_branch_enum, dict_of_branch_enum_reconstruct)

# terminal enum are encoded as <enum>.value, not str(<enum>)
dict_of_terminal_enum = {"one": {"one": tags.ONE, "two": tags.TWO}, "two": {"three": 3, "four": tags.FOUR}}
dict_of_terminal_enum_reconstruct = {
    "one": {"one": tags.ONE.value, "two": tags.TWO.value},
    "two": {"three": 3, "four": tags.FOUR.value},
}
dict_of_terminal_enum_inputs = (dict_of_terminal_enum, dict_of_terminal_enum_reconstruct)

dict_of_all_primitives = {
    "one": 1,
    "two_two": 2.2,
    "three": "three",
    "none": None,
    "now": datetime.fromisoformat("2024-05-15 17:29:28.600717"),
    "four": tags.FOUR,
}
dict_of_all_primitives_reconstruct = {
    "one": 1,
    "two_two": 2.2,
    "three": "three",
    "none": None,
    "now": str(dict_of_all_primitives["now"]),
    "four": tags.FOUR.value,
}
dict_of_all_primitives_inputs = (dict_of_all_primitives, dict_of_all_primitives_reconstruct)


@not_a_test
def test_traversal(inputs):
    paths = n5m._traversal(inputs[0])
    base = n5m._reconstruct(paths)
    assert base == inputs[1]


def test_mixed():
    test_traversal(dict_of_mixed_inputs)


def test_dict_of_list():
    test_traversal(dict_of_list_inputs)


def test_dict_of_list_of_list():
    test_traversal(dict_of_list_of_list_inputs)


def test_dict_of_dict():
    test_traversal(dict_of_dict_inputs)


def test_dict_of_None():
    test_traversal(dict_of_None_inputs)


def test_dict_of_StringDerived():
    test_traversal(dict_of_StringDerived_inputs)


def test_dict_of_branch_enum():
    test_traversal(dict_of_branch_enum_inputs)


def test_dict_of_terminal_enum():
    test_traversal(dict_of_terminal_enum_inputs)


def test_dict_of_all_primitives():
    test_traversal(dict_of_all_primitives_inputs)


def test_reconstruct_hdf5():
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        filePath = Path(tmpDir) / "test.hdf5"
        with h5py.File(filePath, "w") as h5:
            paths = list(n5m._traversal(dict_of_mixed))

            metadata = h5.create_group("/metadata")
            metadata.attrs["NX_class"] = "NXcollection"
            n5m._reconstruct_hdf5(metadata, paths)

        with h5py.File(filePath, "r") as h5:

            def convert_scalar(s):
                s_ = s[()]
                return s_ if not isinstance(s_, bytes) else s_.decode("utf8")

            paths = list(n5m._traversal(h5, convert_scalar=convert_scalar))

            for path in paths:
                print(path)

            base = n5m._reconstruct(paths)

            print("Resulting dict:")
            print(base)
            assert base["metadata"] == dict_of_mixed


def test_insertExtractMetadataGroup():
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        filePath = Path(tmpDir) / "test.hdf5"
        with h5py.File(filePath, "w") as h5:
            n5m.insertMetadataGroup(h5, dict_of_mixed)

        with h5py.File(filePath, "r") as h5:
            dict_ = n5m.extractMetadataGroup(h5)
            assert dict_ == dict_of_mixed
