import json

import pytest
from pydantic import BaseModel

from snapred.backend.dao.Hook import Hook, hookArgumentsOfType, typeName
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName

WORKSPACE_NAME = typeName(WorkspaceName)


def hookFunction(context, mask, masks, options, plain):  # noqa: ARG001
    pass


def makeHook() -> Hook:
    return Hook(
        func=hookFunction,
        mask=WorkspaceName("mask_ws"),
        masks=[WorkspaceName("a_ws"), "not_a_ws", (WorkspaceName("b_ws"),)],
        options={"inner/key": WorkspaceName("c_ws"), "n": 1},
        plain="plain",
    )


class HookHolder(BaseModel):
    hooks: dict[str, list[Hook]] | None = None


def test_typeName():
    assert WORKSPACE_NAME == "snapred.meta.mantid.WorkspaceNameGenerator.WorkspaceName"
    assert typeName(tuple) == "builtins.tuple"


def test_kwargs_unchanged():
    hook = makeHook()
    assert hook.kwargs["mask"] == "mask_ws"
    assert typeName(type(hook.kwargs["mask"])) == WORKSPACE_NAME
    assert hook.kwargs["plain"] == "plain"


def test_argumentTypes():
    assert makeHook().kwargTypes == {
        "/mask": WORKSPACE_NAME,
        "/masks/0": WORKSPACE_NAME,
        "/masks/2": "builtins.tuple",
        "/masks/2/0": WORKSPACE_NAME,
        # "/" in a key is escaped, as in a JSON pointer
        "/options/inner~1key": WORKSPACE_NAME,
    }


def test_argumentTypes_json_types_not_recorded():
    hook = Hook(func=hookFunction, a="x", b=1, c=1.5, d=True, e=None, f=[1, "y"], g={"h": 2})
    assert hook.kwargTypes == {}


def test_argumentsOfType():
    assert makeHook().argumentsOfType(WorkspaceName) == {
        "/mask": "mask_ws",
        "/masks/0": "a_ws",
        "/masks/2/0": "b_ws",
        "/options/inner~1key": "c_ws",
    }
    assert makeHook().argumentsOfType(int) == {}


def test_serialized_types():
    data = json.loads(makeHook().model_dump_json())
    assert data["func"] == "hookFunction"
    # the values are plain strings: their types are in "kwargTypes"
    assert data["kwargs"]["mask"] == "mask_ws"
    assert data["kwargTypes"]["/mask"] == WORKSPACE_NAME


@pytest.mark.parametrize(
    "load",
    [
        lambda data: Hook.model_validate(json.loads(data)),
        lambda data: Hook.model_validate_json(data),
        lambda data: Hook(**json.loads(data)),
    ],
)
def test_round_trip(load):
    hook = makeHook()
    loaded = load(hook.model_dump_json())
    # the arguments are not nested in a second "kwargs"
    assert loaded.kwargs == json.loads(hook.model_dump_json())["kwargs"]
    assert loaded.kwargTypes == hook.kwargTypes
    assert loaded.funcStr == "hookFunction"
    assert loaded.argumentsOfType(WorkspaceName) == hook.argumentsOfType(WorkspaceName)
    # a hook that was read back is a record: its function is not available
    with pytest.raises(TypeError, match="func no longer available"):
        _ = loaded.func


def test_round_trip_twice():
    once = Hook.model_validate_json(makeHook().model_dump_json())
    twice = Hook.model_validate_json(once.model_dump_json())
    assert twice.kwargs == once.kwargs
    assert twice.kwargTypes == once.kwargTypes


def test_round_trip_nested_in_model():
    holder = HookHolder(hooks={"PostPreprocessReductionRecipe": [makeHook(), makeHook()]})
    loaded = HookHolder.model_validate_json(holder.model_dump_json())
    hook = loaded.hooks["PostPreprocessReductionRecipe"][0]
    assert hook.kwargs["plain"] == "plain"
    assert hook.argumentsOfType(WorkspaceName)["/mask"] == "mask_ws"


def test_load_record_without_types():
    # a hook written before "kwargTypes" existed
    hook = Hook.model_validate({"func": "hookFunction", "kwargs": {"mask": "mask_ws"}})
    assert hook.kwargs == {"mask": "mask_ws"}
    assert hook.kwargTypes == {}


def test_load_hook_without_arguments():
    hook = Hook.model_validate_json(Hook(func=hookFunction).model_dump_json())
    assert hook.kwargs == {}
    assert hook.kwargTypes == {}


@pytest.mark.parametrize("name", ["kwargs", "kwargTypes"])
def test_reserved_argument_names(name):
    with pytest.raises(ValueError, match="cannot be named"):
        Hook(func=hookFunction, other=1, **{name: 1})


def test_hookArgumentsOfType():
    hooks = {"A": [makeHook()], "B": [Hook(func=hookFunction, mask=WorkspaceName("d_ws"))]}
    assert hookArgumentsOfType(hooks, WorkspaceName) == ["mask_ws", "a_ws", "b_ws", "c_ws", "d_ws"]
    assert hookArgumentsOfType(None, WorkspaceName) == []
    assert hookArgumentsOfType({}, WorkspaceName) == []
