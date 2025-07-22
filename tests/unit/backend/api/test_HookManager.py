import unittest

import pytest

from snapred.backend.api.HookManager import HookManager
from snapred.backend.dao.Hook import Hook


class TestHookManager(unittest.TestCase):
    def test_readHook(self):
        hookDict = {"func": "test_hook_function", "kwargs": {"arg1": "value1", "arg2": "value2"}}
        hook = Hook(**hookDict)
        assert hook.func == "test_hook_function"

        def test_hook_function(arg1, arg2):
            return f"Hook executed with {arg1} and {arg2}"

        hook = Hook(func=test_hook_function, **hookDict["kwargs"])
        assert hook.func == test_hook_function
        outHookDict = hook.model_dump()
        assert outHookDict["func"] == test_hook_function.__qualname__

    def test_register(self):
        """Test registering a hook"""
        hookManager = HookManager()

        def cheeseHook(_, __):
            """A simple hook function for testing."""
            return "cheese"

        hook = Hook(func=cheeseHook, __=[])
        hooks = {"TestHook": [hook]}
        hookManager.register(hooks)

        assert "TestHook" in hookManager.hooks
        assert hookManager.hooks["TestHook"] == [hook]

        hookManager.reset()
        assert hookManager.hooks == {}
        assert hookManager.executedHooks == {}

    def test_registerExtraEmptyHook(self):
        """Test registering an empty hook"""
        hookManager = HookManager()

        # Registering an empty hook should not raise an error
        hooks = {"EmptyHook": []}
        with pytest.raises(ValueError, match="Hook 'EmptyHook' must have at least one callable registered"):
            hookManager.register(hooks)

    def test_registerArgCountMismatch(self):
        """Test registering a hook with an argument count mismatch"""
        hookManager = HookManager()

        def cheeseHook(_, __, ___):
            """A simple hook function for testing."""
            return "cheese"

        hook = Hook(func=cheeseHook, __=[])
        hooks = {"TestHook": [hook]}

        with pytest.raises(TypeError, match="Hook 'TestHook' must be a callable with one argument"):
            hookManager.register(hooks)

    def test_validateAllHooksExecuted_fail(self):
        """Test validating that all hooks have been executed"""
        hookManager = HookManager()

        def cheeseHook(_, __):
            """A simple hook function for testing."""
            return "cheese"

        hook = Hook(func=cheeseHook, __=[])
        hooks = {"TestHook": [hook]}
        hookManager.register(hooks)

        # Validate that all hooks have been executed
        with pytest.raises(ValueError, match="Not all hooks were executed"):
            hookManager.validateAllHooksExecuted()

    def test_validateAllHooksExecuted_success(self):
        """Test validating that all hooks have been executed"""
        hookManager = HookManager()

        def cheeseHook(_, __):
            """A simple hook function for testing."""
            return "cheese"

        hook = Hook(func=cheeseHook, __=[])
        hooks = {"TestHook": [hook]}
        hookManager.register(hooks)

        # Simulate executing the hook
        hookManager.executedHooks["TestHook"] = 1

        # Validate that all hooks have been executed
        hookManager.validateAllHooksExecuted()

    def test_executeHook(self):
        """Test executing a hook"""
        hookManager = HookManager()
        executed = False

        def cheeseHook(_, __):
            """A simple hook function for testing."""
            nonlocal executed
            executed = True
            return "cheese"

        hook = Hook(func=cheeseHook, __=[])
        hooks = {"TestHook": [hook]}
        hookManager.register(hooks)

        result = hookManager.execute("TestHook", self)
        assert result == "cheese"
        assert executed
        hookManager.validateAllHooksExecuted()
