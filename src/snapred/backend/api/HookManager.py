from typing import Dict, List

from snapred.backend.dao.Hook import Hook
from snapred.backend.log.logger import snapredLogger
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


@Singleton
class HookManager:
    """
    Registers, manages, and executes Callables through a given request.
    """

    def __init__(self):
        self.reset()

    def reset(self):
        """
        Reset the HookManager, clearing all registered hooks.
        """
        self.hooks: Dict[str, List[Hook]] = {}
        self.executedHooks: Dict[str, int] = {}

    def allHooksExecuted(self) -> bool:
        return all(self.executedHooks.get(hookName, 0) == len(hookList) for hookName, hookList in self.hooks.items())

    def validateAllHooksExecuted(self):
        """
        Validate that all hooks have been executed.

        :raises ValueError: If not all hooks have been executed.
        """
        logger.debug(f"Validating all hooks executed: {self.executedHooks}")

        if not self.allHooksExecuted():
            raise ValueError(
                "Not all hooks were executed. "
                + f"Executed: {self.executedHooks}, "
                + f"Registered: {self.hooks.keys()}"
            )

    def register(self, hooks: Dict[str, List[Hook]]):
        """
        Register a set of hooks.

        :param hooks: A dictionary where keys are hook names and values are lists of callables.
        """
        if hooks is None:
            return  # if no hooks are provided, just return

        for hookName, hookList in hooks.items():
            if len(hookList) == 0:
                raise ValueError(f"Hook '{hookName}' must have at least one callable registered.")
            for hook in hookList:
                func, funcKwargs = hook.func, hook.kwargs  # unpack the hook from the package

                numArgs = func.__code__.co_argcount
                if numArgs != 1 + len(funcKwargs):
                    raise TypeError(
                        f"Hook '{hookName}' must be a callable with one argument"
                        + " (the instance in which the callable is executed) plus"
                        + " additional arguments from HookArgs."
                    )
            self.hooks[hookName] = hookList

    def execute(self, hookName: str, instance: object):
        """
        Execute a registered hook with the given instance.

        :param hookName: The name of the hook to execute.
        :param instance: The instance to pass to the hook.
        :raises KeyError: If the hook is not registered.
        """

        if hookName in self.hooks:
            if self.executedHooks.get(hookName, 0) >= len(self.hooks[hookName]):
                raise KeyError(f"Insufficient number of hooks registered for '{hookName}'.")
            hook = self.hooks[hookName][self.executedHooks.get(hookName, 0)]
            result = hook.func(instance, **hook.kwargs)
            self.executedHooks[hookName] = self.executedHooks.get(hookName, 0) + 1
            return result
        else:
            # just pass if user did not provide optional hook
            return None
