from snapred.meta.decorators.Singleton import Singleton


@Singleton
class HookManager:
    """
    Registers, manages, and executes Callables though a given request.
    """

    def __init__(self):
        self.reset()

    def reset(self):
        """
        Reset the HookManager, clearing all registered hooks.
        """
        self.hooks = {}
        self.executed_hooks = set()

    def allHooksExecuted(self) -> bool:
        return len(self.hooks) == len(self.executed_hooks)

    def register(self, hooks: dict[str, callable]):
        """
        Register a set of hooks.

        :param hooks: A dictionary where keys are hook names and values are lists of callables.
        """
        if hooks is None:
            return  # if no hooks are provided, just return

        for hookName, hook in hooks.items():
            # assert that the hook is a callable with one argument
            if not callable(hook):
                raise TypeError(f"Hook '{hookName}' must be a callable.")
            numArgs = hook.__code__.co_argcount
            if numArgs != 1:
                raise TypeError(
                    f"Hook '{hookName}' must be a callable with one argument,"
                    + " the instance in which the callable is executed."
                )
            self.hooks[hookName] = hook

    def execute(self, hookName: str, instance: object):
        """
        Execute a registered hook with the given instance.

        :param hookName: The name of the hook to execute.
        :param instance: The instance to pass to the hook.
        :raises KeyError: If the hook is not registered.
        """
        if hookName in self.hooks:
            return self.hooks[hookName](instance)
        else:
            # just pass if user did not provide optional hook
            return None
