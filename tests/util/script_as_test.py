import os


# DECORATOR:
# * Mark any object so that pytest will not identify it as a test
def not_a_test(obj):
    obj.__test__ = False
    return obj


# DECORATOR:
# * Execute the body of a function, depending on whether pytest is running.
@not_a_test
def test_only_if(run_while_testing: bool):
    def _test_only(func):
        def _wrapper(*args, **kwargs):
            pytest_running = "PYTEST_CURRENT_TEST" in os.environ
            if (run_while_testing and pytest_running) or (not run_while_testing and not pytest_running):
                return func(*args, **kwargs)

        return _wrapper

    return _test_only


# DECORATOR:
# * Execute the body of a function only if pytest is running.
@not_a_test
def test_only(func):
    return test_only_if(True)(func)


# DECORATOR:
# * Execute the body of a function only if pytest is not running.
def script_only(func):
    return test_only_if(False)(func)


@script_only
def pause(msg=None):
    # Pause a script, generally to allow examination of workspaces:
    print(msg)
    # Mantid sets a break (i.e. CNTL-C) handler, which we don't want to bypass;
    #   this next is an OK alternative:
    while True:
        c = input('Type "c"<ENTER> to continue, or "x"<ENTER> to exit...')
        if c == "c":
            break
        if c == "x":
            assert False  # noqa: PT015
