from functools import wraps
import inspect

from unittest import mock

def mock_instance_methods(orig_cls):
    """ A class decorator to mock all of the methods of a class instance, with the default `side_effect`
          being the original methods:
        -- As instance methods (rather than class methods) are mocked, each instance is then associated
           with distinct mocks;
        -- In most cases, `mock.Mock(wraps=<class to wrap>)` should be used as an alternative to this decorator;
           however, this decorator allows already-derived classes to be mocked, producing essentially the same behavior. 
    """
    
    orig_init = orig_cls.__init__
    
    @wraps(orig_cls.__init__)
    def __init__(self, *args, **kwargs):
        orig_init(self, *args, **kwargs)
        
        # Each method still does what it originally did,
        #   but this change allows us to do things like `<instance>.<method>.assert_called_once_with(...)`
        #     without needing to build mocks for all of the methods "by hand".
        for method in inspect.getmembers(self, predicate=inspect.ismethod):
            if method[0].startswith("__"):
                # For the moment, exclude 'builtin' methods (due to possible recursion issues).
                continue
            setattr(self, method[0], mock.Mock(side_effect=method[1]))
    
    orig_cls.__init__ = __init__
    return orig_cls
