from functools import lru_cache
from typing import Any, Type


class CallbackMeta(type):
    """
    Metaclass that automatically generates forwarding magic methods for Callback classes.
    
    This metaclass intercepts class creation and automatically adds magic methods
    that forward operations to the wrapped value. This eliminates the need for
    manually defining each magic method or dynamically adding them after class creation.
    """
    
    # Magic methods that should be forwarded to the wrapped value
    # This comprehensive list includes arithmetic, comparison, container, and conversion operations
    _FORWARDED_MAGIC_METHODS = {
        # Arithmetic operations
        '__add__', '__sub__', '__mul__', '__truediv__', '__floordiv__', '__mod__',
        '__pow__', '__lshift__', '__rshift__', '__and__', '__xor__', '__or__',
        '__radd__', '__rsub__', '__rmul__', '__rtruediv__', '__rfloordiv__', '__rmod__',
        '__rpow__', '__rlshift__', '__rrshift__', '__rand__', '__rxor__', '__ror__',
        '__iadd__', '__isub__', '__imul__', '__itruediv__', '__ifloordiv__', '__imod__',
        '__ipow__', '__ilshift__', '__irshift__', '__iand__', '__ixor__', '__ior__',
        '__neg__', '__pos__', '__abs__', '__invert__', '__round__', '__floor__', '__ceil__',
        
        # Comparison operations
        '__lt__', '__le__', '__eq__', '__ne__', '__gt__', '__ge__',
        
        # Container operations (for lists, dicts, etc.)
        '__len__', '__getitem__', '__setitem__', '__delitem__', '__iter__', '__next__',
        '__contains__', '__reversed__', '__missing__',
        
        # Conversion operations (note: `__str__` and `__repr__` must NOT be forwarded here)
        '__int__', '__float__', '__bool__', '__hash__', '__index__',
        '__complex__', '__bytes__', '__format__',
        
        # Context managers
        '__enter__', '__exit__',
        
        # Callable
        '__call__',
        
        # Pickling/copying
        '__reduce__', '__reduce_ex__', '__copy__', '__deepcopy__',
        
        # Async operations
        '__await__', '__aiter__', '__anext__', '__aenter__', '__aexit__',
    }
    
    def __new__(mcs, name: str, bases: tuple, namespace: dict, **_kwargs) -> type:
        """
        Create a new Callback class with auto-generated magic methods.
        
        Args:
            name: The class name
            bases: Base classes
            namespace: Class namespace (attributes and methods)
            **kwargs: Additional keyword arguments (including _wrapped_type)
        
        Returns:
            The newly created class
        """
        # Generate forwarding magic methods
        for method_name in mcs._FORWARDED_MAGIC_METHODS:
            if method_name not in namespace:  # Don't override explicitly defined methods
                namespace[method_name] = mcs._make_forwarder(method_name)
        
        return super().__new__(mcs, name, bases, namespace)
    
    @staticmethod
    def _make_forwarder(method_name: str):
        """
        Create a magic method that forwards to the wrapped value.
        
        Args:
            method_name: The name of the magic method to forward
        
        Returns:
            A function that forwards the method call to _value
        """
        def forwarder(self, *args, **kwargs):
            if not self._set:
                raise AttributeError(f"Callback method '{method_name}' called before value is set")
            # Use object.__getattribute__ to avoid recursion through __getattr__
            value = object.__getattribute__(self, '_value')
            method_impl = getattr(value, method_name)
            return method_impl(*args, **kwargs)
        
        forwarder.__name__ = method_name
        forwarder.__doc__ = f"Forward {method_name} to the wrapped value"
        return forwarder


class Callback(metaclass=CallbackMeta):
    """
    Callback wrapper that defers access to a value until it's populated.
    
    This class uses a metaclass to automatically forward magic methods to the
    wrapped value, enabling transparent operations on primitive types and objects.
    
    Attributes:
        _wrapped_type: The type being wrapped (set by the factory function)
        _set: Whether the callback has been populated with a value
        _value: The wrapped value (None if not populated)
        _ignore: Set of attribute names that should not be forwarded
    """
    
    # These attributes are handled directly by the Callback class
    # and should not be forwarded to _value
    _ignore = {
        "_ignore", "update", "_set", "get", "__class__", "_value", 
        "__new__", "__init__", "__getattr__", "__getattribute__", 
        "__setattr__", "__subclasscheck__", "__instancecheck__",
        "__repr__", "__str__"
    }
    
    def __init__(self):
        """Initialize an unpopulated callback."""
        self._set = False
        self._value = None
    
    def update(self, value: Any) -> None:
        """
        Populate the callback with a value.
        
        Args:
            value: The value to wrap
        """
        self._set = True
        self._value = value
    
    def get(self) -> Any:
        """
        Get the wrapped value, raising an error if not populated.
        
        Returns:
            The wrapped value
        
        Raises:
            AttributeError: If the callback has not been populated
        """
        if not self._set:
            raise AttributeError("Callback not Populated")
        return self._value
    
    def __getattr__(self, name: str) -> Any:
        """
        Forward attribute access to the wrapped value.
        This is called for non-magic methods and attributes.
        
        Args:
            name: The attribute name
        
        Returns:
            The attribute value from the wrapped object
        
        Raises:
            AttributeError: If the callback is not populated
        """
        if name in self._ignore:
            # Use object's implementation for ignored attributes
            return object.__getattribute__(self, name)
        
        if not self._set:
            raise AttributeError("Callback not Populated")
        
        # Forward to the wrapped value
        return getattr(self._value, name)
    
    def __setattr__(self, name: str, value: Any) -> None:
        """
        Control attribute setting to protect internal attributes.
        
        Args:
            name: The attribute name
            value: The attribute value
        """
        # Allow setting internal attributes during initialization
        if name in {'_ignore', '_set', '_value'} or name.startswith('_'):
            object.__setattr__(self, name, value)
        elif hasattr(self, '_set') and self._set and hasattr(self, '_value'):
            # Forward to wrapped value if populated
            setattr(self._value, name, value)
        else:
            # If not populated, set on self
            object.__setattr__(self, name, value)
    
    def __repr__(self) -> str:
        """String representation of the callback."""
        wrapped_type = getattr(self, '_wrapped_type', None)
        type_name = wrapped_type.__name__ if wrapped_type else 'Unknown'
        
        if self._set:
            return f"Callback({type_name}, value={self._value!r})"
        else:
            return f"Callback({type_name}, not populated)"
    
    def __str__(self) -> str:
        """String conversion - forward to value if populated."""
        if self._set:
            return str(self._value)
        wrapped_type = getattr(self, '_wrapped_type', None)
        type_name = wrapped_type.__name__ if wrapped_type else 'Unknown'
        return f"<Callback({type_name})>"


@lru_cache(maxsize=None)
def _get_callback_class(clazz: Type) -> Type[Callback]:
    """
    Internal cached function that creates or retrieves a cached Callback class for the given type.
    
    This function uses LRU caching to ensure that only one Callback class is created
    per wrapped type, improving memory efficiency and enabling proper type checking.
    
    Args:
        clazz: The type to wrap (e.g., int, str, Workspace)
    
    Returns:
        A Callback class (not an instance) that wraps the given type
    """
    # Create a new Callback subclass with the wrapped type set
    # We use type() to dynamically create a subclass with _wrapped_type set
    return type(
        f'Callback[{clazz.__name__}]',
        (Callback,),
        {
            '_wrapped_type': clazz,
            '__module__': Callback.__module__,
        }
    )


def callback(clazz: Type) -> Callback:
    """
    Factory function that creates a new Callback instance for the given type.
    
    This function uses a cached class creation mechanism to ensure that only one
    Callback class is created per wrapped type, but returns a new instance each time.
    
    Args:
        clazz: The type to wrap (e.g., int, str, Workspace)
    
    Returns:
        A new instance of a Callback class that wraps the given type
    
    Example:
        >>> count = callback(int)
        >>> count.update(5)
        >>> result = count + 10  # Works! Returns 15
        >>> ws = callback(Workspace)
        >>> ws.update(some_workspace)
        >>> name = ws.name()  # Forwarded to underlying workspace
        >>> 
        >>> # Each call returns a new instance, but same class
        >>> cb1 = callback(int)
        >>> cb2 = callback(int)
        >>> cb1 is not cb2  # Different instances
        >>> cb1.__class__ is cb2.__class__  # Same class
    """
    # Get the cached class and instantiate it
    CallbackSubclass = _get_callback_class(clazz)
    return CallbackSubclass()
