import signal
from functools import wraps

from PyQt5.QtCore import QObject, pyqtSignal


def Resettable(orig_cls):
    orig_init = orig_cls.__init__

    class _ResetFriend(QObject):
        """
        This class is necessary because of how pyqtSignals work, they need to be declared in a class.
        """

        signalReset = pyqtSignal(object)

        def connect(self, func):
            self.signalReset.connect(func)

        def emit(self, obj):
            self.signalReset.emit(obj)

    def _reset(self):
        classDict = orig_cls(*self._args, **self._kwargs).__dict__
        self.__dict__.clear()
        self.__dict__.update(classDict)

    @wraps(orig_cls.__init__)
    def __init__(self, *args, **kwargs):
        self._signalReset = _ResetFriend()
        self._signalReset.connect(_reset)
        self._args = args
        self._kwargs = kwargs
        orig_init(self, *args, **kwargs)

    def reset(self):
        self._signalReset.emit(self)

    orig_cls.__init__ = __init__
    orig_cls.reset = reset
    return orig_cls
