from functools import wraps

from qtpy.QtWidgets import QHBoxLayout, QWidget


def Resettable(orig_cls):
    orig_new = orig_cls.__new__

    @wraps(orig_cls.__new__)
    def __new__(cls, *args, **kwargs):  # noqa: ARG001
        return Wrapper(*args, **kwargs)

    class Wrapper(QWidget):
        def __init__(self, *args, **kwargs):
            super().__init__(kwargs.get("parent", None))
            self._args = args
            self._kwargs = kwargs
            orig_cls.__new__ = orig_new
            
            
            # IMPORTANT: do not hide the "layout" method!
            self._layout = QHBoxLayout()
            self._layout.addWidget(orig_cls(*args, **kwargs))
            
            orig_cls.__new__ = __new__
            self.setLayout(self._layout)

        def reset(self):
            widget = self._layout.itemAt(0).widget()
            self._layout.removeWidget(widget)
            widget.deleteLater()
            orig_cls.__new__ = orig_new
            self._layout.addWidget(orig_cls(*self._args, **self._kwargs))
            orig_cls.__new__ = __new__

        def __getattr__(self, name):
            return getattr(self._layout.itemAt(0).widget(), name)

    orig_cls.__new__ = __new__
    return orig_cls
