from abc import ABC, abstractmethod

from qtpy.QtWidgets import QGridLayout, QWidget

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.ui.threading.worker_pool import WorkerPool
from snapred.ui.widget.LabeledCheckBox import LabeledCheckBox
from snapred.ui.widget.LabeledField import LabeledField
from snapred.ui.widget.LabeledToggle import LabeledToggle
from snapred.ui.widget.MultiSelectDropDown import MultiSelectDropDown
from snapred.ui.widget.SampleDropDown import SampleDropDown
from snapred.ui.widget.TrueFalseDropDown import TrueFalseDropDown

class _Meta(type(ABC), type(QWidget)):
    pass

class BackendRequestView(ABC, QWidget, metaclass=_Meta):
    def __init__(self, parent=None):
        super(BackendRequestView, self).__init__(parent)

        # `InterfaceController` and `WorkerPool` are singletons:
        #   declaring them as instance attributes, rather than class attributes,
        #   allows singleton reset during testing.
        self.interfaceController = InterfaceController()
        self.worker_pool = WorkerPool()

        # IMPORTANT: do not hide the "layout" method!
        _layout = QGridLayout()        
        self.setLayout(_layout)

    def _labeledField(self, label, field=None, text=None, orientation=Qt.Horizontal):
        return LabeledField(label, field=field, text=text, parent=self, orientation=orientation)

    def _labeledLineEdit(self, label):
        return LabeledField(label, field=None, text=None, parent=self)

    def _labeledToggle(self, label, state):
        return LabeledToggle(label, state, parent=self)

    def _labeledCheckBox(self, label):
        return LabeledCheckBox(label, self)

    def _sampleDropDown(self, label, items=[]):
        return SampleDropDown(label, items, self)

    def _trueFalseDropDown(self, label):
        return TrueFalseDropDown(label, self)

    def _multiSelectDropDown(self, label, items=[]):
        return MultiSelectDropDown(label, items, self)

    @abstractmethod
    def verify(self):
        pass
