from qtpy.QtCore import Signal
from qtpy.QtWidgets import QHBoxLayout, QLabel, QWidget

from snapred.ui.widget.Toggle import Toggle


class LabeledToggle(QWidget):
    stateChanged = Signal(bool)

    def __init__(self, label, state, parent=None):
        super(LabeledToggle, self).__init__(parent)
        self.setStyleSheet("background-color: #F5E9E2;")

        self._label = QLabel(label + ":", self)
        self.toggle = Toggle(state=state, parent=self)

        _layout = QHBoxLayout()
        _layout.addWidget(self._label)
        _layout.addWidget(self.toggle)
        _layout.addStretch(1)
        _layout.setContentsMargins(5, 5, 5, 5)
        self.setLayout(_layout)

        self.toggle.stateChanged.connect(self.stateChanged)

    def getState(self):
        return self.toggle.getState()

    def setState(self, state):
        self.toggle.setState(state)
