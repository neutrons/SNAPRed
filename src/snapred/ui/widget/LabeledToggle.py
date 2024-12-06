from qtpy.QtCore import Signal
from qtpy.QtWidgets import QHBoxLayout, QLabel, QWidget

from snapred.ui.widget.Toggle import Toggle


class LabeledToggle(QWidget):
    stateChanged = Signal(bool)

    def __init__(self, label, state, parent=None):
        super(LabeledToggle, self).__init__(parent)
        self.setStyleSheet("background-color: #F5E9E2;")

        self._label = QLabel(label + ":", self)
        self._toggle = Toggle(state=state, parent=self)

        layout = QHBoxLayout()
        layout.addWidget(self._label)
        layout.addWidget(self._toggle)
        layout.addStretch(1)
        layout.setContentsMargins(5, 5, 5, 5)
        self.setLayout(layout)

        self._toggle.stateChanged.connect(self.stateChanged)

    def getState(self):
        return self._toggle.getState()

    def setState(self, state):
        self._toggle.setState(state)
