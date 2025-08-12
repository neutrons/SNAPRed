from qtpy.QtCore import Signal, Slot
from qtpy.QtWidgets import QCheckBox, QHBoxLayout, QLabel, QWidget


class LabeledCheckBox(QWidget):
    checkedChanged = Signal(bool)

    def __init__(self, label, parent=None):
        super(LabeledCheckBox, self).__init__(parent)

        self._label = QLabel(label + ":", self)
        self._checkBox = QCheckBox(self)

        layout = QHBoxLayout(self)
        layout.addWidget(self._label)
        layout.addWidget(self._checkBox)
        layout.addStretch(1)
        layout.setContentsMargins(5, 5, 5, 5)

        self._checkBox.stateChanged.connect(self.emitCheckedState)

    @Slot(int)
    def emitCheckedState(self):
        self.checkedChanged.emit(self._checkBox.isChecked())

    @property
    def checkBox(self):
        return self._checkBox

    def isChecked(self):
        return self._checkBox.isChecked()

    def setChecked(self, state):
        self._checkBox.setChecked(state)
