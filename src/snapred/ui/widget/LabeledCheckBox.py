from qtpy.QtCore import Signal as pyqtSignal
from qtpy.QtWidgets import QCheckBox, QHBoxLayout, QLabel, QWidget


class LabeledCheckBox(QWidget):
    checkedChanged = pyqtSignal(bool)

    def __init__(self, label, parent=None):
        super(LabeledCheckBox, self).__init__(parent)
        self.setStyleSheet("background-color: #F5E9E2;")

        self._label = QLabel(label + ":", self)
        self._checkBox = QCheckBox(self)

        layout = QHBoxLayout()
        layout.addWidget(self._label)
        layout.addWidget(self._checkBox)
        layout.addStretch(1)
        layout.setContentsMargins(5, 5, 5, 5)
        self.setLayout(layout)

        self._checkBox.stateChanged.connect(self.emitCheckedState)

    def emitCheckedState(self, state):
        self.checkedChanged.emit(state == QCheckBox.isChecked)

    @property
    def checkBox(self):
        return self._checkBox

    def isChecked(self):
        return self._checkBox.isChecked()

    def setChecked(self, state):
        self._checkBox.setChecked(state)
