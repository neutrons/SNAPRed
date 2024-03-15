from qtpy.QtCore import Signal
from qtpy.QtWidgets import QGridLayout, QLabel, QLineEdit, QWidget

from snapred.ui.widget.JsonFormList import JsonFormList
from snapred.ui.widget.LabeledField import LabeledField


class ReductionView(QWidget):
    signalRunNumberUpdate = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)

        self.layout = QGridLayout()
        self.setLayout(self.layout)

        self.interactionText = QLabel("Which Run would you like to reduce today?")

        self.fieldRunNumber = LabeledField("Run Number :", QLineEdit(), self)
        self.signalRunNumberUpdate.connect(self._updateRunNumber)

        self.layout.addWidget(self.interactionText)
        self.layout.addWidget(self.fieldRunNumber)

    # This signal boilerplate mumbo jumbo is necessary because worker threads cant update the gui directly
    # So we have to send a signal to the main thread to update the gui, else we get an unhelpful segfault
    def _updateRunNumber(self, runNumber):
        self.fieldRunNumber.setText(runNumber)

    def updateRunNumber(self, runNumber):
        self.signalRunNumberUpdate.emit(runNumber)
