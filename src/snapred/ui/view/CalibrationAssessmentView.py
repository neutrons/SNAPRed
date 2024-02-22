from typing import List, Tuple

from PyQt5.QtCore import Qt, pyqtSignal
from PyQt5.QtWidgets import QComboBox, QGridLayout, QLabel, QMessageBox, QPushButton, QWidget

from snapred.backend.dao.calibration import CalibrationIndexEntry
from snapred.meta.decorators.Resettable import Resettable
from snapred.ui.presenter.CalibrationAssessmentPresenter import CalibrationAssessmentPresenter
from snapred.ui.widget.JsonFormList import JsonFormList
from snapred.ui.widget.LabeledField import LabeledField


@Resettable
class CalibrationAssessmentView(QWidget):
    signalRunNumberUpdate = pyqtSignal(str)
    signalError = pyqtSignal(str)

    def __init__(self, name, jsonSchemaMap, parent=None):
        super().__init__(parent)
        self._jsonFormList = JsonFormList(name, jsonSchemaMap, parent=parent)

        self.presenter = CalibrationAssessmentPresenter(self)

        self.layout = QGridLayout()
        self.setLayout(self.layout)

        self.interactionText = QLabel(
            "Calibration Complete! Please examine the calibration assessment workspaces. "
            "You can also load and examine previous calibration assessments for the same "
            "instrument state, if available."
        )
        self.placeHolder = QLabel("")

        self.loadButton = QPushButton("Load")
        self.loadButton.setEnabled(True)
        self.loadButton.clicked.connect(self.presenter.loadSelectedCalibrationAssessment)

        self.calibrationRecordDropdown = QComboBox()
        self.calibrationRecordDropdown.setEnabled(True)
        self.calibrationRecordDropdown.addItem("Select Calibration Record")
        self.calibrationRecordDropdown.model().item(0).setEnabled(False)

        self.signalError.connect(self._displayError)

        self.layout.addWidget(self.interactionText, 0, 0)
        self.layout.addWidget(LabeledField("Calibration Record:", self.calibrationRecordDropdown, self), 1, 0)
        self.layout.addWidget(self.loadButton, 1, 1)
        self.layout.addWidget(self.placeHolder)

        self.signalRunNumberUpdate.connect(self.presenter.loadCalibrationIndex)

    def updateCalibrationRecordList(self, calibrationIndex: List[CalibrationIndexEntry]):
        # reset the combo-box by removing all items except for the first, which is a label
        for item in range(1, self.calibrationRecordDropdown.count()):
            self.calibrationRecordDropdown.removeItem(item)
        if calibrationIndex:
            # populate the combo-box from the input calibration index entries
            for entry in calibrationIndex:
                name = f"Version: {entry.version}; Run: {entry.runNumber}"
                self.calibrationRecordDropdown.addItem(name, (entry.runNumber, entry.version))
        self.calibrationRecordDropdown.setCurrentIndex(0)

    def getCalibrationRecordCount(self):
        return self.calibrationRecordDropdown.count() - 1  # the first item is a label

    def getSelectedCalibrationRecordIndex(self):
        return self.calibrationRecordDropdown.currentIndex() - 1  # the first item (index 0) is a label

    def getSelectedCalibrationRecordData(self):
        return self.calibrationRecordDropdown.currentData()

    def onError(self, msg: str):
        self.signalError.emit(msg)

    def _displayError(self, msg: str):
        msgBox = QMessageBox()
        msgBox.setWindowTitle("Error")
        msgBox.setIcon(QMessageBox.Critical)
        msgBox.setText(msg)
        msgBox.setFixedSize(500, 200)
        msgBox.exec()

    def updateRunNumber(self, runNumber):
        self.signalRunNumberUpdate.emit(runNumber)
