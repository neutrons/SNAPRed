from typing import List, Tuple

from PyQt5.QtCore import Qt, pyqtSignal
from PyQt5.QtWidgets import QComboBox, QGridLayout, QLabel, QMessageBox, QPushButton, QWidget

from snapred.backend.dao.calibration import CalibrationIndexEntry
from snapred.ui.presenter.CalibrationAssessmentPresenter import CalibrationAssessmentLoader
from snapred.ui.widget.JsonFormList import JsonFormList
from snapred.ui.widget.LabeledField import LabeledField


class CalibrationAssessmentView(QWidget):
    signalLoadError = pyqtSignal(str)
    # signalLoadSuccess = pyqtSignal()

    def __init__(self, name, jsonSchemaMap, parent=None):
        super().__init__(parent)
        self._jsonFormList = JsonFormList(name, jsonSchemaMap, parent=parent)

        self.calibrationAssessmentLoader = CalibrationAssessmentLoader(self)

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
        self.loadButton.clicked.connect(self.calibrationAssessmentLoader.handleLoadRequested)

        self.calibrationDropdown = QComboBox()
        self.calibrationDropdown.setEnabled(True)
        self.calibrationDropdown.addItem("Select Calibration Record")
        self.calibrationDropdown.model().item(0).setEnabled(False)

        self.signalLoadError.connect(self._displayLoadError)
        # self.signalLoadSuccess.connect(self._updateOnLoadSuccess)

        self.layout.addWidget(self.interactionText, 0, 0)
        self.layout.addWidget(LabeledField("Calibration Record:", self.calibrationDropdown, self), 1, 0)
        self.layout.addWidget(self.loadButton, 1, 1)
        self.layout.addWidget(self.placeHolder)

    def updateCalibrationList(self, calibrationIndex: List[CalibrationIndexEntry]):
        # reset the combo-box by removing all items except for the first
        for item in range(1, self.calibrationDropdown.count()):
            self.calibrationDropdown.removeItem(item)
        # populate the combo-box from the input calibration index entries
        for entry in calibrationIndex:
            name = f"Version: {entry.version}; Run: {entry.runNumber}"
            self.calibrationDropdown.addItem(name, (entry.runNumber, entry.version))

        self.calibrationDropdown.setCurrentIndex(0)

    def getCalibrationRecordCount(self):
        return self.calibrationDropdown.count()

    def getSelectedCalibrationRecordIndex(self):
        return self.calibrationDropdown.currentIndex()

    def getSelectedCalibrationRecordData(self):
        return self.calibrationDropdown.currentData()

    def onLoadError(self, msg: str):
        self.signalLoadError.emit(msg)

    def _displayLoadError(self, msg: str):
        msgBox = QMessageBox()
        msgBox.setWindowTitle("Error")
        msgBox.setIcon(QMessageBox.Critical)
        msgBox.setText(msg)
        msgBox.setFixedSize(500, 200)
        msgBox.exec()

    # def onLoadSuccess(self):
    #     self.signalLoadSuccess.emit()

    # def _updateOnLoadSuccess(self):
    #     index = self.calibrationDropdown.currentIndex()
    #     self.calibrationDropdown.model().item(index).setEnabled(False)
