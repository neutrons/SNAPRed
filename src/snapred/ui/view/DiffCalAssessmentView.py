from typing import List

from qtpy.QtCore import Signal, Slot
from qtpy.QtWidgets import QComboBox, QLabel, QMessageBox, QPushButton

from snapred.backend.dao.indexing.IndexEntry import IndexEntry
from snapred.meta.decorators.Resettable import Resettable
from snapred.ui.presenter.CalibrationAssessmentPresenter import CalibrationAssessmentPresenter
from snapred.ui.view.BackendRequestView import BackendRequestView


@Resettable
class DiffCalAssessmentView(BackendRequestView):
    """

    The DiffCalAssessmentView serves as a user interface within the SNAPRed application,
    designed to streamline the review and selection of calibration assessments. It employs
    a combination of informative text, a dropdown menu for calibration record selection,
    and interactive buttons, all managed through a grid layout for clear user navigation.
    Integrated with the CalibrationAssessmentPresenter, it facilitates direct communication
    with the backend to load and display calibration data based on user input.

    """

    signalRunNumberUpdate = Signal(str, bool)
    signalError = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)

        self.presenter = CalibrationAssessmentPresenter(self)

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
        self.calibrationRecordField = self._labeledField("Calibration Record:", field=self.calibrationRecordDropdown)

        self.signalError.connect(self._displayError)

        self.layout.addWidget(self.interactionText, 0, 0)
        self.layout.addWidget(self.calibrationRecordField, 1, 0)
        self.layout.addWidget(self.loadButton, 1, 1)
        self.layout.addWidget(self.placeHolder)

        self.signalRunNumberUpdate.connect(self.presenter.loadCalibrationIndex)

    def updateCalibrationRecordList(self, calibrationIndex: List[IndexEntry]):
        # reset the combo-box by removing all items except for the first, which is a label
        for item in range(1, self.calibrationRecordDropdown.count()):
            self.calibrationRecordDropdown.removeItem(item)
        if calibrationIndex:
            # populate the combo-box from the input calibration index entries
            for entry in calibrationIndex:
                name = f"Version: {entry.version}; Run: {entry.runNumber}"
                self.calibrationRecordDropdown.addItem(name, (entry.runNumber, entry.useLiteMode, entry.version))
        self.calibrationRecordDropdown.setCurrentIndex(0)

    def getCalibrationRecordCount(self):
        return self.calibrationRecordDropdown.count() - 1  # the first item is a label

    def getSelectedCalibrationRecordIndex(self):
        return self.calibrationRecordDropdown.currentIndex() - 1  # the first item (index 0) is a label

    def getSelectedCalibrationRecordData(self):
        return self.calibrationRecordDropdown.currentData()

    def onError(self, msg: str):
        self.signalError.emit(msg)

    @Slot()
    def _displayError(self, msg: str):
        # Note: specifically using the static method `QMessageBox.critical` here helps with automated testing.
        # (That is, we can "mock.patch" just that method, and not patch the entire `QMessageBox` class.)
        QMessageBox.critical(
            self,
            "Error",
            msg,
        )

    def updateRunNumber(self, runNumber, useLiteMode):
        self.signalRunNumberUpdate.emit(runNumber, useLiteMode)

    def verify(self):
        # TODO: what fields need to be verified?
        return True
