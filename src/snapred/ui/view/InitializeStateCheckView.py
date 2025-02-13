from qtpy.QtCore import QMetaObject, Qt, Slot
from qtpy.QtWidgets import (
    QDialog,
    QGridLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
)

from snapred.ui.presenter.InitializeStatePresenter import InitializeStatePresenter


class InitializationMenu(QDialog):
    def __init__(self, runNumber=None, parent=None, useLiteMode=None):
        super().__init__(parent)

        # This dialog is using `show`, which by default produces modeless dialogs,
        #   so we need to explicitly specify that we want it to be modal.
        self.setWindowModality(Qt.ApplicationModal)

        self.setWindowTitle("Initialization Menu")
        self.setFixedSize(400, 300)

        self.runNumber = runNumber
        self.useLiteMode = useLiteMode
        self.layout = QGridLayout(self)
        self.setLayout(self.layout)

        self.initializationWorkflow = InitializeStatePresenter(self)

        instructionLabel = QLabel("Please enter a run number and state name to initialize:")
        instructionLabel.setWordWrap(True)
        self.layout.addWidget(instructionLabel, 0, 0, 1, 2)

        self.runNumberField = QLineEdit()
        self.runNumberField.setPlaceholderText("Enter Run Number")
        if self.runNumber is not None:
            self.runNumberField.setText(str(self.runNumber))
        self.layout.addWidget(self.runNumberField, 1, 0, 1, 2)

        self.stateNameField = QLineEdit()
        self.stateNameField.setPlaceholderText("Enter State Name")
        self.layout.addWidget(self.stateNameField, 2, 0, 1, 2)

        self.beginFlowButton = QPushButton("Initialize State")
        self.layout.addWidget(self.beginFlowButton, 3, 0, 1, 2)

        self.beginFlowButton.clicked.connect(self.handleButtonClicked)

    def showEvent(self, event):  # noqa: ARG002
        # Using `self.parent()` here instead of just `self` makes this dialog modal with respect to the workflow,
        #   which is what we want.
        reply = QMessageBox.question(
            self.parent(),
            "Initialize State",
            "Warning! This run number does not have an initialized state. Do you want to initialize it?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.Yes,
        )
        if reply == QMessageBox.No:
            # Here we Allow the `showEvent` to actually complete,
            #   so that the widget is in a defined state,
            #   before actually processing the `close`.
            QMetaObject.invokeMethod(self, "close", Qt.QueuedConnection)

    def getRunNumber(self):
        return self.runNumberField.text() if self.runNumberField else ""

    def getStateName(self):
        return self.stateNameField.text() if self.stateNameField else ""

    def getMode(self):
        return self.useLiteMode

    @Slot()
    def handleButtonClicked(self):
        runNumber = self.getRunNumber()
        stateName = self.getStateName()
        if not runNumber or not stateName:
            QMessageBox.warning(self, "Missing Information", "Both run number and state name are required.")
            return
        self.initializationWorkflow.handleButtonClicked()
