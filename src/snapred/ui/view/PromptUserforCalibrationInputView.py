from qtpy.QtCore import Qt, Signal, Slot
from qtpy.QtWidgets import QDialog, QLabel, QLineEdit, QPushButton, QVBoxLayout


class PromptUserforCalibrationInputView(QDialog):
    dataEntered = Signal(str, str)

    def __init__(self, runNumber=None, parent=None):
        super(PromptUserforCalibrationInputView, self).__init__(parent)
        self.setStyleSheet("background-color: #F5E9E2;")
        layout = QVBoxLayout(self)
        label = QLabel("State not found, please enter a run number and state name!")
        label.setAlignment(Qt.AlignCenter)
        layout.addWidget(label)
        self.run_input = QLineEdit()
        if runNumber:
            self.run_input.setText(runNumber)
        self.run_input.setPlaceholderText("Enter run number")
        layout.addWidget(self.run_input)
        self.name_input = QLineEdit()
        self.name_input.setPlaceholderText("Enter name for state")
        layout.addWidget(self.name_input)
        self.continue_button = QPushButton("Continue")
        layout.addWidget(self.continue_button)
        self.continue_button.clicked.connect(self.handle_continue_click)

    def getRunNumber(self):
        return self.run_input.text()

    def getName(self):
        return self.name_input.text()

    @Slot()
    def handle_continue_click(self):
        run_number = self.getRunNumber()
        state_name = self.getName()
        self.dataEntered.emit(run_number, state_name)
