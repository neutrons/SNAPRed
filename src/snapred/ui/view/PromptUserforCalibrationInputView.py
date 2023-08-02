from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *

class PromptUserforCalibrationInputView(QWidget):

    def __init__(self, jsonForm, parent=None):
        super().__init__(parent)
        
        layout = QVBoxLayout(self)
        layout.setStyleSheet("background-color: #F5E9E2;")
        
        label = QLabel("State not found, please enter a run number and state name!")
        label.setAlignment(Qt.AlignCenter)
        layout.addWidget(label)
        
        self.run_input = QLineEdit() 
        self.run_input.setPlaceholderText("Enter run number")
        layout.addWidget(self.run_input)

        self.name_input = QLineEdit()
        self.name_input.setPlaceholderText("Enter name for state")
        layout.addWidget(self.name_input)

    def getRunNumber(self):
        return self.run_input.text()

    def getName(self): 
        return self.name_input.text()