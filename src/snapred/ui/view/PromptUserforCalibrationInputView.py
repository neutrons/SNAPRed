from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *

class PromptUserforCalibrationInputView(QWidget):
    win = QWidget()
    win.setStyleSheet("background-color: #F5E9E2;")
    vbox = QVBoxLayout()
    label = QLabel("State not found, please enter a run number and state name!")
    run_input = QLineEdit()
    run_input = setPlaceholderText("Enter run number")
    name_input = QLineEdit()
    name_input.setPlaceholderText("Enter name for state")
    vbox.addWidget(run_input)
    vbox.addWidget(name_input)
    label.setAlignment(Qt.AlignCenter)
    vbox.addWidget(label)
    vbox.addStretch()
    win.setLayout(vbox)