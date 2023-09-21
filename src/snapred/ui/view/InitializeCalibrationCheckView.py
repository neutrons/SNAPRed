from PyQt5.QtWidgets import (
    QDialog,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QSizePolicy,
    QSpacerItem,
    QWidget,
)

from snapred.ui.presenter.InitializeCalibrationPresenter import CalibrationCheck
from snapred.ui.widget.Toggle import Toggle


class CalibrationMenu(QDialog):
    def __init__(self, parent=None):
        super(CalibrationMenu, self).__init__(parent)
        self.setWindowTitle("Calibration Menu")
        self.setFixedSize(400, 200)

        layout = QGridLayout(self)

        layout.addItem(QSpacerItem(20, 40, QSizePolicy.Minimum, QSizePolicy.Expanding), 0, 0)

        self.runNumberField = QLineEdit()
        self.runNumberField.setPlaceholderText("Enter Run Number")
        layout.addWidget(self.runNumberField, 0, 0)

        self.beginFlowButton = QPushButton("Check")
        layout.addWidget(self.beginFlowButton, 2, 0, 1, 2)

        layout.addItem(QSpacerItem(20, 40, QSizePolicy.Minimum, QSizePolicy.Expanding), 3, 0)

        self.setLayout(layout)

        self.calibrationCheck = CalibrationCheck(self)

        try:
            self.beginFlowButton.clicked.disconnect()
        except:  # noqa: E722
            pass

        self.beginFlowButton.clicked.connect(self.calibrationCheck.handleButtonClicked)

        self.finished.connect(self.on_close)

    def on_close(self):
        self.beginFlowButton.setEnabled(True)

    def _labeledField(self, label, field):
        widget = QWidget()
        widget.setStyleSheet("background-color: #F5E9E2;")
        layout = QHBoxLayout(widget)
        label = QLabel(label)
        layout.addWidget(label)
        layout.addWidget(field)
        return widget

    def getRunNumber(self):
        return self.runNumberField.text()


class InitializeCalibrationCheckView(QWidget):
    def __init__(self, parent=None):
        super(InitializeCalibrationCheckView, self).__init__(parent)
        self.layout = QGridLayout()
        self.setLayout(self.layout)

        self.beginFlowButton = QPushButton("Check Calibration Initialization")
        self.layout.addWidget(self.beginFlowButton, 4, 0, 1, 2)

        self.calibrationCheck = CalibrationCheck(self)
        self.beginFlowButton.clicked.connect(lambda: self.launchCalibrationCheck())

    def launchCalibrationCheck(self):
        calibrationMenu = CalibrationMenu()
        calibrationMenu.exec_()
