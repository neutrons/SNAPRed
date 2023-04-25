from PyQt5.QtWidgets import QMainWindow, QGridLayout, QPushButton, QWidget


class TestPanelView(QMainWindow):
    position = 1

    def __init__(self, parent=None):
        super(TestPanelView, self).__init__(parent)
        self.centralWidget = QWidget(self)
        self.setCentralWidget(self.centralWidget)

        self.grid = QGridLayout()
        self.grid.columnStretch(1)
        self.grid.rowStretch(1)
        self.centralWidget.setLayout(self.grid)

        # Setup test Buttons
        self.calibrationReductinButton = QPushButton('Test Calibration Reduction', self)
        self.grid.addWidget(self.calibrationReductinButton)
        self.calibrationReductinButton.adjustSize()

        self.adjustSize()

    def calibrationReductinButtonOnClick(self, slot):
        self.calibrationReductinButton.clicked.connect(slot)