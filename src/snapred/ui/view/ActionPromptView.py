from qtpy.QtCore import Slot
from qtpy.QtWidgets import QDesktopWidget, QGridLayout, QLabel, QMainWindow, QPushButton, QWidget


class ActionPromptView(QMainWindow):
    def __init__(self, title, message, parent=None, buttonNames=("Continue", "Cancel")):
        super(ActionPromptView, self).__init__(parent)
        self.title = title
        self.message = message

        self.setWindowTitle(self.title)

        self.centralWidget = QWidget(self)
        self.setCentralWidget(self.centralWidget)

        self.grid = QGridLayout()
        self.grid.columnStretch(1)
        self.grid.rowStretch(1)
        self.centralWidget.setLayout(self.grid)
        self.messageLabel = QLabel(self.message)
        self.grid.addWidget(self.messageLabel)

        self.continueButton = QPushButton(buttonNames[0])
        self.cancelButton = QPushButton(buttonNames[1])
        self.buttonLayout = QGridLayout()
        self.buttonLayout.addWidget(self.continueButton, 0, 0)
        self.buttonLayout.addWidget(self.cancelButton, 0, 1)
        self.grid.addLayout(self.buttonLayout, 1, 0)

        self.onCancelButtonClicked(self.close)
        self._center()
        self.show()

    def _center(self):
        qtRectangle = self.frameGeometry()
        centerPoint = QDesktopWidget().availableGeometry().center()
        qtRectangle.moveCenter(centerPoint)
        self.move(qtRectangle.topLeft())

    def onContinueButtonClicked(self, slot):
        @Slot()
        def slotAndClose():
            slot()
            self.close()

        self.continueButton.clicked.connect(slotAndClose)

    def onCancelButtonClicked(self, slot):
        self.cancelButton.clicked.connect(slot)
