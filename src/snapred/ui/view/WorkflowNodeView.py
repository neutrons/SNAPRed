from PyQt5.QtWidgets import QGridLayout, QMainWindow, QPushButton, QWidget


class WorkflowNodeView(QMainWindow):
    def __init__(self, subview, parent=None):
        super(WorkflowNodeView, self).__init__(parent)
        self.subview = subview
        self.centralWidget = QWidget(self)
        self.setCentralWidget(self.centralWidget)

        self.grid = QGridLayout()
        self.grid.columnStretch(1)
        self.grid.rowStretch(1)
        self.centralWidget.setLayout(self.grid)

        self.buttonActionContinue = self._empty
        self.buttonActionQuit = self._empty

        self.grid.addWidget(self.subview)

        self.continueButton = QPushButton("Continue", self)
        self.continueButton.clicked.connect(self.execButtonActionContinue)
        self.grid.addWidget(self.continueButton)

        self.quitButton = QPushButton("Quit", self)
        self.quitButton.clicked.connect(self.execButtonActionQuit)
        self.grid.addWidget(self.quitButton)

    def updateSubview(self, newSubview):
        self.grid.replaceWidget(self.subview, newSubview)
        self.subview.deleteLater()
        self.subview = newSubview

    def execButtonActionContinue(self):
        self.buttonActionContinue()

    def execButtonActionQuit(self):
        self.buttonActionQuit()

    def _empty(self):
        pass

    def onContinueButtonClicked(self, slot):
        self.buttonActionContinue = slot

    def onQuitButtonClicked(self, slot):
        self.buttonActionQuit = slot
