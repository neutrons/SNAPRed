from qtpy.QtWidgets import QGridLayout, QMainWindow, QPushButton, QWidget


class WorkflowNodeView(QWidget):
    def __init__(self, node, position, parent=None):
        super(WorkflowNodeView, self).__init__(parent)
        self.model = node
        self.view = node.view
        layout = QGridLayout()
        self.setLayout(layout)
        # add a back and forward button to the top left
        self.backButton = QPushButton("Back \U00002B05", self)
        layout.addWidget(self.backButton, 0, 0)
        if position == 0:
            self.backButton.setVisible(False)

        self.forwardButton = QPushButton("Forward \U000027A1", self)
        layout.addWidget(self.forwardButton, 0, 1)
        self.forwardButton.setVisible(False)

        layout.addWidget(self.view, 1, 0, 1, 2)

        # add a continue and quit button to the bottom
        self.continueButton = QPushButton("Continue \U00002705", self)
        layout.addWidget(self.continueButton, 2, 0)

        self.cancelButton = QPushButton("Cancel \U0000274C", self)
        layout.addWidget(self.cancelButton, 2, 1)

    def onBackButtonClicked(self, slot):
        self.backButton.clicked.connect(slot)

    def onForwardButtonClicked(self, slot):
        self.forwardButton.clicked.connect(slot)

    def onContinueButtonClicked(self, slot):
        self.continueButton.clicked.connect(lambda: slot(self.model))

    def onCancelButtonClicked(self, slot):
        self.cancelButton.clicked.connect(slot)
