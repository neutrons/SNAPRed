from qtpy.QtWidgets import QGridLayout, QMainWindow, QPushButton, QWidget


class WorkflowNodeView(QWidget):
    def __init__(self, node, position, parent=None):
        super(WorkflowNodeView, self).__init__(parent)
        self.model = node
        self.view = node.view
        self.layout = QGridLayout()
        self.setLayout(self.layout)
        # add a back and forward button to the top left
        self.backButton = QPushButton("Back \U00002B05", self)
        self.layout.addWidget(self.backButton, 0, 0)
        if position == 0:
            self.backButton.setVisible(False)

        self.forwardButton = QPushButton("Forward \U000027A1", self)
        self.layout.addWidget(self.forwardButton, 0, 1)
        self.forwardButton.setVisible(False)

        self.layout.addWidget(self.view, 1, 0, 1, 2)

        # add a continue and quit button to the bottom
        self.continueButton = QPushButton("Continue \U00002705", self)
        self.layout.addWidget(self.continueButton, 2, 0)

        self.skipButton = QPushButton("Skip \U000023ED", self)
        self.skipButton.setVisible(False)

        self.iterateButton = QPushButton("Iterate \U0001F504", self)
        self.iterateButton.setVisible(False)

        self.cancelButton = QPushButton("Cancel \U0000274C", self)
        self.layout.addWidget(self.cancelButton, 2, 1)

    def reset(self):
        self.view.reset()

    def enableSkip(self):
        self.layout.removeWidget(self.continueButton)
        self.layout.removeWidget(self.cancelButton)
        self.layout.addWidget(self.continueButton, 2, 0)
        self.layout.addWidget(self.skipButton, 2, 1)
        self.layout.addWidget(self.cancelButton, 2, 2)
        self.skipButton.setVisible(True)

    def enableIterate(self):
        self.layout.removeWidget(self.continueButton)
        self.layout.removeWidget(self.cancelButton)
        self.layout.addWidget(self.continueButton, 2, 0)
        self.layout.addWidget(self.iterateButton, 2, 1)
        self.layout.addWidget(self.cancelButton, 2, 2)
        self.iterateButton.setVisible(True)

    def onBackButtonClicked(self, slot):
        self.backButton.clicked.connect(slot)

    def onForwardButtonClicked(self, slot):
        self.forwardButton.clicked.connect(slot)

    def onContinueButtonClicked(self, slot):
        self.continueButton.clicked.connect(lambda: slot(self.model))

    def onCancelButtonClicked(self, slot):
        self.cancelButton.clicked.connect(slot)

    def onSkipButtonClicked(self, slot):
        self.skipButton.clicked.connect(slot)

    def onIterateButtonClicked(self, slot):
        self.iterateButton.clicked.connect(slot)
