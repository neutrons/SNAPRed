from qtpy.QtCore import Slot
from qtpy.QtWidgets import QGridLayout, QPushButton, QWidget


class WorkflowNodeView(QWidget):
    def __init__(self, node, position, parent=None):
        super(WorkflowNodeView, self).__init__(parent)
        self.model = node
        self.view = node.view

        # Do _not_ hide the `layout()` method!
        layout_ = QGridLayout()
        self.setLayout(layout_)

        # add a back and forward button to the top left
        self.backButton = QPushButton("Back \U00002b05", self)
        layout_.addWidget(self.backButton, 0, 0)
        if position == 0:
            self.backButton.setVisible(False)

        self.forwardButton = QPushButton("Forward \U000027a1", self)
        layout_.addWidget(self.forwardButton, 0, 1)
        self.forwardButton.setVisible(False)

        layout_.addWidget(self.view, 1, 0, 1, 2)

        # add a continue and quit button to the bottom
        self.continueButton = QPushButton("Continue \U00002705", self)
        layout_.addWidget(self.continueButton, 2, 0)

        self.skipButton = QPushButton("Skip \U000023ed", self)
        self.skipButton.setVisible(False)

        self.iterateButton = QPushButton("Iterate \U0001f504", self)
        self.iterateButton.setVisible(False)

        self.cancelButton = QPushButton("Cancel \U0000274c", self)
        layout_.addWidget(self.cancelButton, 2, 1)

    @Slot()
    def reset(self):
        self.view.reset()

    @Slot()
    def enableSkip(self):
        layout_ = self.layout()
        layout_.removeWidget(self.continueButton)
        layout_.removeWidget(self.cancelButton)
        layout_.addWidget(self.continueButton, 2, 0)
        layout_.addWidget(self.skipButton, 2, 1)
        layout_.addWidget(self.cancelButton, 2, 2)
        self.skipButton.setVisible(True)

    @Slot()
    def enableIterate(self):
        layout_ = self.layout()
        layout_.removeWidget(self.continueButton)
        layout_.removeWidget(self.cancelButton)
        layout_.addWidget(self.continueButton, 2, 0)
        layout_.addWidget(self.iterateButton, 2, 1)
        layout_.addWidget(self.cancelButton, 2, 2)
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
