from qtpy.QtWidgets import QDialog, QHBoxLayout, QPushButton, QScrollArea, QVBoxLayout, QWidget

from snapred.ui.widget.LabeledField import LabeledField


class MultiRunNumberDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Enter Multiple Run Numbers")

        # Main layout
        self.mainLayout = QVBoxLayout(self)

        # Scroll area setup
        self.scrollArea = QScrollArea(self)
        self.scrollWidget = QWidget()
        self.scrollLayout = QVBoxLayout(self.scrollWidget)
        self.scrollWidget.setLayout(self.scrollLayout)
        self.runNumberFields = []

        # Buttons setup
        self.buttonsLayout = QHBoxLayout()
        self.addButton = QPushButton("+")
        self.addButton.clicked.connect(self.addRunNumberField)
        self.minusButton = QPushButton("-")
        self.minusButton.clicked.connect(self.removeRunNumberField)
        self.okButton = QPushButton("OK")
        self.okButton.clicked.connect(self.updateParentField)

        # Add buttons to the layout
        self.buttonsLayout.addWidget(self.addButton)
        self.buttonsLayout.addWidget(self.minusButton)
        self.mainLayout.addLayout(self.buttonsLayout)
        self.mainLayout.addWidget(self.okButton)

        # Configure the scroll area
        self.scrollArea.setWidgetResizable(True)
        self.scrollArea.setWidget(self.scrollWidget)
        self.mainLayout.addWidget(self.scrollArea)

        # Initialize with one empty field
        self.addRunNumberField()

    def addRunNumberField(self, text=""):
        if isinstance(text, bool):
            text = ""
        newField = LabeledField("Run Number:")
        newField.setText(str(text))
        self.runNumberFields.append(newField)
        self.scrollLayout.addWidget(newField)
        newField.editingFinished.connect(self.runNumberChanged)
        self.updateMinusButtonState()

    def clearRunNumberFields(self):
        while self.runNumberFields:
            fieldToRemove = self.runNumberFields.pop()
            self.scrollLayout.removeWidget(fieldToRemove)
            fieldToRemove.deleteLater()
        self.adjustLayout()

    def removeRunNumberField(self):
        if len(self.runNumberFields) > 1:
            fieldToRemove = self.runNumberFields.pop()
            self.scrollLayout.removeWidget(fieldToRemove)
            fieldToRemove.deleteLater()
            self.updateMinusButtonState()
        self.adjustLayout()

    def runNumberChanged(self):
        lastFieldText = self.runNumberFields[-1].text()
        newNumbers = lastFieldText.split(",")
        if len(newNumbers) > 1:
            self.runNumberFields[-1].setText(newNumbers[0].strip())
            for number in newNumbers[1:]:
                self.addRunNumberField(number.strip())

    def getAllRunNumbers(self):
        return [field.text().strip() for field in self.runNumberFields if field.text().strip()]

    def updateParentField(self):
        runNumbers = self.getAllRunNumbers()
        self.parent().runNumberField.setText(", ".join(runNumbers))
        self.accept()

    def updateMinusButtonState(self):
        self.minusButton.setEnabled(len(self.runNumberFields) > 1)

    def adjustLayout(self):
        self.scrollWidget.adjustSize()
        self.scrollLayout.update()
        self.scrollArea.update()
        self.update()
        self.adjustSize()
