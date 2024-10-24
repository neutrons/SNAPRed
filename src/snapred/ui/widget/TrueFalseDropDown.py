from qtpy.QtWidgets import QComboBox, QVBoxLayout, QWidget


class TrueFalseDropDown(QWidget):
    def __init__(self, label, parent=None):
        super(TrueFalseDropDown, self).__init__(parent)
        self.setStyleSheet("background-color: #F5E9E2;")
        self._label = label

        self.dropDown = QComboBox()
        self._initItems()

        layout = QVBoxLayout()
        layout.addWidget(self.dropDown)
        self.setLayout(layout)

    def _initItems(self):
        self.dropDown.clear()
        self.dropDown.addItem(self._label)
        self.dropDown.addItems(["True", "False"])
        self.dropDown.model().item(0).setEnabled(False)
        self.dropDown.setCurrentIndex(1)

    def currentIndex(self):
        # Subtract 1 because the label is considered an index
        return self.dropDown.currentIndex() - 1

    def setCurrentIndex(self, index):
        # Add 1 to skip the label
        self.dropDown.setCurrentIndex(index + 1)

    def currentText(self):
        return self.dropDown.currentText()
