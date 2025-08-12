from qtpy.QtWidgets import QComboBox, QHBoxLayout, QLabel, QWidget


class TrueFalseDropDown(QWidget):
    def __init__(self, label, parent=None):
        super(TrueFalseDropDown, self).__init__(parent)
        self._label = QLabel(label + ":", self)

        self.dropDown = QComboBox()
        self._initItems()

        layout = QHBoxLayout()
        layout.addWidget(self._label)
        layout.addWidget(self.dropDown)
        layout.setContentsMargins(5, 5, 5, 5)
        self.setLayout(layout)

    def _initItems(self):
        self.dropDown.clear()
        self.dropDown.addItems(["True", "False"])
        self.dropDown.setCurrentIndex(0)

    def currentIndex(self):
        return self.dropDown.currentIndex()

    def setCurrentIndex(self, index):
        self.dropDown.setCurrentIndex(index)

    def currentText(self):
        return self.dropDown.currentText()

    def getValue(self):
        return self.currentText() == "True"
