from PyQt5.QtWidgets import QComboBox, QVBoxLayout, QWidget


class SampleDropDown(QWidget):
    def __init__(self, label, items=[], parent=None):
        super(SampleDropDown, self).__init__(parent)
        self.setStyleSheet("background-color: #F5E9E2;")
        self._label = label
        self._items = items

        self.dropDown = QComboBox()
        self._initItems()

        layout = QVBoxLayout()
        layout.addWidget(self.dropDown)
        self.setLayout(layout)

    def _initItems(self):
        self.dropDown.clear()
        self.dropDown.addItem(str(self._label))
        self.dropDown.addItems(self._items)
        self.dropDown.model().item(0).setEnabled(False)

    def setItems(self, items=[]):
        self._items = items
        self._initItems()

    def currentIndex(self):
        # NOTE because the label is considered an index, must decrement by 1
        return self.dropDown.currentIndex() - 1

    def setCurrentIndex(self, index):
        # NOTE because the label is considered an index, must incredment by 1
        self.dropDown.setCurrentIndex(index + 1)

    def currentText(self):
        return self.dropDown.currentText()
