from PyQt5.QtWidgets import QComboBox, QVBoxLayout, QWidget


class SampleDropDown(QWidget):
    def __init__(self, label, items=[], parent=None):
        super(SampleDropDown, self).__init__(parent)
        self.setStyleSheet("background-color: #F5E9E2;")
        self._label = label
        self._items = items

        self.dropDown = QComboBox()
        self.dropDown.addItem(str(self._label))
        self.dropDown.addItems(self._items)
        self.dropDown.model().item(0).setEnabled(False)

        layout = QVBoxLayout()
        layout.addWidget(self.dropDown)
        self.setLayout(layout)
