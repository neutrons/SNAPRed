from qtpy.QtWidgets import QComboBox, QVBoxLayout

from snapred.ui.widget.SNAPWidget import SNAPWidget


class SampleDropDown(SNAPWidget):
    def __init__(self, label, items=[], parent=None):
        super(SampleDropDown, self).__init__(parent)
        self._label = label
        self._items = items

        self.dropDown = QComboBox()
        self._initItems()
        self._disabledField.setText(self.dropDown.currentText())
        self._disabledField.setWordWrap(False)

        self._layout = QVBoxLayout(self)
        self._layout.addWidget(self.dropDown)

    def setEnabled(self, flag: bool):
        if flag:
            self.dropDown.setVisible(True)
            self._disabledField.setVisible(False)
            self._layout.addWidget(self.dropDown)
            self._layout.removeWidget(self._disabledField)
        else:
            self.dropDown.setVisible(False)
            self._disabledField.setVisible(True)
            self._disabledField.setText(self.dropDown.currentText())
            self._layout.addWidget(self._disabledField)
            self._layout.removeWidget(self.dropDown)

        super(SampleDropDown, self).setEnabled(flag)

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
        self._disabledField.setText(self.dropDown.currentText())

    def currentText(self):
        return self.dropDown.currentText()
