from typing import List

from qtpy.QtCore import Qt
from qtpy.QtGui import QStandardItemModel
from qtpy.QtWidgets import QComboBox, QStyledItemDelegate, QVBoxLayout, QWidget


class CheckableComboBox(QComboBox):
    def __init__(self, parent=None):
        super(CheckableComboBox, self).__init__(parent)
        self.view().pressed.connect(self.handleItemPressed)
        self.setModel(QStandardItemModel(self))

    def handleItemPressed(self, index):
        item = self.model().itemFromIndex(index)
        if item.checkState() == Qt.Checked:
            item.setCheckState(Qt.Unchecked)
        else:
            item.setCheckState(Qt.Checked)

    def checkedItems(self) -> List[str]:
        checked_items = []
        for index in range(self.count()):
            item = self.model().item(index)
            if item.checkState() == Qt.Checked:
                checked_items.append(item.text())
        return checked_items


class CheckableComboBoxDelegate(QStyledItemDelegate):
    def paint(self, painter, option, index):
        item = index.model().itemFromIndex(index)
        if item.checkState() == Qt.Checked:
            option.text = "✔ " + item.text()
        else:
            option.text = item.text()
        super(CheckableComboBoxDelegate, self).paint(painter, option, index)


class MultiSelectComboBox(CheckableComboBox):
    def __init__(self, items: List[str], parent=None):
        super(MultiSelectComboBox, self).__init__(parent)
        self.addItems(items)
        for index in range(self.count()):
            item = self.model().item(index)
            item.setFlags(Qt.ItemIsUserCheckable | Qt.ItemIsEnabled)
            item.setData(Qt.Unchecked, Qt.CheckStateRole)
        self.setItemDelegate(CheckableComboBoxDelegate(self))


class MultiSelectDropDown(QWidget):
    def __init__(self, label: str, items: List[str] = [], parent=None):
        super(MultiSelectDropDown, self).__init__(parent)
        self.setStyleSheet("background-color: #F5E9E2;")
        self._label = label
        self._items = items

        self.dropDown = MultiSelectComboBox(items)
        self._initItems()

        layout = QVBoxLayout()
        layout.addWidget(self.dropDown)
        self.setLayout(layout)

    def _initItems(self):
        self.dropDown.clear()
        self.dropDown.addItem(str(self._label))
        self.dropDown.addItems(self._items)
        self.dropDown.model().item(0).setEnabled(False)

    def setItems(self, items: List[str] = []):
        self._items = items
        self._initItems()

    def checkedItems(self) -> List[str]:
        return self.dropDown.checkedItems()
