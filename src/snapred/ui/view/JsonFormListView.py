from qtpy.QtWidgets import QGridLayout, QMainWindow, QPushButton, QTabWidget, QWidget

from snapred.ui.model.WorkflowNodeModel import WorkflowNodeModel
from snapred.ui.view.WorkflowNodeView import WorkflowNodeView


class JsonFormListView(QWidget):
    def __init__(self, parent=None):
        super(JsonFormListView, self).__init__(parent)
        self.layout = QGridLayout()
        self.setLayout(self.layout)
        self._jsonForms = []

    def addForm(self, jsonForm):
        self._jsonForms.append(jsonForm)
        self.layout.addWidget(jsonForm.widget)

    def getFieldText(self, fieldPath):
        field = self.getField(fieldPath)
        if field is not None:
            return field.text()
        return None

    def getField(self, fieldPath):
        for jsonForm in self._jsonForms:
            field = jsonForm.getField(fieldPath)
            if field is not None:
                return field
        return None
