from PyQt5.QtWidgets import QGridLayout, QMainWindow, QPushButton, QTabWidget, QWidget

from snapred.ui.view.JsonFormListView import JsonFormListView
from snapred.ui.widget.JsonForm import JsonForm


class JsonFormList:
    # Name to be used eventually
    def __init__(self, name, jsonSchemaMap, parent=None):  # noqa: ARG002
        self.view = JsonFormListView(parent)
        self._jsonForms = []
        for key, jsonSchema in jsonSchemaMap.items():
            jsonForm = JsonForm(key, jsonSchema=jsonSchema, parent=self.view)
            self.view.addForm(jsonForm)

    def getFieldText(self, fieldPath):
        return self.getField(fieldPath).text()

    def getField(self, fieldPath):
        return self.view.getField(fieldPath)

    def collectData(self):
        data = {}
        for jsonForm in self._jsonForms:
            data[jsonForm.name] = jsonForm.collectData()
        return data

    @property
    def widget(self):
        return self.view
