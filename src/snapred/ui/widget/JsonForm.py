from snapred.ui.presenter.JsonFormPresenter import JsonFormPresenter
from snapred.ui.view.JsonFormView import JsonFormView


class JsonForm:
    def __init__(self, name, jsonSchema, parent=None):
        self._name = name
        self._view = JsonFormView(name, jsonSchema=jsonSchema, parent=parent)
        self._presenter = JsonFormPresenter(self._view, None)

    @property
    def presenter(self):
        return self._presenter

    @property
    def widget(self):
        return self._presenter.widget

    @property
    def name(self):
        return self._name

    def getField(self, fieldPath):
        return self._presenter.getField(fieldPath)

    def collectData(self):
        return self._presenter.collectData()

    def updateData(self, newData):
        return self._presenter.updateData(newData)
