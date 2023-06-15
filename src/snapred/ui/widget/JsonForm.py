from snapred.ui.presenter.JsonFormPresenter import JsonFormPresenter
from snapred.ui.view.JsonFormView import JsonFormView


class JsonForm:
    def __init__(self, name, jsonSchema, parent=None):
        self._view = JsonFormView(name, jsonSchema=jsonSchema, parent=parent)
        self._presenter = JsonFormPresenter(self._view, None)

    @property
    def presenter(self):
        return self._presenter

    @property
    def widget(self):
        return self._presenter.widget

    def collectData(self):
        return self._presenter.collectData()
