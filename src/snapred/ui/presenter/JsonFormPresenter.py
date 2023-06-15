class JsonFormPresenter:
    def __init__(self, view, model) -> None:
        self.view = view
        self.model = model

    @property
    def widget(self):
        return self.view

    def _collectData(self, data):
        collectedData = {}
        for key, value in data.items():
            if type(value) is dict:
                collectedData[key] = self._collectData(value)
            else:
                collectedData[key] = value.text() if value.text() != "" else None

        return collectedData

    def collectData(self):
        dataSource = self.view.formData
        collectedData = self._collectData(dataSource)
        return collectedData
