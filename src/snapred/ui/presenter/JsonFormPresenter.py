class JsonFormPresenter:
    def __init__(self, view, model) -> None:
        self.view = view
        self.model = model

    @property
    def widget(self):
        return self.view

    def _collectField(self, field):
        fieldData = field.text() if field.text() != "" else None
        if fieldData:
            if "," in fieldData:
                fieldData = fieldData.split(",")
        return fieldData

    def _collectData(self, data):
        if type(data) is dict:
            collectedData = {}
            for key, value in data.items():
                if type(value) is dict:
                    collectedData[key] = self._collectData(value)
                    # check if list, TODO: Handle tuples
                    if len(collectedData[key]) == 1:
                        # This feels jank, but a child dict of len 1 with the same key as the parent
                        # is a list
                        if collectedData[key].get(key, None):
                            collectedData[key] = collectedData[key][key]
                else:
                    collectedData[key] = self._collectField(value)
        else:
            collectedData = self._collectField(data)

        return collectedData

    def collectData(self):
        dataSource = self.view.formData
        collectedData = self._collectData(dataSource)
        return collectedData

    def _updateField(self, field, value):
        if value:
            field.setText(str(value))

    # TODO: Fix this to handle lists of type(dict)
    def _updateData(self, data, newData):
        if not newData:
            return
        if type(data) is dict:
            for key, value in data.items():
                if type(newData) is list:
                    if type(newData[0]) is dict:
                        newData = newData[0]
                    else:
                        newData = {key: ",".join(map(str, newData))}
                if type(value) is dict:
                    self._updateData(value, newData.get(key, None))
                else:
                    self._updateField(value, newData.get(key, None))
        else:
            self._updateField(data, newData)

    def updateData(self, newData):
        dataSource = self.view.formData
        self._updateData(dataSource, newData)

    def getField(self, fieldPath):
        data = self.view.formData
        subPaths = fieldPath.split(".")
        for subPath in subPaths:
            data = data.get(subPath, None)
            if data is None:
                break
        return data
