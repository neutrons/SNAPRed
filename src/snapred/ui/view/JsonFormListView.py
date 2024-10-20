from qtpy.QtWidgets import QGridLayout, QWidget


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

    def getField(self, fieldPath):
        for jsonForm in self._jsonForms:
            field = jsonForm.getField(fieldPath)
            if field is not None:
                return field
        RuntimeError("Field not found in json form list: " + fieldPath)
