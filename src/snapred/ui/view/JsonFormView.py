from PyQt5.QtCore import QRegularExpression
from PyQt5.QtGui import QRegularExpressionValidator
from PyQt5.QtWidgets import QHBoxLayout, QLabel, QLineEdit, QWidget

from snapred.ui.widget.Section import Section

PRIME_TYPES = ["integer", "string", "number"]
COMPOSITE_TYPES = ["array", "object"]


class FormBuilder:
    def __init__(self, jsonSchema):
        self.definitions = {}
        self.jsonSchema = jsonSchema
        self.fields = []

    def lookupRef(self, ref):
        ref = ref.split("/")[-1]
        return self.jsonSchema["definitions"][ref]

    def _createField(self, name, prop):
        validator = None
        # int
        if prop["type"] == PRIME_TYPES[0]:
            validator = QRegularExpressionValidator(QRegularExpression("^(\+|-)?\d+$"))
        # float
        if prop["type"] == PRIME_TYPES[2]:
            validator = QRegularExpressionValidator(QRegularExpression("^[-+]?\d*\.?\d*$"))

        edit = QLineEdit()
        edit.setValidator(validator)
        label = QLabel("{}: ".format(name))
        widget = QWidget()
        layout = QHBoxLayout()
        layout.addWidget(label)
        layout.addWidget(edit)
        widget.setLayout(layout)
        widget.adjustSize()
        self.fields.append(widget)
        return widget

    def buildForm(self, definition, parent=None):
        if definition["title"] in self.definitions:
            return self.definitions["title"]

        form = Section(definition["title"], parent=parent)
        # Prevent Inf Loops
        self.definitions[definition["title"]] = form

        for key, prop in definition["properties"].items():
            if prop.get("type", None) in PRIME_TYPES:
                form.appendWidget(self._createField(key, prop))
            else:
                form.appendWidget(self.buildForm(self.lookupRef(prop["$ref"]), parent=form))
        form.adjustSize()
        self.definitions[definition["title"]] = form
        return form

    def build(self, title="", parent=None):
        form = Section(title, parent=parent)
        for item in self.jsonSchema["items"].values():
            form.appendWidget(self.buildForm(self.lookupRef(item), parent=form))
        form.adjustSize()
        return form


class JsonFormView(QWidget):
    def __init__(self, name, jsonSchema, parent=None):
        super(JsonFormView, self).__init__(parent)
        self.grid = QHBoxLayout(self)
        self.setLayout(self.grid)
        formBuilder = FormBuilder(jsonSchema)
        self.grid.addWidget(formBuilder.build(title=name))
        self.adjustSize()
