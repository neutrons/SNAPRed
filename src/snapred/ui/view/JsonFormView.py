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
        self.formData = {}

    def lookupRef(self, ref):
        ref = ref.split("/")[-1]
        return self.jsonSchema["definitions"][ref]

    def _createField(self, name, prop):
        validator = None
        edit = QLineEdit()

        # int
        if prop["type"] == PRIME_TYPES[0]:
            validator = QRegularExpressionValidator(QRegularExpression("^(\+|-)?\d+$"), edit)
        # float
        if prop["type"] == PRIME_TYPES[2]:
            validator = QRegularExpressionValidator(QRegularExpression("^[-+]?\d*\.?\d*$"), edit)

        edit.setValidator(validator)
        label = QLabel("{}: ".format(name))
        widget = QWidget()
        layout = QHBoxLayout()
        layout.addWidget(label)
        layout.addWidget(edit)
        widget.setLayout(layout)
        widget.adjustSize()
        return widget, edit

    def _generateElement(self, prop, name="", parent=None):
        data = None
        widget = None
        if prop.get("type", None) in PRIME_TYPES:
            field, edit = self._createField(name, prop)
            data = edit
            widget = field
        elif prop.get("$ref", None):
            subform, subdata = self.buildForm(self.lookupRef(prop["$ref"]), parent=parent)
            data = subdata
            widget = subform
        else:
            widget = Section(name, parent=parent)
            data = {}
            for key, item in prop["items"].items():
                if type(item) is str:
                    item = {key: item}
                subform, subdata = self._generateElement(item, name, parent=parent)
                data[key] = subdata
                widget.appendWidget(subform)
        return widget, data

    def buildForm(self, definition, parent=None):
        if definition["title"] in self.definitions:
            return self.definitions["title"]

        form = Section(definition["title"], parent=parent)
        # Prevent Inf Loops
        self.definitions[definition["title"]] = form
        data = {}

        for key, prop in definition["properties"].items():
            widget, subdata = self._generateElement(prop, key, form)
            form.appendWidget(widget)
            data[key] = subdata
        form.adjustSize()
        self.definitions[definition["title"]] = (form, data)
        return form, data

    def build(self, title="", parent=None):
        data = {}
        items = self.jsonSchema.get("items", None)
        definitions = self.jsonSchema.get("definitions", None)
        form = Section(title, parent=parent)

        if items:
            for item in items.values():
                subform, data = self.buildForm(self.lookupRef(item), parent=form)
                self.formData = data
                form.appendWidget(subform)
        elif definitions:
            widget, data = self.buildForm(self.lookupRef(self.jsonSchema["$ref"]), parent=form)
            form.appendWidget(widget)
        else:
            widget, data = self._generateElement(self.jsonSchema, title, form)
            form.appendWidget(widget)

        form.adjustSize()
        return form, data


class JsonFormView(QWidget):
    def __init__(self, name, jsonSchema, parent=None):
        super(JsonFormView, self).__init__(parent)
        self.grid = QHBoxLayout(self)
        self.setLayout(self.grid)
        self.jsonSchema = jsonSchema
        formBuilder = FormBuilder(jsonSchema)
        widget, data = formBuilder.build(title=name)
        self.formData = data
        self.grid.addWidget(widget)
        self.adjustSize()
