from qtpy.QtCore import QRegularExpression
from qtpy.QtGui import QRegularExpressionValidator
from qtpy.QtWidgets import QHBoxLayout, QLabel, QLineEdit, QWidget

from snapred.ui.widget.Section import Section

PRIME_TYPES = ["integer", "string", "number", "boolean"]
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
        elif prop.get("items") is not None:  # list
            widget = Section(name, parent=parent)
            data = {}
            iterable = None
            if prop.get("items") is None:
                breakpoint()
            if isinstance(prop["items"], list):
                iterable = [("type", p["type"]) for p in prop["items"]]
            if isinstance(prop["items"], dict):
                iterable = prop["items"].items()
            for key, item in iterable:
                if isinstance(item, str):
                    item = {key: item}
                subform, subdata = self._generateElement(item, name, parent=parent)
                # tuples
                if len(prop["items"]) > 1:
                    data[key] = subdata
                else:  # lists
                    data[name] = subdata
                widget.appendWidget(subform)
        return widget, data

    def buildForm(self, definition, parent=None):
        if definition["title"] in self.definitions:
            return self.definitions[definition["title"]]

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
        elif len(self.jsonSchema) > 0:
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
        try:
            widget, data = formBuilder.build(title=name)
            self.formData = data
            self.grid.addWidget(widget)
            self.adjustSize()
        except Exception:  # noqa: BLE001
            # Catch and raise exception with invalid json in console.
            raise Exception(f"Invalid JSON Schema:\n {jsonSchema}")
