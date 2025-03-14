from qtpy.QtCore import Qt
from qtpy.QtWidgets import QHBoxLayout, QLabel, QLineEdit, QVBoxLayout, QWidget


class LabeledField(QWidget):
    def __init__(self, label, field=None, text=None, parent=None, orientation=Qt.Horizontal, sizeHint=None):  # noqa: ARG002
        super(LabeledField, self).__init__(parent)

        # TODO: Set this from the application style sheet.
        #    Otherwise, this OVERRIDES the application style sheet!
        self.setStyleSheet("background-color: #F5E9E2;")

        if field is not None:
            # bubble up the size policy from the field
            self.setSizePolicy(field.sizePolicy())

        _layout = None
        match orientation:
            case Qt.Horizontal:
                _layout = QHBoxLayout()
            case Qt.Vertical:
                _layout = QVBoxLayout()
            case _:
                raise RuntimeError(f"unexpected orientation for `LabeledField`: {orientation}")

        self._label = QLabel(label)
        if field is not None:
            self._field = field
        else:
            self._field = QLineEdit(parent=self)
            self._field.setText(text if text is not None else "")

        _layout.addWidget(self._label)
        _layout.addWidget(self._field)
        # adjust layout size such that label has no whitespace
        _layout.setContentsMargins(0, 0, 0, 0)
        _layout.setSpacing(0)
        self.setLayout(_layout)

        self._label.adjustSize()
        self._field.adjustSize()
        self.adjustSize()

    @property
    def label(self):
        return self._label

    @property
    def field(self):
        return self._field

    @property
    def editingFinished(self):
        # signal sent when the text field is no longer selected
        return self._field.editingFinished

    def get(self, default=None):
        if "" == self._field.text():
            return default
        return self.text()

    def convertCommaSeparatedToList(self):
        return self.text().split(",")

    def text(self):
        return self._field.text()

    def setText(self, text):
        self._field.setText(text)

    def labelText(self):
        return self._label.text()

    def setLabelText(self, text):
        self._label.setText(text)

    def clear(self):
        self._field.clear()
