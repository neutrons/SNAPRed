from qtpy.QtCore import Qt
from qtpy.QtWidgets import QHBoxLayout, QLabel, QLineEdit, QSizePolicy, QVBoxLayout

from snapred.ui.widget.SNAPWidget import SNAPWidget


class LabeledField(SNAPWidget):
    def __init__(self, label: str, field=None, text=None, parent=None, orientation=Qt.Horizontal, sizeHint=None):  # noqa: ARG002
        super(LabeledField, self).__init__(parent)

        if field is not None:
            # bubble up the size policy from the field
            self.setSizePolicy(field.sizePolicy())

        self._layout = None
        match orientation:
            case Qt.Horizontal:
                self._layout = QHBoxLayout(self)
            case Qt.Vertical:
                self._layout = QVBoxLayout(self)
            case _:
                raise RuntimeError(f"unexpected orientation for `LabeledField`: {orientation}")

        if not label.endswith(":"):
            label += ":"

        self._label = QLabel(label)
        if field is not None:
            self._field = field
        else:
            self._field = QLineEdit(parent=self)
            self._field.setText(text if text is not None else "")
            self._disabledField.setText(text if text is not None else "")

        self._layout.addWidget(self._label, stretch=0)
        self._layout.addWidget(self._field, stretch=1)

        if orientation == Qt.Horizontal:
            self._field.sizePolicy().setHorizontalPolicy(QSizePolicy.Policy.Expanding)
            self._label.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Preferred)

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

    def _addWidget(self, widget):
        if isinstance(self._layout, QHBoxLayout):
            self._layout.addWidget(widget, stretch=1)
            self._layout.setAlignment(widget, Qt.AlignRight)

    def setEnabled(self, enabled: bool):
        if enabled:
            self._layout.removeWidget(self._disabledField)
            self._field.setText(self._disabledField.text())
            self._addWidget(self._field)
            self._field.setVisible(True)
            self._disabledField.setVisible(False)
        else:
            self._layout.removeWidget(self._field)
            self._addWidget(self._disabledField)
            self._disabledField.setText(self._field.text())
            self._field.setVisible(False)
            self._disabledField.setVisible(True)
        super(LabeledField, self).setEnabled(enabled)

    def get(self, default=None):
        if "" == self._field.text():
            return default
        return self.text()

    def convertCommaSeparatedToList(self):
        return self.text().split(",")

    def fontMetrics(self):
        return self._field.fontMetrics()

    def text(self):
        return self._field.text()

    def setText(self, text):
        self._field.setText(text)
        self._disabledField.setText(text)

    def labelText(self):
        return self._label.text()

    def setLabelText(self, text):
        self._label.setText(text)

    def clear(self):
        self._field.clear()
