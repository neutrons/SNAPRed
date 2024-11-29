from qtpy.QtCore import Qt
from qtpy.QtWidgets import ( 
    QHBoxLayout, 
    QLabel, 
    QLineEdit,
    QPushButton,
    QSizePolicy, 
    QVBoxLayout,
    QWidget
)


class LabeledField(QWidget):
    def __init__(self, label, field=None, text=None, parent=None, orientation=Qt.Horizontal, sizeHint=None):
        super(LabeledField, self).__init__(parent)
        
        # TODO: Set this from the application style sheet.
        #    Otherwise, this overrides the application style sheet!
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
                
        self.setLayout(_layout)

        self._label = QLabel(label)
        if field is not None:
            self._field = field
        else:
            self._field = QLineEdit(parent=self)
            self._field.setText(text if text is not None else "")

        layout.addWidget(self._label)
        layout.addWidget(self._field)
        # adjust layout size such that label has no whitespace
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
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

    def text(self):
        return self._field.text()

    def convertCommaSepartedToList(self):
        return self.text().split(",")

    def setText(self, text):
        self._field.setText(text)

    def clear(self):
        self._field.clear()
