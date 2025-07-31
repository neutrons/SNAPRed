from qtpy.QtWidgets import QWidget

from snapred.ui.widget.DisabledFieldLabel import DisabledFieldLabel


class SNAPWidget(QWidget):
    def __init__(self, parent=None):
        super(SNAPWidget, self).__init__(parent)
        self._layout = None
        self._disabledField = DisabledFieldLabel()
        self._disabledField.setVisible(False)
