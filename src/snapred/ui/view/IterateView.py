from qtpy.QtWidgets import QGridLayout, QLabel, QWidget

from snapred.meta.decorators.Resettable import Resettable


@Resettable
class IterateView(QWidget):
    def __init__(self, parent=None):
        super(IterateView, self).__init__(parent)
        self.layout = QGridLayout()
        self.setLayout(self.layout)
        self.message = QLabel("Repeat process and rename current results?")
        self.layout.addWidget(self.message, 0, 0, 1, 2)
