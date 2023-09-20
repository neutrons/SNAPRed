from qtpy import QtWidgets


class LogTableView(QtWidgets.QWidget):
    position = 1

    def __init__(self, name, parent=None):
        super(LogTableView, self).__init__(parent)
        self.grid = QtWidgets.QGridLayout(self)
        self.message = name

    def _empty(self):
        pass
