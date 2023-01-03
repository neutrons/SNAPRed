
from PyQt5 import QtCore, QtGui, QtWidgets


class LogTableView(QtWidgets.QWidget):

    position = 1

    def __init__(self, name, parent=None):
        super(LogTableView, self).__init__(parent)
        self.grid = QtWidgets.QGridLayout(self)
        self.message = name
        self.buttonAction = self._empty
        self.button = QtWidgets.QPushButton(name, self)
        self.button.clicked.connect(self.execButtonAction)
        self.grid.addWidget(self.button)

    def addRecipeConfig(self, reductionConfigs):
        print(reductionConfigs)
        self.grid.addWidget(QtWidgets.QLabel(str(reductionConfigs)), self.position, 0)
        self.position += 1

    def execButtonAction(self):
        self.buttonAction()

    def _empty(self):
        print("missing")

    def on_button_clicked(self, slot):
        self.buttonAction = slot
        