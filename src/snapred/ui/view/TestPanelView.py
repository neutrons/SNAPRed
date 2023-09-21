from qtpy.QtWidgets import QGridLayout, QMainWindow, QWidget


class TestPanelView(QMainWindow):
    position = 1

    def __init__(self, parent=None):
        super(TestPanelView, self).__init__(parent)
        self.centralWidget = QWidget(self)
        self.setCentralWidget(self.centralWidget)

        self.grid = QGridLayout()
        self.grid.columnStretch(1)
        self.grid.rowStretch(1)
        self.centralWidget.setLayout(self.grid)
        self.adjustSize()
