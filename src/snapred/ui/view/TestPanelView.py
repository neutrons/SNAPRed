from qtpy.QtWidgets import QGridLayout, QMainWindow, QTabWidget, QWidget


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
        self.tabWidget = QTabWidget()
        self.tabWidget.setTabPosition(QTabWidget.West)
        self.grid.addWidget(self.tabWidget)
        self.adjustSize()
