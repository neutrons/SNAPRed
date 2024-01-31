import matplotlib.pyplot as plt
from mantid.simpleapi import CreateWorkspace
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from qtpy.QtWidgets import QGridLayout, QMainWindow, QWidget
from workbench.plotting.figuremanager import MantidFigureCanvas
from workbench.plotting.toolbar import WorkbenchNavigationToolbar


class PlotTestView(QMainWindow):
    position = 1

    def __init__(self, parent=None):
        self._working_init(parent)

    def _working_init(self, parent):
        super(PlotTestView, self).__init__(parent)
        self.centralWidget = QWidget(self)
        self.setCentralWidget(self.centralWidget)

        self.grid = QGridLayout()
        self.grid.columnStretch(2)
        self.grid.rowStretch(1)
        self.centralWidget.setLayout(self.grid)

        wksp = CreateWorkspace(
            DataX=list(range(10)),
            DataY=[10] * 10,
            NSpec=1,
        )

        fig, ax = plt.subplots(
            figsize=(10, 6.5258), nrows=1, ncols=1, num="A Most Excellent Plot", subplot_kw={"projection": "mantid"}
        )
        ax.plot(wksp, specNum=1, label="good")
        ax.legend()

        self.figure = fig
        self.canvas = MantidFigureCanvas(self.figure)

        self.navigation_toolbar = WorkbenchNavigationToolbar(self.canvas, self)

        self.grid.addWidget(self.navigation_toolbar)
        self.grid.addWidget(self.canvas)

        self.adjustSize()
