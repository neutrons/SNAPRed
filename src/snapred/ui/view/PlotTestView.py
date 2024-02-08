import matplotlib.pyplot as plt
from mantid.simpleapi import CreateSampleWorkspace, Rebin
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from qtpy.QtWidgets import QGridLayout, QMainWindow, QPushButton, QWidget
from workbench.plotting.figuremanager import MantidFigureCanvas
from workbench.plotting.toolbar import WorkbenchNavigationToolbar


class PlotTestView(QMainWindow):
    position = 1

    def __init__(self, parent=None):
        super(PlotTestView, self).__init__(parent)
        self.centralWidget = QWidget(self)
        self.setCentralWidget(self.centralWidget)

        self.grid = QGridLayout()
        self.grid.columnStretch(2)
        self.grid.rowStretch(1)
        self.centralWidget.setLayout(self.grid)

        wksp = CreateSampleWorkspace(
            Function="Powder Diffraction",
            XUnit="dSpacing",
            NumBanks=1,
            Xmin=1,
            Xmax=10000,
            BinWidth=1,
            BankPixelWidth=1,
        )
        wksp = Rebin(
            InputWorkspace=wksp,
            Params=(1, -0.01, 10000),
            BinningMode="Logarithmic",
        )

        fig, ax = plt.subplots(
            figsize=(10, 6.5258),
            nrows=1,
            ncols=1,
            subplot_kw={"projection": "mantid"},
        )
        ax.plot(wksp, specNum=1, label="good", normalize_by_bin_width=True)
        ax.legend()

        self.figure = fig
        self.canvas = MantidFigureCanvas(self.figure)

        self.navigation_toolbar = WorkbenchNavigationToolbar(self.canvas, self)

        self.recalculationButton = QPushButton("Recalculate")
        self.recalculationButton.clicked.connect(self.emitValueChange)

        self.grid.addWidget(self.navigation_toolbar)
        self.grid.addWidget(self.canvas)
        self.grid.addWidget(self.recalculationButton)

        self.adjustSize()

    def emitValueChange(self):
        wksp = CreateSampleWorkspace(
            Function="Powder Diffraction",
            XUnit="dSpacing",
            NumBanks=1,
            Xmin=10,
            Xmax=1000,
            BinWidth=1,
            BankPixelWidth=1,
        )
        wksp = Rebin(
            InputWorkspace=wksp,
            Params=(10, -0.01, 1000),
            BinningMode="Logarithmic",
        )

        self.figure.clear()
        ax = self.figure.add_subplot(1, 1, 1, projection="mantid")
        ax.plot(wksp, wkspIndex=0, label="gooder", normalize_by_bin_width=True)
        ax.legend()
        self.canvas.draw()
