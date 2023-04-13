import sys

from mantidqt.widgets.algorithmprogress import AlgorithmProgressWidget
from mantidqt.widgets.workspacewidget.workspacetreewidget import WorkspaceTreeWidget
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import (
    QApplication,
    QHBoxLayout,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QSplitter,
    QStatusBar,
    QVBoxLayout,
    QWidget,
)
from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Resource
from snapred.ui.widget.LogTable import LogTable
from snapred.ui.widget.ToolBar import ToolBar
from snapred.ui.widget.TestPanel import TestPanel

logger = snapredLogger.getLogger(__name__)


class SNAPRedGUI(QMainWindow):
    def __init__(self, parent=None):
        super(SNAPRedGUI, self).__init__(parent)
        self.setAttribute(Qt.WA_TranslucentBackground, True)
        self.setAttribute(Qt.WA_DontCreateNativeAncestors, True)
        logTable = LogTable("load dummy", self)
        splitter = QSplitter(Qt.Vertical)
        splitter.addWidget(logTable.widget)
        # add button to open new window
        button = QPushButton("Open Test Panel")
        button.clicked.connect(self.openNewWindow)
        splitter.addWidget(button)
        # myiv = get_instrumentview(ws)
        # myiv.show_view()

        # import pdb; pdb.set_trace()
        workspaceTreeWidgetWrapper = QWidget()
        workspaceTreeWidgetWrapperLayout = QHBoxLayout()
        workspaceTreeWidget = WorkspaceTreeWidget()
        workspaceTreeWidgetWrapper.setObjectName("workspaceTreeWidget")
        workspaceTreeWidgetWrapperLayout.addWidget(workspaceTreeWidget, alignment=Qt.AlignCenter)
        workspaceTreeWidgetWrapper.setLayout(workspaceTreeWidgetWrapperLayout)
        splitter.addWidget(workspaceTreeWidgetWrapper)
        splitter.addWidget(AlgorithmProgressWidget())

        centralWidget = QWidget()
        centralWidget.setObjectName("centralwidget")
        centralLayout = QVBoxLayout()
        centralWidget.setLayout(centralLayout)

        self.setCentralWidget(centralWidget)
        self.setWindowTitle("SNAPRed")
        self.setWindowFlags(self.windowFlags() | Qt.FramelessWindowHint)

        self.titleBar = ToolBar(centralWidget)
        centralLayout.addWidget(self.titleBar.widget)
        centralLayout.addWidget(splitter)

        self.setContentsMargins(0, self.titleBar.widget.height(), 0, 0)

        self.resize(320, self.titleBar.widget.height() + 480)
        self.setMinimumSize(320, self.titleBar.widget.height() + 480)

        self.statusBar = QStatusBar()
        self.setStatusBar(self.statusBar)

    def openNewWindow(self):
        self.newWindow = TestPanel(self)
        self.newWindow.widget.setWindowTitle("Test Panel")
        self.newWindow.widget.show()

    def closeEvent(self, event):
        event.accept()

    def changeEvent(self, event):
        if event.type() == event.WindowStateChange:
            self.titleBar.presenter.windowStateChanged(self.windowState())

    def resizeEvent(self, event):
        self.titleBar.presenter.resizeEvent(event)


def qapp():
    if QApplication.instance():
        _app = QApplication.instance()
    else:
        _app = QApplication(sys.argv)
    return _app


if __name__ == "__main__":
    app = qapp()
    with Resource.open("style.qss", "r") as styleSheet:
        app.setStyleSheet(styleSheet.read())
    try:
        ex = SNAPRedGUI()

        # ex.resize(700, 700)
        asciiPath = "ascii.txt"
        with Resource.open(asciiPath, "r") as asciiArt:
            print(asciiArt.read())
        logger.info("Welcome User! Happy Reducing!")
        ex.show()
        ret = app.exec_()
        sys.exit(ret)

    except Exception as uncaughtError:  # noqa: BLE001
        ex = QWidget()
        QMessageBox.critical(ex, "Uncaught Error!", str(uncaughtError))
