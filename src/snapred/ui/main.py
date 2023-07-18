import sys

from mantidqt.widgets.algorithmprogress import AlgorithmProgressWidget
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import (
    QApplication,
    QHBoxLayout,
    QMainWindow,
    QPushButton,
    QSplitter,
    QStatusBar,
    QVBoxLayout,
    QWidget,
)
from workbench.plugins.workspacewidget import WorkspaceWidget

from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Resource
from snapred.ui.widget.LogTable import LogTable
from snapred.ui.widget.TestPanel import TestPanel
from snapred.ui.widget.ToolBar import ToolBar


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
        workspaceWidgetWrapper = QWidget()
        workspaceWidgetWrapperLayout = QHBoxLayout()
        workspaceWidget = WorkspaceWidget(self)
        workspaceWidgetWrapper.setObjectName("workspaceTreeWidget")
        workspaceWidgetWrapperLayout.addWidget(workspaceWidget, alignment=Qt.AlignCenter)
        workspaceWidgetWrapper.setLayout(workspaceWidgetWrapperLayout)
        splitter.addWidget(workspaceWidgetWrapper)
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


def start(option=None):  # noqa: ARG001
    logger = snapredLogger.getLogger(__name__)

    app = qapp()
    with Resource.open("style.qss", "r") as styleSheet:
        app.setStyleSheet(styleSheet.read())

    logger.info("Welcome User! Happy Reducing!")
    try:
        ex = SNAPRedGUI()
        ex.show()

        return app.exec_()

    except Exception:
        logger.exception("Uncaught Error bubbled up to main!")
        return -1
