import sys

from mantidqt.widgets.algorithmprogress import AlgorithmProgressWidget
from qtpy.QtCore import Qt, QTimer
from qtpy.QtWidgets import (
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
    def __init__(self, parent=None, window_flags=None, translucentBackground=False):
        super(SNAPRedGUI, self).__init__(parent)
        if window_flags:
            self.setWindowFlags(window_flags)
        self.setAttribute(Qt.WA_TranslucentBackground, translucentBackground)
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
        try:
            self.newWindow = TestPanel(self)
            self.newWindow.widget.setWindowTitle("Test Panel")
            self.newWindow.widget.show()
        except Exception as e:  # noqa: BLE001
            # show error message as popup
            print(e)
            from PyQt5.QtWidgets import QMessageBox

            errorPopup = QMessageBox()
            errorPopup.setIcon(QMessageBox.Critical)
            errorPopup.setText("Sorry!\nError In TestPanel!\nPlease try again avoiding whatever you just did.")
            errorPopup.setDetailedText(str(e))
            errorPopup.setFixedSize(500, 200)
            errorPopup.exec()

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


def start(options=None):
    logger = snapredLogger.getLogger(__name__)

    app = qapp()
    with Resource.open("style.qss", "r") as styleSheet:
        app.setStyleSheet(styleSheet.read())

    logger.info("Welcome User! Happy Reducing!")
    try:
        ex = SNAPRedGUI(translucentBackground=True)
        ex.show()

        if options.headcheck:
            SECONDS = 3  # arbitrarily chosen
            logger.warn(f"Closing in {SECONDS} seconds")
            QTimer.singleShot(SECONDS * 1000, lambda: app.exit(0))
        return app.exec()

    except Exception:
        logger.exception("Uncaught Error bubbled up to main!")
        return -1
