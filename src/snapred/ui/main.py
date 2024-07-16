import sys

from mantidqt.widgets.algorithmprogress import AlgorithmProgressWidget
from qtpy.QtCore import Qt, QTimer
from qtpy.QtWidgets import (
    QApplication,
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

# Conditionally import UserDocsButton (temporary)
try:
    from snapred.ui.widget.UserDocsButton import UserDocsButton
except ImportError:
    UserDocsButton = None


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
        button = QPushButton("Open Calibration Panel")
        button.clicked.connect(self.openNewWindow)
        splitter.addWidget(button)

        # myiv = get_instrumentview(ws)
        # myiv.show_view()

        # import pdb; pdb.set_trace()
        workspaceWidget = WorkspaceWidget(self)
        splitter.addWidget(workspaceWidget)

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

        if UserDocsButton:
            self.userDocButton = UserDocsButton(self)
            splitter.addWidget(self.userDocButton)
        else:
            print("UserDocsButton is not available. Skipping its initialization.")

    def openNewWindow(self):
        try:
            self.newWindow = TestPanel(self)
            self.newWindow.widget.setWindowTitle("Calibration Panel")
            self.newWindow.widget.show()
        except Exception as e:  # noqa: BLE001
            # show error message as popup
            import traceback

            traceback.print_exception(e)
            from qtpy.QtWidgets import QMessageBox

            msg = "Sorry!  Error encountered while opening Calibration Panel.\n"
            msg = msg + "This is usually caused by an issue with the file tree.\n"
            msg = msg + "An expert user can correct this by editing the application.yml file.\n"
            msg = msg + "Contact your IS or CIS for help in resolving this issue."
            errorPopup = QMessageBox()
            errorPopup.setIcon(QMessageBox.Critical)
            errorPopup.setText(msg)
            errorPopup.setDetailedText(str(e))
            errorPopup.setFixedSize(500, 200)
            errorPopup.exec()

    def closeEvent(self, event):
        event.accept()

    def changeEvent(self, event):
        if event.type() == event.WindowStateChange:
            self.titleBar.presenter.windowStateChange(self.windowState())

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
            logger.warning(f"Closing in {SECONDS} seconds")
            QTimer.singleShot(SECONDS * 1000, lambda: app.exit(0))
        return app.exec()

    except Exception:
        logger.exception("Uncaught Error bubbled up to main!")
        return -1
