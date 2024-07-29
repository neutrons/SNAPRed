import sys
import traceback

from mantidqt.widgets.algorithmprogress import AlgorithmProgressWidget
from qtpy.QtCore import Qt, QTimer, Slot
from qtpy.QtWidgets import (
    QApplication,
    QMainWindow,
    QMessageBox,
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
        self.calibrationPanelButton = QPushButton("Open Calibration Panel")
        self.calibrationPanelButton.clicked.connect(self.openCalibrationPanel)
        splitter.addWidget(self.calibrationPanelButton)

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

    @Slot()
    def openCalibrationPanel(self):
        try:
            self.calibrationPanel = TestPanel(self)
            self.calibrationPanel.widget.setWindowTitle("Calibration Panel")
            self.calibrationPanel.widget.show()
        except Exception as e:  # noqa: BLE001
            # Print traceback
            traceback.print_exception(e)

            # Additionally, show error message as popup
            msg = "Sorry!  Error encountered while opening Calibration Panel.\n"
            msg = msg + "This is usually caused by an issue with the file tree.\n"
            msg = msg + "An expert user can correct this by editing the application.yml file.\n"
            msg = msg + "Contact your IS or CIS for help in resolving this issue."

            # Note: specifically using the static method `QMessageBox.critical` here helps with automated testing.
            # (That is, we can "mock.patch" just that method, and not patch the entire `QMessageBox` class.)
            QMessageBox.critical(
                None,
                "Error",
                msg,
            )

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
