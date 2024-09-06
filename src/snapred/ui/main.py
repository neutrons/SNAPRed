import logging
import sys
import traceback

from mantid.kernel import ConfigService
from mantid.utils.logging import log_to_python
from mantidqt.widgets.algorithmprogress import AlgorithmProgressWidget
from qtpy.QtCore import QEvent, QObject, QPoint, Qt, QTimer, Slot
from qtpy.QtGui import QCursor, QMouseEvent
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

from snapred.backend.log.logger import CustomFormatter, snapredLogger
from snapred.meta.Config import Config, Resource
from snapred.ui.widget.LogTable import LogTable
from snapred.ui.widget.TestPanel import TestPanel
from snapred.ui.widget.ToolBar import ToolBar

# Conditionally import UserDocsButton (temporary)
try:
    from snapred.ui.widget.UserDocsButton import UserDocsButton
except ImportError:
    UserDocsButton = None


LOGGERCLASSKEY = "logging.channels.consoleChannel.class"
LOGGERLEVELKEY = "logging.loggers.root.level"

logger = snapredLogger.getLogger(__name__)


class KeyPressEater(QObject):
    def eventFilter(self, obj, event):
        logger.info("Event Filter")
        return True


def widgets_at(pos):
    """Return ALL widgets at `pos`
    Arguments:
            pos (QPoint): Position at which to get widgets
    """

    widgets = []
    widget_at = qapp().widgetAt(pos)

    while widget_at:
        widgets.append(widget_at)

        # Make widget invisible to further enquiries
        widget_at.setAttribute(Qt.WA_TransparentForMouseEvents)
        widget_at = qapp().widgetAt(pos)

    # Restore attribute
    for widget in widgets:
        widget.setAttribute(Qt.WA_TransparentForMouseEvents, False)

    return widgets


class SNAPRedGUI(QMainWindow):
    _streamlevel = Config["logging.mantid.stream.level"]
    _filelevel = Config["logging.mantid.file.level"]
    _outputfile = Config["logging.mantid.file.output"]

    def __init__(self, parent=None, window_flags=None, translucentBackground=False):
        super(SNAPRedGUI, self).__init__(parent)
        if window_flags:
            self.setWindowFlags(window_flags)
        self.setAttribute(Qt.WA_TranslucentBackground, translucentBackground)
        self.setAttribute(Qt.WA_DontCreateNativeAncestors, True)
        logTable = LogTable("load dummy", self)
        splitter = QSplitter(Qt.Vertical)
        splitter.addWidget(logTable.widget)

        # make sure mantid console logging is disabled
        self.redirectMantidConsoleLog()

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
        self.restartMantidConsoleLog()
        event.accept()

    def changeEvent(self, event):
        if event.type() == event.WindowStateChange:
            self.titleBar.presenter.windowStateChange(self.windowState())

    def resizeEvent(self, event):
        self.titleBar.presenter.resizeEvent(event)

    def redirectMantidConsoleLog(self):
        # Configure Mantid to send messages to Python
        log_to_python()
        logger = logging.getLogger("Mantid")
        # NOTE it is necessary to set the log to a nonzero level before adding handlers
        logger.setLevel(logging.DEBUG)

        # the stream handler will print alongside the SNAPRed logs
        ch = logging.StreamHandler(sys.stdout)
        streamformatter = CustomFormatter("mantid.stream")
        ch.setLevel(self._streamlevel)
        ch.setFormatter(streamformatter)

        # the file handler will print to an external file
        file = logging.FileHandler(self._outputfile, "w")
        fileformatter = CustomFormatter("mantid.file")
        file.setLevel(self._filelevel)
        file.setFormatter(fileformatter)

        logger.addHandler(file)
        logger.addHandler(ch)

    def restartMantidConsoleLog(self):
        # return logging to the stream
        ConfigService.setString(LOGGERCLASSKEY, "PythonLoggingChannel")
        ConfigService.setString(LOGGERLEVELKEY, "debug")
        logger = logging.getLogger("Mantid")
        logger.setLevel(logging.DEBUG)
        logger.handlers.clear()


def qapp():
    if QApplication.instance():
        _app = QApplication.instance()
    else:

        class NotifyApplication(QApplication):
            eventRecord = []

            def notify(self, object: QObject, event: QEvent):
                if event.type() == QEvent.KeyPress:
                    print("Key Pressed" + str(event.key()))
                if event.type() == QEvent.MouseButtonPress:
                    print("Mouse Coordinate" + str(QCursor.pos()))

                return super().notify(object, event)

        _app = NotifyApplication(sys.argv)
    logger.info("Filter installed")
    return _app


def start(options=None):
    app = qapp()
    with Resource.open("style.qss", "r") as styleSheet:
        app.setStyleSheet(styleSheet.read())

    logger.info("Welcome User! Happy Reducing!")
    try:
        ex = SNAPRedGUI(translucentBackground=True)
        ex.show()

        def clickScreen(coords=(901, 377)):
            QCursor.setPos(*coords)
            target = qapp().widgetAt(*coords)
            event = QMouseEvent(
                QEvent.MouseButtonPress,
                target.mapFromGlobal(QPoint(*coords)),
                Qt.LeftButton,
                Qt.LeftButton,
                Qt.NoModifier,
            )
            qapp().sendEvent(target, event)
            event = QMouseEvent(
                QEvent.MouseButtonRelease,
                target.mapFromGlobal(QPoint(*coords)),
                Qt.LeftButton,
                Qt.LeftButton,
                Qt.NoModifier,
            )
            qapp().sendEvent(target, event)
            # shuld build in smething t cnfirm the intended target was clicked
            print("target" + str(target))

        # wait 3 seconds
        QTimer.singleShot(3000, lambda: clickScreen())
        QTimer.singleShot(3500, lambda: clickScreen((1043, 900)))
        QTimer.singleShot(4000, lambda: clickScreen((1718, 704)))

        if options.headcheck:
            SECONDS = 3  # arbitrarily chosen
            logger.warning(f"Closing in {SECONDS} seconds")
            QTimer.singleShot(SECONDS * 1000, lambda: app.exit(0))
        return app.exec()

    except Exception:
        logger.exception("Uncaught Error bubbled up to main!")
        return -1
