import logging
import sys
import traceback
from pathlib import Path
from typing import List

from mantid.kernel import ConfigService
from mantid.utils.logging import log_to_python
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

from snapred.backend.log.logger import CustomFormatter, snapredLogger
from snapred.meta.Config import Config, Resource, datasearch_directories, fromMantidLoggingLevel, fromPythonLoggingLevel
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
DATASEARCH_DIR_KEY = "datasearch.directories"
DEFAULT_FACILITY_KEY = "default.facility"
FILEEVENTDATALISTENER_FILENAME_KEY = "fileeventdatalistener.filename"
FILEEVENTDATALISTENER_CHUNKS_KEY = "fileeventdatalistener.chunks"


# This method is also used by tests, so it's declared outside of the `SNAPRedGUI` class
def prependDataSearchDirectories() -> List[str]:
    """data-search directories to prepend to
    mantid.kernel.ConfigService 'datasearch.directories'
    """
    searchDirectories = []
    if Config["IPTS.root"] != Config["IPTS.default"]:
        searchDirectories = datasearch_directories(Path(Config["instrument.home"]))
    return searchDirectories


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

        # Save any overridden Mantid ConfigService values
        self._mantidConfig = ConfigService.Instance()
        self._savedMantidConfigEntries = dict()

        # Redirect the IPTS search directories
        self._redirectIPTSSearchDirectories()

        # Add any required live-data config values
        self._addLiveDataMantidConfigEntries()

        # make sure mantid console logging is disabled
        self._redirectMantidConsoleLog()

        # add button to open new window
        self.calibrationPanelButton = QPushButton("Open Calibration Panel")
        self.calibrationPanelButton.clicked.connect(self.openCalibrationPanel)
        splitter.addWidget(self.calibrationPanelButton)

        self.workspaceWidget = WorkspaceWidget(self)
        splitter.addWidget(self.workspaceWidget)

        splitter.addWidget(AlgorithmProgressWidget())

        if Config["cis_mode.reloadConfigButton"]:
            self.reloadConfigButton = QPushButton("Reload Config")

            def reloadAndInform():
                Config.reload()
                # Inform the user that the configuration has been reloaded
                QMessageBox.information(
                    self,
                    "Configuration Reloaded",
                    f"Env {Config.getCurrentEnv()} configuration has been successfully reloaded.",
                )

            self.reloadConfigButton.clicked.connect(reloadAndInform)
            splitter.addWidget(self.reloadConfigButton)

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

        # Check for incompatible `Config` settings.
        Config.validate()

    @property
    def _streamlevel(self):
        return Config["logging.mantid.stream.level"]

    @property
    def _filelevel(self):
        return Config["logging.mantid.file.level"]

    @property
    def _outputfile(self):
        return Config["logging.mantid.file.output"]

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
        self._restartMantidConsoleLog()
        self._restoreIPTSSearchDirectories()
        self._restoreLiveDataMantidConfigEntries()
        event.accept()

    def changeEvent(self, event):
        if event.type() == event.WindowStateChange:
            self.titleBar.presenter.windowStateChange(self.windowState())

    def resizeEvent(self, event):
        self.titleBar.presenter.resizeEvent(event)

    def _redirectIPTSSearchDirectories(self):
        # Save the current data-search directories
        self._savedMantidConfigEntries[DATASEARCH_DIR_KEY] = self._mantidConfig[DATASEARCH_DIR_KEY]
        self._mantidConfig.setDataSearchDirs(
            prependDataSearchDirectories() + list(self._mantidConfig.getDataSearchDirs())
        )

    def _restoreIPTSSearchDirectories(self):
        self._mantidConfig.setString(DATASEARCH_DIR_KEY, self._savedMantidConfigEntries[DATASEARCH_DIR_KEY])

    def _addLiveDataMantidConfigEntries(self):
        # Save any current default-facility setting:
        self._savedMantidConfigEntries[DEFAULT_FACILITY_KEY] = self._mantidConfig[DEFAULT_FACILITY_KEY]

        # Add the required default-facility value:
        self._mantidConfig.setString(DEFAULT_FACILITY_KEY, Config["liveData.facility.name"])

        if Config["liveData.facility.name"] == "TEST_LIVE":
            # Save any current values from the file-listener keys:
            self._savedMantidConfigEntries[FILEEVENTDATALISTENER_FILENAME_KEY] = self._mantidConfig[
                FILEEVENTDATALISTENER_FILENAME_KEY
            ]
            self._savedMantidConfigEntries[FILEEVENTDATALISTENER_CHUNKS_KEY] = self._mantidConfig[
                FILEEVENTDATALISTENER_CHUNKS_KEY
            ]

            # Add the required 'ADARA_FileReader' values:
            self._mantidConfig.setString(FILEEVENTDATALISTENER_FILENAME_KEY, Config["liveData.testInput.inputFilename"])
            self._mantidConfig.setString(FILEEVENTDATALISTENER_CHUNKS_KEY, str(Config["liveData.testInput.chunks"]))

    def _restoreLiveDataMantidConfigEntries(self):
        # Restore any previous default-facility setting:
        self._mantidConfig.setString(DEFAULT_FACILITY_KEY, self._savedMantidConfigEntries[DEFAULT_FACILITY_KEY])

        if Config["liveData.facility.name"] == "TEST_LIVE":
            # Restore any previous values to the file-listener keys:
            self._mantidConfig.setString(
                FILEEVENTDATALISTENER_FILENAME_KEY, self._savedMantidConfigEntries[FILEEVENTDATALISTENER_FILENAME_KEY]
            )
            self._mantidConfig.setString(
                FILEEVENTDATALISTENER_CHUNKS_KEY, self._savedMantidConfigEntries[FILEEVENTDATALISTENER_CHUNKS_KEY]
            )

    def _redirectMantidConsoleLog(self):
        # Save the current Mantid logging configuration
        self._savedMantidConfigEntries[LOGGERCLASSKEY] = self._mantidConfig[LOGGERCLASSKEY]
        self._savedMantidConfigEntries[LOGGERLEVELKEY] = self._mantidConfig[LOGGERLEVELKEY]

        logger = logging.getLogger("Mantid")
        if ConfigService.getLogLevel().lower() == "debug" and _within_mantid():
            logger.warning(
                "WARNING: 'messages' pane is set to 'DEBUG' logging level: this may cause LOCK UP in live-data mode!"
            )

        # Configure Mantid to send messages to Python.

        # Despite what the documentation says:
        #   the level passed in here _only_ controls the log-level in the "messages" panel.
        #     For live-data, it's recommended _not_ to set this to *debug*!
        log_to_python(level=fromPythonLoggingLevel(Config["logging.mantid.root.level"]))

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

    def _restartMantidConsoleLog(self):
        # return logging to the stream
        ConfigService.setString(LOGGERCLASSKEY, self._savedMantidConfigEntries[LOGGERCLASSKEY])
        ConfigService.setString(LOGGERLEVELKEY, self._savedMantidConfigEntries[LOGGERLEVELKEY])

        # TODO: Question: Possibly 'ConfigService.setLogLevel()' should be used here instead?
        logger = logging.getLogger("Mantid")
        logger.setLevel(fromMantidLoggingLevel(self._mantidConfig[LOGGERLEVELKEY]))
        logger.handlers.clear()


# ------ Duplicate `mantidqt.gui_helper.get_qapplication`, but as two separate methods: ------


# Be careful here: the _key_ pytest-qt fixture is named `qapp`!
def _qapp():
    if QApplication.instance():
        _app = QApplication.instance()
    else:
        _app = QApplication(sys.argv)
    return _app


def _within_mantid():
    return _qapp().applicationName().lower().startswith("mantid")


# ------


def start(options=None):
    logger = snapredLogger.getLogger(__name__)

    app = _qapp()
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
