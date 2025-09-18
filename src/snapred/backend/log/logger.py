# init custom logger
# Date : Time : Level : Module.Class : Machine/Node : Message
import logging
import logging.handlers
import os
import socket
import sys
from pathlib import Path

from mantid.api import Progress

from snapred.meta.Config import Config
from snapred.meta.decorators.Singleton import Singleton

logger = logging.getLogger(__name__)


class CustomFormatter(logging.Formatter):
    _grey = "\x1b[38;20m"
    _yellow = "\x1b[33;20m"
    _red = "\x1b[31;20m"
    _bold_red = "\x1b[31;1m"
    _reset = "\x1b[0m"
    _defaults = {"hostname": socket.gethostname().split(".")[0]}

    def __init__(self, name="SNAP.stream"):
        self._name = name
        self.FORMATS = {
            logging.DEBUG: self._grey + self._format + self._reset,
            logging.INFO: self._grey + self._format + self._reset,
            logging.WARNING: self._yellow + self._format + self._reset,
            logging.ERROR: self._red + self._format + self._reset,
            logging.CRITICAL: self._bold_red + self._format + self._reset,
        }

    @property
    def _format(self):
        return Config[f"logging.{self._name}.format"]

    @property
    def _datefmt(self):
        return Config[f"logging.{self._name}.datefmt"]

    def _colorCodeFormat(self, rawFormat):
        fields = rawFormat.split("-")
        fields[0] = "\033[38:2:0:{r}:{g}:{b}m".format(r=136, g=51, b=46) + fields[0]
        fields[-1] = "\033[00m" + fields[-1]
        return "-".join(fields)

    def format(self, record):  # noqa: A003
        logFormat = (
            self.FORMATS.get(record.levelno) if record.levelno > logging.INFO else self._colorCodeFormat(self._format)
        )
        formatter = logging.Formatter(logFormat, datefmt=self._datefmt, defaults=self._defaults)
        return formatter.format(record)


@Singleton
class _SnapRedLogger:
    _warnings = []
    _handler = None
    _handler_name = Config["logging.SNAP.stream.handler.name"]

    def __init__(self):
        self.resetProgress(0, 1.0, 100)
        if not bool(_SnapRedLogger._handler):
            # Create a single handler serving all of the SNAPRed loggers.
            _SnapRedLogger._handler = logging.StreamHandler(sys.stdout)
            _SnapRedLogger._handler.setFormatter(CustomFormatter())
            # Name it so we can identify it again.
            _SnapRedLogger._handler.name = _SnapRedLogger._handler_name

    @property
    def _level(self):
        return Config["logging.SNAP.stream.level"]

    def _warning(self, message, vanillaWarn, **kwargs):
        self._warnings.append(message)
        vanillaWarn(message, **kwargs)

    def resetProgress(self, start, end, nreports):
        self._progressReporter = Progress(None, start=start, end=end, nreports=nreports)
        self._progressCounter = 0

    def getProgress(self):
        return self._progressReporter

    def reportProgress(self, message):
        if self._progressReporter is not None:
            self._progressReporter.report(self._progressCounter, message)
            self._progressCounter += 1

    def getWarnings(self):
        return self._warnings

    def hasWarnings(self):
        return len(self._warnings) > 0

    def clearWarnings(self):
        self._warnings = []

    def getLogger(self, name):
        logger = logging.getLogger(name)
        logger.setLevel(self._level)
        logger.addHandler(_SnapRedLogger._handler)

        # TODO: this isn't really the best way to derive a class from `Logger`!
        #   `logging.getLogger` does cause an issue, but the standard approach is to
        #   subclass `logging.Adapter`.
        vanillaWarn = logger.warning
        logger.warning = lambda message, **kwargs: self._warning(message, vanillaWarn, **kwargs)
        logger.warn = lambda message, **kwargs: self._warning(message, vanillaWarn, **kwargs)

        return logger


snapredLogger = _SnapRedLogger()


def getIPCSocketPath(name: str, PID: int | str) -> Path:
    # The path to be used for an the IPC logging handler socket:
    # -- unless overridden, this path will be: ${XDG_RUNTIME_DIR}/sock-${name}-${PID};
    # -- alternatively: ${TMPDIR} is used on macOS.
    path = None
    try:
        path_fmt = Config["logging.IPC.socket_path"]
        runtime_dir = os.environ["XDG_RUNTIME_DIR"] if "XDG_RUNTIME_DIR" in os.environ else os.environ["TMPDIR"]
        path = Path(path_fmt.format(XDG_RUNTIME_DIR=runtime_dir, name=name, PID=str(PID)))
    except KeyError:
        # Github runners may not have either of the temporary directories: we do not care.
        pass
    return path


def getIPCHandler(name: str) -> logging.Handler:
    # Initialize and return a unix-domain socket (UDS) based IPC logging handler.
    handler = None
    try:
        PID = os.getpid()
        socketPath = getIPCSocketPath(name, PID)
        if bool(socketPath):
            logger.info(f"Initializing IPC logging handler at '{socketPath}'.")
            handler = logging.handlers.SocketHandler(str(socketPath), None)
            handler.setLevel(Config[f"logging.IPC.handlers.{name}.level"])
            handler.setFormatter(CustomFormatter(name=f"IPC.handlers.{name}"))

            # Name it so we can identify it again.
            handler.name = "IPC-" + name
    except OSError as e:
        logger.warning(f"Error when creating socket at '{socketPath}':\n   {str(e)}.")
    return handler


#######################################################################
## Initialize any required IPC handlers when this module is imported.##
#######################################################################

if "IPC" in Config["logging"]:
    for handlerName in Config["logging.IPC.handlers"]:
        handler = getIPCHandler(handlerName)
        if bool(handler):
            for name in Config[f"logging.IPC.handlers.{handlerName}.loggers"]:
                # Do not use `snapredLogger.getLogger` here:
                #   otherwise its handler will be added twice!
                logger = logging.getLogger(name)
                logger.addHandler(handler)
        else:
            logger.warning(f"Could not create IPC logging handler '{handlerName}'.")
