# init custom logger
# Date : Time : Level : Module.Class : Machine/Node : Message
import logging
import socket
import sys

from mantid.api import Progress

from snapred.meta.Config import Config
from snapred.meta.decorators.Singleton import Singleton


class CustomFormatter(logging.Formatter):
    _grey = "\x1b[38;20m"
    _yellow = "\x1b[33;20m"
    _red = "\x1b[31;20m"
    _bold_red = "\x1b[31;1m"
    _reset = "\x1b[0m"
    _host = socket.gethostname().split(".")[0]

    def __init__(self, name="SNAP.stream"):
        self._format = self._host + " - " + Config[f"logging.{name}.format"]

        self.FORMATS = {
            logging.DEBUG: self._grey + self._format + self._reset,
            logging.INFO: self._grey + self._format + self._reset,
            logging.WARNING: self._yellow + self._format + self._reset,
            logging.ERROR: self._red + self._format + self._reset,
            logging.CRITICAL: self._bold_red + self._format + self._reset,
        }

    def _colorCodeFormat(self, rawFormat):
        fields = rawFormat.split("-")
        fields[0] = "\033[38:2:0:{r}:{g}:{b}m".format(r=136, g=51, b=46) + fields[0]
        fields[-1] = "\033[00m" + fields[-1]
        return "-".join(fields)

    def format(self, record):  # noqa: A003
        logFormat = (
            self.FORMATS.get(record.levelno) if record.levelno > logging.INFO else self._colorCodeFormat(self._format)
        )
        formatter = logging.Formatter(logFormat)
        return formatter.format(record)


@Singleton
class _SnapRedLogger:
    _level = Config["logging.SNAP.stream.level"]
    _warnings = []

    def __init__(self):
        self.resetProgress(0, 1.0, 100)

    def _setFormatter(self, logger):
        ch = logging.StreamHandler(sys.stdout)

        # create formatter and add it to the handler
        formatter = CustomFormatter()
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        return logger

    def _warning(self, message, vanillaWarn):
        self._warnings.append(message)
        vanillaWarn(message)

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
        self._setFormatter(logger)
        vanillaWarn = logger.warning
        logger.warning = lambda message: self._warning(message, vanillaWarn)
        logger.warn = lambda message: self._warning(message, vanillaWarn)
        return logger


snapredLogger = _SnapRedLogger()
