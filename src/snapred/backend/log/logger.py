# init custom logger
# Date : Time : Level : Module.Class : Machine/Node : Message
import logging
import socket

from mantid.utils.logging import log_to_python

from snapred.meta.Config import Config

# Configure Mantid to send messages to Python
log_to_python()


class CustomFormatter(logging.Formatter):
    _grey = "\x1b[38;20m"
    _yellow = "\x1b[33;20m"
    _red = "\x1b[31;20m"
    _bold_red = "\x1b[31;1m"
    _reset = "\x1b[0m"
    _host = socket.gethostname().split(".")[0]
    _format = _host + " - " + Config["logging.SNAP.format"]

    FORMATS = {
        logging.DEBUG: _grey + _format + _reset,
        logging.INFO: _grey + _format + _reset,
        logging.WARNING: _yellow + _format + _reset,
        logging.ERROR: _red + _format + _reset,
        logging.CRITICAL: _bold_red + _format + _reset,
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


class _SnapRedLogger:
    _level = Config["logging.level"]

    def __init__(self):
        pass

    def getLogger(self, name):
        logger = logging.getLogger(name)
        logger.setLevel(self._level)
        ch = logging.StreamHandler()

        # create formatter and add it to the handlers
        formatter = CustomFormatter()
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        return logger


snapredLogger = _SnapRedLogger()
snapredLogger.getLogger("Mantid")
