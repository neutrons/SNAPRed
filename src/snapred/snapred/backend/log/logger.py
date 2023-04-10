# init custom logger
# Date : Time : Level : Module.Class : Machine/Node : Message
import logging
import socket

from mantid.kernel import ConfigService

from snapred.meta.Config import Config


class _SnapRedLogger:
    _host = socket.gethostname().split(".")[0]
    _format = _host + " - " + Config["logging.SNAP.format"]
    _level = Config["logging.level"]
    _mantidFormat = _host + " - " + Config["logging.mantid.format"]

    _logLevels = dict([(50, "critical"), (40, "error"), (30, "warning"), (20, "info"), (10, "debug"), (0, "notset")])

    def __init__(self):
        config = ConfigService.Instance()
        config["logging.loggers.root.level"] = self._logLevelToString(self._level)
        config["logging.channels.consoleChannel.formatter"] = "f1"
        # Output only the message text and let Python take care of formatting.
        config["logging.formatters.f1.class"] = "PatternFormatter"
        config["logging.formatters.f1.pattern"] = self._mantidFormat

        # flip flop class to trigger update to mantid logger
        config["logging.channels.consoleChannel.class"] = "PythonLoggingChannel"
        config["logging.channels.consoleChannel.class"] = "StdoutChannel"

    def _logLevelToString(self, code):
        return self._logLevels[code]

    def getLogger(self, name):
        logger = logging.getLogger(name)
        logging.DEBUG
        logger.setLevel(self._level)
        ch = logging.StreamHandler()

        # create formatter and add it to the handlers
        formatter = logging.Formatter(self._format)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        return logger


snapredLogger = _SnapRedLogger()
