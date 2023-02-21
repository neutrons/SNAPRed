# init custom logger
# Date : Time : Level : Module.Class : Machine/Node : Message
import logging
from snapred.meta.Config import Config

class _SnapRedLogger:
    _format = Config['logging.format']
    def __init__(self):
        pass

    def getLogger(self, name):
        logger = logging.getLogger(name)
        ch = logging.StreamHandler()

        # create formatter and add it to the handlers
        formatter = logging.Formatter(self._format)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        return logger

snapredLogger = _SnapRedLogger()