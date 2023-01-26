# init custom logger
# Date : Time : Level : Module.Class : Machine/Node : Message
import logging

class _SnapRedLogger:
    def __init__(self):
        pass

    def getLogger(self, name):
        logger = logging.getLogger(name)
        ch = logging.StreamHandler()

        # create formatter and add it to the handlers
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        return logger

snapredLogger = _SnapRedLogger()