import os
import traceback
from pathlib import Path
from typing import Tuple

from snapred.backend.log.logger import snapredLogger

logger = snapredLogger.getLogger(__name__)


class StateValidationException(Exception):
    "Raised when an Instrument State is invalid"

    def __init__(self, exception: Exception):
        exceptionStr = str(exception)
        tb = exception.__traceback__

        if tb is not None:
            tb_info = traceback.extract_tb(tb)
            if tb_info:
                filePath = tb_info[-1].filename
                lineNumber = tb_info[-1].lineno
                functionName = tb_info[-1].name
            else:
                filePath, lineNumber, functionName = None, lineNumber, functionName
        else:
            filePath, lineNumber, functionName = None, None, None

        doesFileExist, hasWritePermission = self._checkFileAndPermissions(filePath)

        if filePath and doesFileExist and hasWritePermission:
            self.message = f"The following error occurred:{exceptionStr}\n\n" "Please contact your IS or CIS."
        elif filePath and doesFileExist:
            self.message = f"You do not have write permissions: {filePath}"
        elif filePath:
            self.message = f"The file does not exist: {filePath}"
        else:
            self.message = "Instrument State for given Run Number is invalid! (see logs for details.)"

        logger.error(exceptionStr)
        super().__init__(self.message)

    @staticmethod
    def _checkFileAndPermissions(filePath) -> Tuple[bool, bool]:
        if filePath is None:
            return False, False
        fileExists = Path(filePath).exists()
        writePermission = os.access(filePath, os.W_OK) if fileExists else False
        return fileExists, writePermission
