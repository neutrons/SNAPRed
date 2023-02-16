from dataclasses import dataclass
from typing import Optional

# https://docs.python.org/3/library/dataclasses.html
@dataclass
class RunConfig:
    """Class to hold the instrument configuration."""
    runNumber: str
    IPTS: Optional[str] = None
    maskFileName: Optional[str] = None
    maskFileDirectory: Optional[str] = None
    gsasFileDirectory: Optional[str] = None
    calibrationState: Optional[str] = None

    # if we need specific getter and setter methods, we can use the @property decorator
    # https://docs.python.org/3/library/functions.html#property
    #
    # @property
    # def key(self) -> str:
    #     return self._key

    # @name.setter
    # def key(self, v: str) -> None:
    #     self._key = v