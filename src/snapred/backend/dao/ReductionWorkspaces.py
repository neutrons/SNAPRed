from mantid.api import Workspace

from dataclasses import dataclass

# https://docs.python.org/3/library/dataclasses.html
@dataclass
class ReductionWorkspaces:
    """Class to hold the instrument configuration."""
    rawWorkspace: Workspace
    calibrantWorkspace: Workspace
    calibrationGeometryWorkspace: Workspace
    rawVanadiumCorrectionWorkspace: Workspace
    calibrationMaskWorkspace: Workspace
    

    # if we need specific getter and setter methods, we can use the @property decorator
    # https://docs.python.org/3/library/functions.html#property
    #
    # @property
    # def key(self) -> str:
    #     return self._key

    # @name.setter
    # def key(self, v: str) -> None:
    #     self._key = v