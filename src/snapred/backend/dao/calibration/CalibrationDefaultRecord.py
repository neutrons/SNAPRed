from typing import Dict, List

from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.indexing.Record import Record
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName, WorkspaceType


class CalibrationDefaultRecord(Record, extra="ignore"):
    """

    The refer to the CalibrationRecord class for a more in-depth explanation of Calibration Records.
    This class contains the default, most basic information contained within the default Calibration.

    """

    # inherits from Record
    # - runNumber
    # - useLiteMode
    # - version
    # override this to point at the correct daughter class
    # NOTE the version on the calculationParameters MUST match the version on the record
    # this should be enforced by a validator
    calculationParameters: Calibration

    # specific to calibration records
    workspaces: Dict[WorkspaceType, List[WorkspaceName]]
