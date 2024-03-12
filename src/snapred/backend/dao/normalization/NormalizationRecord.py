from typing import List, Optional

from pydantic import BaseModel

from snapred.backend.dao.calibration.Calibration import Calibration


class NormalizationRecord(BaseModel):
    """
    Represents a Normalization Record with various attributes for tracking normalization operations.

    Attributes:
        runNumber (str): Unique identifier for the normalization run. This is essential for
            uniquely identifying and referencing each normalization operation within a dataset
            or system.
        backgroundRunNumber (str): Identifier for the background run associated with this
            normalization. This detail is crucial for understanding the context and conditions
            under which the normalization was performed.
        smoothingParameter (float): Controls the amount of smoothing applied during normalization.
            The smoothing parameter is a critical factor in the normalization process, affecting
            the outcome and quality of the normalized data.
        calibration (Calibration): Calibration data used for this normalization. Calibration is
            key to ensuring that the normalization is accurate and reflects the true state of the
            instrument or system being normalized.
        workspaceNames (List[str]): List of workspace names associated with this normalization,
            defaulting to an empty list. These names facilitate the organization and retrieval of
            workspaces related to the normalization.
        version (Optional[int]): Version of the normalization record; optional, may be None.
            Allows for tracking of different versions of the normalization record as it is
            updated or revised over time.
        dMin (float): Minimum d-spacing value considered in this normalization. The dMin value is
            important for defining the lower limit of the data range to be normalized, ensuring
            that the normalization process is tailored to the specific data set.

    This class is crucial for tracking the specifics of each normalization step, facilitating
    reproducibility and data management within scientific workflows. It serves as a comprehensive
    record of the parameters and context of normalization operations, contributing to the integrity
    and utility of the resulting data.
    """

    runNumber: str
    backgroundRunNumber: str
    smoothingParameter: float
    calibration: Calibration
    workspaceNames: List[str] = []
    version: Optional[int]
    dMin: float
