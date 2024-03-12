from pydantic import BaseModel

from snapred.backend.dao.normalization.NormalizationIndexEntry import NormalizationIndexEntry
from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord


class NormalizationExportRequest(BaseModel):
    """
    Defines a Normalization Export Request object class for persisting normalization results.

    Attributes:
        normalizationRecord (NormalizationRecord): Contains detailed information about the
            normalization process, including run numbers, calibration details, and parameters
            used for the normalization. This component encapsulates the entirety of the
            normalization procedure's output, providing a comprehensive record of the operation.
        normalizationIndexEntry (NormalizationIndexEntry): Stores metadata relevant for indexing
            and later retrieval of the normalization record. This includes identifiers,
            versioning information, and potentially comments or tags for easier search and
            categorization. This element is essential for organizing and accessing the
            normalization records within a database or file system.

    This class is utilized to encapsulate the necessary data for saving a completed normalization
    process to disk, following a satisfactory assessment by the user. It packages both the
    comprehensive details of the normalization process and its contextual metadata, ensuring that
    significant normalization efforts are archived in a structured and accessible manner. This
    approach facilitates not only the preservation of critical scientific data but also supports
    data governance, compliance, and reproducibility within the research workflow.
    """

    normalizationRecord: NormalizationRecord
    normalizationIndexEntry: NormalizationIndexEntry
