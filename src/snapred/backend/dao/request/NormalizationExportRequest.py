from typing import Optional

from pydantic import BaseModel

from snapred.backend.dao.normalization.NormalizationIndexEntry import NormalizationIndexEntry
from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord


class NormalizationExportRequest(BaseModel):
    """

    This class is utilized to encapsulate the necessary data for saving a completed normalization
    process to disk, following a satisfactory assessment by the user. It packages both the
    comprehensive details of the normalization process and its contextual metadata, ensuring that
    significant normalization efforts are archived in a structured and accessible manner. This
    approach facilitates not only the preservation of critical scientific data but also supports
    data governance, compliance, and reproducibility within the research workflow.

    """

    normalizationRecord: NormalizationRecord
    normalizationIndexEntry: NormalizationIndexEntry
    version: Optional[int] = None
