from pydantic import BaseModel

from snapred.backend.dao.normalization.NormalizationIndexEntry import NormalizationIndexEntry
from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord


class NormalizationExportRequest(BaseModel):
    """
    Once a normalizaiton is completed and assessed as satisfactory
    a user may request to persist it to disk
    This request is mostly passing back the result of that assessment step.
    """

    normalizationRecord: NormalizationRecord
    normalizationIndexEntry: NormalizationIndexEntry
