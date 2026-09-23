from pydantic import BaseModel

from snapred.backend.dao.indexing.RunRange import RunRange


class VersionSegment(BaseModel):
    """
    A contiguous range of runs together with the single indexed version that governs them.

    Segments partition a range of interest, so that a caller can act on each stretch of runs
    with the version actually in force over it rather than assuming one version covers all.
    """

    runRange: RunRange
    version: int
