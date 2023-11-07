from typing import List

from pydantic import BaseModel, root_validator


class FocusGroup(BaseModel):
    name: str
    FWHM: List[int]
    # these props apply to allgroups? TODO: Move up a level?
    nHst: int
    dBin: List[float]
    dMax: List[float]
    dMin: List[float]
    definition: str

    @root_validator(pre=True, allow_reuse=True)
    def validate_all_lists_equal(cls, v):
        nHst = v["nHst"]
        if len(v["FWHM"]) != nHst:
            raise ValueError("Mismatch in focus group size and length of FWHM list")
        if len(v["dMin"]) != nHst:
            raise ValueError("Mismatch in focus group size and length of dMin list")
        if len(v["dMax"]) != nHst:
            raise ValueError("Mismatch in focus group size and length of dMax list")
        if len(v["dBin"]) != nHst:
            raise ValueError("Mismatch in focus group size and length of dBin list")
        return v

    @root_validator(pre=True, allow_reuse=True)
    def validate_correct_limits(cls, v):
        nHst = v["nHst"]
        dMin = v["dMin"]
        dMax = v["dMax"]
        for i in range(nHst):
            if dMin[i] >= dMax[i]:
                raise ValueError("Within focus group, all dMins must be strictly less than dMax")
        return v
