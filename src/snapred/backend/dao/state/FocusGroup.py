from typing import Dict, List

from pydantic import BaseModel, root_validator

from snapred.backend.dao.Limit import BinnedValue


class FocusGroup(BaseModel):
    name: str
    nHst: int
    FWHM: Dict[int, int]
    # these props apply to allgroups? TODO: Move up a level?
    dSpaceParams: Dict[int, BinnedValue]
    definition: str

    @root_validator(allow_reuse=True)
    def validate_keys(cls, values):
        if values.get("FWHM").keys() != values.get("dSpaceParams").keys():
            raise ValueError("Inconsident group IDs in FWHM, d-space parameters")
        return values

    @root_validator(allow_reuse=True)
    def validateNHist(cls, values):
        if values.get("nHst") != len(values.get("FWHM")):
            raise ValueError("Number of histograms does not match number of groups")
        return values
