from typing import Literal, Optional

from pydantic import BaseModel, root_validator

from snapred.backend.dao import RunConfig
from snapred.meta.Config import Config


class GroceryListItem(BaseModel):
    """Holds necessary information for a single item in grocery list"""

    workspaceType: Literal["nexus", "grouping"]
    runConfig: Optional[RunConfig]
    loader: Literal["", "LoadGroupingDefinition", "LoadNexus", "LoadEventNexus", "LoadNexusProcessed"] = ""
    isLite: Optional[bool]
    groupingScheme: Optional[str]
    instrumentPropertySource: Optional[Literal["InstrumentName", "InstrumentFilename", "InstrumentDonor"]]
    instrumentSource: Optional[str]
    keepItClean: bool = True

    @root_validator(pre=True, allow_reuse=True)
    def validate_ingredients_for_gorceries(cls, v):
        if v["workspaceType"] == "nexus" and v.get("runConfig") is None:
            raise ValueError("you must set the run config to load nexus data")
        if v["workspaceType"] == "grouping":
            if v.get("isLite") is None:
                raise ValueError("you must specify whether to use Lite mode for grouping workspace")
            if v.get("groupingScheme") is None:
                raise ValueError("you must specify the grouping scheme to use")
            if v["groupingScheme"] == "Lite":
                # the Lite grouping scheme reduces native resolution to Lite mode
                v["instrumentPropertySource"] = "InstrumentFilename"
                v["instrumentSource"] = str(Config["instrument.native.definition.file"])
                v["isLite"] = False
            else:
                if v.get("instrumentPropertySource") is None:
                    raise ValueError("a grouping workspace requires an instrument source")
                if v.get("instrumentSource") is None:
                    raise ValueError("a grouping workspace requries an instrument source")
        return v
