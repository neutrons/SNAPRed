from typing import Literal, Optional

from pydantic import BaseModel, root_validator

from snapred.backend.dao import RunConfig


class GroceryListItem(BaseModel):
    """Class to hold the ingredients necessary for pixel grouping parameter calculation."""

    workspaceType: Literal["nexus", "grouping"]
    runConfig: Optional[RunConfig]
    loader: str = ""
    isLite: Optional[bool]
    groupingScheme: Optional[str]

    @root_validator(pre=True, allow_reuse=True)
    def validate_ingredients_for_gorceries(cls, v):
        if v["workspaceType"] == "nexus" and v.get("runConfig") is None:
            raise ValueError("you must set the run config to load nexus data")
        if v["workspaceType"] == "grouping" and v.get("isLite") is None:
            raise ValueError("you must specify whether to use Lite mode for grouping workspace")
        if v["workspaceType"] == "grouping" and v.get("groupingScheme") is None:
            raise ValueError("you must specify the grouping scheme to use")
        return v
