import json
from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, PrivateAttr, root_validator

from snapred.backend.dao.RunConfig import RunConfig
from snapred.meta.Config import Config


class GroceryListItem(BaseModel):
    """
    Holds necessary information for a single item in grocery list
    """

    workspaceType: Literal["nexus", "grouping"]
    useLiteMode: bool  # indicates if data should be reduced to lite mode
    # optional loader:
    # -- "" tells FetchGroceries to choose the loader
    loader: Literal["", "LoadGroupingDefinition", "LoadNexus", "LoadEventNexus", "LoadNexusProcessed"] = ""
    # one of the below must be set -- nexus requires a runNumber, grouping a groupingScheme
    runNumber: Optional[str]
    groupingScheme: Optional[str]
    # grouping workspaces require an instrument definition
    # these indicate which property defines the sourt, and then that source
    instrumentPropertySource: Optional[Literal["InstrumentName", "InstrumentFilename", "InstrumentDonor"]]
    instrumentSource: Optional[str]
    # if set to False, nexus data will not be loaded in a clean, cached way
    # this is faster and uses less memory, if you know you only need one copy
    keepItClean: bool = True

    # IPTS is implemented lazily and is not a true model field
    # the _ipts private field stores its value, and methods handle getting/setting
    # this can be avoided with @property getter/setter, only supported in pydantic >v2
    _ipts: str = PrivateAttr()

    def __init__(self, **kwargs):
        # set the model as usual
        super().__init__(**kwargs)
        # also if IPTS was set in construction, set its value
        object.__setattr__(self, "_ipts", kwargs.get("IPTS"))

    def dict(self, **kwargs) -> Dict[str, Any]:  # noqa: A003, ARG002
        # return the usual dictionary
        ans = super().dict(**kwargs)
        # if IPTS is set, also return its value
        if self._ipts is not None:
            ans["IPTS"] = self.IPTS
        return ans

    def json(self, **kwargs) -> str:
        # get the dictionary and dump it
        return json.dumps(self.dict(), **kwargs)

    def __setattr__(self, name, value):
        # set as usual
        try:
            super().__setattr__(name, value)
        # if a key error occured, check if the key was IPTS
        # if it was IPTS, then set the IPTS
        # otherwise raise an error
        except ValueError as e:
            if name == "IPTS":
                object.__setattr__(self, "_ipts", value)
            else:
                raise e

    # if something calls for the IPTS, and it is not set,
    # then call the GetIPTS mantid function to set the IPTS
    @property
    def IPTS(self):
        from mantid.simpleapi import GetIPTS

        if self._ipts is None:
            self._ipts = GetIPTS(self.runNumber, Config["instrument.name"])
        return self._ipts

    def toggleLiteMode(self, set_to: bool = None):
        if set_to is not None:
            self.useLiteMode = set_to
        else:
            self.useLiteMode = not self.useLiteMode
        return self

    @classmethod
    def makeNativeNexusItem(cls, runNumber: str):
        return GroceryListItem(
            workspaceType="nexus",
            runNumber=runNumber,
            useLiteMode=False,
        )

    @classmethod
    def makeLiteNexusItem(cls, runNumber: str):
        return GroceryListItem(
            workspaceType="nexus",
            runNumber=runNumber,
            useLiteMode=True,
        )

    @classmethod
    def makeNativeGroupingItem(cls, groupingScheme: str):
        return GroceryListItem(
            workspaceType="grouping",
            groupingScheme=groupingScheme,
            useLiteMode=False,
            instrumentPropertySource="InstrumentFilename",
            instrumentSource=str(Config["instrument.native.definition"]),
        )

    @classmethod
    def makeLiteGroupingItem(cls, groupingScheme: str):
        return GroceryListItem(
            workspaceType="grouping",
            groupingScheme=groupingScheme,
            useLiteMode=True,
            instrumentPropertySource="InstrumentFilename",
            instrumentSource=str(Config["instrument.lite.definition"]),
        )

    @root_validator(pre=True, allow_reuse=True)
    def validate_ingredients_for_groceries(cls, v):
        if v["workspaceType"] == "nexus":
            if v.get("runNumber") is None:
                raise ValueError("Loading nexus data requires a run number")
            if v.get("groupingScheme") is not None:
                v["groupingScheme"] = None
        if v["workspaceType"] == "grouping":
            if v.get("groupingScheme") is None:
                raise ValueError("you must specify the grouping scheme to use")
            if v["groupingScheme"] == "Lite":
                # the Lite grouping scheme reduces native resolution to Lite mode
                v["instrumentPropertySource"] = "InstrumentFilename"
                v["instrumentSource"] = str(Config["instrument.native.definition.file"])
                v["useLiteMode"] = False  # the lite data map only works on native data
            else:
                if v.get("instrumentPropertySource") is None:
                    raise ValueError("a grouping workspace requires an instrument source")
                if v.get("instrumentSource") is None:
                    raise ValueError("a grouping workspace requries an instrument source")
        return v
