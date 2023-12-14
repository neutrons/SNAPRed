from typing import Literal, Optional

from pydantic import BaseModel, root_validator

from snapred.meta.Config import Config


class GroceryListItem(BaseModel):
    """
    Holds necessary information for a single item in grocery list
    """

    workspaceType: Literal["nexus", "grouping", "diffcal"]
    useLiteMode: bool  # indicates if data should be reduced to lite mode
    # optional loader:
    # -- "" tells FetchGroceries to choose the loader
    loader: Literal["", "LoadGroupingDefinition", "LoadNexus", "LoadEventNexus", "LoadNexusProcessed"] = ""
    # one of the below must be set -- nexus requires a runNumber, grouping a groupingScheme
    runNumber: Optional[str]
    groupingScheme: Optional[str]
    # grouping workspaces require an instrument definition
    # these indicate which property defines the source, and then that source
    instrumentPropertySource: Optional[Literal["InstrumentName", "InstrumentFilename", "InstrumentDonor"]]
    instrumentSource: Optional[str]
    # if set to False, nexus data will not be loaded in a clean, cached way
    # this is faster and uses less memory, if you know you only need one copy
    keepItClean: bool = True
    # name the property the workspace will be used for
    propertyName: Optional[str]

    def toggleLiteMode(self, set_to: bool = None):
        if set_to is not None:
            self.useLiteMode = set_to
        else:
            self.useLiteMode = not self.useLiteMode
        return self

    # some helpful constructor methods
    # TODO remove in preference of list builder?
    @classmethod
    def makeNexusItem(cls, runNumber: str, useLiteMode: bool):
        return GroceryListItem(
            workspaceType="nexus",
            runNumber=runNumber,
            useLiteMode=useLiteMode,
        )

    @classmethod
    def makeNativeNexusItem(cls, runNumber: str):
        return cls.makeNexusItem(runNumber, useLiteMode=False)

    @classmethod
    def makeLiteNexusItem(cls, runNumber: str):
        return cls.makeNexusItem(runNumber, useLiteMode=True)

    @classmethod
    def makeGroupingItem(cls, groupingScheme: str, useLiteMode: bool):
        return GroceryListItem(
            workspaceType="grouping",
            groupingScheme=groupingScheme,
            useLiteMode=useLiteMode,
            instrumentPropertySource="InstrumentFilename",
            instrumentSource=str(Config["instrument.native.definition"]),
        )

    @classmethod
    def makeNativeGroupingItem(cls, groupingScheme: str):
        return cls.makeGroupingItem(groupingScheme, useLiteMode=False)

    @classmethod
    def makeLiteGroupingItem(cls, groupingScheme: str):
        return cls.makeGroupingItem(groupingScheme, useLiteMode=True)

    @classmethod
    def makeGroupingItemFrom(cls, groupingScheme: str, useLiteMode: bool, instrumentDonor: str):
        return GroceryListItem(
            workspaceType="grouping",
            groupingScheme=groupingScheme,
            useLiteMode=useLiteMode,
            instrumentPropertySource="InstrumentDonor",
            instrumentSource=instrumentDonor,
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
