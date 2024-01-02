from typing import Literal, Optional

from pydantic import BaseModel, root_validator

from snapred.meta.Config import Config


class GroceryListItem(BaseModel):
    """
    Holds necessary information for a single item in grocery list
    """

    workspaceType: Literal["neutron", "grouping", "diffcal"]
    useLiteMode: bool  # indicates if data should be reduced to lite mode
    # optional loader:
    # -- "" tells FetchGroceries to choose the loader
    loader: Literal["", "LoadGroupingDefinition", "LoadNexus", "LoadEventNexus", "LoadNexusProcessed"] = ""
    # one of the below must be set -- neutron requires a runNumber, grouping a groupingScheme
    runNumber: Optional[str]
    groupingScheme: Optional[str]
    # grouping workspaces require an instrument definition
    # these indicate which property defines the source, and then that source
    instrumentPropertySource: Optional[Literal["InstrumentName", "InstrumentFilename", "InstrumentDonor"]]
    instrumentSource: Optional[str]
    # if set to False, neutron data will not be loaded in a clean, cached way
    # this is faster and uses less memory, if you know you only need one copy
    keepItClean: bool = True
    # name the property the workspace will be used for
    propertyName: Optional[str]

    def builder():
        # NOTE this import is here to avoid circular dependencies -- don't bother trying to move it
        from snapred.meta.builder.GroceryListBuilder import GroceryListBuilder

        return GroceryListBuilder()

    @root_validator(pre=True, allow_reuse=True)
    def validate_ingredients_for_groceries(cls, v):
        if v["workspaceType"] == "neutron":
            if v.get("runNumber") is None:
                raise ValueError("Loading neutron data requires a run number")
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
