from typing import Literal, Optional, ClassVar

from pydantic import BaseModel, root_validator

from snapred.meta.Config import Config
from snapred.backend.log.logger import snapredLogger

logger = snapredLogger.getLogger(__name__)

class GroceryListItem(BaseModel):
    """
    Holds necessary information for a single item in grocery list
    """
    # Reserved instrument-cache run-number values:
    RESERVED_NATIVE_RUNID: ClassVar[str] = "000000" # unmodified _native_ instrument:
                                                    #   from 'SNAP_Definition.xml'
    RESERVED_LITE_RUNID: ClassVar[str] = "000001"   # unmodified _lite_ instrument  :
                                                    #   from 'SNAPLite.xml' 

    workspaceType: Literal["neutron", "grouping", "diffcal", "diffcal_output", "diffcal_table", "diffcal_mask"]
    useLiteMode: bool  # indicates if data should be reduced to lite mode
    
    # optional loader:    
    # -- "" tells FetchGroceries to choose the loader
    loader: Literal["", "LoadGroupingDefinition", "LoadNexus", "LoadEventNexus", "LoadNexusProcessed"] = ""
    
    # the correct combinaton of the below must be set -- neutron and grouping require a runNumber,
    #   grouping additionally requires a groupingScheme
    runNumber: Optional[str]
    version: Optional[str]
    groupingScheme: Optional[str]

    # SpecialWorkspace2D-derived workspaces (e.g. grouping or mask workspaces)
    #   require an instrument definition, these next two properties indicate
    #   which property defines the instrument source,
    #   and then what type of source it is.
    # Warning: in general these properties should no longer be used, except to override
    #   the automatic instrument-donor caching system.
    instrumentPropertySource: Optional[Literal["InstrumentName", "InstrumentFilename", "InstrumentDonor"]]
    instrumentSource: Optional[str]

    # if set to False, neutron data will not be loaded in a clean, cached way
    # this is faster and uses less memory, if you know you only need one copy
    keepItClean: bool = True
    
    # name the property the workspace will be used for
    propertyName: Optional[str]
    
    # flag to indicate if this is an _output_ workspace:
    # an output workspace will not be loaded,
    # it may or may not already exist in the ADS
    isOutput: bool = False

    def builder():
        # NOTE this import is here to avoid circular dependencies -- don't bother trying to move it
        from snapred.meta.builder.GroceryListBuilder import GroceryListBuilder

        return GroceryListBuilder()

    @root_validator(pre=True, allow_reuse=True)
    def validate_ingredients_for_groceries(cls, v):
        if v.get("instrumentPropertySource") is not None:
            if v.get("instrumentSource") is None:
                raise ValueError("if 'instrumentPropertySource' is specified then 'instrumentSource' must be specified")
        if v.get("instrumentSource") is not None:
            if v.get("instrumentPropertySource") is None:
                raise ValueError("if 'instrumentSource' is specified then 'instrumentPropertySource' must be specified")

        match v["workspaceType"]:
            case "neutron":
                if v.get("runNumber") is None:
                    raise ValueError("Loading neutron data requires a run number")
                if v.get("groupingScheme") is not None:
                    v["groupingScheme"] = None
                if v.get("instrumentPropertySource") is not None:
                    raise ValueError("Loading neutron data should not specify an instrument")
            case "grouping":
                if v.get("groupingScheme") is None:
                    raise ValueError("you must specify the grouping scheme to use")
                if v["groupingScheme"] == "Lite":
                    # the Lite grouping scheme reduces native resolution to Lite mode
                    if v.get("useLiteMode"):
                        logger.warning("the lite-mode flag must be False for the 'lite' grouping scheme -- this cannot be overridden")                    
                    v["useLiteMode"] = False  # the lite data map only works on native data
                    
                    if v.get("runNumber") is not None:
                        logger.warning("the run number must not be specified for 'lite' grouping scheme -- this cannot be overridden")                    
                    # the Lite grouping scheme uses the unmodified native instrument
                    v["runNumber"] = cls.RESERVED_NATIVE_RUNID
                if v.get("runNumber") is None:
                    # A run number is required in order to cache instrument parameters
                    raise ValueError("Loading a grouping scheme requires a run number")
            case "diffcal":
                if v.get("runNumber") is None:
                    raise ValueError("diffraction-calibration input table workspace requires a run number")
            # output (i.e. special-order) workspaces
            case "diffcal_output":
                if v.get("runNumber") is None:
                    raise ValueError(f"diffraction-calibration {v['workspaceType']} requires a run number")
                if v.get("isOutput") is False:
                    raise ValueError(
                        f"diffraction-calibration {v['workspaceType']} output specification is special-order only"
                    )
                if v.get("instrumentPropertySource") is not None:
                    raise ValueError("Loading diffcal-output data should not specify an instrument")
            case "diffcal_table" | "diffcal_mask":
                if v.get("runNumber") is None:
                    raise ValueError(f"diffraction-calibration {v['workspaceType']} requires a run number")
                if v.get("isOutput") is False:
                    raise ValueError(
                        f"diffraction-calibration {v['workspaceType']} output specification is special-order only"
                    )
            case _:
                raise ValueError(f"unrecognized 'workspaceType': '{v['workspaceType']}'")
        return v
