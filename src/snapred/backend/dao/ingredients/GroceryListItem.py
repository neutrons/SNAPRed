from typing import ClassVar, Literal, Optional, get_args

from pydantic import BaseModel, root_validator

from snapred.backend.log.logger import snapredLogger
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng

logger = snapredLogger.getLogger(__name__)

KnownUnits = Literal[wng.Units.TOF, wng.Units.DSP, wng.Units.DIAG]
known_units = list(get_args(KnownUnits))


GroceryTypes = Literal[
    "neutron",
    "grouping",
    "diffcal",
    "diffcal_output",
    "diffcal_diagnostic",
    "diffcal_table",
    "diffcal_mask",
    "normalization",
]

grocery_types = list(get_args(GroceryTypes))


class GroceryListItem(BaseModel):
    """
    Holds necessary information for a single item in grocery list
    """

    # Reserved instrument-cache run-number values:
    RESERVED_NATIVE_RUNNUMBER: ClassVar[str] = "000000"  # unmodified _native_ instrument:
    #   from 'SNAP_Definition.xml'
    RESERVED_LITE_RUNNUMBER: ClassVar[str] = "000001"  # unmodified _lite_ instrument  :
    #   from 'SNAPLite.xml'

    workspaceType: GroceryTypes
    useLiteMode: bool  # indicates if data should be reduced to lite mode

    # optional loader:
    # -- "" tells FetchGroceries to choose the loader
    loader: Literal["", "LoadGroupingDefinition", "LoadNexus", "LoadEventNexus", "LoadNexusProcessed"] = ""

    # the correct combination of the below must be set -- neutron and grouping require a runNumber,
    #   grouping additionally requires a groupingScheme
    runNumber: Optional[str]
    version: Optional[int]
    groupingScheme: Optional[str]

    unit: Optional[KnownUnits]

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
        if v.get("unit") is not None:
            unit_ = v.get("unit")
            if unit_ not in known_units:
                raise ValueError(f"unknown unit '{unit_}' specified")
        match v["workspaceType"]:
            case "neutron":
                if v.get("runNumber") is None:
                    raise ValueError("Loading neutron data requires a run number")
                if v.get("groupingScheme") is not None:
                    del v["groupingScheme"]
                if v.get("instrumentPropertySource") is not None:
                    raise ValueError("Loading neutron data should not specify an instrument")
            case "grouping":
                if v.get("groupingScheme") is None:
                    raise ValueError("you must specify the grouping scheme to use")
                if v["groupingScheme"] == "Lite":
                    # the Lite grouping scheme reduces native resolution to Lite mode
                    if v.get("useLiteMode"):
                        logger.warning(
                            "the lite-mode flag must be False for the 'lite' grouping scheme"
                            + " -- this cannot be overridden"
                        )
                    v["useLiteMode"] = False  # the lite data map only works on native data

                    if v.get("runNumber") is not None:
                        logger.warning(
                            "the run number must not be specified for 'lite' grouping scheme"
                            + " -- this cannot be overridden"
                        )
                    # the Lite grouping scheme uses the unmodified native instrument
                    v["runNumber"] = cls.RESERVED_NATIVE_RUNNUMBER
                if v.get("runNumber") is None:
                    # A run number is required in order to cache instrument parameters
                    raise ValueError("Loading a grouping scheme requires a run number")
            case "diffcal":
                if v.get("runNumber") is None:
                    raise ValueError("diffraction-calibration input table workspace requires a run number")
            # output (i.e. special-order) workspaces
            case "diffcal_output" | "diffcal_diagnostic":
                if v.get("runNumber") is None:
                    raise ValueError(f"diffraction-calibration {v['workspaceType']} requires a run number")
                if v.get("instrumentPropertySource") is not None:
                    raise ValueError("Loading diffcal-output data should not specify an instrument")
                if not v.get("useLiteMode"):
                    v["useLiteMode"] = True  # don't care
            case "diffcal_table" | "diffcal_mask":
                if v.get("runNumber") is None:
                    raise ValueError(f"diffraction-calibration {v['workspaceType']} requires a run number")
                if not v.get("useLiteMode"):
                    v["useLiteMode"] = True  # don't care
            case "normalization":
                if v.get("runNumber") is None:
                    raise ValueError(f"normalization {v['workspaceType']} requires run number")
            case _:
                raise ValueError(f"unrecognized 'workspaceType': '{v['workspaceType']}'")
        return v
