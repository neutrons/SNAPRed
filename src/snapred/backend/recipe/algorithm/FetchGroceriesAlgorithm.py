import json
from typing import Dict

from mantid.api import (
    AlgorithmFactory,
    FileAction,
    FileProperty,
    PropertyMode,
    PythonAlgorithm,
    WorkspaceProperty,
)
from mantid.kernel import (
    Direction,
    StringListValidator,
)

from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

logger = snapredLogger.getLogger(__name__)


class FetchGroceriesAlgorithm(PythonAlgorithm):
    """
    For general-purpose loading of nexus data, or any workspace type that uses a loader algorithm
    """

    def category(self):
        return "SNAPRed Data Handling"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            FileProperty(
                "Filename",
                defaultValue="",
                # `Filename` is an optional property when `loader == 'LoadLiveDataInterval'`.
                action=FileAction.OptionalLoad,
                extensions=["xml", "h5", "nxs", "hd5"],
                direction=Direction.Input,
            ),
            doc="Path to file to be loaded",
        )
        self.declareProperty(
            WorkspaceProperty("OutputWorkspace", "", Direction.Output, PropertyMode.Optional),
            doc="Workspace containing the loaded data",
        )
        self.declareProperty(
            "LoaderType",
            "",
            StringListValidator(
                [
                    "",
                    "LoadCalibrationWorkspaces",
                    "LoadEventNexus",
                    "LoadGroupingDefinition",
                    "LoadLiveDataInterval",
                    "LoadNexus",
                    "LoadNexusProcessed",
                    "LoadNexusMonitors",
                ]
            ),
            direction=Direction.InOut,
        )
        self.declareProperty(
            "LoaderArgs",
            defaultValue="",
            direction=Direction.Input,
            doc="loader keyword args (JSON format)",
        )
        self.declareProperty(
            "InstrumentName",
            defaultValue="",
            direction=Direction.Input,
            doc="Name of an associated instrument",
        )
        self.declareProperty(
            "InstrumentFilename",
            defaultValue="",
            direction=Direction.Input,
            doc="Path of an associated instrument definition file",
        )
        self.declareProperty(
            WorkspaceProperty("InstrumentDonor", "", Direction.Input, PropertyMode.Optional),
            doc="Workspace to optionally take the instrument from",
        )
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def validateInputs(self) -> Dict[str, str]:
        # cannot load a grouping workspace with a nexus loader
        issues: Dict[str, str] = {}
        loader = self.getPropertyValue("LoaderType")

        # `OutputWorkspace` property:
        if loader not in [
            "LoadCalibrationWorkspaces",
        ]:
            if self.getProperty("OutputWorkspace").isDefault:
                issues["OutputWorkspace"] = f"loader '{loader}' requires an 'OutputWorkspace' argument"

        # `LoaderArgs` property:
        if loader in ["LoadCalibrationWorkspaces", "LoadLiveDataInterval"]:
            if self.getProperty("LoaderArgs").isDefault:
                issues["LoaderArgs"] = f"loader '{loader}' requires additional keyword arguments"
        elif not self.getProperty("LoaderArgs").isDefault:
            issues["LoaderArgs"] = f"Loader '{loader}' does not have any keyword arguments"

        # `InstrumentName`, `InstrumentFilename`, and `InstrumentDonor` properties:
        instrumentSources = ["InstrumentName", "InstrumentFilename", "InstrumentDonor"]
        specifiedSources = [s for s in instrumentSources if not self.getProperty(s).isDefault]
        if len(specifiedSources) > 1:
            issue = "Only one of InstrumentName, InstrumentFilename, or InstrumentDonor can be set"
            issues.update({s: issue for s in specifiedSources})

        return issues

    def PyExec(self) -> None:
        filename = self.getPropertyValue("Filename")
        outWS = self.getPropertyValue("OutputWorkspace")
        loaderType = self.getPropertyValue("LoaderType")

        # Allow live-data mode to replace an existing workspace
        addOrReplace = loaderType == "LoadLiveDataInterval"

        # TODO: the `if .. doesExist` should be centralized to cache treatment
        #    here, it is redundant.
        if addOrReplace or not self.mantidSnapper.mtd.doesExist(outWS):
            match loaderType:
                case "":
                    _, loaderType, _ = self.mantidSnapper.Load(
                        "Loading with unspecified loader",
                        Filename=filename,
                        OutputWorkspace=outWS,
                    )
                case "LoadCalibrationWorkspaces":
                    loaderArgs = json.loads(self.getPropertyValue("LoaderArgs"))
                    self.mantidSnapper.LoadCalibrationWorkspaces(
                        "Loading diffraction-calibration workspaces",
                        Filename=filename,
                        InstrumentDonor=self.getPropertyValue("InstrumentDonor"),
                        **loaderArgs,
                    )
                case "LoadGroupingDefinition":
                    for x in ["InstrumentName", "InstrumentFilename", "InstrumentDonor"]:
                        if not self.getProperty(x).isDefault:
                            instrumentPropertySource = x
                            instrumentSource = self.getPropertyValue(x)
                    self.mantidSnapper.LoadGroupingDefinition(
                        "Loading grouping definition",
                        GroupingFilename=filename,
                        OutputWorkspace=outWS,
                        **{instrumentPropertySource: instrumentSource},
                    )
                case "LoadLiveDataInterval":
                    loaderArgs = json.loads(self.getPropertyValue("LoaderArgs"))
                    self.mantidSnapper.LoadLiveDataInterval(
                        "loading live-data interval",
                        OutputWorkspace=outWS,
                        Instrument=loaderArgs["Instrument"],
                        PreserveEvents=loaderArgs["PreserveEvents"],
                        StartTime=loaderArgs["StartTime"],
                    )
                case _:
                    getattr(self.mantidSnapper, loaderType)(
                        f"Loading data using {loaderType}",
                        Filename=filename,
                        OutputWorkspace=outWS,
                    )

            self.mantidSnapper.executeQueue()

        else:
            logger.debug(f"A workspace with name {outWS} already exists in the ADS, and so will not be loaded")
            loaderType = ""

        if loaderType in ["LoadNexus", "LoadEventNexus", "LoadNexusProcessed"]:
            # TODO: (1) Does `LoadLiveData` / `LoadLiveDataInterval` correctly set the instrument parameters?
            # TODO: (2) See EWM#7437:
            #   this clause is necessary to be able to accurately set detector positions
            #   on files written prior to the merge of the
            #   `SaveNexus` 'instrument_parameter_map' write-precision fix.
            # It probably should not be removed, even after that fix is merged.
            self.mantidSnapper.mtd[outWS].populateInstrumentParameters()

        self.setPropertyValue("OutputWorkspace", outWS)
        self.setPropertyValue("LoaderType", str(loaderType))


# Register algorithm with Mantid
AlgorithmFactory.subscribe(FetchGroceriesAlgorithm)
