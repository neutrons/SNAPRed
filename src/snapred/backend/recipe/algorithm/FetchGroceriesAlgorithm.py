from contextlib import contextmanager
import datetime
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
    ConfigService,
    Direction,
    StringListValidator,
)

from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

logger = snapredLogger.getLogger(__name__)


class FetchGroceriesAlgorithm(PythonAlgorithm):
    """
    For general-purpose loading of nexus data, or grouping workspaces
    """

    def category(self):
        return "SNAPRed Data Handling"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            FileProperty(
                "Filename",
                defaultValue="",
                action=FileAction.Load,
                extensions=["xml", "h5", "nxs", "hd5"],
                direction=Direction.Input,
            ),
            doc="Path to file to be loaded",
        )
        self.declareProperty(
            WorkspaceProperty("OutputWorkspace", "", Direction.Output),
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
                    "LoadLiveData"
                    "LoadNexus",
                    "LoadNexusProcessed",
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
        if loader not in [
            "LoadCalibrationWorkspaces",
        ]:
            if self.getProperty("OutputWorkspace").isDefault:
                issues["OutputWorkspace"] = f"loader '{loader}' requires an 'OutputWorkspace' argument"
            if not self.getProperty("LoaderArgs").isDefault:
                issues["LoaderArgs"] = f"loader '{loader}' does not have any keyword arguments"
        else:
            if self.getProperty("LoaderArgs").isDefault:
                issues["LoaderArgs"] = f"Loader '{loader}' requires additional keyword arguments"
        instrumentSources = ["InstrumentName", "InstrumentFilename", "InstrumentDonor"]
        specifiedSources = [s for s in instrumentSources if not self.getProperty(s).isDefault]
        if len(specifiedSources) > 1:
            issue = "Only one of InstrumentName, InstrumentFilename, or InstrumentDonor can be set"
            issues.update({s: issue for s in specifiedSources})
        return issues

    @contextmanager
    def _useFacility(self, facility: str):
        _facilitySave: str = ConfigService.getFacility()
        ConfigService.setFacility(facility)
        yield facility

        # exit
        ConfigService.setFacility(_facilitySave)

    def PyExec(self) -> None:
        filename = self.getPropertyValue("Filename")
        outWS = self.getPropertyValue("OutputWorkspace")
        loaderType = self.getPropertyValue("LoaderType")
        # TODO: do we need to guard this with an if?
        if not self.mantidSnapper.mtd.doesExist(outWS):
            match loaderType:
                case "":
                    _, loaderType, _ = self.mantidSnapper.Load(
                        "Loading with unspecified loader",
                        Filename=filename,
                        OutputWorkspace=outWS,
                    )
                    self.mantidSnapper.executeQueue()
                case "LoadCalibrationWorkspaces":
                    loaderArgs = json.loads(self.getPropertyValue("LoaderArgs"))
                    self.mantidSnapper.LoadCalibrationWorkspaces(
                        "Loading diffraction-calibration workspaces",
                        Filename=filename,
                        InstrumentDonor=self.getPropertyValue("InstrumentDonor"),
                        **loaderArgs,
                    )
                    self.mantidSnapper.executeQueue()
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
                    self.mantidSnapper.executeQueue()
                case "LoadLiveData":
                    loaderArgs = json.loads(self.getPropertyValue("LoaderArgs"))
                    instrument = loaderArgs["Instrument"]
                    facility = loaderArgs["Facility"]
                    duration = loaderArgs["Duration"]
                    with self._useFacility(facility):
                        self.mantidSnapper.LoadLiveData(
                            "Loading live data",
                            OutputWorkspace=outWs,
                            Instrument=loaderArgs["Instrument"],
                            AccumulationMethod="Replace",
                            StartTime=(datetime.datetime.utcnow() + datetime.timedelta(seconds=-duration)).isoformat()
                        )
                        self.mantidSnapper.executeQueue()
                case _:
                    getattr(self.mantidSnapper, loaderType)(
                        f"Loading data using {loaderType}",
                        Filename=filename,
                        OutputWorkspace=outWS,
                    )
                    self.mantidSnapper.executeQueue()    
        else:
            # TODO: should this throw a warning?  Or warn in logger?
            logger.warning(f"A workspace with name {outWS} already exists in the ADS, and so will not be loaded")
            loaderType = ""
        self.setPropertyValue("OutputWorkspace", outWS)
        self.setPropertyValue("LoaderType", str(loaderType))


# Register algorithm with Mantid
AlgorithmFactory.subscribe(FetchGroceriesAlgorithm)
