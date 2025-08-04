import json
from pathlib import Path
from typing import Dict

from mantid.api import (
    AlgorithmFactory,
    FileAction,
    FileProperty,
    PropertyMode,
    PythonAlgorithm,
    WorkspaceGroup,
    WorkspaceProperty,
)
from mantid.dataobjects import TableWorkspace
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
            defaultValue="{}",
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
        elif loader not in [
            "LoadEventNexus",
        ]:
            if not self.getProperty("LoaderArgs").isDefault:
                issues["LoaderArgs"] = f"Loader '{loader}' does not have any keyword arguments"

        # `InstrumentName`, `InstrumentFilename`, and `InstrumentDonor` properties:
        instrumentSources = ["InstrumentName", "InstrumentFilename", "InstrumentDonor"]
        specifiedSources = [s for s in instrumentSources if not self.getProperty(s).isDefault]
        if len(specifiedSources) > 1:
            issue = "Only one of InstrumentName, InstrumentFilename, or InstrumentDonor can be set"
            issues.update({s: issue for s in specifiedSources})

        return issues

    def validateCalibrationFile(self, filepath: str):
        # confirm the .h5 file contains a group called "calibration"
        path = Path(filepath)
        if not path.exists():
            raise RuntimeError(f"Calibration file {filepath} does not exist")

        # check table with h5py
        import h5py

        with h5py.File(filepath, "r") as f:
            if "calibration" not in f:
                raise RuntimeError(f"Calibration file {filepath} does not contain a 'calibration' group")
            # ensure that "calibration" group contains datasets [detid, difc, group, instrument, use]
            calibrationGroup = f["calibration"]
            requiredDatasets = ["detid", "difc", "group", "instrument", "use"]
            optionalDatasets = ["tzero", "difa"]

            # check for unexpected datasets
            for dataset in calibrationGroup.keys():
                if dataset not in requiredDatasets and dataset not in optionalDatasets:
                    raise RuntimeError(f"Calibration file {filepath} contains unexpected dataset '{dataset}'")

            # check that at least the required datasets are present
            for dataset in requiredDatasets:
                if dataset not in calibrationGroup:
                    raise RuntimeError(f"Calibration file {filepath} does not contain the required dataset '{dataset}'")

    def PyExec(self) -> None:
        filename = self.getPropertyValue("Filename")
        outWS = self.getPropertyValue("OutputWorkspace")
        loaderType = self.getPropertyValue("LoaderType")

        # Allow live-data mode to replace an existing workspace
        addOrReplace = loaderType == "LoadLiveDataInterval"

        # TODO: the `if .. doesExist` should be centralized to cache treatment
        #    here, it is redundant.
        if addOrReplace or not self.mantidSnapper.mtd.doesExist(outWS):
            loaderArgs = json.loads(self.getPropertyValue("LoaderArgs"))
            match loaderType:
                case "":
                    _, loaderType, _ = self.mantidSnapper.Load(
                        "Loading with unspecified loader", Filename=filename, OutputWorkspace=outWS, **loaderArgs
                    )
                case "LoadCalibrationWorkspaces":
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
                    self.mantidSnapper.LoadLiveDataInterval(
                        "loading live-data interval",
                        OutputWorkspace=outWS,
                        Instrument=loaderArgs["Instrument"],
                        PreserveEvents=loaderArgs["PreserveEvents"],
                        StartTime=loaderArgs["StartTime"],
                    )
                case _:
                    getattr(self.mantidSnapper, loaderType)(
                        f"Loading data using {loaderType}", Filename=filename, OutputWorkspace=outWS, **loaderArgs
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
            names = (
                # `outWS` may be a workspace group
                [
                    outWS,
                ]
                if not isinstance(self.mantidSnapper.mtd[outWS], WorkspaceGroup)
                else self.mantidSnapper.mtd[outWS].getNames()
            )
            for name in names:
                if not isinstance(self.mantidSnapper.mtd[name], TableWorkspace):
                    self.mantidSnapper.mtd[name].populateInstrumentParameters()

        self.setPropertyValue("OutputWorkspace", outWS)
        self.setPropertyValue("LoaderType", str(loaderType))


# Register algorithm with Mantid
AlgorithmFactory.subscribe(FetchGroceriesAlgorithm)
