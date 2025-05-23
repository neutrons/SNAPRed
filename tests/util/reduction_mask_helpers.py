from pathlib import Path

import pytest
from mantid.api import MatrixWorkspace
from mantid.dataobjects import MaskWorkspace
from mantid.simpleapi import (
    CreateSampleWorkspace,
    LoadInstrument,
    mtd,
)
from util.helpers import (
    createCompatibleMask,
    initializeRandomMask,
)
from util.instrument_helpers import addInstrumentLogs, getInstrumentLogDescriptors

from snapred.backend.dao.state.DetectorState import DetectorState
from snapred.meta.mantid.WorkspaceNameGenerator import (
    WorkspaceName,
)

# Import required test fixtures at the end of either the main `conftest.py`,
#   or any `conftest.py` at the test-module directory.


@pytest.fixture(scope="class")
def create_sample_workspace(cleanup_class_workspace_at_exit):
    """
    This test fixture creates a sample workspace with the specified name and instrument settings:

      * instrument settings are established from the provided `DetectorState` instance;
        note that the corresponding state-SHA is derivable using
        `LocalDataService._stateIdFromDetectorState(detectorState: DetectorState) -> ObjectSHA`;

      * the workspace will be automatically cleaned up at the teardown of the test class.
    """

    def _create_sample_workspace(
        wsName: WorkspaceName,
        detectorState: DetectorState,
        instrumentFilePath: Path,
        runNumber: str,
        runTitle: str = "",
        units="TOF",
        numBanks=1,
        bankPixelWidth=4,
        numMonitors=0,
    ) -> MatrixWorkspace:
        # Descriptor info to set instrument logs (to assign state to workspaces)
        instrumentLogsInfo = getInstrumentLogDescriptors(detectorState)

        CreateSampleWorkspace(
            OutputWorkspace=wsName,
            Function="One Peak",
            NumBanks=numBanks,
            NumMonitors=numMonitors,
            BankPixelWidth=bankPixelWidth,
            NumEvents=500,
            Random=True,
            XUnit=units,
            XMin=0,
            XMax=8000,
            BinWidth=100,
        )
        LoadInstrument(
            Workspace=wsName,
            Filename=str(instrumentFilePath),
            RewriteSpectraMap=True,
        )
        addInstrumentLogs(wsName, **instrumentLogsInfo)

        # The following properties will have been added to the logs for all workspaces.
        # These properties are _required_ for `RunMetadata`.
        run = mtd[wsName].mutableRun()
        run.addProperty("run_number", runNumber, True)
        run.addProperty("run_title", runTitle, True)

        cleanup_class_workspace_at_exit(wsName)
        return mtd[wsName]

    yield _create_sample_workspace

    # teardown...
    pass


@pytest.fixture(scope="class")
def create_sample_pixel_mask(cleanup_class_workspace_at_exit, create_sample_workspace):
    """
    This test fixture creates a sample mask workspace with the specified name and instrument settings,
      and initializes it with random values:

      * instrument settings are established from the provided `DetectorState` instance;
        note that the corresponding state-SHA is derivable using
        `LocalDataService._stateIdFromDetectorState(detectorState: DetectorState) -> ObjectSHA`;

      * the mask initialization applies the specified random fraction in [0.0, 1.0);

      * the workspace will be automatically cleaned up at the teardown of the test class.
    """

    def _create_sample_pixel_mask(
        maskWSName: WorkspaceName, detectorState: DetectorState, instrumentFilePath: Path, fraction: float
    ) -> MaskWorkspace:
        sampleWS = mtd.unique_name(prefix="donor_for_sample_mask_")
        create_sample_workspace(sampleWS, detectorState, instrumentFilePath, runNumber="0")
        createCompatibleMask(maskWSName, sampleWS)
        initializeRandomMask(maskWSName, fraction)
        cleanup_class_workspace_at_exit(maskWSName)
        return mtd[maskWSName]

    yield _create_sample_pixel_mask

    # teardown...
    pass


@pytest.fixture(scope="function")  # noqa: PT003
def create_per_test_sample_workspace(cleanup_workspace_at_exit):
    """
    This test fixture creates a sample workspace with the specified name and instrument settings:

      * instrument settings are established from the provided `DetectorState` instance;
        note that the corresponding state-SHA is derivable using
        `LocalDataService._stateIdFromDetectorState(detectorState: DetectorState) -> ObjectSHA`;

      * the workspace will be automatically cleaned up at the teardown of the test function.
    """

    def _create_sample_workspace(
        wsName: WorkspaceName,
        detectorState: DetectorState,
        instrumentFilePath: Path,
        runNumber: str,
        runTitle: str = "",
        units="TOF",
        numBanks=1,
        bankPixelWidth=4,
        numMonitors=0,
    ) -> MatrixWorkspace:
        # Descriptor info to set instrument logs (to assign state to workspaces)
        instrumentLogsInfo = getInstrumentLogDescriptors(detectorState)

        CreateSampleWorkspace(
            OutputWorkspace=wsName,
            Function="One Peak",
            NumBanks=numBanks,
            NumMonitors=numMonitors,
            BankPixelWidth=bankPixelWidth,
            NumEvents=500,
            Random=True,
            XUnit=units,
            XMin=0,
            XMax=8000,
            BinWidth=100,
        )
        LoadInstrument(
            Workspace=wsName,
            Filename=str(instrumentFilePath),
            RewriteSpectraMap=True,
        )
        addInstrumentLogs(wsName, **instrumentLogsInfo)

        # The following properties will have been added to the logs for all workspaces.
        # These properties are _required_ for `RunMetadata`.
        run = mtd[wsName].mutableRun()
        run.addProperty("run_number", runNumber, True)
        run.addProperty("run_title", runTitle, True)

        cleanup_workspace_at_exit(wsName)
        return mtd[wsName]

    yield _create_sample_workspace

    # teardown...
    pass
