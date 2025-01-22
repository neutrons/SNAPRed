import os
import tempfile
import time
from contextlib import ExitStack
from pathlib import Path
from typing import List, Optional
from unittest import mock

import pytest
from mantid.simpleapi import DeleteWorkspaces, mtd
from qtpy.QtCore import Qt
from qtpy.QtWidgets import (
    QMessageBox,
)
from util.Config_helpers import Config_override

# I would prefer not to access `LocalDataService` within an integration test,
#   however, for the moment, the reduction-data output relocation fixture is defined in the current file.
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.meta.Config import Config
from snapred.ui.view import InitializeStateCheckView

# Import required test fixtures at the end of either the main `conftest.py`,
#   or any `conftest.py` at the test-module level directory.


## WARNING:
#  * The following two methods duplicate code, however the _closures_ need to be distinct!


@pytest.fixture(scope="function")  # noqa: PT003
def cleanup_workspace_at_exit():
    # Allow cleanup of workspaces in the ADS
    #   in a manner compatible with _parallel_ testing.
    _workspaces: List[str] = []

    def _cleanup_workspace_at_exit(wsName: str):
        _workspaces.append(wsName)

    yield _cleanup_workspace_at_exit

    # teardown
    try:
        if _workspaces:
            # Warning: `DeleteWorkspaces`' input validator throws an exception
            #   if a specified workspace doesn't exist in the ADS;

            # Provide an error diagnostic message, but do not bypass the error:
            #     the workspaces list must be correct.
            non_existent_workspaces = set([ws for ws in _workspaces if not mtd.doesExist(ws)])
            print(f"Non-existent workspaces: {non_existent_workspaces}.")

            DeleteWorkspaces(_workspaces)
    except RuntimeError:
        pass


@pytest.fixture(scope="class")
def cleanup_class_workspace_at_exit():
    # Allow cleanup of workspaces in the ADS
    #   in a manner compatible with _parallel_ testing.
    _workspaces: List[str] = []

    def _cleanup_workspace_at_exit(wsName: str):
        _workspaces.append(wsName)

    yield _cleanup_workspace_at_exit

    # teardown
    try:
        if _workspaces:
            # Warning: `DeleteWorkspaces`' input validator throws an exception
            #   if a specified workspace doesn't exist in the ADS;

            # Provide an error diagnostic message, but do not bypass the error:
            #     the workspaces list must be correct.
            print(f"Non-existent workspaces: {set([ws for ws in _workspaces if not mtd.doesExist(ws)])}.")

            DeleteWorkspaces(_workspaces)
    except RuntimeError:
        pass


@pytest.fixture
def get_unique_timestamp():
    """
    This method re-uses code from `LocalDataService.getUniqueTimestamp`.

    Generate a unique timestamp:

    * on some operating systems `time.time()` only has resolution to seconds;

    * this method checks its own most-recently returned value, and if necessary,
      increments it.

    * the complete `float` representation of the unix timestamp is retained,
      in order to allow arbitrary formatting.

    """
    _previousTimestamp = None

    def _get_unique_timestamp() -> float:
        nextTimestamp = time.time()
        nonlocal _previousTimestamp
        if _previousTimestamp is not None:
            # compare as `time.struct_time`
            if nextTimestamp < _previousTimestamp or time.gmtime(nextTimestamp) == time.gmtime(_previousTimestamp):
                nextTimestamp = _previousTimestamp + 1.0
        _previousTimestamp = nextTimestamp
        return nextTimestamp

    yield _get_unique_timestamp

    # teardown ...
    pass


@pytest.fixture
def calibration_home_from_mirror():
    # Test fixture to create a copy of the calibration home directory from an existing mirror:
    # * creates a temporary calibration home directory under the optional `prefix` path;
    #   when not specified, the temporary directory is created under the existing
    #   `Config["instrument.calibration.powder.home"]`;
    # * creates symlinks within the directory to required metadata files and directories
    #   from the already existing `Config["instrument.calibration.powder.home"]`;
    # * ignores any existing diffraction-calibration and normalization-calibration subdirectories;
    # * and finally, overrides the `Config` entry for "instrument.calibration.powder.home".

    # IMPLEMENTATION notes:
    # * The functionality of this fixture is deliberately NOT implemented as a context manager,
    #   although certain context-manager features are used.
    # * If this were a context manager, it would  be terminated at any exception throw.  For example,
    #   it would be terminated by the "initialize state" `RecoverableException`.  Such termination would interfere with
    #   the requirements of the integration tests.
    _stack = ExitStack()

    def _calibration_home_from_mirror(prefix: Optional[Path] = None):
        originalCalibrationHome: Path = Path(Config["instrument.calibration.powder.home"])
        if prefix is None:
            prefix = originalCalibrationHome

        # Override calibration home directory:
        tmpCalibrationHome = Path(_stack.enter_context(tempfile.TemporaryDirectory(dir=prefix, suffix=os.sep)))
        assert tmpCalibrationHome.exists()
        _stack.enter_context(Config_override("instrument.calibration.powder.home", str(tmpCalibrationHome)))

        # WARNING: for these integration tests `LocalDataService` is a singleton.
        #   The Indexer's `lru_cache` MUST be reset after the Config override, otherwise
        #     it will return indexers synched to the previous `Config["instrument.calibration.powder.home"]`.
        LocalDataService()._indexer.cache_clear()

        # Create symlinks to metadata files and directories.
        metadatas = [Path("LiteGroupMap.hdf"), Path("PixelGroupingDefinitions"), Path("SNAPLite.xml")]
        for path_ in metadatas:
            os.symlink(originalCalibrationHome / path_, tmpCalibrationHome / path_)
        return tmpCalibrationHome

    yield _calibration_home_from_mirror

    # teardown => __exit__
    _stack.close()
    LocalDataService()._indexer.cache_clear()


@pytest.fixture
def reduction_home_from_mirror():
    # Test fixture to write reduction data to a temporary directory under `Config["instrument.reduction.home"]`.
    # * creates a temporary reduction state root directory under the optional `prefix` path;
    #   when not specified, the temporary directory is created under the existing
    #   `Config["instrument.reduction.home"]` (with the substituted 'IPTS' tag).
    # * overrides the `Config` entry for "instrument.reduction.home".

    # IMPLEMENTATION notes: (see previous).
    _stack = ExitStack()

    def _reduction_home_from_mirror(runNumber: str, prefix: Optional[Path] = None):
        if prefix is None:
            dataService = LocalDataService()
            originalReductionHome = dataService._constructReductionStateRoot(runNumber)

            # WARNING: this 'mkdir' step will not be reversed at exit,
            #   but that shouldn't matter very much.
            originalReductionHome.mkdir(parents=True, exist_ok=True)
            prefix = originalReductionHome

            tmpReductionHome = Path(_stack.enter_context(tempfile.TemporaryDirectory(dir=prefix, suffix=os.sep)))

            # Ensure that `_createReductionStateRoot` will return the temporary directory,
            #   while still exercising it's IPTS-substitution functionality.
            _stack.enter_context(
                Config_override(
                    "instrument.reduction.home", Config["instrument.reduction.home"] + os.sep + tmpReductionHome.name
                )
            )

            # No `LocalDataService._indexer.cache_clear()` should be required here, but keep it in mind, just in case!

        else:
            # Specified prefix => just use that, without any substitution.
            # In this case `_constructReductionStateRoot` will return a path
            #   which does not depend on the IPTS-directory for the run number.
            tmpReductionHome = Path(_stack.enter_context(tempfile.TemporaryDirectory(dir=prefix, suffix=os.sep)))
            _stack.enter_context(Config_override("instrument.reduction.home", str(tmpReductionHome)))

        assert tmpReductionHome.exists()
        return tmpReductionHome

    yield _reduction_home_from_mirror

    # teardown => __exit__
    _stack.close()


def handleStateInit(waitForStateInit, stateId, qtbot, qapp, actionCompleted, workflowNodeTabs):
    if (Path(Config["instrument.calibration.powder.home"]) / stateId).exists():
        # raise RuntimeError(
        #           f"The state root directory for '{stateId}' already exists! "\
        #           + "Please move it out of the way."
        # )
        waitForStateInit = False
    if waitForStateInit:
        # ---------------------------------------------------------------------------
        # IMPORTANT: "initialize state" dialog is triggered by an exception throw:
        #   => do _not_ patch using a with clause!
        questionMessageBox = mock.patch(  # noqa: PT008
            "qtpy.QtWidgets.QMessageBox.question",
            lambda *args, **kwargs: QMessageBox.Yes,  # noqa: ARG005
        )
        questionMessageBox.start()
        successPrompt = mock.patch(
            "snapred.ui.widget.SuccessPrompt.SuccessPrompt.prompt",
            lambda parent: parent.close() if parent is not None else None,
        )
        successPrompt.start()
        # --------------------------------------------------------------------------
        #    (1) respond to the "initialize state" request
        with qtbot.waitSignal(actionCompleted, timeout=60000):
            qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
        qtbot.waitUntil(
            lambda: len(
                [o for o in qapp.topLevelWidgets() if isinstance(o, InitializeStateCheckView.InitializationMenu)]
            )
            > 0,
            timeout=1000,
        )
        stateInitDialog = [
            o for o in qapp.topLevelWidgets() if isinstance(o, InitializeStateCheckView.InitializationMenu)
        ][0]
        stateInitDialog.stateNameField.setText("my happy state")
        qtbot.mouseClick(stateInitDialog.beginFlowButton, Qt.MouseButton.LeftButton)
        # State initialization dialog is "application modal" => no need to explicitly wait
        questionMessageBox.stop()
        successPrompt.stop()
