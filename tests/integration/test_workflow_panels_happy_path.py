import os
import re
import tempfile
from contextlib import ExitStack, suppress
from pathlib import Path
from typing import Optional
from unittest import mock

import pytest
from mantid.kernel import amend_config
from qtpy import QtCore
from qtpy.QtCore import Qt
from qtpy.QtWidgets import (
    QMessageBox,
    QTabWidget,
)

# I would prefer not to access `LocalDataService` within an integration test,
#   however, for the moment, the reduction-data output relocation fixture is defined in the current file.
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.meta.Config import Config, Resource
from snapred.ui.main import SNAPRedGUI, prependDataSearchDirectories
from snapred.ui.view import InitializeStateCheckView
from snapred.ui.view.DiffCalAssessmentView import DiffCalAssessmentView
from snapred.ui.view.DiffCalRequestView import DiffCalRequestView
from snapred.ui.view.DiffCalSaveView import DiffCalSaveView
from snapred.ui.view.DiffCalTweakPeakView import DiffCalTweakPeakView
from snapred.ui.view.NormalizationRequestView import NormalizationRequestView
from snapred.ui.view.NormalizationSaveView import NormalizationSaveView
from snapred.ui.view.NormalizationTweakPeakView import NormalizationTweakPeakView
from snapred.ui.view.reduction.ReductionRequestView import ReductionRequestView
from snapred.ui.view.reduction.ReductionSaveView import ReductionSaveView
from util.Config_helpers import Config_override

# TODO: WorkflowNodeComplete signal, at end of each node!


class InterruptWithBlock(BaseException):
    pass


@pytest.fixture()
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


@pytest.fixture()
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


@pytest.mark.integration()
class TestGUIPanels:
    @pytest.fixture(scope="function", autouse=True)  # noqa: PT003
    def _setup_gui(self, qapp):
        # ---------------------------------------------------------------------------
        # DEFAULT PATCHES:
        #   FAIL TEST if any 'warning' OR 'critical' message boxes occur.
        #   In the test body, these patches are overridden for special cases.

        self._warningMessageBox = mock.patch(
            "qtpy.QtWidgets.QMessageBox.warning",
            lambda *args, **kwargs: pytest.fail(
                "WARNING messagebox:\n" + f"    args: {args}\n" + f"    kwargs: {kwargs}", pytrace=False
            ),
        )
        self._warningMessageBox.start()

        self._criticalMessageBox = mock.patch(
            "qtpy.QtWidgets.QMessageBox.critical",
            lambda *args, **kwargs: pytest.fail(
                "CRITICAL messagebox:\n" + f"    args: {args}\n" + f"    kwargs: {kwargs}", pytrace=False
            ),
        )
        self._criticalMessageBox.start()

        # patch log-warnings QMessage box: runs using `QMessageBox.exec`.
        self._logWarningsMessageBox = mock.patch(
            "qtpy.QtWidgets.QMessageBox.exec",
            lambda self, *args, **kwargs: QMessageBox.Ok
            if (
                "The backend has encountered warning(s)" in self.text()
                and "InstrumentDonor will only be used if GroupingFilename is in XML format." in self.detailedText()
            )
            else pytest.fail(
                "unexpected QMessageBox.exec:\n"
                + f"    args: {args}\n"
                + f"    kwargs: {kwargs}\n"
                + f"    text: '{self.text()}'\n"
                + f"    detailed text: '{self.detailedText()}'",
                pytrace=False,
            ),
        )
        self._logWarningsMessageBox.start()

        # Automatically continue at the end of each workflow.
        self._actionPrompt = mock.patch(
            "snapred.ui.presenter.WorkflowPresenter.ActionPrompt.prompt",
            lambda *args: TestGUIPanels._actionPromptContinue(
                *args, match=r".*The workflow has been completed successfully.*"
            ),
        )
        self._actionPrompt.start()
        # ---------------------------------------------------------------------------

        with Resource.open("../../src/snapred/resources/style.qss", "r") as styleSheet:
            qapp.setStyleSheet(styleSheet.read())

        # Establish context for each test: these normally run as part of `src/snapred/__main__.py`.
        self.exitStack = ExitStack()
        self.exitStack.enter_context(amend_config(data_dir=prependDataSearchDirectories(), prepend_datadir=True))
        yield

        # teardown...
        self._warningMessageBox.stop()
        self._criticalMessageBox.stop()
        self._logWarningsMessageBox.stop()
        self._actionPrompt.stop()
        self.exitStack.close()

    @staticmethod
    def _actionPromptContinue(title, message, action, parent=None, match=r".*"):  # noqa: ARG004
        _pattern = re.compile(match)
        if not _pattern.match(message):
            pytest.fail(
                f"unexpected: ActionPrompt.prompt('{title}', '{message}'...)\n"
                + f"    expecting:  ActionPrompt.prompt(...'{message}'...)"
            )

    @pytest.mark.skip(reason="each workflow panel now has a separate test")
    def test_calibration_and_reduction_panels_happy_path(
        self, qtbot, qapp, calibration_home_from_mirror, reduction_home_from_mirror
    ):
        # TODO: these could be initialized in the 'setup', but the current plan is to use a YML test template.
        reductionRunNumber = "46680"
        reductionStateId = "04bd2c53f6bf6754"

        # Override the mirror with a new calibration-home directory, omitting any existing
        #   calibration or normalization data.
        tmpCalibrationHomeDirectory = calibration_home_from_mirror()  # noqa: F841

        # Override the standard reduction-output location, using a temporary directory
        #   under the existing location within the mirror.
        tmpReductionHomeDirectory = reduction_home_from_mirror(reductionRunNumber)  # noqa: F841

        with (
            qtbot.captureExceptions() as exceptions,
            suppress(InterruptWithBlock),
        ):
            gui = SNAPRedGUI(translucentBackground=True)
            gui.show()
            qtbot.addWidget(gui)

            """
            SNAPRedGUI owns the following widgets:

              * self.calibrationPanelButton [new] = QPushButton("Open Calibration Panel")

                <button click> => self.newWindow = TestPanel(self).setWindowTitle("Calibration Panel")

              * LogTable("load dummy", self).widget

              * WorkspaceWidget(self)

              * AlgorithmProgressWidget()

              * <central widget>: QWidget().setObjectName("centralWidget")

              * self.userDocButton = UserDocsButton(self)
            """

            # Open the calibration panel:
            # QPushButton* button = pWin->findChild<QPushButton*>("Button name");
            qtbot.mouseClick(gui.calibrationPanelButton, QtCore.Qt.LeftButton)
            if len(exceptions):
                raise InterruptWithBlock
            calibrationPanel = gui.calibrationPanel

            # tab indices: (0) diffraction calibration, (1) normalization calibration, and (2) reduction
            calibrationPanelTabs = calibrationPanel.presenter.view.tabWidget

            ##########################################################################################################
            ##################### DIFFRACTION CALIBRATION PANEL: #####################################################
            ##########################################################################################################

            # Diffraction calibration:
            calibrationPanelTabs.setCurrentIndex(0)
            diffractionCalibrationWidget = calibrationPanelTabs.currentWidget()

            ### The use of this next signal is somewhat cryptic, but it makes testing the GUI much more stable:
            ##    The `actionCompleted` signal is emitted whenever a workflow node completes its action.
            ##    Otherwise, we would need to infer that the action is completed by other means.
            actionCompleted = (
                calibrationPanel.presenter.diffractionCalibrationWorkflow.workflow.presenter.actionCompleted
            )
            ###

            # node-tab indices: (0) request view, (1) tweak-peak view, (2) assessment view, and (4) save view
            workflowNodeTabs = diffractionCalibrationWidget.findChild(QTabWidget, "nodeTabs")

            requestView = workflowNodeTabs.currentWidget().view
            assert isinstance(requestView, DiffCalRequestView)

            #    set "Run Number", "Convergence Threshold", ,:
            requestView.runNumberField.setText(reductionRunNumber)
            requestView.fieldConvergenceThreshold.setText("0.1")
            requestView.fieldNBinsAcrossPeakWidth.setText("10")

            #    set all dropdown selections, but make sure that the dropdown contents are as expected
            requestView.sampleDropdown.setCurrentIndex(0)
            assert requestView.sampleDropdown.currentIndex() == 0
            assert requestView.sampleDropdown.currentText().endswith("Diamond_001.json")

            #    Without this next 'qtbot.wait(1000)',
            #      the 'groupingFileDropdown' gets reset after this successful initialization.
            #    I assume this is because somehow the 'populateGroupingDropdown',
            #      triggered by the 'runNumberField' 'editComplete', hasn't actually occurred yet?
            qtbot.wait(1000)
            requestView.groupingFileDropdown.setCurrentIndex(1)
            assert requestView.groupingFileDropdown.currentIndex() == 1
            assert requestView.groupingFileDropdown.currentText() == "Bank"

            requestView.peakFunctionDropdown.setCurrentIndex(0)
            assert requestView.peakFunctionDropdown.currentIndex() == 0
            assert requestView.peakFunctionDropdown.currentText() == "Gaussian"

            #    execute the request

            # TODO: make sure that there's no initialized state => abort the test if there is!
            waitForStateInit = True
            if (Path(Config["instrument.calibration.powder.home"]) / reductionStateId).exists():
                # raise RuntimeError(
                #           f"The state root directory for '{reductionStateId}' already exists! "\
                #           + " Please move it out of the way."
                #       )
                waitForStateInit = False

            if waitForStateInit:
                # ---------------------------------------------------------------------------
                # IMPORTANT: "initialize state" dialog is triggered by an exception throw:
                #   therefore, we cannot patch using a `with` clause!
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
                # ---------------------------------------------------------------------------

                #    (1) respond to the "initialize state" request
                with qtbot.waitSignal(actionCompleted, timeout=60000):
                    qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
                qtbot.waitUntil(
                    lambda: len(
                        [
                            o
                            for o in qapp.topLevelWidgets()
                            if isinstance(o, InitializeStateCheckView.InitializationMenu)
                        ]
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

            #    (2) execute the calibration workflow
            with qtbot.waitSignal(actionCompleted, timeout=60000):
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
            qtbot.waitUntil(
                lambda: isinstance(workflowNodeTabs.currentWidget().view, DiffCalTweakPeakView), timeout=60000
            )

            tweakPeakView = workflowNodeTabs.currentWidget().view

            # ---------------------------------------------------------------------------
            # Required patch for "TweakPeakView": ensure continuation if only two peaks are found.
            # IMPORTANT: "greater than two peaks" warning is triggered by an exception throw:
            #   => do _not_ patch using a with clause!
            warningMessageBox = mock.patch(  # noqa: PT008
                "qtpy.QtWidgets.QMessageBox.warning",
                lambda *args, **kwargs: QMessageBox.Yes,  # noqa: ARG005
            )
            warningMessageBox.start()
            # ---------------------------------------------------------------------------

            #    set "xtal dMin", "FWHM left", and "FWHM right": these are sufficient to get "46680" to pass.
            #    TODO: set ALL of the relevant fields, and use a test initialization template for this.
            tweakPeakView.fieldXtalDMin.setText("0.72")
            tweakPeakView.fieldFWHMleft.setText("2.0")
            tweakPeakView.fieldFWHMright.setText("2.0")
            tweakPeakView.peakFunctionDropdown.setCurrentIndex(0)
            assert tweakPeakView.peakFunctionDropdown.currentIndex() == 0
            assert tweakPeakView.peakFunctionDropdown.currentText() == "Gaussian"

            #    recalculate using the new values
            #    * recalculate => peak display is recalculated,
            #      not the entire calibration.
            qtbot.mouseClick(tweakPeakView.recalculationButton, Qt.MouseButton.LeftButton)
            qtbot.wait(10000)

            #    continue to the next panel
            with qtbot.waitSignal(actionCompleted, timeout=60000):
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)

            qtbot.waitUntil(
                lambda: isinstance(workflowNodeTabs.currentWidget().view, DiffCalAssessmentView), timeout=60000
            )

            # Placing this next `stop` correctly, causes some difficulty:
            #   if it's placed too early, the `warning` box patch doesn't correctly trap the "tweak peaks" warning.
            # So, for this reason this stop is placed _after_ the `waitUntil`.
            #   ("Tweak peaks" view should have definitely completed by this point.)
            warningMessageBox.stop()

            assessmentView = workflowNodeTabs.currentWidget().view  # noqa: F841
            #    nothing to do here, for this test

            #    continue to the next panel
            with qtbot.waitSignal(actionCompleted, timeout=60000):
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)

            qtbot.waitUntil(lambda: isinstance(workflowNodeTabs.currentWidget().view, DiffCalSaveView), timeout=5000)
            saveView = workflowNodeTabs.currentWidget().view

            #    set "author" and "comment"
            saveView.fieldAuthor.setText("kat")
            saveView.fieldComments.setText("calibration-panel integration test")

            #    continue in order to save workspaces and to finish the workflow
            with qtbot.waitSignal(actionCompleted, timeout=60000):
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)

            #   `ActionPrompt.prompt("..The workflow has completed successfully..)` gives immediate mocked response:
            #      However, here we still need to wait until the ADS cleanup has occurred,
            #      or else it will finish up in the middle of the next workflow. :(
            qtbot.waitUntil(
                lambda: isinstance(workflowNodeTabs.currentWidget().view, DiffCalRequestView), timeout=10000
            )

            #####################################################################################################
            ##################### NORMALIZATION PANEL: ##########################################################
            #####################################################################################################

            # Normalization calibration:
            calibrationPanelTabs.setCurrentIndex(1)
            normalizationCalibrationWidget = calibrationPanelTabs.currentWidget()

            #################################################################
            #### We need to access the signal specific to each workflow. ####
            #################################################################
            actionCompleted = (
                calibrationPanel.presenter.normalizationCalibrationWorkflow.workflow.presenter.actionCompleted
            )
            ###

            # node-tab indices: (0) request view, (1) tweak-peak view, and (3) save view
            workflowNodeTabs = normalizationCalibrationWidget.findChild(QTabWidget, "nodeTabs")

            requestView = workflowNodeTabs.currentWidget().view
            assert isinstance(requestView, NormalizationRequestView)

            #    set "Run Number", "Background run number":
            requestView.runNumberField.setText(reductionRunNumber)
            requestView.backgroundRunNumberField.setText(reductionRunNumber)

            #    set all dropdown selections, but make sure that the dropdown contents are as expected
            requestView.sampleDropdown.setCurrentIndex(0)
            assert requestView.sampleDropdown.currentIndex() == 0
            assert requestView.sampleDropdown.currentText().endswith("Diamond_001.json")

            #    Without this next 'qtbot.wait(1000)',
            #      the 'groupingFileDropdown' gets reset after this successful initialization.
            #    I assume this is because somehow the 'populateGroupingDropdown',
            #      triggered by the 'runNumberField' 'editComplete' hasn't actually occurred yet?
            qtbot.wait(1000)
            requestView.groupingFileDropdown.setCurrentIndex(1)
            assert requestView.groupingFileDropdown.currentIndex() == 1
            assert requestView.groupingFileDropdown.currentText() == "Bank"

            """ # Why no "peak function" for normalization calibration?!
            requestView.peakFunctionDropdown.setCurrentIndex(0)
            assert requestView.peakFunctionDropdown.currentIndex() == 0
            assert requestView.peakFunctionDropdown.currentText() == "Gaussian"
            """

            #    execute the request

            # TODO: make sure that there's no initialized state => abort the test if there is!
            waitForStateInit = True
            if (Path(Config["instrument.calibration.powder.home"]) / reductionStateId).exists():
                # raise RuntimeError(
                #           f"The state root directory for '{reductionStateId}' already exists! "\
                #           + "Please move it out of the way."
                #       )
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
                # ---------------------------------------------------------------------------

                #    (1) respond to the "initialize state" request
                with qtbot.waitSignal(actionCompleted, timeout=60000):
                    qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
                qtbot.waitUntil(
                    lambda: len(
                        [
                            o
                            for o in qapp.topLevelWidgets()
                            if isinstance(o, InitializeStateCheckView.InitializationMenu)
                        ]
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

            #    (2) execute the normalization workflow
            with qtbot.waitSignal(actionCompleted, timeout=60000):
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
            qtbot.waitUntil(
                lambda: isinstance(workflowNodeTabs.currentWidget().view, NormalizationTweakPeakView), timeout=60000
            )

            tweakPeakView = workflowNodeTabs.currentWidget().view

            #    set "Smoothing", "xtal dMin", "xtal dMax", "intensity threshold", and "groupingDropDown"
            tweakPeakView.smoothingSlider.field.setValue("0.4")
            tweakPeakView.fieldXtalDMin.setText("0.4")
            tweakPeakView.fieldXtalDMax.setText("90.0")

            #    See comment at prevous "groupingFileDropdown".
            qtbot.wait(1000)
            tweakPeakView.groupingFileDropdown.setCurrentIndex(0)
            assert tweakPeakView.groupingFileDropdown.currentIndex() == 0
            assert tweakPeakView.groupingFileDropdown.currentText() == "All"

            #    recalculate using the new values
            #    * recalculate => peak display is recalculated,
            #      not the entire calibration.
            qtbot.mouseClick(tweakPeakView.recalculationButton, Qt.MouseButton.LeftButton)
            qtbot.wait(5000)

            #    continue to the next panel
            with qtbot.waitSignal(actionCompleted, timeout=60000):
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
            qtbot.waitUntil(
                lambda: isinstance(workflowNodeTabs.currentWidget().view, NormalizationSaveView), timeout=60000
            )
            saveView = workflowNodeTabs.currentWidget().view

            #    set "author" and "comment"
            saveView.fieldAuthor.setText("kat")
            saveView.fieldComments.setText("calibration-panel integration test")

            #    continue in order to save workspaces and to finish the workflow
            with qtbot.waitSignal(actionCompleted, timeout=60000):
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)

            #   `ActionPrompt.prompt("..The workflow has completed successfully..)` gives immediate mocked response:
            #     Here we still need to wait until the ADS cleanup has occurred,
            #     or else it will happen in the middle of the next workflow. :(
            qtbot.waitUntil(
                lambda: isinstance(workflowNodeTabs.currentWidget().view, NormalizationRequestView), timeout=10000
            )

            #################################################################################################
            ##################### REDUCTION PANEL: ##########################################################
            #################################################################################################

            # Reduction:
            calibrationPanelTabs.setCurrentIndex(2)
            reductionWidget = calibrationPanelTabs.currentWidget()

            #################################################################
            #### We need to access the signal specific to each workflow. ####
            #################################################################
            actionCompleted = calibrationPanel.presenter.reductionWorkflow.workflow.presenter.actionCompleted
            ###

            # node-tab indices: (0) request view, and (2) save view
            workflowNodeTabs = reductionWidget.findChild(QTabWidget, "nodeTabs")

            requestView = workflowNodeTabs.currentWidget().view
            assert isinstance(requestView, ReductionRequestView)

            #    enter a "Run Number":
            requestView.runNumberInput.setText(reductionRunNumber)
            qtbot.mouseClick(requestView.enterRunNumberButton, Qt.MouseButton.LeftButton)
            _currentText = requestView.runNumberDisplay.toPlainText()
            _runNumbers = [num.strip() for num in _currentText.split("\n") if num.strip()]
            assert reductionRunNumber in _runNumbers

            """
            request.liteModeToggle.setState(True);
            request.retainUnfocusedDataCheckbox.setValue(False);
            """

            """
            # Set some pixel masks in the ADS, and on the filesystem, prior to this test section.  Then we can test
            #   the pixel-mask dropdown.
            request.pixelMaskDropdown.setCurrentIndex() # or set-selected range? or by index?
            """

            #    execute the request

            # TODO: make sure that there's no initialized state => abort the test if there is!
            waitForStateInit = True
            if (Path(Config["instrument.calibration.powder.home"]) / reductionStateId).exists():
                # raise RuntimeError(
                #           f"The state root directory for '{reductionStateId}' already exists! "\
                #           + "Please move it out of the way."
                #       )
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
                # ---------------------------------------------------------------------------

                #    (1) respond to the "initialize state" request
                with qtbot.waitSignal(actionCompleted, timeout=60000):
                    qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
                qtbot.waitUntil(
                    lambda: len(
                        [
                            o
                            for o in qapp.topLevelWidgets()
                            if isinstance(o, InitializeStateCheckView.InitializationMenu)
                        ]
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

            #    (2) execute the reduction workflow
            with qtbot.waitSignal(actionCompleted, timeout=120000):
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
            qtbot.waitUntil(lambda: isinstance(workflowNodeTabs.currentWidget().view, ReductionSaveView), timeout=60000)
            saveView = workflowNodeTabs.currentWidget().view

            """
            #    set "author" and "comment"
            saveView.fieldAuthor.setText("kat")
            saveView.fieldComments.setText("calibration-panel integration test")
            """

            #    continue in order to save workspaces and to finish the workflow
            with qtbot.waitSignal(actionCompleted, timeout=60000):
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)

            #   `ActionPrompt.prompt("..The workflow has completed successfully..)` gives immediate mocked response:
            #      Here we still need to wait until the ADS cleanup has occurred,
            #      or else it will happen in the middle of the next workflow. :(
            qtbot.waitUntil(
                lambda: isinstance(workflowNodeTabs.currentWidget().view, ReductionRequestView), timeout=5000
            )

            ###############################
            ########### END OF TEST #######
            ###############################

            calibrationPanel.widget.close()
            gui.close()

        #####################################################################
        # Force a printout of information about any exceptions that happened#
        #   within the Qt event loop.                                       #
        #####################################################################
        if len(exceptions):
            # sys.exc_info ::= type(e), e, <traceback>
            _, e, _ = exceptions[0]
            raise e

        # Force a clean exit
        qtbot.wait(5000)

    def test_diffraction_calibration_panel_happy_path(self, qtbot, qapp, calibration_home_from_mirror):
        # Override the mirror with a new home directory, omitting any existing
        #   calibration or normalization data.
        tmpCalibrationHomeDirectory = calibration_home_from_mirror()  # noqa: F841

        with (
            qtbot.captureExceptions() as exceptions,
            suppress(InterruptWithBlock),
        ):
            gui = SNAPRedGUI(translucentBackground=True)
            gui.show()
            qtbot.addWidget(gui)

            """
            SNAPRedGUI owns the following widgets:

              * self.calibrationPanelButton [new] = QPushButton("Open Calibration Panel")

                <button click> => self.newWindow = TestPanel(self).setWindowTitle("Calibration Panel")

              * LogTable("load dummy", self).widget

              * WorkspaceWidget(self)

              * AlgorithmProgressWidget()

              * <central widget>: QWidget().setObjectName("centralWidget")

              * self.userDocButton = UserDocsButton(self)
            """

            # Open the calibration panel:
            # QPushButton* button = pWin->findChild<QPushButton*>("Button name");
            qtbot.mouseClick(gui.calibrationPanelButton, QtCore.Qt.LeftButton)
            if len(exceptions):
                raise InterruptWithBlock
            calibrationPanel = gui.calibrationPanel

            # tab indices: (0) diffraction calibration, (1) normalization calibration, and (2) reduction
            calibrationPanelTabs = calibrationPanel.presenter.view.tabWidget

            ##########################################################################################################
            ##################### DIFFRACTION CALIBRATION PANEL: #####################################################
            ##########################################################################################################

            # Diffraction calibration:
            calibrationPanelTabs.setCurrentIndex(0)
            diffractionCalibrationWidget = calibrationPanelTabs.currentWidget()

            ### The use of this next signal is somewhat cryptic, but it makes testing the GUI much more stable:
            ##    The `actionCompleted` signal is emitted whenever a workflow node completes its action.
            ##    Otherwise, we would need to infer that the action is completed by other means.
            actionCompleted = (
                calibrationPanel.presenter.diffractionCalibrationWorkflow.workflow.presenter.actionCompleted
            )
            ###

            # node-tab indices: (0) request view, (1) tweak-peak view, (2) assessment view, and (4) save view
            workflowNodeTabs = diffractionCalibrationWidget.findChild(QTabWidget, "nodeTabs")

            requestView = workflowNodeTabs.currentWidget().view
            assert isinstance(requestView, DiffCalRequestView)

            #    set "Run Number", "Convergence Threshold", ,:
            requestView.runNumberField.setText("46680")
            requestView.fieldConvergenceThreshold.setText("0.1")
            requestView.fieldNBinsAcrossPeakWidth.setText("10")

            #    set all dropdown selections, but make sure that the dropdown contents are as expected
            requestView.sampleDropdown.setCurrentIndex(0)
            assert requestView.sampleDropdown.currentIndex() == 0
            assert requestView.sampleDropdown.currentText().endswith("Diamond_001.json")

            #    Without this next 'qtbot.wait(1000)',
            #      the 'groupingFileDropdown' gets reset after this successful initialization.
            #    I assume this is because somehow the 'populateGroupingDropdown',
            #    triggered by the 'runNumberField' 'editComplete' hasn't actually occurred yet?
            qtbot.wait(1000)
            requestView.groupingFileDropdown.setCurrentIndex(1)
            assert requestView.groupingFileDropdown.currentIndex() == 1
            assert requestView.groupingFileDropdown.currentText() == "Bank"

            requestView.peakFunctionDropdown.setCurrentIndex(0)
            assert requestView.peakFunctionDropdown.currentIndex() == 0
            assert requestView.peakFunctionDropdown.currentText() == "Gaussian"

            #    execute the request

            # TODO: make sure that there's no initialized state => abort the test if there is!
            #   At the moment, we preserve some options here to speed up development.
            stateId = "04bd2c53f6bf6754"
            waitForStateInit = True
            if (Path(Config["instrument.calibration.powder.home"]) / stateId).exists():
                # raise RuntimeError(
                #           f"The state root directory for '{stateId}' already exists! "\
                #           + "Please move it out of the way."
                #       )
                waitForStateInit = False

            if waitForStateInit:
                # ---------------------------------------------------------------------------
                # IMPORTANT: "initialize state" dialog is triggered by an exception throw:
                #   therefore, we cannot patch using a `with` clause!
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
                # ---------------------------------------------------------------------------

                #    (1) respond to the "initialize state" request
                with qtbot.waitSignal(actionCompleted, timeout=60000):
                    qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
                qtbot.waitUntil(
                    lambda: len(
                        [
                            o
                            for o in qapp.topLevelWidgets()
                            if isinstance(o, InitializeStateCheckView.InitializationMenu)
                        ]
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

            #    (2) execute the calibration workflow
            with qtbot.waitSignal(actionCompleted, timeout=60000):
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
            qtbot.waitUntil(
                lambda: isinstance(workflowNodeTabs.currentWidget().view, DiffCalTweakPeakView), timeout=60000
            )

            tweakPeakView = workflowNodeTabs.currentWidget().view

            # ---------------------------------------------------------------------------
            # Required patch for "TweakPeakView": ensure continuation if only two peaks are found.
            # IMPORTANT: "greater than two peaks" warning is triggered by an exception throw:
            #   => do _not_ patch using a with clause!
            warningMessageBox = mock.patch(  # noqa: PT008
                "qtpy.QtWidgets.QMessageBox.warning",
                lambda *args, **kwargs: QMessageBox.Yes,  # noqa: ARG005
            )
            warningMessageBox.start()
            # ---------------------------------------------------------------------------

            #    set "xtal dMin", "FWHM left", and "FWHM right": these are sufficient to get "46680" to pass.
            #    TODO: set ALL of the relevant fields, and use a test initialization template for this.
            tweakPeakView.fieldXtalDMin.setText("0.72")
            tweakPeakView.fieldFWHMleft.setText("2.0")
            tweakPeakView.fieldFWHMright.setText("2.0")
            tweakPeakView.peakFunctionDropdown.setCurrentIndex(0)
            assert tweakPeakView.peakFunctionDropdown.currentIndex() == 0
            assert tweakPeakView.peakFunctionDropdown.currentText() == "Gaussian"

            #    recalculate using the new values
            #    * recalculate => peak display is recalculated,
            #      not the entire calibration.
            qtbot.mouseClick(tweakPeakView.recalculationButton, Qt.MouseButton.LeftButton)
            qtbot.wait(10000)

            #    continue to the next panel
            with qtbot.waitSignal(actionCompleted, timeout=60000):
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)

            qtbot.waitUntil(
                lambda: isinstance(workflowNodeTabs.currentWidget().view, DiffCalAssessmentView), timeout=60000
            )

            # Placing this next `stop` correctly, causes some difficulty:
            #   if it's placed too early, the `warning` box patch doesn't correctly trap the "tweak peaks" warning.
            # So, for this reason this stop is placed _after_ the `waitUntil`.
            # ("Tweak peaks" view should have definitely completed by this point.)
            warningMessageBox.stop()

            assessmentView = workflowNodeTabs.currentWidget().view  # noqa: F841
            #    nothing to do here, for this test

            #    continue to the next panel
            with qtbot.waitSignal(actionCompleted, timeout=60000):
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)

            qtbot.waitUntil(lambda: isinstance(workflowNodeTabs.currentWidget().view, DiffCalSaveView), timeout=5000)
            saveView = workflowNodeTabs.currentWidget().view

            #    set "author" and "comment"
            saveView.fieldAuthor.setText("kat")
            saveView.fieldComments.setText("calibration-panel integration test")

            #    continue in order to save workspaces and to finish the workflow
            with qtbot.waitSignal(actionCompleted, timeout=60000):
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)

            #   `ActionPrompt.prompt("..The workflow has completed successfully..)` gives immediate mocked response:
            #      Here we still need to wait until the ADS cleanup has occurred,
            #      or else it will happen in the middle of the next workflow. :(
            qtbot.waitUntil(
                lambda: isinstance(workflowNodeTabs.currentWidget().view, DiffCalRequestView), timeout=10000
            )

            ###############################
            ########### END OF TEST #######
            ###############################

            calibrationPanel.widget.close()
            gui.close()

        #####################################################################
        # Force a printout of information about any exceptions that happened#
        #   within the Qt event loop.                                       #
        #####################################################################
        if len(exceptions):
            # sys.exc_info ::= type(e), e, <traceback>
            _, e, _ = exceptions[0]
            raise e

        # Force a clean exit
        qtbot.wait(5000)

    def test_normalization_panel_happy_path(self, qtbot, qapp, calibration_home_from_mirror):
        # Override the mirror with a new home directory, omitting any existing
        #   calibration or normalization data.
        tmpCalibrationHomeDirectory = calibration_home_from_mirror()  # noqa: F841

        with (
            qtbot.captureExceptions() as exceptions,
            suppress(InterruptWithBlock),
        ):
            gui = SNAPRedGUI(translucentBackground=True)
            gui.show()
            qtbot.addWidget(gui)

            """
            SNAPRedGUI owns the following widgets:

              * self.calibrationPanelButton [new] = QPushButton("Open Calibration Panel")

                <button click> => self.newWindow = TestPanel(self).setWindowTitle("Calibration Panel")

              * LogTable("load dummy", self).widget

              * WorkspaceWidget(self)

              * AlgorithmProgressWidget()

              * <central widget>: QWidget().setObjectName("centralWidget")

              * self.userDocButton = UserDocsButton(self)
            """

            # Open the calibration panel:
            # QPushButton* button = pWin->findChild<QPushButton*>("Button name");
            qtbot.mouseClick(gui.calibrationPanelButton, QtCore.Qt.LeftButton)
            if len(exceptions):
                raise InterruptWithBlock
            calibrationPanel = gui.calibrationPanel

            # tab indices: (0) diffraction calibration, (1) normalization calibration, and (2) reduction
            calibrationPanelTabs = calibrationPanel.presenter.view.tabWidget

            #####################################################################################################
            ##################### NORMALIZATION PANEL: ##########################################################
            #####################################################################################################

            # Normalization calibration:
            calibrationPanelTabs.setCurrentIndex(1)
            normalizationCalibrationWidget = calibrationPanelTabs.currentWidget()

            #################################################################
            #### We need to access the signal specific to each workflow. ####
            #################################################################
            actionCompleted = (
                calibrationPanel.presenter.normalizationCalibrationWorkflow.workflow.presenter.actionCompleted
            )
            ###

            # node-tab indices: (0) request view, (1) tweak-peak view, and (3) save view
            workflowNodeTabs = normalizationCalibrationWidget.findChild(QTabWidget, "nodeTabs")

            requestView = workflowNodeTabs.currentWidget().view
            assert isinstance(requestView, NormalizationRequestView)

            #    set "Run Number", "Background run number":
            requestView.runNumberField.setText("46680")
            requestView.backgroundRunNumberField.setText("46680")

            #    set all dropdown selections, but make sure that the dropdown contents are as expected
            requestView.sampleDropdown.setCurrentIndex(0)
            assert requestView.sampleDropdown.currentIndex() == 0
            assert requestView.sampleDropdown.currentText().endswith("Diamond_001.json")

            #    Without this next 'qtbot.wait(1000)',
            #      the 'groupingFileDropdown' gets reset after this successful initialization.
            #    I assume this is because somehow the 'populateGroupingDropdown',
            #    triggered by the 'runNumberField' 'editComplete' hasn't actually occurred yet?
            qtbot.wait(1000)
            requestView.groupingFileDropdown.setCurrentIndex(1)
            assert requestView.groupingFileDropdown.currentIndex() == 1
            assert requestView.groupingFileDropdown.currentText() == "Bank"

            """ # Why no "peak function" for normalization calibration?!
            requestView.peakFunctionDropdown.setCurrentIndex(0)
            assert requestView.peakFunctionDropdown.currentIndex() == 0
            assert requestView.peakFunctionDropdown.currentText() == "Gaussian"
            """

            #    execute the request

            # TODO: make sure that there's no initialized state => abort the test if there is!
            #   At the moment, we preserve some options here to speed up development.
            stateId = "04bd2c53f6bf6754"
            waitForStateInit = True
            if (Path(Config["instrument.calibration.powder.home"]) / stateId).exists():
                # raise RuntimeError(
                #           f"The state root directory for '{stateId}' already exists! "\
                #           + "Please move it out of the way."
                #       )
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
                # ---------------------------------------------------------------------------

                #    (1) respond to the "initialize state" request
                with qtbot.waitSignal(actionCompleted, timeout=60000):
                    qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
                qtbot.waitUntil(
                    lambda: len(
                        [
                            o
                            for o in qapp.topLevelWidgets()
                            if isinstance(o, InitializeStateCheckView.InitializationMenu)
                        ]
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

            #    (2) execute the normalization workflow
            with qtbot.waitSignal(actionCompleted, timeout=60000):
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
            qtbot.waitUntil(
                lambda: isinstance(workflowNodeTabs.currentWidget().view, NormalizationTweakPeakView), timeout=60000
            )

            tweakPeakView = workflowNodeTabs.currentWidget().view

            #    set "Smoothing", "xtal dMin", "xtal dMax", "intensity threshold", and "groupingDropDown"
            tweakPeakView.smoothingSlider.field.setValue("0.4")
            tweakPeakView.fieldXtalDMin.setText("0.4")
            tweakPeakView.fieldXtalDMax.setText("90.0")

            #    See comment at prevous "groupingFileDropdown".
            qtbot.wait(1000)
            tweakPeakView.groupingFileDropdown.setCurrentIndex(0)
            assert tweakPeakView.groupingFileDropdown.currentIndex() == 0
            assert tweakPeakView.groupingFileDropdown.currentText() == "All"

            #    recalculate using the new values
            #    * recalculate => peak display is recalculated,
            #      not the entire calibration.
            qtbot.mouseClick(tweakPeakView.recalculationButton, Qt.MouseButton.LeftButton)
            qtbot.wait(5000)

            #    continue to the next panel
            with qtbot.waitSignal(actionCompleted, timeout=60000):
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
            qtbot.waitUntil(
                lambda: isinstance(workflowNodeTabs.currentWidget().view, NormalizationSaveView), timeout=60000
            )
            saveView = workflowNodeTabs.currentWidget().view

            #    set "author" and "comment"
            saveView.fieldAuthor.setText("kat")
            saveView.fieldComments.setText("calibration-panel integration test")

            #    continue in order to save workspaces and to finish the workflow
            with qtbot.waitSignal(actionCompleted, timeout=60000):
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)

            #   `ActionPrompt.prompt("..The workflow has completed successfully..)` gives immediate mocked response:
            #      Here we still need to wait until the ADS cleanup has occurred,
            #      or else it will happen in the middle of the next workflow. :(
            qtbot.waitUntil(
                lambda: isinstance(workflowNodeTabs.currentWidget().view, NormalizationRequestView), timeout=10000
            )

            ###############################
            ########### END OF TEST #######
            ###############################

            calibrationPanel.widget.close()
            gui.close()

        #####################################################################
        # Force a printout of information about any exceptions that happened#
        #   within the Qt event loop.                                       #
        #####################################################################
        if len(exceptions):
            # sys.exc_info ::= type(e), e, <traceback>
            _, e, _ = exceptions[0]
            raise e

        # Force a clean exit
        qtbot.wait(5000)

    def test_reduction_panel_happy_path(self, qtbot, qapp, reduction_home_from_mirror):
        ##
        ## WARNING: this test requires EXISTING diffraction-calibration and normalization-calibration data!
        ##   As an alternative `test_calibration_and_reduction_panels_happy_path`, now skipped, could be run instead.
        ##

        # TODO: these could be initialized in the 'setup', but the current plan is to use a YML test template.
        reductionRunNumber = "46680"
        reductionStateId = "04bd2c53f6bf6754"

        # Override the standard reduction-output location, using a temporary directory
        #   under the existing location within the mirror.
        tmpReductionHomeDirectory = reduction_home_from_mirror(reductionRunNumber)  # noqa: F841

        with (
            qtbot.captureExceptions() as exceptions,
            suppress(InterruptWithBlock),
        ):
            gui = SNAPRedGUI(translucentBackground=True)
            gui.show()
            qtbot.addWidget(gui)

            """
            SNAPRedGUI owns the following widgets:

              * self.calibrationPanelButton [new] = QPushButton("Open Calibration Panel")

                <button click> => self.newWindow = TestPanel(self).setWindowTitle("Calibration Panel")

              * LogTable("load dummy", self).widget

              * WorkspaceWidget(self)

              * AlgorithmProgressWidget()

              * <central widget>: QWidget().setObjectName("centralWidget")

              * self.userDocButton = UserDocsButton(self)
            """

            # Open the calibration panel:
            # QPushButton* button = pWin->findChild<QPushButton*>("Button name");
            qtbot.mouseClick(gui.calibrationPanelButton, QtCore.Qt.LeftButton)
            if len(exceptions):
                raise InterruptWithBlock
            calibrationPanel = gui.calibrationPanel

            # tab indices: (0) diffraction calibration, (1) normalization calibration, and (2) reduction
            calibrationPanelTabs = calibrationPanel.presenter.view.tabWidget

            #################################################################################################
            ##################### REDUCTION PANEL: ##########################################################
            #################################################################################################

            # Reduction:
            calibrationPanelTabs.setCurrentIndex(2)
            reductionWidget = calibrationPanelTabs.currentWidget()

            #################################################################
            #### We need to access the signal specific to each workflow. ####
            #################################################################
            actionCompleted = calibrationPanel.presenter.reductionWorkflow.workflow.presenter.actionCompleted
            ###

            # node-tab indices: (0) request view, and (2) save view
            workflowNodeTabs = reductionWidget.findChild(QTabWidget, "nodeTabs")

            requestView = workflowNodeTabs.currentWidget().view
            assert isinstance(requestView, ReductionRequestView)

            # Without this next wait, the "run number entry" section happens too fast.
            # (And I'd love to understand _why_!  :(  )
            qtbot.wait(1000)

            #    enter a "Run Number":
            requestView.runNumberInput.setText(reductionRunNumber)
            qtbot.mouseClick(requestView.enterRunNumberButton, Qt.MouseButton.LeftButton)

            _currentText = requestView.runNumberDisplay.toPlainText()
            _runNumbers = [num.strip() for num in _currentText.split("\n") if num.strip()]

            assert reductionRunNumber in _runNumbers

            """
            request.liteModeToggle.setState(True);
            request.retainUnfocusedDataCheckbox.setValue(False);
            """

            """
            # Set some pixel masks in the ADS, and on the filesystem, prior to this test section.  Then we can test
            #   the pixel-mask dropdown.
            request.pixelMaskDropdown.setCurrentIndex() # or set-selected range? or by index?
            """

            #    execute the request

            # TODO: make sure that there's no initialized state => abort the test if there is!
            waitForStateInit = True
            if (Path(Config["instrument.calibration.powder.home"]) / reductionStateId).exists():
                # raise RuntimeError(
                #           f"The state root directory for '{reductionStateId}' already exists! "\
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
                # ---------------------------------------------------------------------------

                #    (1) respond to the "initialize state" request
                with qtbot.waitSignal(actionCompleted, timeout=60000):
                    qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
                qtbot.waitUntil(
                    lambda: len(
                        [
                            o
                            for o in qapp.topLevelWidgets()
                            if isinstance(o, InitializeStateCheckView.InitializationMenu)
                        ]
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

            #    (2) execute the reduction workflow
            with qtbot.waitSignal(actionCompleted, timeout=120000):
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
            qtbot.waitUntil(lambda: isinstance(workflowNodeTabs.currentWidget().view, ReductionSaveView), timeout=60000)
            saveView = workflowNodeTabs.currentWidget().view  # noqa: F841

            """
            #    set "author" and "comment"
            saveView.fieldAuthor.setText("kat")
            saveView.fieldComments.setText("calibration-panel integration test")
            """

            #    continue in order to save workspaces and to finish the workflow
            with qtbot.waitSignal(actionCompleted, timeout=60000):
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)

            #   `ActionPrompt.prompt("..The workflow has completed successfully..)` gives immediate mocked response:
            #      Here we still need to wait until the ADS cleanup has occurred,
            #      or else it will happen in the middle of the next workflow. :(
            qtbot.waitUntil(
                lambda: isinstance(workflowNodeTabs.currentWidget().view, ReductionRequestView), timeout=5000
            )

            ###############################
            ########### END OF TEST #######
            ###############################

            calibrationPanel.widget.close()
            gui.close()

        #####################################################################
        # Force a printout of information about any exceptions that happened#
        #   within the Qt event loop.                                       #
        #####################################################################
        if len(exceptions):
            # sys.exc_info ::= type(e), e, <traceback>
            _, e, _ = exceptions[0]
            raise e

        # Force a clean exit
        qtbot.wait(5000)
