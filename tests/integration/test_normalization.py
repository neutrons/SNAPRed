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
from util.Config_helpers import Config_override
from util.qt_mock_util import MockQMessageBox

# I would prefer not to access `LocalDataService` within an integration test,
#   however, for the moment, the reduction-data output relocation fixture is defined in the current file.
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.meta.Config import Config, Resource
from snapred.meta.Enum import StrEnum
from snapred.ui.main import SNAPRedGUI, prependDataSearchDirectories
from snapred.ui.view import InitializeStateCheckView
from snapred.ui.view.NormalizationRequestView import NormalizationRequestView
from snapred.ui.view.NormalizationSaveView import NormalizationSaveView
from snapred.ui.view.NormalizationTweakPeakView import NormalizationTweakPeakView


class InterruptWithBlock(BaseException):
    pass


class TestSummary:
    def __init__(self):
        self._index = 0
        self._steps = []

    def SUCCESS(self):
        step = self._steps[self._index]
        step.status = self.TestStep.StepStatus.SUCCESS
        self._index += 1

    def FAILURE(self):
        step = self._steps[self._index]
        step.status = self.TestStep.StepStatus.FAILURE
        self._index += 1

    def isComplete(self):
        return self._index == len(self._steps)

    def isFailure(self):
        return any(step.status == self.TestStep.StepStatus.FAILURE for step in self._steps)

    def builder():
        return TestSummary.TestSummaryBuilder()

    def __str__(self):
        longestStatus = max(len(step.status) for step in self._steps)
        longestName = max(len(step.name) for step in self._steps)
        tableCapStr = "#" * (longestName + longestStatus + 6)
        tableStr = (
            f"\n{tableCapStr}\n"
            + "\n".join(f"# {step.name:{longestName}}: {step.status:{longestStatus}} #" for step in self._steps)
            + f"\n{tableCapStr}\n"
        )
        return tableStr

    class TestStep:
        class StepStatus(StrEnum):
            SUCCESS = "SUCCESS"
            FAILURE = "FAILURE"
            INCOMPLETE = "INCOMPLETE"

        def __init__(self, name: str):
            self.name = name
            self.status = self.StepStatus.INCOMPLETE

    class TestSummaryBuilder:
        def __init__(self):
            self.summary = TestSummary()

        def step(self, name: str):
            self.summary._steps.append(TestSummary.TestStep(name))
            return self

        def build(self):
            return self.summary


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


@pytest.mark.datarepo
@pytest.mark.integration
class TestGUIPanels:
    @pytest.fixture(scope="function", autouse=True)  # noqa: PT003
    def _setup_gui(self, qapp):
        # ---------------------------------------------------------------------------
        # DEFAULT PATCHES:
        #   FAIL TEST if any 'warning' OR 'critical' message boxes occur.
        #   In the test body, these patches are overridden for special cases.
        # pass
        # self._warningMessageBox = mock.patch(
        #     "qtpy.QtWidgets.QMessageBox.warning",
        #     lambda *args, **kwargs: pytest.fail(
        #         "WARNING This test seems to be missing expected calibration and/or normalization data", pytrace=False
        #     )
        #     if "Reduction is missing calibration data," in args[2]
        #     else pytest.fail("WARNING messagebox:\n" + f"    args: {args}\n" + f" kwargs: {kwargs}", pytrace=False),
        # )
        # self._warningMessageBox.start()

        # self._criticalMessageBox = mock.patch(
        #     "qtpy.QtWidgets.QMessageBox.critical",
        #     lambda *args, **kwargs: pytest.fail(
        #         "CRITICAL messagebox:\n" + f"    args: {args}\n" + f"    kwargs: {kwargs}", pytrace=False
        #     ),
        # )
        # self._criticalMessageBox.start()

        # patch log-warnings QMessage box: runs using `QMessageBox.exec`.
        self._logWarningsMessageBox = mock.patch(
            "qtpy.QtWidgets.QMessageBox.exec",
            lambda self, *args, **kwargs: QMessageBox.Ok
            if (
                "The backend has encountered warning(s)" in self.text()
                and (
                    "InstrumentDonor will only be used if GroupingFilename is in XML format." in self.detailedText()
                    or "No valid FocusGroups were specified for mode: 'lite'" in self.detailedText()
                )
            )
            else pytest.fail(
                "unexpected QMessageBox.exec:"
                + f"    args: {args}"
                + f"    kwargs: {kwargs}"
                + f"    text: '{self.text()}'"
                + f"    detailed text: '{self.detailedText()}'",
                pytrace=False,
            ),
        )
        self._logWarningsMessageBox.start()

        # # Automatically continue at the end of each workflow.
        # self._actionPrompt = mock.patch(
        #     "qtpy.QtWidgets.QMessageBox.information",
        #     lambda *args: TestGUIPanels._actionPromptContinue(*args, match=r".*has been completed successfully.*"),
        # )
        # self._actionPrompt.start()
        # # ---------------------------------------------------------------------------

        with Resource.open("../../src/snapred/resources/style.qss", "r") as styleSheet:
            qapp.setStyleSheet(styleSheet.read())

        # # Establish context for each test: these normally run as part of `src/snapred/__main__.py`.
        self.exitStack = ExitStack()
        self.exitStack.enter_context(amend_config(data_dir=prependDataSearchDirectories(), prepend_datadir=True))

        self.testSummary = None
        yield

        if isinstance(self.testSummary, TestSummary):
            if not self.testSummary.isComplete():
                self.testSummary.FAILURE()
            if self.testSummary.isFailure():
                pytest.fail(f"Test Summary (-vv for full table): {self.testSummary}")
        # # teardown...
        # self._warningMessageBox.stop()
        # self._criticalMessageBox.stop()
        self._logWarningsMessageBox.stop()
        # self._actionPrompt.stop()
        self.exitStack.close()

    @staticmethod
    def _actionPromptContinue(parent, title, message, match=r".*"):  # noqa: ARG004
        _pattern = re.compile(match)
        if not _pattern.match(message):
            pytest.fail(
                f"unexpected: QMessageBox.information('{title}', '{message}'...)\n"
                + f"    expecting:  QMessageBox.information(...'{message}'...)"
            )

    def test_normalization_regression(self, qtbot, qapp, calibration_home_from_mirror):
        # Override the mirror with a new home directory, omitting any existing
        #   calibration or normalization data.
        tmpCalibrationHomeDirectory = calibration_home_from_mirror()  # noqa: F841
        self.testSummary = (
            TestSummary.builder()
            .step("Open the GUI")
            .step("Open the calibration panel")
            .step("Set the diffraction calibration request")
            .step("Execute the diffraction calibration request")
            .step("Tweak the peaks")
            .step("Assess the peaks")
            .step("Save the diffraction calibration")
            .step("Close the GUI")
            .build()
        )
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
            calibrationPanelTabs.setCurrentIndex(2)
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

            # Verify tweak peak and save tabs are disabled

            # Set negative number and small positive number for RunNumber
            # and verify the 2 corresponding msg boxes show up.
            # Repeat for Background Run number box

            # The expected msg is set so that the test should fail, but it fails later
            # patch log-warnings QMessage box: runs using `QMessageBox.exec`.
            # self._customlogWarningsMessageBox = mock.patch(
            #     "qtpy.QtWidgets.QMessageBox.exec",
            #     lambda self, *args, **kwargs: QMessageBox.Ok
            #     if (
            #         "The backend has encountered warning(s)" in self.text()
            #         and "Run number -2 must be a positive integer" in self.detailedText()
            #     )
            #     else pytest.fail(
            #         "unexpected QMessageBox.exec:"
            #         + f"    args: {args}"
            #         + f"    kwargs: {kwargs}"
            #         + f"    text: '{self.text()}'"
            #         + f"    detailed text: '{self.detailedText()}'",
            #         pytrace=True,
            #     ),
            # )
            testMock = MockQMessageBox()
            self._customlogWarningsMessageBox = testMock.exec(testMock, ref=self, msg="it failed")
            self._customlogWarningsMessageBox.start()
            requestView.runNumberField.setText("-1")
            qtbot.keyPress(requestView.runNumberField.field, Qt.Key_Enter)
            qtbot.wait(1000)
            self._customlogWarningsMessageBox.stop()
            # expect error msg

            requestView.runNumberField.setText("1")
            qtbot.keyPress(requestView.runNumberField.field, Qt.Key_Enter)
            qtbot.wait(1000)
            # expect error msg

            requestView.backgroundRunNumberField.setText("-1")
            qtbot.keyPress(requestView.backgroundRunNumberField.field, Qt.Key_Enter)
            qtbot.wait(1000)
            # expect error msg

            requestView.backgroundRunNumberField.setText("1")
            qtbot.keyPress(requestView.backgroundRunNumberField.field, Qt.Key_Enter)
            qtbot.wait(1000)
            # expect error msg

            # Enter 58810 for run number and 58813 for background and click continue

            requestView.runNumberField.setText("58810")
            requestView.backgroundRunNumberField.setText("58813")
            qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)

            # Verify error msg box to select sample shows up

            # Select Vanadium Cylinder as sample and click continue
            requestView.sampleDropdown.setCurrentIndex(3)
            assert requestView.sampleDropdown.currentIndex() == 3
            assert requestView.sampleDropdown.currentText().endswith("Silicon_NIST_640D_001.json")
            qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)

            # Verify error msg box to select grouping shows up

            # Select column grouping and click continue
            requestView.groupingFileDropdown.setCurrentIndex(0)
            assert requestView.groupingFileDropdown.currentIndex() == 0
            assert requestView.groupingFileDropdown.currentText() == "Column"
            qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)

            # On tweak peaks tab, test each text field(3 of these) by putting non numeric chars and verify error msg
            tweakPeakView = workflowNodeTabs.currentWidget().view

            tweakPeakView.smoothingSlider.field.setValue("a")
            # expect error msg
            tweakPeakView.fieldXtalDMin.setText("a")
            # expect error msg
            tweakPeakView.fieldXtalDMax.setText("a")
            # expect error msg

            qtbot.mouseClick(tweakPeakView.recalculationButton, Qt.MouseButton.LeftButton)
            qtbot.wait(5000)

            # Then set negative values and verify errors on recalculate
            tweakPeakView.smoothingSlider.field.setValue("-1")
            # expect error msg
            tweakPeakView.fieldXtalDMin.setText("-1")
            # expect error msg
            tweakPeakView.fieldXtalDMax.setText("-1")
            # expect error msg

            qtbot.mouseClick(tweakPeakView.recalculationButton, Qt.MouseButton.LeftButton)
            qtbot.wait(5000)

            # Set "0" on smoothing param, should error(maybe crash?)
            tweakPeakView.smoothingSlider.field.setValue("0")
            # expect error msg
            qtbot.mouseClick(tweakPeakView.recalculationButton, Qt.MouseButton.LeftButton)
            qtbot.wait(5000)

            # Set Dmin to 1, recalculate
            qtbot.mouseClick(tweakPeakView.recalculationButton, Qt.MouseButton.LeftButton)
            qtbot.wait(5000)

            # Set Dmax to 2.8, recalculate
            qtbot.mouseClick(tweakPeakView.recalculationButton, Qt.MouseButton.LeftButton)
            qtbot.wait(5000)

            # check smoothing is logarithmic, go to saving tab
            qtbot.mouseClick(tweakPeakView.recalculationButton, Qt.MouseButton.LeftButton)
            qtbot.wait(5000)
            qtbot.mouseClick(tweakPeakView.recalculationButton, Qt.MouseButton.LeftButton)
            qtbot.wait(5000)

            with qtbot.waitSignal(actionCompleted, timeout=60000):
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
            qtbot.waitUntil(
                lambda: isinstance(workflowNodeTabs.currentWidget().view, NormalizationSaveView), timeout=60000
            )
            saveView = workflowNodeTabs.currentWidget().view

            # Do error checking on all text fields

            # Set Comments to "This is a test"
            saveView.fieldComments.setText("This is a test")

            # Set author to "Test"
            saveView.fieldAuthor.setText("Test")

            # Finish and observe "Workflow complete"
            #    continue in order to save workspaces and to finish the workflow
            with qtbot.waitSignal(actionCompleted, timeout=60000):
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)

            #   `ActionPrompt.prompt("..The workflow has completed successfully..)` gives immediate mocked response:
            #      Here we still need to wait until the ADS cleanup has occurred,
            #      or else it will happen in the middle of the next workflow. :(
            qtbot.waitUntil(
                lambda: isinstance(workflowNodeTabs.currentWidget().view, NormalizationRequestView), timeout=10000
            )

            # Verify NormalizationIndex.json is updated

            # Verify 5 output files created

            #    set "Run Number", "Background run number":
            requestView.runNumberField.setText("58882")
            requestView.backgroundRunNumberField.setText("58882")

            requestView.litemodeToggle.setState(False)

            #    set all dropdown selections, but make sure that the dropdown contents are as expected
            requestView.sampleDropdown.setCurrentIndex(3)
            assert requestView.sampleDropdown.currentIndex() == 3
            assert requestView.sampleDropdown.currentText().endswith("Silicon_NIST_640D_001.json")

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

            warningMessageBox = mock.patch(  # noqa: PT008
                "qtpy.QtWidgets.QMessageBox.warning",
                lambda *args, **kwargs: QMessageBox.Yes,  # noqa: ARG005
            )
            warningMessageBox.start()

            #    (2) execute the normalization workflow
            with qtbot.waitSignal(actionCompleted, timeout=60000):
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
            qtbot.waitUntil(
                lambda: isinstance(workflowNodeTabs.currentWidget().view, NormalizationTweakPeakView), timeout=60000
            )
            warningMessageBox.stop()
            tweakPeakView = workflowNodeTabs.currentWidget().view

            #    set "Smoothing", "xtal dMin", "xtal dMax", "intensity threshold", and "groupingDropDown"
            tweakPeakView.smoothingSlider.field.setValue("0.4")
            tweakPeakView.fieldXtalDMin.setText("0.4")
            tweakPeakView.fieldXtalDMax.setText("90.0")

            #    See comment at prevous "groupingFileDropdown".
            qtbot.wait(1000)
            tweakPeakView.groupingFileDropdown.setCurrentIndex(0)
            assert tweakPeakView.groupingFileDropdown.currentIndex() == 0
            assert tweakPeakView.groupingFileDropdown.currentText() == "Column"

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
