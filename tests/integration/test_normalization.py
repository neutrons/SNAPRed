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
from util.TestSummary import TestSummary

# I would prefer not to access `LocalDataService` within an integration test,
#   however, for the moment, the reduction-data output relocation fixture is defined in the current file.
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.meta.Config import Config, Resource
from snapred.ui.main import SNAPRedGUI, prependDataSearchDirectories
from snapred.ui.view import InitializeStateCheckView
from snapred.ui.view.NormalizationRequestView import NormalizationRequestView
from snapred.ui.view.NormalizationSaveView import NormalizationSaveView
from snapred.ui.view.NormalizationTweakPeakView import NormalizationTweakPeakView


class InterruptWithBlock(BaseException):
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


@pytest.mark.datarepo
@pytest.mark.integration
class TestGUIPanels:
    @pytest.fixture(scope="function", autouse=True)  # noqa: PT003
    def _setup_gui(self, qapp):
        testMock = MockQMessageBox()
        msg = "No valid FocusGroups were specified for mode: 'lite'"
        self._logWarningsMessageBox = testMock.exec(testMock, ref=self, msg=msg)
        self._logWarningsMessageBox.start()

        # Automatically continue at the end of each workflow.
        self._actionPrompt = mock.patch(
            "qtpy.QtWidgets.QMessageBox.information",
            lambda *args: TestGUIPanels._actionPromptContinue(*args, match=r".*has been completed successfully.*"),
        )
        self._actionPrompt.start()
        # ---------------------------------------------------------------------------

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
        # self._logWarningsMessageBox.stop()
        self._actionPrompt.stop()
        self.exitStack.close()

    @staticmethod
    def _actionPromptContinue(parent, title, message, match=r".*"):  # noqa: ARG004
        _pattern = re.compile(match)
        if not _pattern.match(message):
            pytest.fail(
                f"unexpected: QMessageBox.information('{title}', '{message}'...)\n"
                + f"    expecting:  QMessageBox.information(...'{message}'...)"
            )

    @pytest.mark.qt_no_exception_capture
    def test_normalization_regression(self, qtbot, qapp, calibration_home_from_mirror, capsys):  # noqa ARG002
        # Override the mirror with a new home directory, omitting any existing
        #   calibration or normalization data.
        tmpCalibrationHomeDirectory = calibration_home_from_mirror()  # noqa: F841
        # print(tmpCalibrationHomeDirectory)
        # assert False
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
            self.testSummary.SUCCESS()

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

            # node-tab indices: (0) request view, (1) tweak-peak view, and (3) save view
            workflowNodeTabs = normalizationCalibrationWidget.findChild(QTabWidget, "nodeTabs")

            requestView = workflowNodeTabs.currentWidget().view
            assert isinstance(requestView, NormalizationRequestView)
            requestView.litemodeToggle.setState(False)
            self.testSummary.SUCCESS()

            # Verify tweak peak and save tabs are disabled

            # Set negative number and small positive number for RunNumber
            # and verify the 2 corresponding msg boxes show up.
            # Repeat for Background Run number box

            self._logWarningsMessageBox.stop()
            qtbot.wait(100)
            msg = "Run number -1 must be a positive integer"

            # @staticmethod # could also be an instance method, if you need to use the "self" for something,
            # but right now you're not using it!
            def mock2(msg):
                # this creates the closure, which includes `msg` but does _not_ include `self_`.
                def _mockExec(self_):
                    return (
                        QMessageBox.Ok
                        if ("The backend has encountered warning(s)" in self_.text() and msg in self_.detailedText())
                        else (print(f"Expected warning not found:  {msg}")),
                    )

                return mock.Mock(side_effect=_mockExec)

            mp = mock.patch("qtpy.QtWidgets.QMessageBox.exec", mock2(msg))
            mp.start()

            # Code that causes warning box to show up
            requestView.runNumberField.setText("-2")
            qtbot.keyPress(requestView.runNumberField.field, Qt.Key_Enter)
            qtbot.wait(500)

            assert len(exceptions) == 0
            assert mp.start().call_count == 1
            # So the whole test does not run
            assert False  # noqa PT015

            testMock = MockQMessageBox()
            msg = "Run number 1 is below minimum value"
            self._customlogWarningsMessageBox = testMock.exec(testMock, ref=self, msg=msg)
            self._customlogWarningsMessageBox.start()
            requestView.runNumberField.setText("1")
            qtbot.keyPress(requestView.runNumberField.field, Qt.Key_Enter)
            qtbot.wait(100)
            assert len(exceptions) == 0
            self._customlogWarningsMessageBox.stop()

            testMock = MockQMessageBox()
            msg = "Run number -1 must be a positive integer"
            self._customlogWarningsMessageBox = testMock.exec(testMock, ref=self, msg=msg)
            self._customlogWarningsMessageBox.start()
            requestView.backgroundRunNumberField.setText("-1")
            qtbot.keyPress(requestView.backgroundRunNumberField.field, Qt.Key_Enter)
            qtbot.wait(100)
            assert len(exceptions) == 0
            self._customlogWarningsMessageBox.stop()

            testMock = MockQMessageBox()
            msg = "Run number 1 is below minimum value"
            self._customlogWarningsMessageBox = testMock.exec(testMock, ref=self, msg=msg)
            self._customlogWarningsMessageBox.start()
            requestView.backgroundRunNumberField.setText("1")
            qtbot.keyPress(requestView.backgroundRunNumberField.field, Qt.Key_Enter)
            qtbot.wait(100)
            assert len(exceptions) == 0
            self._customlogWarningsMessageBox.stop()

            # Enter 58810 for run number and 58813 for background and click continue
            mockCritical = MockQMessageBox()
            msg = "Please select a sample"
            self._customlogCriticalMessageBox = mockCritical.critical(mockCritical, msg=msg)
            self._customlogCriticalMessageBox.start()
            requestView.runNumberField.setText("58810")
            requestView.backgroundRunNumberField.setText("58813")
            qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
            qtbot.wait(100)

            # Verify error msg box to select sample shows up
            self._customlogCriticalMessageBox.stop()
            assert len(exceptions) == 0

            # Select Vanadium Cylinder as sample and click continue
            mockCritical = MockQMessageBox()
            msg = "Please select a grouping file"
            self._customlogCriticalMessageBox = mockCritical.critical(mockCritical, msg=msg)
            self._customlogCriticalMessageBox.start()
            requestView.sampleDropdown.setCurrentIndex(3)
            assert requestView.sampleDropdown.currentIndex() == 3
            assert requestView.sampleDropdown.currentText().endswith("Silicon_NIST_640D_001.json")
            qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
            qtbot.wait(100)

            # Verify error msg box to select grouping shows up
            self._customlogCriticalMessageBox.stop()
            assert len(exceptions) == 0

            # Select column grouping and click continue
            qtbot.wait(100)
            requestView.groupingFileDropdown.setCurrentIndex(0)
            qtbot.wait(100)
            assert requestView.groupingFileDropdown.currentIndex() == 0
            assert requestView.groupingFileDropdown.currentText() == "Column"
            self.testSummary.SUCCESS()

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

            testMock = MockQMessageBox()
            msg = "No valid FocusGroups were specified for mode: 'lite'"
            self._logWarningsMessageBox = testMock.exec(testMock, ref=self, msg=msg)
            self._logWarningsMessageBox.start()

            with qtbot.waitSignal(actionCompleted, timeout=60000):
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
            qtbot.waitUntil(
                lambda: isinstance(workflowNodeTabs.currentWidget().view, NormalizationTweakPeakView), timeout=60000
            )
            qtbot.wait(100)
            self.testSummary.SUCCESS()

            # On tweak peaks tab, test each text field(3 of these) by putting non numeric chars and verify error msg
            tweakPeakView = workflowNodeTabs.currentWidget().view
            self._logWarningsMessageBox.stop()

            testMock = MockQMessageBox()
            msg = "Smoothing parameter must be a numerical value"
            self._logWarningsMessageBox = testMock.warning(testMock, msg=msg)
            self._logWarningsMessageBox.start()
            tweakPeakView.smoothingSlider.field.setValue("a")
            qtbot.wait(100)
            self._logWarningsMessageBox.stop()
            assert len(exceptions) == 0

            testMock = MockQMessageBox()
            msg = "One of xtal dMin, xtal dMax, smoothing, or peak threshold is invalid:\
                    could not convert string to float: 'a'"
            self._logWarningsMessageBox = testMock.warning(testMock, msg=msg)
            self._logWarningsMessageBox.start()
            tweakPeakView.fieldXtalDMin.setText("a")
            qtbot.wait(100)
            self._logWarningsMessageBox.stop()
            assert len(exceptions) == 0

            testMock = MockQMessageBox()
            msg = "One of xtal dMin, xtal dMax, smoothing, or peak threshold is invalid:\
                    could not convert string to float: 'a'"
            self._logWarningsMessageBox = testMock.warning(testMock, msg=msg)
            self._logWarningsMessageBox.start()
            tweakPeakView.fieldXtalDMax.setText("a")
            qtbot.wait(100)
            self._logWarningsMessageBox.stop()
            assert len(exceptions) == 0

            testMock = MockQMessageBox()
            msg = "One of xtal dMin, xtal dMax, smoothing, or peak threshold is invalid:\
                    could not convert string to float: 'a'"
            self._logWarningsMessageBox = testMock.warning(testMock, msg=msg)
            self._logWarningsMessageBox.start()
            qtbot.mouseClick(tweakPeakView.recalculationButton, Qt.MouseButton.LeftButton)
            qtbot.wait(5000)
            self._logWarningsMessageBox.stop()
            assert len(exceptions) == 0

            # Then set negative values and verify errors on recalculate
            testMock = MockQMessageBox()
            msg = "Smoothing parameter must be a nonnegative number"
            self._logWarningsMessageBox = testMock.warning(testMock, msg=msg)
            self._logWarningsMessageBox.start()
            tweakPeakView.smoothingSlider.field.setValue("-1")
            qtbot.wait(100)
            self._logWarningsMessageBox.stop()
            assert len(exceptions) == 0

            tweakPeakView.fieldXtalDMin.setText("-1")
            qtbot.wait(100)
            # expect error msg
            tweakPeakView.fieldXtalDMax.setText("-1")
            qtbot.wait(100)
            # expect error msg

            testMock = MockQMessageBox()
            msg = "Are you sure you want to do this? This may cause memory overflow or may take a long time to compute."
            self._logWarningsMessageBox = testMock.warning(testMock, msg=msg)
            self._logWarningsMessageBox.start()
            mockCritical = MockQMessageBox()
            msg = "CrystallographicInfoAlgorithm  -- has failed -- dMin cannot be <= 0."
            self._customlogCriticalMessageBox = mockCritical.critical(mockCritical, msg=msg)
            self._customlogCriticalMessageBox.start()
            qtbot.mouseClick(tweakPeakView.recalculationButton, Qt.MouseButton.LeftButton)
            qtbot.wait(5000)
            self._logWarningsMessageBox.stop()
            self._customlogCriticalMessageBox.stop()
            assert len(exceptions) == 0

            # # Set "0" on smoothing param, should error(maybe crash?)
            # tweakPeakView.smoothingSlider.field.setValue("0")
            # qtbot.wait(1000)
            # # expect error msg
            # qtbot.mouseClick(tweakPeakView.recalculationButton, Qt.MouseButton.LeftButton)
            # qtbot.wait(5000)

            # Set Dmin to 1, Dmax to 2.8, recalculate
            tweakPeakView.fieldXtalDMin.setText("1")
            tweakPeakView.fieldXtalDMax.setText("2.8")
            qtbot.mouseClick(tweakPeakView.recalculationButton, Qt.MouseButton.LeftButton)
            qtbot.wait(5000)

            # check smoothing is logarithmic, go to saving tab
            # qtbot.mouseClick(tweakPeakView.recalculationButton, Qt.MouseButton.LeftButton)
            # qtbot.wait(5000)
            # qtbot.mouseClick(tweakPeakView.recalculationButton, Qt.MouseButton.LeftButton)
            # qtbot.wait(5000)

            with qtbot.waitSignal(actionCompleted, timeout=60000):
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
            qtbot.waitUntil(
                lambda: isinstance(workflowNodeTabs.currentWidget().view, NormalizationSaveView), timeout=60000
            )
            self.testSummary.SUCCESS()
            saveView = workflowNodeTabs.currentWidget().view

            # Do error checking on all text fields

            # Set author to "Test"You must specify the author
            mockCritical = MockQMessageBox()
            msg = "You must specify the author"
            self._customlogCriticalMessageBox = mockCritical.critical(mockCritical, msg=msg)
            self._customlogCriticalMessageBox.start()
            qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
            qtbot.wait(100)
            self._customlogCriticalMessageBox.stop()
            assert len(exceptions) == 0
            saveView.fieldAuthor.setText("Test")

            # Set Comments to "This is a test"You must add comments
            mockCritical = MockQMessageBox()
            msg = "You must add comments"
            self._customlogCriticalMessageBox = mockCritical.critical(mockCritical, msg=msg)
            self._customlogCriticalMessageBox.start()
            qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
            qtbot.wait(100)
            self._customlogCriticalMessageBox.stop()
            assert len(exceptions) == 0
            saveView.fieldComments.setText("This is a test")

            # Finish and observe "Workflow complete"
            #    continue in order to save workspaces and to finish the workflow
            with qtbot.waitSignal(actionCompleted, timeout=60000):
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
            self.testSummary.SUCCESS()

            #   `ActionPrompt.prompt("..The workflow has completed successfully..)` gives immediate mocked response:
            #      Here we still need to wait until the ADS cleanup has occurred,
            #      or else it will happen in the middle of the next workflow. :(
            qtbot.waitUntil(
                lambda: isinstance(workflowNodeTabs.currentWidget().view, NormalizationRequestView), timeout=10000
            )

            # Verify NormalizationIndex.json is updated

            # Verify 5 output files created
            qtbot.wait(5000)
            folderDir = str(tmpCalibrationHomeDirectory) + "/04bd2c53f6bf6754/native/normalization/v_0000"
            contents = os.listdir(folderDir)

            assert "NormalizationParameters.json" in contents
            assert "NormalizationRecord.json" in contents
            assert "dsp_column_058810_fitted_van_corr_v0000.nxs" in contents
            assert "tof_column_s+f-vanadium_058810_v0000.nxs" in contents
            assert "tof_unfoc_058810_raw_van_corr_v0000.nxs" in contents

            ###############################
            ########### END OF TEST #######
            ###############################

            calibrationPanel.widget.close()
            gui.close()
            self.testSummary.SUCCESS()

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
