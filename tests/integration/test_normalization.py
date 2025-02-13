import math
import os
import re
from contextlib import ExitStack, suppress
from unittest import mock

import pytest
from mantid.kernel import amend_config
from qtpy import QtCore
from qtpy.QtCore import Qt
from qtpy.QtWidgets import (
    QTabWidget,
)
from util.pytest_helpers import calibration_home_from_mirror, handleStateInit  # noqa: F401
from util.qt_mock_util import MockQMessageBox
from util.TestSummary import TestSummary

from snapred.meta.Config import Resource
from snapred.ui.main import SNAPRedGUI, prependDataSearchDirectories
from snapred.ui.view.NormalizationRequestView import NormalizationRequestView
from snapred.ui.view.NormalizationSaveView import NormalizationSaveView
from snapred.ui.view.NormalizationTweakPeakView import NormalizationTweakPeakView


class InterruptWithBlock(BaseException):
    pass


@pytest.mark.datarepo
@pytest.mark.integration
class TestNormalizationPanels:
    @pytest.fixture(scope="function", autouse=True)  # noqa: PT003
    def _setup_gui(self, qapp):
        testMock = MockQMessageBox()
        msg = "No valid FocusGroups were specified for mode: 'lite'"
        print(self)
        self._logWarningsMessageBox = testMock.exec(msg)
        self._logWarningsMessageBox[0].start()

        # Automatically continue at the end of each workflow.
        self._actionPrompt = mock.patch(
            "qtpy.QtWidgets.QMessageBox.information",
            lambda *args: TestNormalizationPanels._actionPromptContinue(
                *args, match=r".*has been completed successfully.*"
            ),
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
            .step("Open the Normalization panel")
            .step("Set the normalization request")
            .step("Execute the normalization request")
            .step("Tweak the peaks")
            .step("Save the normalization calibration")
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
            requestView.liteModeToggle.setState(False)
            self.testSummary.SUCCESS()

            # Verify tweak peak and save tabs are disabled

            # Set negative number and small positive number for RunNumber
            # and verify the 2 corresponding msg boxes show up.
            # Repeat for Background Run number box

            self._logWarningsMessageBox[0].stop()
            qtbot.wait(100)

            msg = "Invalid run number: -1"
            mp = MockQMessageBox().critical(msg)
            with mp[0]:
                requestView.runNumberField.setText("-1")
                qtbot.keyPress(requestView.runNumberField.field, Qt.Key_Enter)
                qtbot.wait(500)
                assert len(exceptions) == 0
                assert mp[1].call_count == 1

            msg = "Invalid run number: 1"
            mp = MockQMessageBox().critical(msg)
            with mp[0]:
                requestView.runNumberField.setText("1")
                qtbot.keyPress(requestView.runNumberField.field, Qt.Key_Enter)
                qtbot.wait(100)
                assert len(exceptions) == 0
                assert mp[1].call_count == 1

            msg = "Invalid run number: -1"
            mp = MockQMessageBox().critical(msg)
            with mp[0]:
                requestView.backgroundRunNumberField.setText("-1")
                qtbot.keyPress(requestView.backgroundRunNumberField.field, Qt.Key_Enter)
                qtbot.wait(100)
                assert len(exceptions) == 0
                assert mp[1].call_count == 1

            msg = "Invalid run number: 1"
            mp = MockQMessageBox().critical(msg)
            with mp[0]:
                requestView.backgroundRunNumberField.setText("1")
                qtbot.keyPress(requestView.backgroundRunNumberField.field, Qt.Key_Enter)
                qtbot.wait(100)
                assert len(exceptions) == 0
                assert mp[1].call_count == 1

            # Enter 58810 for run number and 58813 for background and click continue
            msg = "Please select a sample"
            mpCrit = MockQMessageBox().critical(msg)
            with mpCrit[0]:
                requestView.runNumberField.setText("58810")
                qtbot.keyClick(requestView.runNumberField._field, Qt.Key_Return)
                requestView.backgroundRunNumberField.setText("58813")
                qtbot.keyClick(requestView.backgroundRunNumberField._field, Qt.Key_Return)
                qapp.processEvents()

                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
                qtbot.wait(100)
                # Verify error msg box to select sample shows up
                assert len(exceptions) == 0
                assert mpCrit[1].call_count == 1

            # Select Vanadium Cylinder as sample and click continue
            msg = "Please select a grouping file"
            mpCrit = MockQMessageBox().critical(msg)
            with mpCrit[0]:
                requestView.sampleDropdown.setCurrentIndex(3)
                assert requestView.sampleDropdown.currentIndex() == 3
                assert requestView.sampleDropdown.currentText().endswith("Silicon_NIST_640D_001.json")
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
                qtbot.wait(100)

                # Verify error msg box to select grouping shows up
                assert len(exceptions) == 0
                assert mpCrit[1].call_count == 1

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

            handleStateInit(waitForStateInit, stateId, qtbot, qapp, actionCompleted, workflowNodeTabs)

            msg = "No valid FocusGroups were specified for mode: 'lite'"
            mp = MockQMessageBox().exec(msg)
            mp[0].start()

            with qtbot.waitSignal(actionCompleted, timeout=60000):
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
            qtbot.waitUntil(
                lambda: isinstance(workflowNodeTabs.currentWidget().view, NormalizationTweakPeakView), timeout=60000
            )
            qtbot.wait(100)
            self.testSummary.SUCCESS()

            # On tweak peaks tab, test each text field(3 of these) by putting non numeric chars and verify error msg
            tweakPeakView = workflowNodeTabs.currentWidget().view
            mp[0].stop()

            msg = "Smoothing parameter must be a numerical value"
            mpWarn = MockQMessageBox().warning(msg)
            with mpWarn[0]:
                tweakPeakView.smoothingSlider.field.setValue("a")
                qtbot.wait(100)
                assert len(exceptions) == 0
                assert mpWarn[1].call_count == 1

            msg = (
                "One of xtal dMin, xtal dMax, smoothing, or peak threshold is invalid: "
                + "could not convert string to float: 'a'"
            )
            mpWarn = MockQMessageBox().warning(msg)
            with mpWarn[0]:
                tweakPeakView.smoothingSlider.field.setValue("0")
                tweakPeakView.fieldXtalDMin.setText("a")
                qtbot.mouseClick(tweakPeakView.recalculationButton, Qt.MouseButton.LeftButton)
                qtbot.wait(100)
                assert len(exceptions) == 0
                assert mpWarn[1].call_count == 1

            msg = (
                "One of xtal dMin, xtal dMax, smoothing, or peak threshold is invalid: "
                + "could not convert string to float: 'a'"
            )
            mpWarn = MockQMessageBox().warning(msg)
            with mpWarn[0]:
                tweakPeakView.fieldXtalDMin.setText("0.1")
                tweakPeakView.fieldXtalDMax.setText("a")
                qtbot.mouseClick(tweakPeakView.recalculationButton, Qt.MouseButton.LeftButton)
                qtbot.wait(100)
                assert len(exceptions) == 0
                assert mpWarn[1].call_count == 1

            msg = (
                "One of xtal dMin, xtal dMax, smoothing, or peak threshold is invalid: "
                + "could not convert string to float: 'a'"
            )
            mpWarn = MockQMessageBox().warning(msg)
            with mpWarn[0]:
                qtbot.mouseClick(tweakPeakView.recalculationButton, Qt.MouseButton.LeftButton)
                qtbot.wait(500)
                assert len(exceptions) == 0
                assert mpWarn[1].call_count == 1

            # Then set negative values and verify errors on recalculate
            msg = "Smoothing parameter must be a nonnegative number"
            mpWarn = MockQMessageBox().warning(msg)
            with mpWarn[0]:
                tweakPeakView.fieldXtalDMax.setText("1")
                tweakPeakView.smoothingSlider.field.setValue("-1")
                qtbot.wait(100)
                assert len(exceptions) == 0
                assert mpWarn[1].call_count == 1

            msg = (
                "Are you sure you want to do this?"
                + " This may cause memory overflow or may take a long time to compute."
            )
            mpWarn = MockQMessageBox().warning(msg)
            msgCrit = "CrystallographicInfoAlgorithm  -- has failed -- dMin cannot be <= 0."
            mpCrit = MockQMessageBox().critical(msgCrit)
            with mpWarn[0], mpCrit[0]:
                tweakPeakView.fieldXtalDMin.setText("-1")
                qtbot.mouseClick(tweakPeakView.recalculationButton, Qt.MouseButton.LeftButton)
                qtbot.wait(100)
                assert len(exceptions) == 0
                assert mpWarn[1].call_count == 1

            msg = "The minimum crystal d-spacing exceeds the maximum (-1.0). Please enter a smaller value"
            mpWarn = MockQMessageBox().warning(msg)
            with mpWarn[0]:
                tweakPeakView.fieldXtalDMin.setText("0.1")
                tweakPeakView.fieldXtalDMax.setText("-1")
                qtbot.mouseClick(tweakPeakView.recalculationButton, Qt.MouseButton.LeftButton)
                qtbot.wait(100)
                assert len(exceptions) == 0
                assert mpWarn[1].call_count == 1

            msgWarn = (
                "Are you sure you want to do this? This may cause memory overflow or may take a long time to compute."
            )
            mpWarn = MockQMessageBox().warning(msgWarn)
            msgCrit = "CrystallographicInfoAlgorithm  -- has failed -- dMin cannot be <= 0."
            mpCrit = MockQMessageBox().critical(msgCrit)
            with mpWarn[0], mpCrit[0]:
                tweakPeakView.fieldXtalDMin.setText("-1")
                qtbot.mouseClick(tweakPeakView.recalculationButton, Qt.MouseButton.LeftButton)
                qtbot.wait(500)
                assert len(exceptions) == 0
                assert mpCrit[1].call_count == 1
                assert mpWarn[1].call_count == 1

                # Set "0" on smoothing param, should throw error
                tweakPeakView.smoothingSlider.field.setValue("0")
                qtbot.wait(100)
                qtbot.mouseClick(tweakPeakView.recalculationButton, Qt.MouseButton.LeftButton)
                qtbot.wait(500)
                assert len(exceptions) == 0
                assert mpCrit[1].call_count == 2
                assert mpWarn[1].call_count == 2

            # Set Dmin to 1, Dmax to 2.8, recalculate
            tweakPeakView.fieldXtalDMin.setText("1")
            tweakPeakView.fieldXtalDMax.setText("2.8")
            qtbot.mouseClick(tweakPeakView.recalculationButton, Qt.MouseButton.LeftButton)
            qtbot.wait(500)

            # check smoothing is logarithmic, go to saving tab
            tweakPeakView.smoothingSlider.field.setValue("1.0E-05")
            qtbot.mouseClick(tweakPeakView.recalculationButton, Qt.MouseButton.LeftButton)
            qtbot.wait(500)
            sliderValue = tweakPeakView.smoothingSlider.field._slider.value()
            # When setting the value, SmoothingSlider converts to log and multiplies by 100
            expectedValue = int(math.log10(float("1.0E-05")) * 100)
            assert sliderValue == expectedValue

            tweakPeakView.smoothingSlider.field.setValue("1.0E-06")
            qtbot.mouseClick(tweakPeakView.recalculationButton, Qt.MouseButton.LeftButton)
            qtbot.wait(500)
            sliderValue = tweakPeakView.smoothingSlider.field._slider.value()
            # When setting the value, SmoothingSlider converts to log and multiplies by 100
            expectedValue = int(math.log10(float("1.0E-06")) * 100)
            assert sliderValue == expectedValue

            with qtbot.waitSignal(actionCompleted, timeout=60000):
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
            qtbot.waitUntil(
                lambda: isinstance(workflowNodeTabs.currentWidget().view, NormalizationSaveView), timeout=60000
            )
            self.testSummary.SUCCESS()
            saveView = workflowNodeTabs.currentWidget().view

            # Do error checking on all text fields

            # Set author to "Test"You must specify the author
            msg = "You must specify the author"
            mpCrit = MockQMessageBox().critical(msg)
            with mpCrit[0]:
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
                qtbot.wait(100)
                assert len(exceptions) == 0
                assert mpCrit[1].call_count == 1
            saveView.fieldAuthor.setText("Test")

            # Set Comments to "This is a test"You must add comments
            msg = "You must add comments"
            mpCrit = MockQMessageBox().critical(msg)
            with mpCrit[0]:
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
                qtbot.wait(100)
                mpCrit[0].stop()
                assert len(exceptions) == 0
                assert mpCrit[1].call_count == 1
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
            qtbot.wait(500)
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
        qtbot.wait(500)
