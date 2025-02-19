import os
import re
from contextlib import ExitStack, suppress
from unittest import mock

import pytest
from mantid.kernel import amend_config
from qtpy import QtCore
from qtpy.QtCore import Qt
from qtpy.QtWidgets import (
    QMessageBox,
    QTabWidget,
)
from util.pytest_helpers import calibration_home_from_mirror, handleStateInit, reduction_home_from_mirror  # noqa: F401
from util.qt_mock_util import MockQMessageBox
from util.TestSummary import TestSummary

from snapred.backend.service.ReductionService import ReductionService
from snapred.meta.Config import Config, Resource
from snapred.ui.main import SNAPRedGUI, prependDataSearchDirectories
from snapred.ui.view.reduction.ArtificialNormalizationView import ArtificialNormalizationView
from snapred.ui.view.reduction.ReductionRequestView import ReductionRequestView

# TODO: WorkflowNodeComplete signal, at end of each node!


class InterruptWithBlock(BaseException):
    pass


@pytest.mark.datarepo
@pytest.mark.integration
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
                "WARNING This test seems to be missing expected calibration and/or normalization data", pytrace=False
            )
            if "Reduction is missing calibration data," in args[2]
            else pytest.fail("WARNING messagebox:\n" + f"    args: {args}\n" + f"    kwargs: {kwargs}", pytrace=False),
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

        # Automatically continue at the end of each workflow.
        self._actionPrompt = mock.patch(
            "qtpy.QtWidgets.QMessageBox.information",
            lambda *args: TestGUIPanels._actionPromptContinue(*args, match=r".*has been completed successfully.*"),
        )
        self._actionPrompt.start()
        # ---------------------------------------------------------------------------

        with Resource.open("../../src/snapred/resources/style.qss", "r") as styleSheet:
            qapp.setStyleSheet(styleSheet.read())

        # Establish context for each test: these normally run as part of `src/snapred/__main__.py`.
        self.exitStack = ExitStack()
        self.exitStack.enter_context(amend_config(data_dir=prependDataSearchDirectories(), prepend_datadir=True))

        self.testSummary = None
        yield

        if isinstance(self.testSummary, TestSummary):
            if not self.testSummary.isComplete():
                self.testSummary.FAILURE()
            if self.testSummary.isFailure():
                pytest.fail(f"Test Summary (-vv for full table): {self.testSummary}")

        # teardown...
        self._warningMessageBox.stop()
        self._criticalMessageBox.stop()
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

    def test_reduction_regression(self, qtbot, qapp, reduction_home_from_mirror):  # noqa: F811
        ##
        ## NOTE: WARNING: this test requires EXISTING diffraction-calibration and normalization-calibration data!
        ##   As an alternative `test_calibration_and_reduction_panels_happy_path`, now skipped, could be run instead.
        ##

        # TODO: these could be initialized in the 'setup', but the current plan is to use a YML test template.
        reductionRunNumber = "58882"
        reductionStateId = "04bd2c53f6bf6754"

        # Override the standard reduction-output location, using a temporary directory
        #   under the existing location within the mirror.
        tmpReductionHomeDirectory = reduction_home_from_mirror(reductionRunNumber)  # noqa: F841

        self.testSummary = (
            TestSummary.builder()
            .step("Open the GUI")
            .step("Open the Reduction panel")
            .step("Set the reduction request")
            .step("Test retaining unfocused data")
            .step("Test using artificial normalization")
            .step("Test having no write permissions")
            .step("Test using a pixelmask")
            .step("Test case of using multiple run numbers")
            .step("Execute the reduction request")
            .step("Close the GUI")
            .build()
        )

        self.completionMessageHasAppeared = False

        def completionMessageBoxAssert(*args, **kwargs):  # noqa: ARG001
            self.completionMessageHasAppeared = True
            assert "Reduction has completed successfully!" in args[2]
            return QMessageBox.Ok

        self._actionPrompt.stop()
        completionMessageBox = mock.patch(
            "qtpy.QtWidgets.QMessageBox.information",
            completionMessageBoxAssert,  # noqa: ARG005
        )
        completionMessageBox.start()

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
            assert calibrationPanelTabs.currentIndex() == 0
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
            self.testSummary.SUCCESS()
            # Without this next wait, the "run number entry" section happens too fast.
            # (And I'd love to understand _why_!  :(  )
            qtbot.wait(1000)

            #    enter a "Run Number":
            requestView._requestView.liteModeToggle.setState(False)
            qtbot.wait(1000)

            # # Test case of using non numeric
            msg = (
                "Reduction run numbers were incorrectly formatted: please read mantid docs"
                + " for `IntArrayProperty` on how to format input"
            )
            mp = MockQMessageBox().warning(msg)
            with mp[0]:
                requestView._requestView.runNumberInput.setText("a")
                qtbot.mouseClick(requestView._requestView.enterRunNumberButton, Qt.MouseButton.LeftButton)
                qtbot.wait(100)
                assert len(exceptions) == 0
                assert mp[1].call_count == 1

            # # Test case of using low run number
            msg = (
                "Value error, Run number -1 is below the minimum value or data does not exist."
                + "Please enter a valid run number"
            )
            mp = MockQMessageBox().exec(msg)
            with mp[0]:
                requestView._requestView.runNumberInput.setText("-1")
                qtbot.mouseClick(requestView._requestView.enterRunNumberButton, Qt.MouseButton.LeftButton)
                qtbot.wait(100)
                assert len(exceptions) == 0
                assert mp[1].call_count == 1

            requestView._requestView.runNumberInput.setText(reductionRunNumber)
            qtbot.mouseClick(requestView._requestView.enterRunNumberButton, Qt.MouseButton.LeftButton)
            qtbot.wait(1000)

            _count = requestView._requestView.runNumberDisplay.count()
            _runNumbers = [requestView._requestView.runNumberDisplay.item(x).text() for x in range(_count)]

            assert reductionRunNumber in _runNumbers
            self.testSummary.SUCCESS()

            """
            # Set some pixel masks in the ADS, and on the filesystem, prior to this test section.  Then we can test
            #   the pixel-mask dropdown.
            request.pixelMaskDropdown.setCurrentIndex() # or set-selected range? or by index?
            """

            #    execute the request

            # TODO: make sure that there's no initialized state => abort the test if there is!
            waitForStateInit = True
            handleStateInit(waitForStateInit, reductionStateId, qtbot, qapp, actionCompleted, workflowNodeTabs)

            #    (2) execute the reduction workflow
            with qtbot.waitSignal(actionCompleted, timeout=120000):
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
            qtbot.wait(1000)

            home = str(Config["instrument.reduction.home"].format(IPTS="IPTS-24641"))
            folderDir = str(home) + "/" + reductionStateId + "/native/" + reductionRunNumber

            def list_folders(path):
                qtbot.wait(1000)
                """Lists all folders in the specified directory."""
                folders = []
                for entry in os.scandir(path):
                    if entry.is_dir():
                        folders.append(entry.name)
                return folders

            def verifyContents():
                folders = list_folders(folderDir)
                timeStamp = folders[0]
                contents = os.listdir(folderDir + "/" + timeStamp)
                fullPath = folderDir + "/" + timeStamp

                reduced = "_reduced_0" + reductionRunNumber + "_" + timeStamp + ".nxs"
                assert "ReductionRecord.json" in contents
                assert reduced in contents

                #   `ActionPrompt.prompt("..The workflow has completed successfully..)` gives immediate mocked response:
                #      Here we still need to wait until the ADS cleanup has occurred,
                #      or else it will happen in the middle of the next workflow. :(
                qtbot.waitUntil(
                    lambda: self.completionMessageHasAppeared,
                    timeout=60000,
                )
                qtbot.wait(1000)

                adsList = gui.workspaceWidget._ads.getObjectNames()
                assert len(adsList) > 1
                expectedWorksapce = "_reduced_dsp_column_0" + reductionRunNumber + "_" + timeStamp
                assert expectedWorksapce in adsList
                import shutil

                shutil.rmtree(fullPath)
                qtbot.wait(1000)

            verifyContents()
            gui.workspaceWidget._ads.clear()

            # Test retain unfocused data
            def testRetainUnfocusedData(dropdownIndex: int, unit: str):
                requestView._requestView.retainUnfocusedDataCheckbox.setChecked(True)
                requestView._requestView.convertUnitsDropdown.setCurrentIndex(dropdownIndex)
                with qtbot.waitSignal(actionCompleted, timeout=120000):
                    qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
                qtbot.wait(1000)

                assert gui.workspaceWidget._ads.doesExist(f"{unit}_unfoc_058882")
                verifyContents()
                # qtbot.waitUntil(
                #     lambda: self.completionMessageHasAppeared,
                #     timeout=60000,
                # )
                # qtbot.wait(1000)
                requestView._requestView.retainUnfocusedDataCheckbox.setChecked(False)
                gui.workspaceWidget._ads.clear()

            testRetainUnfocusedData(0, "tof")
            testRetainUnfocusedData(1, "dsp")
            testRetainUnfocusedData(2, "lam")
            testRetainUnfocusedData(3, "qsp")
            self.testSummary.SUCCESS()

            # Artificial Normalization
            def testArtificialNormalization():
                # Use run number that forces artificial normalization
                requestView._requestView.clearRunNumbers()
                requestView._requestView.runNumberInput.setText("58810")
                qtbot.mouseClick(requestView._requestView.enterRunNumberButton, Qt.MouseButton.LeftButton)
                qtbot.wait(1000)

                handleStateInit(waitForStateInit, reductionStateId, qtbot, qapp, actionCompleted, workflowNodeTabs)

                msg = (
                    "Warning: Reduction is missing normalization data."
                    + " Artificial normalization will be created in place of actual normalization."
                    + " Would you like to continue?"
                )
                mp = MockQMessageBox().continueWarning(msg)
                with mp[0]:
                    qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
                    qtbot.wait(1000)
                    assert len(exceptions) == 0
                    assert mp[1].call_count == 1

                qtbot.wait(1000)
                artNormView = workflowNodeTabs.currentWidget().view
                assert isinstance(artNormView, ArtificialNormalizationView)

                msg = "Smoothing or peak window clipping size is invalid: could not convert string to float: 'a'"
                mp = MockQMessageBox().warning(msg)
                with mp[0]:
                    artNormView.smoothingSlider.field.setText("a")
                    qtbot.mouseClick(artNormView.recalculationButton, Qt.MouseButton.LeftButton)
                    qtbot.wait(1000)
                    assert len(exceptions) == 0
                    assert mp[1].call_count == 1
                artNormView.smoothingSlider.field.setText("5")

                msg = "Smoothing or peak window clipping size is invalid: invalid literal for int() with base 10: 'a'"
                mp = MockQMessageBox().warning(msg)
                with mp[0]:
                    artNormView.peakWindowClippingSize.field.setText("a")
                    qtbot.mouseClick(artNormView.recalculationButton, Qt.MouseButton.LeftButton)
                    qtbot.wait(1000)
                    assert len(exceptions) == 0
                    assert mp[1].call_count == 1
                artNormView.peakWindowClippingSize.field.setText("10")

                artNormView.lssDropdown.setCurrentIndex(1)
                assert not artNormView.lssDropdown.getValue()
                artNormView.decreaseParameterDropdown.setCurrentIndex(1)
                assert not artNormView.decreaseParameterDropdown.getValue()
                qtbot.mouseClick(artNormView.recalculationButton, Qt.MouseButton.LeftButton)
                qtbot.wait(1000)

                with qtbot.waitSignal(actionCompleted, timeout=120000):
                    qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
                qtbot.wait(1000)

                gui.workspaceWidget._ads.clear()
                requestView._requestView.clearRunNumbers()

            testArtificialNormalization()
            self.testSummary.SUCCESS()

            def testNoWritePermissions():
                requestView._requestView.runNumberInput.setText(reductionRunNumber)
                qtbot.mouseClick(requestView._requestView.enterRunNumberButton, Qt.MouseButton.LeftButton)
                qtbot.wait(1000)

                handleStateInit(waitForStateInit, reductionStateId, qtbot, qapp, actionCompleted, workflowNodeTabs)

                self._actionPromptNoPermission = mock.patch(
                    "qtpy.QtWidgets.QMessageBox.information",
                    lambda *args: TestGUIPanels._actionPromptContinue(
                        *args,
                        match=r".*but you can still save using the workbench tools."
                        + "</p><p>Please remember to save your output workspaces!</p>*",
                    ),
                )
                self._actionPromptNoPermission.start()

                from unittest.mock import patch

                def denyPerm(*args):  # noqa ARG001
                    return False

                with patch.object(ReductionService, "checkReductionWritePermissions", denyPerm):
                    msg2 = "<p>It looks like you don't have permissions to write to <br><b>"
                    mp2 = MockQMessageBox().continueWarning(msg2)
                    with mp2[0]:
                        qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
                        qtbot.wait(1000)
                        assert len(exceptions) == 0
                        assert mp2[1].call_count == 1

                self._actionPromptNoPermission.stop()

                folders = list_folders(folderDir)
                assert len(folders) == 0

                requestView._requestView.clearRunNumbers()

            testNoWritePermissions()
            self.testSummary.SUCCESS()

            # Test pixel mask
            def testPixelMask():
                def executeReduction():
                    # Add run number to run list
                    requestView._requestView.runNumberInput.setText(reductionRunNumber)
                    qtbot.mouseClick(requestView._requestView.enterRunNumberButton, Qt.MouseButton.LeftButton)
                    qtbot.wait(1000)

                    handleStateInit(waitForStateInit, reductionStateId, qtbot, qapp, actionCompleted, workflowNodeTabs)

                    # Execute reduction to generate a pixelmask
                    with qtbot.waitSignal(actionCompleted, timeout=120000):
                        qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
                    qtbot.wait(1000)

                    qtbot.waitUntil(
                        lambda: self.completionMessageHasAppeared,
                        timeout=60000,
                    )
                    qtbot.wait(1000)

                # Run reduction twice to generate a pixel mask. Not sure why it does not work on just one reduction run
                executeReduction()
                executeReduction()

                qtbot.wait(1000)

                # Make sure a pixelmask exists and set and check the pixelmask
                numOfPixelMasks = len(requestView._requestView.pixelMaskDropdown._items)
                assert numOfPixelMasks > 0
                requestView._requestView.pixelMaskDropdown.dropDown.setCurrentIndex(1)
                requestView._requestView.pixelMaskDropdown.dropDown.model().item(1).setCheckState(Qt.Checked)
                qtbot.mouseClick(requestView._requestView.pixelMaskDropdown.dropDown, Qt.MouseButton.LeftButton)
                qtbot.wait(1000)

                qtbot.mouseClick(requestView._requestView.pixelMaskDropdown.dropDown, Qt.MouseButton.LeftButton)
                qtbot.wait(1000)

                with qtbot.waitSignal(actionCompleted, timeout=120000):
                    qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
                qtbot.wait(1000)

                verifyContents()
                gui.workspaceWidget._ads.clear()

            testPixelMask()
            self.testSummary.SUCCESS()

            # Test multiple run numbers
            def testMultipleRunNumbers():
                requestView._requestView.clearRunNumbers()

                requestView._requestView.runNumberInput.setText("58813")
                qtbot.mouseClick(requestView._requestView.enterRunNumberButton, Qt.MouseButton.LeftButton)
                qtbot.wait(1000)

                requestView._requestView.runNumberInput.setText("58810")
                qtbot.mouseClick(requestView._requestView.enterRunNumberButton, Qt.MouseButton.LeftButton)
                qtbot.wait(1000)

                msg = (
                    "Warning: Reduction is missing normalization data."
                    + " Artificial normalization will be created in place of actual normalization."
                    + " Would you like to continue?"
                )
                critMsg = (
                    "Error 500: Currently, Artificial Normalization can only be performed"
                    + " on a single run at a time.  Please clear your run list and try again."
                )
                mp = MockQMessageBox().continueWarning(msg)
                mc = MockQMessageBox().critical(critMsg)
                with mp[0], mc[0]:
                    qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
                    qtbot.wait(10000)
                    assert len(exceptions) == 0
                    assert mp[1].call_count == 1
                    assert mc[1].call_count == 1

                gui.workspaceWidget._ads.clear()

            testMultipleRunNumbers()
            self.testSummary.SUCCESS()

            completionMessageBox.stop()
            self.testSummary.SUCCESS()
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
