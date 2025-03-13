import re
from contextlib import ExitStack, suppress

##
## In order to retain the normal import order as much as possible:
##   place test-specific imports at the end.
##
from unittest import mock

import pytest
from mantid.kernel import amend_config
from qtpy import QtCore
from qtpy.QtCore import Qt
from qtpy.QtWidgets import (
    QMessageBox,
    QTabWidget,
)
from util.pytest_helpers import handleStateInit
from util.qt_mock_util import MockQMessageBox
from util.TestSummary import TestSummary

# I would prefer not to access `LocalDataService` within an integration test,
#   however, for the moment, the reduction-data output relocation fixture is defined in the current file.
from snapred.meta.Config import Resource
from snapred.ui.main import SNAPRedGUI, prependDataSearchDirectories
from snapred.ui.view.DiffCalAssessmentView import DiffCalAssessmentView
from snapred.ui.view.DiffCalRequestView import DiffCalRequestView
from snapred.ui.view.DiffCalSaveView import DiffCalSaveView
from snapred.ui.view.DiffCalTweakPeakView import DiffCalTweakPeakView
from snapred.ui.view.NormalizationRequestView import NormalizationRequestView
from snapred.ui.view.NormalizationSaveView import NormalizationSaveView
from snapred.ui.view.NormalizationTweakPeakView import NormalizationTweakPeakView
from snapred.ui.view.reduction.ReductionRequestView import ReductionRequestView
from snapred.ui.view.reduction.ReductionSaveView import ReductionSaveView


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
        self._logWarningsMessageBox.stop()
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

    ##
    ## This test exists primarily for use during development, where combining the workflows into one test sequence
    ## allows the convenient generation of input calibration and normalization data for use by the other workflows.
    ##
    @pytest.mark.skip(reason="each workflow panel now has a separate test")
    def test_calibration_and_reduction_panels_happy_path(
        self,
        qtbot,
        qapp,
        calibration_home_from_mirror,  # noqa: F811
        reduction_home_from_mirror,  # noqa: F811
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
            calibrationPanelTabs.setCurrentIndex(1)
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
            qtbot.keyClick(requestView.runNumberField._field, Qt.Key_Return)

            requestView.fieldConvergenceThreshold.setText("0.1")
            requestView.fieldNBinsAcrossPeakWidth.setText("10")

            #    set all dropdown selections, but make sure that the dropdown contents are as expected
            requestView.sampleDropdown.setCurrentIndex(0)
            assert requestView.sampleDropdown.currentIndex() == 0
            assert requestView.sampleDropdown.currentText().endswith("Diamond_001.json")

            requestView.groupingFileDropdown.setCurrentIndex(1)
            assert requestView.groupingFileDropdown.currentIndex() == 1
            assert requestView.groupingFileDropdown.currentText() == "Bank"

            requestView.peakFunctionDropdown.setCurrentIndex(0)
            assert requestView.peakFunctionDropdown.currentIndex() == 0
            assert requestView.peakFunctionDropdown.currentText() == "Gaussian"

            #    execute the request

            # TODO: make sure that there's no initialized state => abort the test if there is!
            waitForStateInit = True
            handleStateInit(waitForStateInit, reductionStateId, qtbot, qapp, actionCompleted, workflowNodeTabs)

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
            self._logWarningsMessageBox.stop()
            warningMessageBox = mock.patch(  # noqa: PT008
                "qtpy.QtWidgets.QMessageBox.exec",
                lambda *args, **kwargs: QMessageBox.Yes,  # noqa: ARG005
            )
            warningMessageBox.start()
            mb = MockQMessageBox().continueButton("Yes")
            mb[0].start()
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
            mb[0].stop()
            self._logWarningsMessageBox.start()

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

            #    set "Run Number", "Background run number":
            requestView.runNumberField.setText(reductionRunNumber)
            qtbot.keyClick(requestView.runNumberField._field, Qt.Key_Return)

            requestView.backgroundRunNumberField.setText(reductionRunNumber)
            qtbot.keyClick(requestView.backgroundRunNumberField._field, Qt.Key_Return)

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
            handleStateInit(waitForStateInit, reductionStateId, qtbot, qapp, actionCompleted, workflowNodeTabs)

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
            calibrationPanelTabs.setCurrentIndex(1)
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

            _count = requestView.runNumberDisplay.count()
            _runNumbers = [requestView.runNumberDisplay.item(x).text() for x in range(_count)]

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
            handleStateInit(waitForStateInit, reductionStateId, qtbot, qapp, actionCompleted, workflowNodeTabs)

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

    def test_diffraction_calibration_panel_happy_path(self, qtbot, qapp, calibration_home_from_mirror):  # noqa: F811
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
            self.testSummary.SUCCESS()
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
            calibrationPanelTabs.setCurrentIndex(1)
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
            self.testSummary.SUCCESS()

            requestView.liteModeToggle.setState(False)

            #    set "Run Number", "Convergence Threshold", ,:
            requestView.runNumberField.setText("58882")
            qtbot.keyClick(requestView.runNumberField._field, Qt.Key_Return)
            qapp.processEvents()
            qtbot.wait(1000)

            #    set all dropdown selections, but make sure that the dropdown contents are as expected
            requestView.sampleDropdown.setCurrentIndex(3)
            assert requestView.sampleDropdown.currentIndex() == 3
            assert requestView.sampleDropdown.currentText().endswith("Silicon_NIST_640D_001.json")

            requestView.groupingFileDropdown.setCurrentIndex(0)
            assert requestView.groupingFileDropdown.currentIndex() == 0
            assert requestView.groupingFileDropdown.currentText() == "Column"

            requestView.peakFunctionDropdown.setCurrentIndex(0)
            assert requestView.peakFunctionDropdown.currentIndex() == 0
            assert requestView.peakFunctionDropdown.currentText() == "Gaussian"

            requestView.skipPixelCalToggle.setState(False)

            self.testSummary.SUCCESS()
            #    execute the request

            # TODO: make sure that there's no initialized state => abort the test if there is!
            #   At the moment, we preserve some options here to speed up development.
            stateId = "04bd2c53f6bf6754"
            waitForStateInit = True
            handleStateInit(waitForStateInit, stateId, qtbot, qapp, actionCompleted, workflowNodeTabs)

            # Now that there is a new state, we need to reselect the grouping file ... :
            # Why was this error box being swallowed?
            requestView.groupingFileDropdown.setCurrentIndex(0)

            #    (2) execute the calibration workflow
            with qtbot.waitSignal(actionCompleted, timeout=60000):
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
            qtbot.waitUntil(
                lambda: isinstance(workflowNodeTabs.currentWidget().view, DiffCalTweakPeakView), timeout=60000
            )
            self.testSummary.SUCCESS()
            tweakPeakView = workflowNodeTabs.currentWidget().view

            # ---------------------------------------------------------------------------
            # Required patch for "TweakPeakView": ensure continuation if only two peaks are found.
            # IMPORTANT: "greater than two peaks" warning is triggered by an exception throw:
            #   => do _not_ patch using a with clause!
            self._logWarningsMessageBox.stop()
            warningMessageBox = mock.patch(  # noqa: PT008
                "qtpy.QtWidgets.QMessageBox.exec",
                lambda *args, **kwargs: QMessageBox.Yes,  # noqa: ARG005
            )
            warningMessageBox.start()
            mb = MockQMessageBox().continueButton("Yes")
            mb[0].start()
            # ---------------------------------------------------------------------------

            #    set "xtal dMin", "FWHM left", and "FWHM right": these are sufficient to get "58882" to pass.
            #    TODO: set ALL of the relevant fields, and use a test initialization template for this.
            tweakPeakView.fieldFWHMleft.setText("1.5")
            tweakPeakView.fieldFWHMright.setText("2")
            tweakPeakView.maxChiSqField.setText("1000.0")
            tweakPeakView.peakFunctionDropdown.setCurrentIndex(0)
            assert tweakPeakView.peakFunctionDropdown.currentIndex() == 0
            assert tweakPeakView.peakFunctionDropdown.currentText() == "Gaussian"

            #    recalculate using the new values
            #    * recalculate => peak display is recalculated,
            #      not the entire calibration.
            qtbot.mouseClick(tweakPeakView.recalculationButton, Qt.MouseButton.LeftButton)
            qtbot.wait(10000)

            #    continue to the next panel
            with qtbot.waitSignal(actionCompleted, timeout=80000):
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
            self.testSummary.SUCCESS()

            qtbot.waitUntil(
                lambda: isinstance(workflowNodeTabs.currentWidget().view, DiffCalAssessmentView), timeout=80000
            )

            # Placing this next `stop` correctly, causes some difficulty:
            #   if it's placed too early, the `warning` box patch doesn't correctly trap the "tweak peaks" warning.
            # So, for this reason this stop is placed _after_ the `waitUntil`.
            # ("Tweak peaks" view should have definitely completed by this point.)
            warningMessageBox.stop()
            mb[0].stop()
            self._logWarningsMessageBox.start()

            assessmentView = workflowNodeTabs.currentWidget().view  # noqa: F841
            #    nothing to do here, for this test

            #    continue to the next panel
            with qtbot.waitSignal(actionCompleted, timeout=80000):
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
            self.testSummary.SUCCESS()

            qtbot.waitUntil(lambda: isinstance(workflowNodeTabs.currentWidget().view, DiffCalSaveView), timeout=5000)
            saveView = workflowNodeTabs.currentWidget().view

            #    set "author" and "comment"
            saveView.fieldAuthor.setText("kat")
            saveView.fieldComments.setText("calibration-panel integration test")

            #    continue in order to save workspaces and to finish the workflow
            with qtbot.waitSignal(actionCompleted, timeout=60000):
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)
            self.testSummary.SUCCESS()
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

    def test_normalization_panel_happy_path(self, qtbot, qapp, calibration_home_from_mirror):  # noqa: F811
        # Override the mirror with a new home directory, omitting any existing
        #   calibration or normalization data.
        tmpCalibrationHomeDirectory = calibration_home_from_mirror()  # noqa: F841
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
            self.testSummary.SUCCESS()

            requestView.liteModeToggle.setState(False)

            #    set "Run Number", "Background run number":
            requestView.runNumberField.setText("58882")
            #    click return, otherwise `editingFinished` signal is never sent
            qtbot.keyClick(requestView.runNumberField._field, Qt.Key_Return)

            requestView.backgroundRunNumberField.setText("58882")
            qtbot.keyClick(requestView.backgroundRunNumberField._field, Qt.Key_Return)
            qapp.processEvents()
            #
            qtbot.wait(1000)

            #    set all dropdown selections, but make sure that the dropdown contents are as expected
            requestView.sampleDropdown.setCurrentIndex(3)
            assert requestView.sampleDropdown.currentIndex() == 3
            assert requestView.sampleDropdown.currentText().endswith("Silicon_NIST_640D_001.json")

            requestView.groupingFileDropdown.setCurrentIndex(0)
            assert requestView.groupingFileDropdown.currentIndex() == 0
            assert requestView.groupingFileDropdown.currentText() == "Column"
            self.testSummary.SUCCESS()

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
            handleStateInit(waitForStateInit, stateId, qtbot, qapp, actionCompleted, workflowNodeTabs)

            # Now that there is a new state, we need to reselect the grouping file ... :
            # Why was this error box being swallowed?
            requestView.groupingFileDropdown.setCurrentIndex(0)

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

            self.testSummary.SUCCESS()
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
            self.testSummary.SUCCESS()
            saveView = workflowNodeTabs.currentWidget().view

            #    set "author" and "comment"
            saveView.fieldAuthor.setText("kat")
            saveView.fieldComments.setText("calibration-panel integration test")

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

    def test_reduction_panel_happy_path(self, qtbot, qapp, reduction_home_from_mirror):  # noqa: F811
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
            calibrationPanelTabs.setCurrentIndex(0)
            reductionWidget = calibrationPanelTabs.currentWidget()

            #################################################################
            #### We need to access the signal specific to each workflow. ####
            #################################################################
            actionCompleted = calibrationPanel.presenter.reductionWorkflow.workflow.presenter.actionCompleted
            ###

            # node-tab indices: (0) request view, and (2) save view
            workflowNodeTabs = reductionWidget.findChild(QTabWidget, "nodeTabs")

            # WARNING:
            # `ReductionRequestView` now has two variants:
            # `<reduction request view>._requestView` and `<reduction request view>._liveDataView`.

            requestView = workflowNodeTabs.currentWidget().view
            assert isinstance(requestView, ReductionRequestView)
            self.testSummary.SUCCESS()
            # Without this next wait, the "run number entry" section happens too fast.
            # (And I'd love to understand _why_!  :(  )
            qtbot.wait(1000)

            requestView._requestView.liteModeToggle.setState(False)

            #    enter a "Run Number":
            requestView._requestView.runNumberInput.setText(reductionRunNumber)
            qtbot.mouseClick(requestView._requestView.enterRunNumberButton, Qt.MouseButton.LeftButton)

            _count = requestView._requestView.runNumberDisplay.count()
            _runNumbers = [requestView._requestView.runNumberDisplay.item(x).text() for x in range(_count)]

            assert reductionRunNumber in _runNumbers
            self.testSummary.SUCCESS()
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
            handleStateInit(waitForStateInit, reductionStateId, qtbot, qapp, actionCompleted, workflowNodeTabs)

            #    (2) execute the reduction workflow
            with qtbot.waitSignal(actionCompleted, timeout=120000):
                qtbot.mouseClick(workflowNodeTabs.currentWidget().continueButton, Qt.MouseButton.LeftButton)

            """
            #    set "author" and "comment"
            saveView.fieldAuthor.setText("kat")
            saveView.fieldComments.setText("calibration-panel integration test")
            """

            #   `ActionPrompt.prompt("..The workflow has completed successfully..)` gives immediate mocked response:
            #      Here we still need to wait until the ADS cleanup has occurred,
            #      or else it will happen in the middle of the next workflow. :(
            qtbot.waitUntil(
                lambda: self.completionMessageHasAppeared,
                timeout=60000,
            )
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
