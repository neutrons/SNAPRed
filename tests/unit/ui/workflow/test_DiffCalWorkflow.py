import threading
from random import randint
from unittest.mock import MagicMock, patch

from mantid.simpleapi import (
    CreateSingleValuedWorkspace,
    CreateTableWorkspace,
    GroupWorkspaces,
    mtd,
)
from qtpy.QtCore import Qt
from qtpy.QtWidgets import QApplication, QMessageBox
from snapred.meta.mantid.FitPeaksOutput import FIT_PEAK_DIAG_SUFFIX, FitOutputEnum
from snapred.meta.pointer import create_pointer
from snapred.ui.workflow.DiffCalWorkflow import DiffCalWorkflow


@patch("snapred.ui.workflow.DiffCalWorkflow.WorkflowImplementer.request")
def test_purge_bad_peaks(workflowRequest, qtbot):  # noqa: ARG001
    """
    Test that when purge peaks is called, improper peaks are removed.
    """
    diffcalWorkflow = DiffCalWorkflow()

    maxChiSq = 100
    num_peaks = 100
    peaks = list(range(num_peaks))
    is_good_peak = [bool(randint(0, 1)) for peak in peaks]
    good_peaks = [peak for is_good, peak in zip(is_good_peak, peaks) if is_good]

    wsindex = [0] * num_peaks
    chi2 = [0 if is_good else maxChiSq for is_good in is_good_peak]

    # setup some mocks
    diffcalWorkflow.ingredients = MagicMock(groupedPeakLists=[MagicMock(peaks=peaks)])
    diagWS = mtd.unique_name(prefix="difc_wf_tab_")
    ws1 = diagWS + FIT_PEAK_DIAG_SUFFIX[FitOutputEnum.PeakPosition]
    ws2 = diagWS + FIT_PEAK_DIAG_SUFFIX[FitOutputEnum.ParameterError]
    tabWS = diagWS + FIT_PEAK_DIAG_SUFFIX[FitOutputEnum.Parameters]
    CreateSingleValuedWorkspace(OutputWorkspace=ws1)
    CreateSingleValuedWorkspace(OutputWorkspace=ws2)
    CreateTableWorkspace(
        OutputWorkspace=tabWS,
        Data=create_pointer({"wsindex": wsindex, "chi2": chi2}),
    )
    GroupWorkspaces(
        OutputWorkspace=diagWS,
        InputWorkspaces=[ws1, ws2, tabWS],
    )
    diffcalWorkflow.fitPeaksDiagnostic = diagWS

    diffcalWorkflow.purgeBadPeaks(maxChiSq)
    assert diffcalWorkflow.ingredients.groupedPeakLists[0].peaks == good_peaks


@patch("snapred.ui.workflow.DiffCalWorkflow.WorkflowImplementer.request")
def test_purge_bad_peaks_two_wkspindex(workflowRequest, qtbot):  # noqa: ARG001
    """
    Test that purge bad peaks can handle several workspace index
    """
    diffcalWorkflow = DiffCalWorkflow()

    maxChiSq = 100
    num_peaks = 4
    peaks1 = list(range(num_peaks))
    peaks2 = list(range(num_peaks))
    is_good_peak1 = [0, 1, 0, 1]
    is_good_peak2 = [1, 0, 1, 0]
    good_peaks1 = [peak for is_good, peak in zip(is_good_peak1, peaks1) if is_good]
    good_peaks2 = [peak for is_good, peak in zip(is_good_peak2, peaks2) if is_good]

    wsindex = [0, 0, 0, 0, 1, 1, 1, 1]
    chi2 = [0 if is_good else maxChiSq for is_good in is_good_peak1]
    chi2.extend([0 if is_good else maxChiSq for is_good in is_good_peak2])

    # setup some mocks
    diffcalWorkflow.ingredients = MagicMock(
        groupedPeakLists=[
            MagicMock(peaks=peaks1),
            MagicMock(peaks=peaks2),
        ]
    )
    diagWS = mtd.unique_name(prefix="difc_wf_tab_")
    ws1 = diagWS + FIT_PEAK_DIAG_SUFFIX[FitOutputEnum.PeakPosition]
    ws2 = diagWS + FIT_PEAK_DIAG_SUFFIX[FitOutputEnum.ParameterError]
    tabWS = diagWS + FIT_PEAK_DIAG_SUFFIX[FitOutputEnum.Parameters]
    CreateSingleValuedWorkspace(OutputWorkspace=ws1)
    CreateSingleValuedWorkspace(OutputWorkspace=ws2)
    CreateTableWorkspace(
        OutputWorkspace=tabWS,
        Data=create_pointer({"wsindex": wsindex, "chi2": chi2}),
    )
    GroupWorkspaces(
        OutputWorkspace=diagWS,
        InputWorkspaces=[ws1, ws2, tabWS],
    )
    diffcalWorkflow.fitPeaksDiagnostic = diagWS

    diffcalWorkflow.purgeBadPeaks(maxChiSq)
    assert diffcalWorkflow.ingredients.groupedPeakLists[0].peaks == good_peaks1
    assert diffcalWorkflow.ingredients.groupedPeakLists[1].peaks == good_peaks2


@patch("snapred.ui.workflow.DiffCalWorkflow.WorkflowImplementer.request")
def test_purge_bad_peaks_too_few(workflowRequest, qtbot):  # noqa: ARG001
    """
    Test that if too few peaks would be generated, a warning is raised
    """
    diffcalWorkflow = DiffCalWorkflow()

    maxChiSq = 100
    num_peaks = 3
    peaks = list(range(num_peaks))
    is_good_peak = [0, 0, 1]
    good_peaks = [peak for is_good, peak in zip(is_good_peak, peaks) if is_good]

    wsindex = [0] * num_peaks
    chi2 = [0 if is_good else maxChiSq for is_good in is_good_peak]

    # setup some mocks
    diffcalWorkflow.ingredients = MagicMock(groupedPeakLists=[MagicMock(peaks=peaks)])
    diagWS = mtd.unique_name(prefix="difc_wf_tab_")
    ws1 = diagWS + FIT_PEAK_DIAG_SUFFIX[FitOutputEnum.PeakPosition]
    ws2 = diagWS + FIT_PEAK_DIAG_SUFFIX[FitOutputEnum.ParameterError]
    tabWS = diagWS + FIT_PEAK_DIAG_SUFFIX[FitOutputEnum.Parameters]
    CreateSingleValuedWorkspace(OutputWorkspace=ws1)
    CreateSingleValuedWorkspace(OutputWorkspace=ws2)
    CreateTableWorkspace(
        OutputWorkspace=tabWS,
        Data=create_pointer({"wsindex": wsindex, "chi2": chi2}),
    )
    GroupWorkspaces(
        OutputWorkspace=diagWS,
        InputWorkspaces=[ws1, ws2, tabWS],
    )
    diffcalWorkflow.fitPeaksDiagnostic = diagWS

    def execute_click():
        w = QApplication.activeWindow()
        if isinstance(w, QMessageBox):
            close_button = w.button(QMessageBox.Ok)
            qtbot.mouseClick(close_button, Qt.LeftButton)

    # setup the qtbot to intercept the window
    qtbot.addWidget(diffcalWorkflow._tweakPeakView)
    threading.Timer(0.1, execute_click).start()
    diffcalWorkflow.purgeBadPeaks(maxChiSq)

    assert diffcalWorkflow.ingredients.groupedPeakLists[0].peaks == peaks
    assert diffcalWorkflow.ingredients.groupedPeakLists[0].peaks != good_peaks
