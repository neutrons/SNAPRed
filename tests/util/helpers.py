# @file: tests/util/helpers.py:
#
# Helper utility methods to be used by unit tests and CIS tests.
#

import unittest
from collections.abc import Sequence
from typing import Any, Tuple

import numpy
import numpy as np
from mantid.api import ITableWorkspace, MatrixWorkspace
from mantid.dataobjects import GroupingWorkspace, MaskWorkspace
from mantid.simpleapi import (
    CreateEmptyTableWorkspace,
    CreateWorkspace,
    DeleteWorkspace,
    ExtractMask,
    mtd,
)


def createCompatibleDiffCalTable(tableWSName: str, templateWSName: str) -> ITableWorkspace:
    """
    Create an diffraction-calibration `ITableWorkspace` compatible with a template workspace.
    """
    ws = CreateEmptyTableWorkspace(OutputWorkspace=tableWSName)
    ws.addColumn(type="int", name="detid", plottype=6)
    ws.addColumn(type="float", name="difc", plottype=6)
    ws.addColumn(type="float", name="difa", plottype=6)
    ws.addColumn(type="float", name="tzero", plottype=6)
    # Add same number of rows as the dummy mask workspace:
    templateWS = mtd[templateWSName]
    for n in range(templateWS.getInstrument().getNumberDetectors(True)):
        ws.addRow({"detid": n, "difc": 1000.0, "difa": 0.0, "tzero": 0.0})
    return ws


def createCompatibleMask(maskWSName: str, templateWSName: str) -> MaskWorkspace:
    """
    Create a `MaskWorkspace` compatible with a template workspace
    """

    templateWS = mtd[templateWSName]

    # Number of non-monitor pixels
    pixelCount = templateWS.getInstrument().getNumberDetectors(True)

    mask = CreateWorkspace(
        OutputWorkspace=maskWSName,
        NSpec=pixelCount,
        DataX=list(np.zeros((pixelCount,))),
        DataY=list(np.zeros((pixelCount,))),
        ParentWorkspace=templateWSName,
    )

    # Rebuild the spectra map "by hand" to exclude detectors which are monitors.
    info = mask.detectorInfo()
    ids = info.detectorIDs()

    # Warning: <detector info>.indexOf(id_) != <workspace index of detectors excluding monitors>
    wi = 0
    for id_ in ids:
        if info.isMonitor(info.indexOf(int(id_))):
            continue
        s = mask.getSpectrum(wi)
        s.setSpectrumNo(wi)
        s.setDetectorID(int(id_))
        wi += 1

    # Convert workspace to a MaskWorkspace instance.
    ExtractMask(OutputWorkspace=maskWSName, InputWorkspace=maskWSName)
    assert isinstance(mtd[maskWSName], MaskWorkspace)

    return mtd[maskWSName]


def arrayFromMask(maskWSName: str) -> numpy.ndarray:
    """
    Initialize a 1D numpy boolean array from a mask workspace.
    """
    mask = mtd[maskWSName]

    # Number of non-monitor pixels
    pixelCount = mask.getInstrument().getNumberDetectors(True)
    assert pixelCount == mask.getNumberHistograms()

    flags = np.zeros((pixelCount,), dtype=bool)
    for wi in range(pixelCount):
        flags[wi] = mask.readY(wi)[0] != 0.0
    assert mask.getNumberMasked() == np.count_nonzero(flags)
    return flags


def initializeRandomMask(maskWSName: str, fraction: float) -> MaskWorkspace:
    """
    Initialize an existing mask workspace by masking a random fraction of its values:
      * maskWSName: input mask workspace;
      * fraction: a value in [0.0, 1.0) indicating the fraction of values to mask.
    """
    mask = mtd[maskWSName]

    # Number of non-monitor pixels
    pixelCount = mask.getInstrument().getNumberDetectors(True)
    assert pixelCount == mask.getNumberHistograms()

    flags = np.random.random_sample((pixelCount,))
    flags = flags < fraction
    for wi in range(pixelCount):
        mask.setY(wi, [1.0 if flags[wi] else 0.0])
    assert mask.getNumberMasked() == np.count_nonzero(flags)
    return mask


def setSpectraToZero(inputWS: MatrixWorkspace, nss: Sequence[int]):
    # Zero out all spectra in the list of spectra
    if "EventWorkspace" not in inputWS.id():
        for ns in nss:
            # allow "ragged" case
            zs = np.zeros_like(inputWS.readY(ns))
            inputWS.setY(ns, zs)
    else:
        for ns in nss:
            inputWS.getSpectrum(ns).clear(False)


def maskSpectra(maskWS: MaskWorkspace, inputWS: MatrixWorkspace, nss: Sequence[int]):
    # Set mask flags for all detectors contributing to each spectrum in the list of spectra
    for ns in nss:
        dets = inputWS.getSpectrum(ns).getDetectorIDs()
        for det in dets:
            maskWS.setValue(det, True)


def setGroupSpectraToZero(ws: MatrixWorkspace, groupingWS: GroupingWorkspace, gids: Sequence[int]):
    # Zero out all spectra contributing to each group in the list of groups
    detInfo = ws.detectorInfo()
    for gid in gids:
        dets = groupingWS.getDetectorIDsOfGroup(gid)
        if "EventWorkspace" not in ws.id():
            for det in dets:
                ns = detInfo.indexOf(int(det))
                # allow "ragged" case
                zs = np.zeros_like(ws.readY(ns))
                ws.setY(ns, zs)
        else:
            for det in dets:
                ns = detInfo.indexOf(int(det))
                ws.getSpectrum(ns).clear(False)


def maskGroups(maskWS: MaskWorkspace, groupingWS: GroupingWorkspace, gs: Sequence[int]):
    # Set mask flags for all detectors contributing to each group in the list of groups
    for gid in gs:
        dets = groupingWS.getDetectorIDsOfGroup(gid)
        for det in dets:
            maskWS.setValue(int(det), True)


def maskComponentByName(maskWSName: str, componentName: str):
    """
    Set mask values for all (non-monitor) detectors contributing to a component
    -- maskWSName: name of MaskWorkspace to modify
    (warning: only mask values will be changed, not detector mask flags)
    -- componentName: the name of a component in the workspace's instrument
    To enable the masking of multiple components, mask values are only set,
    and never cleared.
    """
    mask = mtd[maskWSName]
    detectors = mask.detectorInfo()
    idFromIndex = detectors.detectorIDs()
    info = mask.componentInfo()
    componentDetectorIndices = info.detectorsInSubtree(info.indexOfAny(componentName))

    for ix in componentDetectorIndices:
        if detectors.isMonitor(int(ix)):
            continue
        mask.setValue(int(idFromIndex[ix]), True)


def mutableWorkspaceClones(
    sourceWorkspaceNames: Sequence[str], uniquePrefix: str, name_only: bool = False
) -> Tuple[Any]:
    """
    Clone workspaces so that they can be modified by simultaneously running tests.
    Each cloned workspace will have a name: <unique prefix> + <original workspace name>
    """
    from mantid.simpleapi import mtd

    wss = []
    ws_names = []
    for name in sourceWorkspaceNames:
        if name not in mtd:
            raise RuntimeError(f'workspace "{name}" is not in the ADS')
        if not name_only:
            clone = mtd[name].clone()
            clone_name = uniquePrefix + name
            mtd[clone_name] = clone
            wss.append(clone)
        else:
            clone_name = uniquePrefix + name
            ws_names.append(clone_name)
    return tuple(wss) if not name_only else tuple(ws_names)


def deleteWorkspaceNoThrow(wsName: str):
    try:
        DeleteWorkspace(wsName)
    except:  # noqa: E722
        pass


def nameOfRunningTestMethod(testCaseInstance: unittest.TestCase):
    # Call anywhere in the unit test methods (or in its 'setUp' method) as 'nameOfRunningTestMethod(self)':
    #   returns the _name_ of the running test method
    id_: str = testCaseInstance.id()
    id_ = id_[id_.rfind(".") + 1 :]
    return id_
