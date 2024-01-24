# @file: tests/util/helpers.py:
#
# Helper utility methods to be used by unit tests and CIS tests.
#

import unittest
from collections.abc import Sequence
from typing import Any, Dict, List, Tuple

import numpy as np
import pytest
from mantid.api import ITableWorkspace, MatrixWorkspace
from mantid.dataobjects import GroupingWorkspace, MaskWorkspace
from mantid.simpleapi import (
    CopyInstrumentParameters,
    DeleteWorkspace,
    ExtractMask,
    LoadInstrument,
    ScaleX,
    WorkspaceFactory,
    mtd,
)
from snapred.meta.Config import Resource


def createCompatibleMask(maskWSName: str, templateWSName: str, instrumentFilePath: str) -> MaskWorkspace:
    """
    Create a `MaskWorkspace` compatible with a template workspace and instrument
      (At present, due to limitations in the Mantid python API, the instrumentFilePath is necessary.)
    """
    templateWS = mtd[templateWSName]
    inst = templateWS.getInstrument()
    # Exclude pixels which are monitors:
    mask = WorkspaceFactory.create("SpecialWorkspace2D", NVectors=inst.getNumberDetectors(True), XLength=1, YLength=1)
    mtd[maskWSName] = mask
    LoadInstrument(
        Workspace=maskWSName,
        Filename=instrumentFilePath,
        RewriteSpectraMap=False,
    )
    if mask.getInstrument().getNumberDetectors(True) != templateWS.getInstrument().getNumberDetectors(True):
        raise RuntimeError(
            f'Instrument file resource "{instrumentFilePath}" does not describe the template workspace' "s instrument"
        )
    # Copy any configurable instrument parameters from the template workspace.
    CopyInstrumentParameters(InputWorkspace=templateWSName, OutputWorkspace=maskWSName)

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
    ExtractMask(InputWorkspace=maskWSName, OutputWorkspace=maskWSName)
    assert isinstance(mtd[maskWSName], MaskWorkspace)
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
