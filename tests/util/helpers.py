# @file: tests/util/helpers.py:
#
# Helper utility methods to be used by unit tests and CIS tests.
#

import unittest
from collections.abc import Sequence
from typing import Any, List, Tuple

import numpy
import numpy as np
from mantid.api import ITableWorkspace, MatrixWorkspace
from mantid.dataobjects import GroupingWorkspace, MaskWorkspace
from mantid.simpleapi import (
    AddSampleLog,
    AddSampleLogMultiple,
    CompareWorkspaces,
    CreateEmptyTableWorkspace,
    CreateWorkspace,
    DeleteWorkspace,
    ExtractMask,
    LoadInstrument,
    MaskDetectors,
    mtd,
)

from snapred.backend.dao.state.DetectorState import DetectorState


def createNPixelInstrumentXML(numberOfPixels):
    """
    Given a number of pixels, create an instrument XML file with that many pixels.
    These pixels have no locations and cannot be used for any valid geometry checks.
    However, they will allow for certain algorithms, such as MaskDetectors to work.
    """
    instrumentXML = f"""
        <instrument xmlns="none"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation="none"
            name="tmp{numberOfPixels}" valid-from="1970-01-01 00:00:00">
        <!-- Pixel for Detectors-->\n
        <type name="panel" is="rectangular_detector" type="pixel"
        xpixels="1" xstart="-0.001" xstep="+0.001"
        ypixels="1" ystart="-0.078795" ystep="+0.079104" >
        <properties/>
        </type>
    """
    for n in range(numberOfPixels):
        instrumentXML += f"""
            <component type="pixel" idlist="{n}"><location /></component>\n"""
    for n in range(numberOfPixels):
        instrumentXML += f"""<idlist idname="{n}"><id val="{n}"/></idlist>\n"""
    instrumentXML += """
        <type name="pixel" is="detector">
        <cuboid id="pixel-shape">
            <left-front-bottom-point  y="-0.079104" x="-0.0005" z="0.0"/>
            <left-front-top-point     y="+0.079104" x="-0.0005" z="0.0"/>
            <left-back-bottom-point   y="-0.079104" x="-0.0005" z="0.0001"/>
            <right-front-bottom-point y="-0.079104" x="+0.0005" z="0.0"/>
        </cuboid>
        <algebra val="pixel-shape"/>
        </type>
    """
    instrumentXML += "</instrument>\n"
    return instrumentXML


def addInstrumentParameters(wsname, detectorState: DetectorState = None):
    if detectorState is None:
        # if not detector state given, use a default with arbitrary values
        detectorState = DetectorState(
            arc=(1, 2),
            lin=(3, 4),
            wav=10.0,
            freq=60.0,
            guideStat=1,
        )
    logs = detectorState.getLogValues()
    lognames = list(logs.keys())
    logvalues = list(logs.values())
    logtypes = ["Number Series"] * len(lognames)
    AddSampleLogMultiple(
        Workspace=wsname,
        LogNames=lognames[:-1],
        LogValues=logvalues[:-1],
        LogTypes=logtypes[:-1],
        ParseType=False,
    )
    AddSampleLog(
        Workspace=wsname,
        LogName=lognames[-1],
        LogText=logvalues[-1],
        LogType=logtypes[-1],
        UpdateInstrumentParameters=True,
    )


def createNPixelWorkspace(wsname, numberOfPixels, detectorState: DetectorState = None):
    """
    Given a number of pixels, create an workspace with that many pixels.
    The instrument on the workspace will have that many pixels defined,
    but they have no locations or geometry.
    """
    CreateWorkspace(
        OutputWorkspace=wsname,
        DataX=[0] * numberOfPixels,
        DataY=[0] * numberOfPixels,
        NSpec=numberOfPixels,
    )
    LoadInstrument(
        Workspace=wsname,
        InstrumentName=f"tmp{numberOfPixels}",
        InstrumentXML=createNPixelInstrumentXML(numberOfPixels),
        RewriteSpectraMap=True,
    )
    addInstrumentParameters(wsname, detectorState)
    return mtd[wsname]


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


def maskFromArray(mask: List[bool], maskWSname: str, parentWSname=None, detectorState=None):
    """
    Create a mask workspace with given name, with the indicated pixels masked.
    If a parent workspace is passed, create the mask workspace compatible with
    the parent's instrument.
    If not, but a detector state is passed, will create a mask and load parameters
    corresponding to the detector state.
    """

    nspec = len(mask)
    if parentWSname is None:
        ws = createNPixelWorkspace(maskWSname, nspec, detectorState)
    else:
        ws = createCompatibleMask(maskWSname, parentWSname)
    assert len(mask) == ws.getNumberHistograms(), "The mask array was incompatible with the parent workspace."

    toMask = np.argwhere(mask == 1)
    MaskDetectors(
        Workspace=maskWSname,
        WorkspaceIndexList=toMask,
    )
    ExtractMask(
        InputWorkspace=maskWSname,
        OutputWorkspace=maskWSname,
    )
    return maskWSname


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


def workspacesNotEqual(Workspace1: str, Workspace2: str, **other_options):
    """
    Meant to be called as
    ``` python
    assert workspacesNotEqual(ws1, ws2)
    ```
    Parameters:
    - Workspace1: str -- one of the workspaces to compare
    - Workspace2: str -- one of the workspaces to compare
    - other_options: kwargs dict -- other options available to CompareWorkspaces
    Returns: if the workspaces are NOT equal, will return True
    If the workspaces ARE equal, will raise an assertion error
    """
    equal, _ = CompareWorkspaces(
        Workspace1=Workspace1,
        Workspace2=Workspace2,
        **other_options,
    )
    if equal:
        raise AssertionError(f"Workspaces {Workspace1} and {Workspace2} incorrectly evaluated as equal")
    return not equal


def nameOfRunningTestMethod(testCaseInstance: unittest.TestCase):
    # Call anywhere in the unit test methods (or in its 'setUp' method) as 'nameOfRunningTestMethod(self)':
    #   returns the _name_ of the running test method
    id_: str = testCaseInstance.id()
    id_ = id_[id_.rfind(".") + 1 :]
    return id_
