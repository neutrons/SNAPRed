import json
from typing import Tuple

import numpy as np
from mantid.api import (
    AlgorithmFactory,
    PythonAlgorithm,
    WorkspaceGroup,
    mtd,
)
from mantid.kernel import Direction
from scipy.interpolate import make_smoothing_spline, splev

from snapred.backend.dao.ingredients import ReductionIngredients as Ingredients
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

name = "VanadiumRawCorrection"


# TODO: Rename so it matches filename
class VanadiumRawCorrection(PythonAlgorithm):
  def PyInit(self):
    # declare properties
    self.declareProperty("Ingredients", defaultValue="", direction=Direction.Input)
    self.declareProperty("OutputWorkspace", defaultValue="SmoothPeaks_out", direction=Direction.Output)
    self.setRethrows(True)
    self.mantidSnapper = MantidSnapper(self, name)

  def chopIngredients(self, ingredients: Ingredients) -> None:
    #
    self.geomCalibFile: str = ""  # iPrm['calibrationDirectory'] + sPrm['stateID'] +'/057514/'+ sPrm['calFileName']

    # geomCalibFile= calibrationPath + geomCalibFilename
    self.rawVFile = ""  # iPrm['calibrationDirectory'] + sPrm['stateID'] +'/057514/'+ f'RVMB{VRun}
    if ingredients.liteMode:
        extns = ".lite.nxs"
    else:
        extns = ".nxs"
    self.rawVFile = self.rawVFile + extns

    self.TOFPars: Tuple[float, float, float] = (0, 0, 0)  # (sPrm['tofMin'], sPrm['tofBin'], sPrm['tofMax'] )
    self.IPTSLoc = self.mantidSnapper.GetIPTS(RunNumber=self.runNumber, Instrument="SNAP")
    self.mantidSnapper.executeQueue()
    if liteMode:
        self.vanadium_filename = f"{self.IPTSLoc}shared/lite/SNAP_{self.runNumber}.lite.nxs.h5"
    else:
        self.vanadium_filename = f"{self.IPTSLoc}nexus/SNAP_{self.runNumber}.nxs.h5"
    pass

  def raidPantry(self, wsName: str) -> None:
    self.mantidSnapper.LoadEventNexus(
        Filename=self.vanadium_filename,
        OutputWorkspace=wsName,
        FilterByTofMin=self.TOFPars[0],
        FilterByTofMax=self.TOFPars[2],
        NumberOfBins=1,
    )
    self.mantidSnapper.NormaliseByCurrent(
        InputWorkspace=wsName,
        OutputWorkspace=wsName,
    )
    self.mantidSnapper.ApplyDiffCal(
        InstrumentWorkspace=wsName,
        CalibrationFile=self.geomCalibFile,
    )

    self.mantidSnapper.Rebin(
        InputWorkspace=wsName,
        Params=self.TOFPars,
        PreserveEvents=False,
        OutputWorkspace=wsName,
        BinningMode="Logarithmic",
    )
    self.mantidSnapper.executeQueue()

  def restockPantry(self, wsName: str, filename: str) -> None:
    self.mantidSnapper.SaveNexus(
        InputWorkspace=wsName,
        Filename=fileName,
    )

  def PyExec(self):
    wsNameV = "TOF_V"
    wsNameVB = "TOF_VB"
    wsName_cylinder = "cylAbsCalc"
    outputWS = "output_workspace"

    # Load and pre-process vanadium and empty datasets

    process_raw_data(wsNameV, VRun, TOFPars, geomCalibFile)
    process_raw_data(wsNameVB, VBRun, TOFPars, geomCalibFile)

    # take difference
    self.mantidSnapper.Minus(LHSWorkspace=wsNameV, RHSWorkspace=wsNameVB, OutputWorkspace=outputWS)
    self.mantidSnapper.DeleteWorkspaces(WorkspaceList=[wsNameV, wsNameVB])

    if not self.liteMode:
        self.mantidSnapper.SumNeighbours(InputWorkspace=outputWS, SumX=8, SumY=8, OutputWorkspace=outpuwWS)

    # # calculate and apply cylindrical absorption

    self.mantidSnapper.ConvertUnits(
        InputWorkspace=outputWS,
        OutputWorkspace=outputWS,
        Target="Wavelength",
    )
    self.mantidSnapper.CylinderAbsorption(
        InputWorkspace=outputWS,
        OutputWorkspace=wsName_cylinder,
        AttenuationXSection=4.878,
        ScatteringXSection=5.159,
        SampleNumberDensity=0.070,
        CylinderSampleHeight=0.3,  # cm
        CylinderSampleRadius=0.15,  # cm
        NumberOfSlices=10,
        NumberOfAnnuli=10,
    )
    self.mantidSnapper.Divide(
        LHSWorkspace=outputWS,
        RHSWorkspace=wsName_cylinder,
        OutputWorkspace=outputWS,
    )
    self.mantidSnapper.DeleteWorkspace(Workspace=wsName_cylinder)
    self.mantidSnapper.ConvertUnits(
        InputWorkspace=outputWS,
        OutputWorkspace=outputWS,
        Target="TOF",
    )
    self.mantidSnapper.executeQueue()

    self.restockPantry(outputWS, rawVFile)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(VanadiumRawCorrection)
