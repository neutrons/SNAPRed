import numpy as np
from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction
from mantid.simpleapi import CreateEmptyTableWorkspace, mtd

name = "ConvertDiffCalLog"


class ConvertDiffCalLog(PythonAlgorithm):
    """
    This algo extends the functionality of the usual mantid ConvertDifCal algorithm, to work
    with the proper log-scale binning correction with offsets:
        
        DIFCnew = DIFCold * (1 + dBin)^{-offset}
    
    It is intended as a stop-gap, until that algorithm is patched with this behavior here
    Delete this algo once the new work has been completed in mantid
    inputs:
        
        OffsetsWorkspace: str -- name of an OffsetsWorkspace containing signed offsets
        PreviousCalibration: str -- name of a TableWorkspace of DIFCs which are to be corrected (cols 'detid', 'difc')
        BinWidth: float -- the binwidth, dBin, used in the update equations
    
    output:
        
        OutputWorkspace -- the corrected calibration workspace of DIFCs, as a TableWorkspace
    """

    def PyInit(self) -> None:
        # declare properties
        self.declareProperty("OffsetsWorkspace", defaultValue="", direction=Direction.Input)
        self.declareProperty("PreviousCalibration", defaultValue="", direction=Direction.Input)  # a table workspace
        self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
        self.declareProperty("OffsetMode", defaultValue="", direction=Direction.Input)  # does nothing
        self.declareProperty("BinWidth", defaultValue=-0.001, direction=Direction.Input)
        self.setRethrows(True)

    def PyExec(self) -> None:
        # get offsets
        offsetWS = self.getProperty("OffsetsWorkspace").value
        difcWS = self.getProperty("PreviousCalibration").value
        outputWS = self.getProperty("OutputWorkspace").value
        dBin = self.getProperty("BinWidth").value
        previousCal = mtd[difcWS]

        # calculate multiplicative offsets
        #  DIFCnew = DIFCold * (1 + dBin)^{-offset}
        offsets = mtd[offsetWS].extractY().ravel()
        detid_old = previousCal.column("detid")
        difc_old = previousCal.column("difc")
        multOff = np.power(np.ones_like(difc_old) + np.abs(dBin), -1 * offsets)
        difc_new = np.multiply(difc_old, multOff)

        # create diffCal workspace with new values
        outws = CreateEmptyTableWorkspace(
            OutputWorkspace=outputWS,
        )
        outws.addColumn(type="int", name="detid", plottype=6)
        outws.addColumn(type="float", name="difc", plottype=6)
        outws.addColumn(type="float", name="difa", plottype=6)
        outws.addColumn(type="float", name="tzero", plottype=6)
        outws.addColumn(type="float", name="tofmin", plottype=6)
        outws.setLinkedYCol(0, 1)

        for detid, difc in zip(detid_old, difc_new):
            nextRow = {"detid": detid, "difc": difc, "difa": 0, "tzero": 0, "tofmin": 0}
            outws.addRow(nextRow)
        return


# Register algorithm with Mantid
AlgorithmFactory.subscribe(ConvertDiffCalLog)
