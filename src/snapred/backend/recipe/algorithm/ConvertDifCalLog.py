import numpy as np

from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.simpleapi import mtd, CreateEmptyTableWorkspace
from mantid.kernel import Direction

name = "ConvertDifCalLog"

class ConvertDifCalLog(PythonAlgorithm):
    # This algo extends the functionality of the usual mantid ConvertDifCal algorithm
    # It is intended as a stop-gap, until that algorithm is patched with this behavior here
    # Delete this algo once the new work has been completed in mantid
    def PyInit(self):
        # declare properties
        self.declareProperty("OffsetsWorkspace", defaultValue="", direction=Direction.Input)
        self.declareProperty("PreviousCalibration", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
        self.declareProperty("BinWidth", defaultValue=-0.001, direction=Direction.Input)
        self.setRethrows(True)

    def PyExec(self):
        #get offsets
        offsetWS = self.getProperty("OffsetsWorkspace").value
        difcWS = self.getProperty("PreviousCalibration").value
        outputWS = self.getProperty("OutputWorkspace").value
        dBin = self.getProperty("BinWidth").value

        #calculate multiplicative offsets
        #  DIFCnew = DIFCold * (1 + dBin)^{-offset}
        offsets = mtd[offsetWS].extractY().ravel()
        difc_old = mtd[difcWS].extractY().ravel()
        multOff = np.power(np.ones_like(difc_old)+np.abs(dBin),-1*offsets)
        difc_new = np.multiply(difc_old,multOff)

        #create diffCal workspace with new values
        outws = CreateEmptyTableWorkspace(
            OutputWorkspace=outputWS,
        )
        outws.addColumn(type="int",name="detid",plottype=6)
        outws.addColumn(type="float",name="difc",plottype=6)
        outws.addColumn(type="float",name="difa",plottype=6)
        outws.addColumn(type="float",name="tzero",plottype=6)
        outws.addColumn(type="float",name="tofmin",plottype=6)
        outws.setLinkedYCol(0, 1)

        for i in range(len(difc_old)):
            nextRow = { 'detid': i,
                    'difc': difc_new[i],
                    'difa': 0,
                    'tzero': 0,
                    'tofmin': 0 }
            outws.addRow ( nextRow )
        return outws


# Register algorithm with Mantid
AlgorithmFactory.subscribe(ConvertDifCalLog)
