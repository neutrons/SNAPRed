import matplotlib.pyplot as plt
import numpy as np
from mantid.api import mtd
from mantid.simpleapi import Minus


def FitPeaksPlot(wsName):
    ws = mtd[wsName]
    wsGroup = mtd["fitPeaksWSGroup"]
    numSpec = ws.getNumberHistograms()
    sqrtSize = int(np.sqrt(numSpec))
    # Get best size for layout
    if sqrtSize == np.sqrt(numSpec):
        rowSize = sqrtSize
        colSize = sqrtSize
    elif numSpec <= ((sqrtSize + 1) * sqrtSize):
        rowSize = sqrtSize
        colSize = sqrtSize + 1
    else:
        rowSize = sqrtSize + 1
        colSize = sqrtSize + 1

    fig = plt.figure()
    plts = []
    for index in range(numSpec):
        fitWS = mtd[f"{wsName}_fitted_{index}"]
        Minus(ws, fitWS, AllowDifferentNumberSpectra=True, OutputWorkspace=f"res_{index}")
        res = mtd[f"res_{index}"]
        wsGroup.add(f"res_{index}")
        ax = fig.add_subplot(rowSize, colSize, index + 1, projection="mantid")
        ax.plot(res, specNum=res.getSpectrum(index).getSpectrumNo(), label="Residual")
        ax.plot(ws, specNum=ws.getSpectrum(index).getSpectrumNo(), label="Spectrum")
        ax.plot(fitWS, specNum=fitWS.getSpectrum(0).getSpectrumNo(), label="Fitted Peaks")
        ax.legend()
        plts.append(ax)
    fig.show()
