import matplotlib.pyplot as plt
from mantid.api import mtd


def VanadiumFoucussedReductionPlot():
    foc_group = list(mtd["diffraction_focused_vanadium"].getNames())
    smooth_group = list(mtd["SmoothedDataExcludingPeaks"].getNames())

    for idx, ws in enumerate(foc_group):
        fig, ax = plt.subplots(subplot_kw={"projection": "mantid"})
        mtd_ws = mtd[ws]
        numSpec = mtd_ws.getNumberHistograms()
        for index in range(numSpec):
            spec_num = mtd_ws.getSpectrum(index).getSpectrumNo()
            ax.plot(mtd_ws, specNum=spec_num, label=f"Focussed Spectrum {spec_num}")
        for each in smooth_group:
            if ws in each:
                spec_num = each[-2:].replace("_", "")
                _label = "Smoothed Spectrum " + spec_num
                ax.plot(mtd[each], specNum=1, label=_label)
        ax.legend()
        fig.show()
