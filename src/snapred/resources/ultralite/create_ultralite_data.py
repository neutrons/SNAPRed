# import mantid algorithms, numpy and matplotlib

from mantid.simpleapi import *
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.data.GroceryService import GroceryService
from snapred.meta.Config import Resource

Resource._resourcesPath = os.path.expanduser("~/SNS/SNAP/shared/Calibration_next/Powder/")
liteInstrumentFile = Resource.getPath("CRACKLE_Definition.xml")
dfs = DataFactoryService()


def superID(nativeID, xdim, ydim):
    # accepts a numpy array of native ID from standard SNAP nexus file and returns a numpy array with
    # super pixel ID according to provided dimensions xdim and ydim of the super pixel.
    # xdim and ydim shall be multiples of 2

    Nx = 256  # native number of horizontal pixels
    Ny = 256  # native number of vertical pixels
    NNat = Nx * Ny  # native number of pixels per panel

    firstPix = (nativeID // NNat) * NNat
    redID = nativeID % NNat  # reduced ID beginning at zero in each panel

    (i, j) = divmod(redID, Ny)  # native (reduced) coordinates on pixel face
    superi = divmod(i, xdim)[0]
    superj = divmod(j, ydim)[0]

    # some basics of the super panel
    superNx = Nx / xdim  # 32 running from 0 to 31
    superNy = Ny / ydim
    superN = superNx * superNy

    superFirstPix = (firstPix / NNat) * superN

    superVal = superi * superNy + superj + superFirstPix

    return superVal


# create the mapping
LoadEmptyInstrument(
    Filename="/SNS/SNAP/shared/Malcolm/dataFiles/SNAP_Definition.xml",
    OutputWorkspace="SNAP",
)

mapToCrackle = "map_from_SNAP_to_CRACKLE"
if mapToCrackle not in mtd:
    # create the lite grouping ws using input run as template
    CreateGroupingWorkspace(
        InputWorkspace="SNAP",
        GroupDetectorsBy="All",
        OutputWorkspace=mapToCrackle,
    )
    ws = mtd[mapToCrackle]
    nHst = ws.getNumberHistograms()
    for spec in range(nHst):
        ws.setY(spec, [superID(spec, 128, 128) + 1])

# select run to convert to ultralite data, can convert multiple runs at once
runs_to_reduce = ["58882"]  # ["46680", "58810", "58813", "57514"]

clerk = GroceryListItem.builder()
for x in runs_to_reduce:
    clerk.neutron(x).native().add()
groceries = GroceryService().fetchGroceryList(clerk.buildList())


# The FileName should point to a "diffract_consts_<runNumber>_v#.h5 file, this gets saved at the end of a diffcal run
LoadDiffCal(
    InputWorkspace=groceries[0],
    FileName="/SNS/users/8l2/SNS/SNAP/shared/Calibration_next/Powder/04bd2c53f6bf6754/native/diffraction/v_0003/diffract_consts_057514_v0003.h5",
    WorkspaceName="57514",
)
# If set to False, will output data as histograms
eventMode = True

for grocery in groceries:
    ws = mtd[grocery]
    ultralite = f"{grocery}_ULTRALITE"
    CloneWorkspace(
        InputWorkspace=grocery,
        OutputWorkspace=ultralite,
    )
    ConvertUnits(
        InputWorkspace=ultralite,
        OutputWorkspace=ultralite,
        Target="dSpacing",
    )
    if not eventMode:
        uws = mtd[ultralite]
        Rebin(InputWorkspace=ultralite, OutputWorkspace=ultralite, Params=(uws.getTofMin(), -0.001, uws.getTofMax()))
    DiffractionFocussing(
        InputWorkspace=ultralite,
        OutputWorkspace=ultralite,
        GroupingWorkspace=mapToCrackle,
        PreserveEvents=eventMode,
    )
    LoadInstrument(
        Workspace=ultralite,
        Filename=liteInstrumentFile,
        RewriteSpectraMap=True,
    )
    ConvertUnits(
        InputWorkspace=ultralite,
        OutputWorkspace=ultralite,
        Target="TOF",
    )
    if eventMode:
        CompressEvents(
            InputWorkspace=ultralite,
            OutputWorkspace=ultralite,
            BinningMode="Logarithmic",
            Tolerance=-0.0001,
        )
        uws = mtd[ultralite]
        Rebin(InputWorkspace=ultralite, OutputWorkspace=ultralite, Params=(uws.getTofMax() - uws.getTofMin()))
    logs = (
        "BL3:Det:TH:BL:Frequency",
        "BL3:Mot:OpticsPos:Pos",
        "BL3:Chop:Gbl:WavelengthReq",
        "BL3:Chop:Skf1:WavelengthUserReq",
        "BL3:Chop:Gbl:WavelengthReq",
        "BL3:Chop:Skf1:WavelengthUserReq",
        "det_arc1",
        "det_arc2",
        "BL3:Det:TH:BL:Frequency",
        "BL3:Mot:OpticsPos:Pos",
        "det_lin1",
        "det_lin2",
        "proton_charge",
        "gd_prtn_chrg",
    )
    RemoveLogs(Workspace=ultralite, KeepLogs=logs)
    SaveNexusProcessed(
        InputWorkspace=ultralite,
        Filename=f"~/Documents/ultralite/{ultralite}.nxs.h5",
        CompressNexus=True,
    )
