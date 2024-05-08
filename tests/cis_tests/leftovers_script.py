import snapred.backend.recipe.algorithm.data.WrapLeftovers
import snapred.backend.recipe.algorithm.data.ReheatLeftovers
from mantid.simpleapi import *

from time import time


# Load focussed data
# Load(Filename='/SNS/users/wqp/SNAP/shared/Calibration_dynamic/Powder/04bd2c53f6bf6754/normalization/v_0014/tof_column_s+f-vanadium_058810_v0014.nxs', OutputWorkspace='raw')

## create a workspace to be rebin-ragged
wsname = mtd.unique_name(prefix="leftovers_")
CreateSampleWorkspace(
    OutputWorkspace=wsname,
    BankPixelWidth=3,
)
GroupDetectors(
    InputWorkspace=wsname,
    OutputWorkspace=wsname,
    GroupingPattern="0-3,4-5,6-8,9-12,13-14,15-17",
)

# rebin the workspace raggedly
xMin = [0.05,0.06,0.1,0.07,0.04, 0.04]
xMax = [0.36,0.41,0.64,0.48,0.48,0.48]
delta = [-0.000401475,-0.000277182,-0.000323453,-0.000430986,-0.000430986,-0.000430986]
RebinRagged(
    InputWorkspace=wsname, 
    XMin=xMin, 
    XMax=xMax, 
    Delta=delta, 
    PreserveEvents=False, 
    OutputWorkspace=wsname,
)

# store leftovers, reheat, verify workspaces are equivalent
filename = "~/tmp/leftovers.tar"

timeWrapStart = time()

WrapLeftovers(
    InputWorkspace = wsname,
    Filename = filename,
)

timeWrapEnd = timeReheatStart = time()

ReheatLeftovers(
    OutputWorkspace ="reheated",
    Filename = filename,
)
timeReheatEnd = time()

original = mtd[wsname]
reheated = mtd["reheated"]
assert original.getNumberHistograms() == reheated.getNumberHistograms()
assert original.getRun() == reheated.getRun()
for i in range(original.getNumberHistograms()):
    assert list(original.readX(i)) == list(reheated.readX(i))
    assert list(original.readY(i)) == list(reheated.readY(i))

print(f"TIME TO WRAP:\t{timeWrapEnd - timeWrapStart}")
print(f"TIME TO REHEAT:\t{timeReheatEnd - timeReheatStart}")
print(f"TOTAL TIME BOTH:\t{timeReheatEnd - timeWrapStart}")

# CompareWorkspaces(Workspace1="raw", Workspace2="reheated") Doesnt work with ragged!
    