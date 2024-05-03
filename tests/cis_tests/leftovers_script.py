from snapred.backend.recipe.algorithm.data.WrapLeftovers import WrapLeftovers
from snapred.backend.recipe.algorithm.data.ReheatLeftovers import ReheatLeftovers

from mantid.simpleapi import *
#from mantid.testing import assert_almost_equal as assert_wksp_almost_equal

# Load focussed data
Load(Filename='/SNS/users/wqp/SNAP/shared/Calibration_dynamic/Powder/04bd2c53f6bf6754/normalization/v_0014/tof_column_s+f-vanadium_058810_v0014.nxs', OutputWorkspace='raw')

xMin = [0.05,0.06,0.1,0.07,0.04, 0.04]
xMax = [0.36,0.41,0.64,0.48,0.48,0.48]
delta = [-0.000401475,-0.000277182,-0.000323453,-0.000430986,-0.000430986,-0.000430986]
# RebinRagged(InputWorkspace="raw", XMin=[0.05,0.06,0.1,0.07], XMax=[0.36,0.41,0.64,0.48], Delta=[-0.000401475,-0.000277182,-0.000323453,-0.000430986], PreserveEvents=False, OutputWorkspace="rebinRaggeded")
RebinRagged(InputWorkspace="raw", XMin=[0.05,0.06,0.1,0.07,0.04, 0.04], XMax=[0.36,0.41,0.64,0.48,0.48,0.48], Delta=[-0.000401475,-0.000277182,-0.000323453,-0.000430986,-0.000430986,-0.000430986], PreserveEvents=False, OutputWorkspace="raw")

filename = "~/tmp/leftovers.tar"
wrapLeftovers = WrapLeftovers()
wrapLeftovers.initialize()
wrapLeftovers.setPropertyValue("InputWorkspace","raw")
wrapLeftovers.setPropertyValue("Filename", filename)
wrapLeftovers.execute()

reheatLeftovers = ReheatLeftovers()
reheatLeftovers.initialize()
reheatLeftovers.setPropertyValue("OutputWorkspace","reheated")
reheatLeftovers.setPropertyValue("Filename", filename)
reheatLeftovers.execute()

# assert_wksp_almost_equal(Workspace1="raw", Workspace2="reheated") Doesnt work with ragged!
