from mantid.simpleapi import *

from time import time


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

plusResult = Plus(wsname,wsname)
minusResult = Minus(wsname,wsname)
multiplyResult = Multiply(wsname,wsname)
divideResult = Divide(wsname,wsname)

print(f"Ragged workspace values: {mtd[wsname].readY(0)}")
print(f"Plus result: {plusResult.readY(0)}")
print(f"Minus result: {minusResult.readY(0)}")
print(f"Multiply result: {multiplyResult.readY(0)}")
print(f"Divide result: {divideResult.readY(0)}")


