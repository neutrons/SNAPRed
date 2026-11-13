"""
CIS test: neutron data registered for a run, as the ``livereduce`` post-processing script will do it.

Run it on an analysis node, from the repository root, with a run number that has its NeXus file in IPTS:

    pixi run python tests/cis_tests/registered_neutron_data_script.py <runNumber>

The script:

1. loads the run's NeXus file into an ``accumulation_`` workspace, which stands in for the
   ``livereduce`` accumulation workspace, and removes its 'run_number' log, as the live-data listener
   may do;
2. registers that workspace for the run, through the ``workspace/registerNeutronData`` endpoint;
3. fetches the run's neutron data, native and lite, and checks that the data comes from the registered
   workspace (``dataSource``), that the registered workspace is not changed, and that no lite-mode file
   is written for the run;
4. checks that a live-data request for the registered run is an error, and that ``clearADS`` keeps the
   registered workspace;
5. removes the registration, through the ``workspace/unregisterNeutronData`` endpoint, and checks that
   the data then comes from the file again.

The last line starts with ``ALL OK`` if every check passes.  Otherwise the script shows ``FAIL``, and
its exit code is not 0.
"""

import datetime
import sys

from mantid.simpleapi import DeleteLog, DeleteWorkspace, LoadEventNexus, mtd

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem, LiveDataArgs
from snapred.backend.dao.request import RegisterNeutronDataRequest, UnregisterNeutronDataRequest
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.dao.SNAPResponse import ResponseCode
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.meta.Config import Config

if len(sys.argv) != 2:
    print(__doc__)
    sys.exit(2)
runNumber = sys.argv[1]

failures = []


def check(description: str, condition: bool):
    print(f"{'OK  ' if condition else 'FAIL'} {description}")
    if not condition:
        failures.append(description)


def request(path: str, payload):
    return InterfaceController().executeRequest(SNAPRequest(path=path, payload=payload.model_dump_json()))


def item(useLiteMode: bool, liveDataArgs=None) -> GroceryListItem:
    return GroceryListItem(
        workspaceType="neutron", runNumber=runNumber, useLiteMode=useLiteMode, liveDataArgs=liveDataArgs
    )


def logValue(workspace: str, name: str):
    run = mtd[workspace].getRun()
    return run.getProperty(name).value if run.hasProperty(name) else None


dataSourceLog = f"{Config['metadata.tagPrefix']}dataSource"
dataService = LocalDataService()
groceries = GroceryService()

nativePath = dataService.createNeutronFilePath(runNumber, False)
litePath = dataService.createNeutronFilePath(runNumber, True)
if not nativePath or not nativePath.exists():
    print(f"FAIL: there is no NeXus file for run {runNumber}")
    sys.exit(1)
liteFileExisted = bool(litePath) and litePath.exists()

# 1. The stand-in for the `livereduce` accumulation workspace.
accumulation = mtd.unique_name(prefix="accumulation_")
LoadEventNexus(Filename=str(nativePath), OutputWorkspace=accumulation, NumberOfBins=1)
DeleteLog(Workspace=accumulation, Name="run_number")
eventCount = mtd[accumulation].getNumberEvents()

# 2. Register it.
response = request(
    "workspace/registerNeutronData", RegisterNeutronDataRequest(runNumber=runNumber, workspace=accumulation)
)
check("registration succeeds", response.code == ResponseCode.OK)
response = request(
    "workspace/registerNeutronData", RegisterNeutronDataRequest(runNumber=runNumber, workspace=accumulation)
)
check("registering again succeeds", response.code == ResponseCode.OK)

# 3. Fetch the data.
native = groceries.fetchNeutronDataSingleUse(item(False))
check("native data comes from the registered workspace", native["dataSource"] == "registered")
check("native data is tagged", logValue(native["workspace"], dataSourceLog) == "registered")
check("native data has the run number", str(logValue(native["workspace"], "run_number")) == runNumber)
DeleteWorkspace(native["workspace"])

lite = groceries.fetchNeutronDataCached(item(True))
check("lite data is converted from the registered workspace", lite["dataSource"] == "registered>liteConversion")
check(
    "no lite-mode file is written for the run",
    liteFileExisted or not (bool(litePath) and litePath.exists()),
)

check("the registered workspace still exists", mtd.doesExist(accumulation))
check("the registered workspace has no 'run_number' log", logValue(accumulation, "run_number") is None)
check("the registered workspace has no dataSource tag", logValue(accumulation, dataSourceLog) is None)
check("the registered workspace has the same events", mtd[accumulation].getNumberEvents() == eventCount)

# 4. A live-data request is an error; `clearADS` keeps the registered workspace.
try:
    groceries.fetchNeutronDataSingleUse(item(False, LiveDataArgs(duration=datetime.timedelta(seconds=10))))
    check("a live-data request for the registered run is an error", False)
except RuntimeError as e:
    check("a live-data request for the registered run is an error", "Remove the registration first" in str(e))

groceries.clearADS(exclude=[], clearCache=True)
check("clearADS keeps the registered workspace", mtd.doesExist(accumulation))

# 5. Remove the registration.
response = request("workspace/unregisterNeutronData", UnregisterNeutronDataRequest(runNumber=runNumber))
check("removing the registration succeeds", response.code == ResponseCode.OK)
native = groceries.fetchNeutronDataSingleUse(item(False))
check("after the registration is removed, native data comes from the file", native["dataSource"] == "nexusFile")
check("the registered workspace is not deleted", mtd.doesExist(accumulation))

DeleteWorkspace(native["workspace"])
DeleteWorkspace(accumulation)

if failures:
    print(f"FAIL: {len(failures)} check(s) failed")
    sys.exit(1)
print("ALL OK")
