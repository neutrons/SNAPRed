"""
Reduce a ``livereduce`` accumulation workspace with SNAPRed, as the post-processing script does it.

The accumulation workspace is registered as the neutron data for its run, through the
``workspace/registerNeutronData`` endpoint, and the run is then reduced through the ``reduction``
endpoint.  SNAPRed does not know where the data came from: it only finds the registration.

The checks are written to the Mantid log (which livereduce forwards to 'livereduce.log') as ``OK``/``FAIL``
lines, then one summary line per update: ``ALL OK`` or ``FAIL: <n> check(s) failed``.  Nothing is saved.
"""

from typing import Callable, List

from mantid.kernel import logger as mantidLogger
from mantid.simpleapi import DeleteWorkspace, mtd

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao.request import RegisterNeutronDataRequest
from snapred.backend.dao.request.ReductionRequest import ReductionRequest
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.dao.SNAPResponse import ResponseCode
from snapred.meta.Config import Config


def _request(path: str, payload):
    # As from the GUI: the payload is the request object.
    return InterfaceController().executeRequest(SNAPRequest(path=path, payload=payload))


def _log(workspace, name: str):
    run = mtd[str(workspace)].getRun()
    return run.getProperty(name).value if run.hasProperty(name) else None


class Checks:
    def __init__(self, report: Callable[[str], None]):
        self.report = report
        self.failures: List[str] = []

    def __call__(self, description: str, condition: bool):
        self.report(f"{'OK  ' if condition else 'FAIL'} {description}")
        if not condition:
            self.failures.append(description)

    def summary(self) -> str:
        return f"FAIL: {len(self.failures)} check(s) failed" if self.failures else "ALL OK"


def reduceAccumulation(
    accumulation, useLiteMode: bool = True, report: Callable[[str], None] = mantidLogger.notice
) -> bool:
    """
    Register the accumulation workspace for its run, and reduce the run.

    `accumulation` is the workspace (or its name) that ``StartLiveData`` passes to the post-processing
    script.  Returns whether every check passed.
    """
    accumulation = str(accumulation)
    check = Checks(report)
    dataSourceLog = f"{Config['metadata.tagPrefix']}dataSource"

    runNumber = _log(accumulation, "run_number")
    if not runNumber or int(runNumber) == 0:
        # Tier 2 (no run number in the live data) is not handled yet.
        check(f"the accumulation workspace has a run number (it has '{runNumber}')", False)
        report(check.summary())
        return False
    runNumber = str(runNumber)
    eventCount = mtd[accumulation].getNumberEvents()
    report(f"update: run {runNumber}, {eventCount} events in '{accumulation}'")

    response = _request(
        "workspace/registerNeutronData", RegisterNeutronDataRequest(runNumber=runNumber, workspace=accumulation)
    )
    check("registration succeeds", response.code == ResponseCode.OK)

    response = _request("reduction/", ReductionRequest(runNumber=runNumber, useLiteMode=useLiteMode))
    check(f"reduction succeeds ({response.code}: {response.message or ''})", response.code == ResponseCode.OK)

    if response.code == ResponseCode.OK:
        outputs = response.data.record.workspaceNames
        check("reduction has output workspaces", len(outputs) > 0)
        expected = "registered>liteConversion" if useLiteMode else "registered"
        for workspace in outputs:
            if mtd.doesExist(workspace) and hasattr(mtd[workspace], "getRun"):
                check(f"'{workspace}' comes from the registered data", _log(workspace, dataSourceLog) == expected)
        for workspace in [*outputs, response.data.unfocusedData]:
            if workspace and mtd.doesExist(workspace):
                DeleteWorkspace(workspace)

    check("the accumulation workspace still exists", mtd.doesExist(accumulation))
    check("the accumulation workspace has no dataSource tag", _log(accumulation, dataSourceLog) is None)
    check("the accumulation workspace has the same events", mtd[accumulation].getNumberEvents() == eventCount)
    report(check.summary())
    return not check.failures
