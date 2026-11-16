"""The names of neutron-data workspaces, as the tests expect them."""

from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng


def _builder(runNumber: str, useLiteMode: bool):
    return wng.run().runNumber(runNumber).lite(wng.Lite.fromLiteMode(useLiteMode))


def plain(runNumber: str, useLiteMode: bool) -> WorkspaceName:
    return _builder(runNumber, useLiteMode).build()


def raw(runNumber: str, useLiteMode: bool) -> WorkspaceName:
    return _builder(runNumber, useLiteMode).auxiliary(wng.Auxiliary.CACHED_RAW).build()


def copy(runNumber: str, useLiteMode: bool, copyNumber: int) -> WorkspaceName:
    return _builder(runNumber, useLiteMode).auxiliary(wng.Auxiliary.cachedCopy(copyNumber)).build()
