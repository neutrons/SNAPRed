from typing import Dict, List, Tuple

from snapred.backend.data.source.WorkspaceOperations import WorkspaceOperations
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng

CacheKey = Tuple[str, bool]


class NeutronDataCache:
    """
    The cache of neutron-data workspaces, keyed by (run number, use lite mode).

    A cached run is held in the ADS as its raw workspace; each cached fetch returns a numbered copy.
    """

    def __init__(self, workspaces: WorkspaceOperations):
        self.workspaces = workspaces

        # `loadedRuns` holds a count of the number of copies made from the raw workspace for each key:
        #     None: if a workspace is not loaded;
        #        0: if it is loaded, but no copies have been made;
        #       >0: if any copies have been made, since loading
        self.loadedRuns: Dict[CacheKey, int] = {}

        # `uncacheableKeys`: keys into `loadedRuns` for data from a source that is not cacheable (live data).
        #   These entries have no raw workspace, so that the data is never reused.
        self.uncacheableKeys: List[CacheKey] = []

    @staticmethod
    def rawWorkspace(runNumber: str, useLiteMode: bool) -> WorkspaceName:
        """The name of the cached workspace of a run."""
        return (
            wng.run()
            .runNumber(runNumber)
            .lite(wng.Lite.fromLiteMode(useLiteMode))
            .auxiliary(wng.Auxiliary.CACHED_RAW)
            .build()
        )

    @staticmethod
    def key(runNumber: str, useLiteMode: bool) -> CacheKey:
        return (runNumber, useLiteMode)

    def contains(self, key: CacheKey) -> bool:
        return self.loadedRuns.get(key) is not None

    def updateFromADS(self, runNumber: str, useLiteMode: bool):
        """Make the entry for a key consistent with the ADS."""
        workspace = self.rawWorkspace(runNumber, useLiteMode)
        key = self.key(runNumber, useLiteMode)
        if self.workspaces.workspaceDoesExist(workspace) and self.loadedRuns.get(key) is None:
            # 0 => loaded with no copies
            self.loadedRuns[key] = 0
        elif self.loadedRuns.get(key) is not None and not self.workspaces.workspaceDoesExist(workspace):
            del self.loadedRuns[key]

    def rebuild(self):
        for key in self.loadedRuns.copy():
            self.updateFromADS(*key)

    def cachedWorkspaces(self) -> List[WorkspaceName]:
        return [self.rawWorkspace(runNumber, useLiteMode) for runNumber, useLiteMode in self.loadedRuns.keys()]

    def recordUncacheable(self, key: CacheKey):
        self.loadedRuns[key] = 0
        self.uncacheableKeys.append(key)

    def clearUncacheable(self):
        while self.uncacheableKeys:
            key = self.uncacheableKeys.pop()
            # The entry may already have been dropped by `updateFromADS`, or recorded more than once.
            self.loadedRuns.pop(key, None)
            self.workspaces.deleteWorkspaceUnconditional(self.rawWorkspace(*key))
