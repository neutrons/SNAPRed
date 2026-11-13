from snapred.meta.mantid.WorkspaceNameGenerator import NameBuilder, WorkspaceName
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng


class NeutronWorkspaceNames:
    """The ADS names of neutron-data workspaces."""

    @staticmethod
    def builder(runNumber: str, useLiteMode: bool) -> NameBuilder:
        runNameBuilder = wng.run().runNumber(runNumber)
        if useLiteMode:
            runNameBuilder.lite(wng.Lite.TRUE)
        return runNameBuilder

    @classmethod
    def plain(cls, runNumber: str, useLiteMode: bool) -> WorkspaceName:
        return cls.builder(runNumber, useLiteMode).build()

    @classmethod
    def raw(cls, runNumber: str, useLiteMode: bool) -> WorkspaceName:
        return cls.builder(runNumber, useLiteMode).auxiliary("Raw").build()

    @classmethod
    def copy(cls, runNumber: str, useLiteMode: bool, numCopies: int) -> WorkspaceName:
        return cls.builder(runNumber, useLiteMode).auxiliary(f"Copy{numCopies}").build()
