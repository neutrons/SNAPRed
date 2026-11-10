from dataclasses import dataclass, field
from typing import List, Optional

from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName

PROVENANCE_SEPARATOR = ">"


@dataclass
class SourcedData:
    """
    A workspace produced by a data source.

    `provenance` is the chain of sources that produced it, from the original source to the final one.
    `inputData` is the data that a transforming source was given, if any.
    """

    workspace: WorkspaceName
    loader: str
    provenance: List[DataSourceKind] = field(default_factory=list)
    inputData: Optional["SourcedData"] = None

    @property
    def dataSource(self) -> str:
        """The provenance, as it is recorded in the workspace metadata."""
        return PROVENANCE_SEPARATOR.join(str(kind) for kind in self.provenance)

    @staticmethod
    def provenanceFrom(dataSource: str) -> List[DataSourceKind]:
        """The provenance recorded in workspace metadata, as a list of kinds."""
        return [DataSourceKind(kind) for kind in dataSource.split(PROVENANCE_SEPARATOR)]
