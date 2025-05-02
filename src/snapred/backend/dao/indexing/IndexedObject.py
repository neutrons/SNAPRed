from snapred.backend.dao.indexing.IndexEntry import IndexEntry
from snapred.backend.dao.indexing.VersionedObject import VersionedObject


class IndexedObject(VersionedObject):
    indexEntry: IndexEntry
