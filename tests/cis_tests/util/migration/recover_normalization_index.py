

from snapred.backend.data.Indexer import Indexer, IndexerType
from snapred.backend.data.IndexStore import IndexStore
from snapred.backend.data.LocalDataService import LocalDataService


state = "stateid"
useLiteMode = True

lds = LocalDataService()
path = lds._constructNormalizationStatePath(state, useLiteMode)
normalizationIndexer = Indexer(IndexStore(IndexerType.NORMALIZATION, path, recoveryMode=True))

normalizationIndexer.recoverIndex(dryrun=False)

