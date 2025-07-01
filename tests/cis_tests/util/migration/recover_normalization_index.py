

from snapred.backend.data.Indexer import Indexer, IndexerType
from snapred.backend.data.LocalDataService import LocalDataService


state = "stateid"
useLiteMode = True

lds = LocalDataService()
path = lds._constructNormalizationStatePath(state, useLiteMode)
normalizationIndexer = Indexer(indexerType=IndexerType.NORMALIZATION, directory=path, recoveryMode=True)

normalizationIndexer.recoverIndex(dryrun=False)

