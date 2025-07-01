

from snapred.backend.data import LocalDataService


state = "stateid"
useLiteMode = True

lds = LocalDataService()
normalizationIndexer = lds.normalizationIndexer(useLiteMode, state)

normalizationIndexer.recoverIndex()

