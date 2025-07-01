from snapred.backend.data import LocalDataService


state = "stateid"
useLiteMode = True

lds = LocalDataService()
calibrationIndexer = lds.calibrationIndexer(useLiteMode, state)

calibrationIndexer.recoverIndex()