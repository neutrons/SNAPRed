
from snapred.backend.data.Indexer import Indexer, IndexerType
from snapred.backend.data.IndexStore import IndexStore
from snapred.backend.data.LocalDataService import LocalDataService


state = "04bd2c53f6bf6754"
useLiteMode = True

lds = LocalDataService()
path = lds._constructCalibrationStatePath(state, useLiteMode)
calibrationIndexer = Indexer(IndexStore(IndexerType.CALIBRATION, path, recoveryMode=True))

calibrationIndexer.recoverIndex(dryrun=False)

