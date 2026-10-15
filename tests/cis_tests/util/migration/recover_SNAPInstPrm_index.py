
from pathlib import Path

from snapred.backend.data.Indexer import Indexer, IndexerType
from snapred.backend.data.IndexStore import IndexStore
from snapred.meta.Config import Config


snapInstPrmIndexer = Indexer(IndexStore(IndexerType.INSTRUMENT_PARAMETER, Path(Config["instrument.parameters.home"]), recoveryMode=True))

snapInstPrmIndexer.recoverIndex(dryrun=False)

