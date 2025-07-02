
from pathlib import Path

from snapred.backend.data.Indexer import Indexer, IndexerType
from snapred.meta.Config import Config


snapInstPrmIndexer = Indexer(indexerType=IndexerType.INSTRUMENT_PARAMETER, directory=Path(Config["instrument.parameters.home"]), recoveryMode=True)

snapInstPrmIndexer.recoverIndex(dryrun=False)

