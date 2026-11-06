from util.Config_helpers import Config_override

from snapred.backend.data.IndexerType import IndexerType, indexFilename


def test_indexFilename():
    assert indexFilename(IndexerType.CALIBRATION) == "CalibrationIndex.json"
    assert indexFilename(IndexerType.INSTRUMENT_PARAMETER) == "InstrumentParameterIndex.json"


def test_indexFilename_from_config():
    with Config_override("indexer.filename", "{indexerType}.index.json"):
        assert indexFilename(IndexerType.NORMALIZATION) == "Normalization.index.json"
