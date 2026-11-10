from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.Resolution import Resolution
from snapred.backend.data.source.SourcedData import SourcedData


def test_dataSource_is_the_provenance_chain():
    data = SourcedData(
        workspace="ws", loader="L", provenance=[DataSourceKind.NEXUS_FILE, DataSourceKind.LITE_CONVERSION]
    )
    assert data.dataSource == "nexusFile>liteConversion"


def test_provenance_round_trip():
    provenance = [DataSourceKind.LIVE, DataSourceKind.LITE_CONVERSION]
    data = SourcedData(workspace="ws", loader="L", provenance=provenance)
    assert SourcedData.provenanceFrom(data.dataSource) == provenance


def test_resolution():
    assert Resolution.fromLiteMode(True) is Resolution.LITE
    assert Resolution.fromLiteMode(False) is Resolution.NATIVE
    assert Resolution.LITE.useLiteMode
    assert not Resolution.NATIVE.useLiteMode
