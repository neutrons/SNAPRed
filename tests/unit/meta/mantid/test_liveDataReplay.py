import pytest
from mantid.kernel import ConfigService
from util.Config_helpers import Config_override

from snapred.meta.mantid import liveDataReplay


@pytest.fixture
def mantidConfig():
    # Restore Mantid's replay-listener settings.
    saved = {key: ConfigService.getString(key) for key in (liveDataReplay.FILENAME_KEY, liveDataReplay.CHUNKS_KEY)}
    yield ConfigService
    for key, value in saved.items():
        ConfigService.setString(key, value)


@pytest.mark.parametrize(
    ("facility", "instrument", "expected"),
    [
        ("TEST_LIVE", "ADARA_FileReader", True),
        ("SNS", "SNAP", False),
        ("TEST_LIVE", "SNAP", False),
    ],
)
def test_usingFileReplay(facility, instrument, expected):
    with Config_override("liveData.facility.name", facility), Config_override("liveData.instrument.name", instrument):
        assert liveDataReplay.usingFileReplay() == expected


def test_replayFile(mantidConfig, tmp_path):
    nexus = tmp_path / "SNAP_12345.nxs.h5"
    nexus.write_bytes(b"")
    mantidConfig.setString(liveDataReplay.FILENAME_KEY, str(nexus))
    assert liveDataReplay.replayFile() == nexus


@pytest.mark.parametrize("filename", ["", "/does/not/exist/SNAP_12345.nxs.h5"])
def test_replayFile_not_set_or_not_found(mantidConfig, filename):
    mantidConfig.setString(liveDataReplay.FILENAME_KEY, filename)
    assert liveDataReplay.replayFile() is None


@pytest.mark.parametrize(("chunks", "expected"), [("3", 3), ("1", 1), ("0", 1), ("", 1), ("x", 1)])
def test_replayChunks(mantidConfig, chunks, expected):
    mantidConfig.setString(liveDataReplay.CHUNKS_KEY, chunks)
    assert liveDataReplay.replayChunks() == expected
