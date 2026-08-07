import mantid.simpleapi  # noqa: F401  (import registers algorithms with the `AlgorithmFactory`)
import pytest
from mantid.kernel import ConfigService

from snapred.backend.recipe.algorithm.CheckIPTS import CheckIPTS
from snapred.meta.Config import Config


@pytest.fixture
def algorithm():
    algo = CheckIPTS()
    algo.initialize()
    return algo


def test_getValidInstrumentsIncludesTheSNAPRedInstrument(algorithm):
    """The instrument SNAPRed actually uses must be an allowed value.

    Note that `getValidInstruments` queries "SNS" and "HFIR" *by name*, so unlike Mantid's live-data
      algorithms this list does not depend on the user's default facility.
    """
    assert Config["instrument.name"] in algorithm.getValidInstruments()


def test_getValidInstrumentsExcludesDAS(algorithm):
    """ "DAS" is not a real instrument and must be filtered out.

    Regression test: the exclusion previously compared an `InstrumentInfo` against the string "DAS",
      which is never equal, so the filter silently did nothing.  "DAS" matters here because it is the
      first instrument in the SNS facility, and so is what `ConfigService.setFacility("SNS")` selects
      as the default instrument.
    """
    assert "DAS" not in algorithm.getValidInstruments()


def test_getValidInstrumentsAllowsEmptyString(algorithm):
    """An empty `Instrument` is documented as "uses default instrument", so it must stay allowed."""
    assert "" in algorithm.getValidInstruments()


def test_getValidInstrumentsCoversBothFacilities(algorithm):
    """Both SNS and HFIR instruments are included, independent of the default facility."""
    instruments = algorithm.getValidInstruments()

    for facilityName in ("SNS", "HFIR"):
        facility = ConfigService.getFacility(facilityName)
        expected = {item.shortName() for item in facility.instruments() if item.shortName() != "DAS"}
        assert expected <= set(instruments)


def test_instrumentPropertyAcceptsTheSNAPRedInstrument(algorithm):
    """End-to-end on the property itself: `Instrument=SNAP` must be settable."""
    algorithm.setProperty("Instrument", Config["instrument.name"])

    assert algorithm.getProperty("Instrument").value == Config["instrument.name"]
