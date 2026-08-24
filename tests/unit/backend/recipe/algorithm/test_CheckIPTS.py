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
    """`Instrument=SNAP` must be settable, and must survive validation."""
    algorithm.setProperty("RunNumber", 12345)
    algorithm.setProperty("Instrument", Config["instrument.name"])

    assert algorithm.getProperty("Instrument").value == Config["instrument.name"]
    assert algorithm.validateInputs() == {}


##
## OPTIONAL 'Facility' ARGUMENT
##
## An instrument name is only meaningful with respect to a facility, so anywhere SNAPRed passes an
##   instrument it should be able to pass a facility too.
##


def test_instrumentPropertyHasNoFrozenValidator(algorithm):
    """'Instrument' must not carry a `StringListValidator`.

    Such a validator is built once at initialization and never re-evaluated, so it could not account
      for the 'Facility' property -- property values are only set *after* initialization.  The check
      lives in `validateInputs` instead.
    """
    assert not list(algorithm.getProperty("Instrument").allowedValues)


def test_facilityDefaultsToTheORNLFacilities(algorithm):
    """An empty 'Facility' preserves the previous behavior: search SNS and HFIR."""
    assert algorithm.getProperty("Facility").isDefault

    instruments = algorithm.getValidInstruments("")
    for facilityName in CheckIPTS.DEFAULT_FACILITIES:
        expected = {i.shortName() for i in ConfigService.getFacility(facilityName).instruments()} - {"DAS"}
        assert expected <= set(instruments)


def test_namedFacilityNarrowsTheInstrumentList(algorithm):
    snsOnly = algorithm.getValidInstruments("SNS")
    hfirOnly = algorithm.getValidInstruments("HFIR")

    assert Config["instrument.name"] in snsOnly
    assert Config["instrument.name"] not in hfirOnly


def test_validateInputsAcceptsTheSNAPRedInstrumentAndFacility(algorithm):
    algorithm.setProperty("RunNumber", 12345)
    algorithm.setProperty("Instrument", Config["instrument.name"])
    algorithm.setProperty("Facility", Config["facility.name"])

    assert algorithm.validateInputs() == {}


def test_validateInputsAcceptsAnEmptyInstrument(algorithm):
    """Empty means "let `FileFinder` resolve the bare run number", so it must remain valid."""
    algorithm.setProperty("RunNumber", 12345)

    assert algorithm.validateInputs() == {}


def test_validateInputsRejectsAnInstrumentOutsideTheNamedFacility(algorithm):
    algorithm.setProperty("RunNumber", 12345)
    algorithm.setProperty("Instrument", Config["instrument.name"])
    algorithm.setProperty("Facility", "HFIR")

    errors = algorithm.validateInputs()

    assert "Instrument" in errors
    assert "is not part of facility 'HFIR'" in errors["Instrument"]


def test_validateInputsRejectsAnUnknownFacility(algorithm):
    algorithm.setProperty("RunNumber", 12345)
    algorithm.setProperty("Instrument", Config["instrument.name"])
    algorithm.setProperty("Facility", "NOT_A_FACILITY")

    errors = algorithm.validateInputs()

    assert "Facility" in errors
    assert "is not known to Mantid" in errors["Facility"]


def test_validationDoesNotDependOnTheMantidDefaultFacility(algorithm):
    """The regression: SNAP must validate while the Mantid default facility is something else."""
    mantidConfig = ConfigService.Instance()
    saved = (mantidConfig["default.facility"], mantidConfig["default.instrument"])
    try:
        mantidConfig.setString("default.facility", "ILL")
        mantidConfig.setString("default.instrument", "IN5")

        algorithm.setProperty("RunNumber", 12345)
        algorithm.setProperty("Instrument", Config["instrument.name"])
        algorithm.setProperty("Facility", Config["facility.name"])

        assert algorithm.validateInputs() == {}
    finally:
        mantidConfig.setString("default.facility", saved[0])
        mantidConfig.setString("default.instrument", saved[1])
