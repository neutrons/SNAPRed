import pytest
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng


def testRunNames():
    assert "tof_all_123" == wng.run().runNumber(123).build()
    assert "dsp_all_123" == wng.run().runNumber(123).unit(wng.Units.DSP).build()
    assert "dsp_column_123" == wng.run().runNumber(123).unit(wng.Units.DSP).group(wng.Groups.COLUMN).build()
    assert (
        "dsp_column_123_test"
        == wng.run().runNumber(123).unit(wng.Units.DSP).group(wng.Groups.COLUMN).auxiliary("Test").build()
    )


def testDiffCalInputNames():
    assert "_tof_123_raw" == wng.diffCalInput().runNumber(123).build()
    assert "_dsp_123_raw" == wng.diffCalInput().runNumber(123).unit(wng.Units.DSP).build()


def testDiffCalTableNames():
    assert "_difc_123" == wng.diffCalTable().runNumber(123).build()
    assert "_difc_123_24" == wng.diffCalTable().runNumber(123).version(24).build()


def testDiffCalOutputNames():
    assert "_tof_123_diffoc" == wng.diffCalOutput().runNumber(123).build()
    assert "_dsp_123_diffoc" == wng.diffCalOutput().runNumber(123).unit(wng.Units.DSP).build()
    assert "_dsp_123_25_diffoc" == wng.diffCalOutput().runNumber(123).unit(wng.Units.DSP).version(25).build()


def testDiffCalMaskNames():
    assert "_difc_123_mask" == wng.diffCalMask().runNumber(123).build()
    assert "_difc_123_26_mask" == wng.diffCalMask().runNumber(123).version(26).build()


def testDiffCalMetricsNames():
    assert "123_calibration_metrics_strain" == wng.diffCalMetrics().runNumber(123).metricName("strain").build()
    assert (
        "123_1_calibration_metrics_strain"
        == wng.diffCalMetrics().runNumber(123).version(1).metricName("strain").build()
    )


# this at teardown removes the loggers, eliminating logger error printouts
# see https://github.com/pytest-dev/pytest/issues/5502#issuecomment-647157873
@pytest.fixture(autouse=True)
def clear_loggers():  # noqa: PT004
    """Remove handlers from all loggers"""
    import logging

    yield  # ... teardown follows:
    loggers = [logging.getLogger()] + list(logging.Logger.manager.loggerDict.values())
    for logger in loggers:
        handlers = getattr(logger, "handlers", [])
        for handler in handlers:
            logger.removeHandler(handler)
