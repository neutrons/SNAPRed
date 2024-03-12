import pytest
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng

runNumber = 123
fRunNumber = str(runNumber).zfill(6)
version = 1
fVersion = "v" + str(version).zfill(4)


def testRunNames():
    assert "tof_all_" + fRunNumber == wng.run().runNumber(runNumber).build()
    assert "dsp_all_" + fRunNumber == wng.run().runNumber(runNumber).unit(wng.Units.DSP).build()
    assert (
        "dsp_column_" + fRunNumber
        == wng.run().runNumber(runNumber).unit(wng.Units.DSP).group(wng.Groups.COLUMN).build()
    )
    assert (
        "dsp_column_test_" + fRunNumber
        == wng.run().runNumber(runNumber).unit(wng.Units.DSP).group(wng.Groups.COLUMN).auxiliary("Test").build()
    )


def testDiffCalInputNames():
    assert "_tof_" + fRunNumber + "_raw" == wng.diffCalInput().runNumber(runNumber).build()
    assert "_dsp_" + fRunNumber + "_raw" == wng.diffCalInput().runNumber(runNumber).unit(wng.Units.DSP).build()


def testDiffCalOutputName():
    assert f"_tof_diffoc_{fRunNumber}" == wng.diffCalOutput().runNumber(runNumber).build()
    assert f"_tof_diffoc_{fRunNumber}_{fVersion}" == wng.diffCalOutput().runNumber(runNumber).version(version).build()


def testDiffCalTableName():
    assert f"_diffract_consts_{fRunNumber}" == wng.diffCalTable().runNumber(runNumber).build()
    assert (
        f"_diffract_consts_{fRunNumber}_{fVersion}" == wng.diffCalTable().runNumber(runNumber).version(version).build()
    )


def testDiffCalMaskName():
    assert f"_diffract_consts_mask_{fRunNumber}" == wng.diffCalMask().runNumber(runNumber).build()
    assert (
        f"_diffract_consts_mask_{fRunNumber}_{fVersion}"
        == wng.diffCalMask().runNumber(runNumber).version(version).build()
    )


def testDiffCalMetricsNames():
    assert (
        f"_calib_metrics_strain_{fRunNumber}" == wng.diffCalMetric().runNumber(runNumber).metricName("strain").build()
    )
    assert (
        f"_calib_metrics_strain_{fRunNumber}_{fVersion}"
        == wng.diffCalMetric().metricName("strain").runNumber(runNumber).version(version).build()
    )


def testInvalidKey():
    with pytest.raises(RuntimeError, match=r"Key \[unit\] not a valid property"):
        wng.diffCalMask().runNumber(runNumber).unit(wng.Units.TOF).build()


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
