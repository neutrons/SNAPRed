import pytest
from snapred.backend.dao.indexing.Versioning import VERSION_DEFAULT, VERSION_DEFAULT_NAME
from snapred.meta.mantid.WorkspaceNameGenerator import ValueFormatter as wnvf
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng

runNumber = 123
fRunNumber = str(runNumber).zfill(6)
version = 1
fVersion = "v" + str(version).zfill(4)
stateId = "ab8704b0bc2a2342"
fStateId = "bc2a2342"


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
    assert f"_tof_unfoc_{fRunNumber}" == wng.diffCalOutput().runNumber(runNumber).unit(wng.Units.TOF).build()
    assert (
        f"_tof_unfoc_{fRunNumber}_{fVersion}"
        == wng.diffCalOutput().runNumber(runNumber).unit(wng.Units.TOF).version(version).build()
    )


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


def testReductionOutputName():
    assert (  # "reduced_data,{unit},{group},{runNumber},{version}"
        f"_reduced_dsp_lahdeedah_{fRunNumber}" == wng.reductionOutput().group("lahDeeDah").runNumber(runNumber).build()
    )
    assert (
        f"_reduced_dsp_column_{fRunNumber}_{fVersion}"
        == wng.reductionOutput().group(wng.Groups.COLUMN).runNumber(runNumber).version(version).build()
    )
    assert (
        f"_reduced_tof_column_{fRunNumber}_{fVersion}"
        == wng.reductionOutput()
        .unit(wng.Units.TOF)
        .group(wng.Groups.COLUMN)
        .runNumber(runNumber)
        .version(version)
        .build()
    )


def testReductionOutputGroupName():
    assert (  # "reduced_data,{stateId},{version}"
        f"_reduced_{fStateId}" == wng.reductionOutputGroup().stateId(stateId).build()
    )
    assert f"_reduced_{fStateId}_{fVersion}" == wng.reductionOutputGroup().stateId(stateId).version(version).build()


def testInvalidKey():
    with pytest.raises(RuntimeError, match=r"Key \[unit\] not a valid property"):
        wng.diffCalMask().runNumber(runNumber).unit(wng.Units.TOF).build()


def test_formatVersion_use_v_prefix():
    expected = "v"
    ans = wnvf.formatVersion(None, use_v_prefix=True)
    assert ans == expected


def test_formatVersion_none():
    expected = ""
    ans = wnvf.formatVersion(None, use_v_prefix=False)
    assert ans == expected


def test_formatVersion_default():
    expected = VERSION_DEFAULT_NAME
    ans = wnvf.formatVersion(VERSION_DEFAULT, use_v_prefix=False)
    assert ans == expected


def test_formatVersion_str_to_int():
    expected = "0001"
    ans = wnvf.formatVersion("1", use_v_prefix=False)
    assert ans == expected


def test_formatVersion_int():
    expected = "0001"
    ans = wnvf.formatVersion(1, use_v_prefix=False)
    assert ans == expected


def test_formatVersion_nonsense():
    expected = ""
    ans = wnvf.formatVersion("matt", use_v_prefix=False)
    assert ans == expected
