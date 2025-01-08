import time
import typing
from datetime import datetime

import pytest
from pydantic import BaseModel, ConfigDict

from snapred.meta.Config import Config
from snapred.meta.mantid.WorkspaceNameGenerator import (
    ValueFormatter as wnvf,
)
from snapred.meta.mantid.WorkspaceNameGenerator import (
    VersionState,
    WorkspaceName,
)
from snapred.meta.mantid.WorkspaceNameGenerator import (
    WorkspaceNameGenerator as wng,
)

runNumber = 123
fRunNumber = str(runNumber).zfill(6)
version = 1
fVersion = "v" + str(version).zfill(4)

stateId = "ab8704b0bc2a2342"
fStateId = "ab8704b0"

timestamp = time.time()
timestampFormat = Config["mantid.workspace.nameTemplate.formatter.timestamp.workspace"]
fTimestamp = (
    "ts" + str(int(round(timestamp * 1000.0)))
    if "{timestamp}" in timestampFormat
    else datetime.fromtimestamp(timestamp).strftime(timestampFormat)
)


def testRunNames():
    assert "_tof_all_" + fRunNumber == wng.run().runNumber(runNumber).build()
    assert "_dsp_all_" + fRunNumber == wng.run().runNumber(runNumber).unit(wng.Units.DSP).build()
    assert (
        "_dsp_column_" + fRunNumber
        == wng.run().runNumber(runNumber).unit(wng.Units.DSP).group(wng.Groups.COLUMN).build()
    )
    assert (
        "_dsp_column_test_" + fRunNumber
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


def testNormCalNames():
    # raw: optional version
    assert f"_tof_unfoc_raw_van_corr_{fRunNumber}" == wng.rawVanadium().unit(wng.Units.TOF).runNumber(runNumber).build()
    # raw: with version
    assert (
        f"_tof_unfoc_raw_van_corr_{fRunNumber}_{fVersion}"
        == wng.rawVanadium().unit(wng.Units.TOF).runNumber(runNumber).version(version).build()
    )
    # focused
    assert (
        f"_dsp_bank_raw_van_corr_{fRunNumber}_{fVersion}"
        == wng.focusedRawVanadium().unit(wng.Units.DSP).group("bank").runNumber(runNumber).version(version).build()
    )
    # focused and smoothed
    assert (
        f"_dsp_bank_fitted_van_corr_{fRunNumber}_{fVersion}"
        == wng.smoothedFocusedRawVanadium()
        .unit(wng.Units.DSP)
        .group("bank")
        .runNumber(runNumber)
        .version(version)
        .build()
    )


def testReductionOutputName():
    assert (  # "reduced_data,{unit},{runNumber},{timestamp}"
        f"_reduced_dsp_lahdeedah_{fRunNumber}" == wng.reductionOutput().group("lahdeedah").runNumber(runNumber).build()
    )
    assert (
        f"_reduced_dsp_lahdeedah_{fRunNumber}_{fTimestamp}"
        == wng.reductionOutput().group("lahdeedah").runNumber(runNumber).timestamp(timestamp).build()
    )
    assert (
        f"_reduced_tof_lahdeedah_{fRunNumber}_{fTimestamp}"
        == wng.reductionOutput()
        .unit(wng.Units.TOF)
        .group("lahdeedah")
        .runNumber(runNumber)
        .timestamp(timestamp)
        .build()
    )


def testReductionOutputGroupName():
    assert (  # "reduced_data,{stateId},{timestamp}"
        f"_reduced_{fRunNumber}" == wng.reductionOutputGroup().runNumber(runNumber).build()
    )
    assert (
        f"_reduced_{fRunNumber}_{fTimestamp}"
        == wng.reductionOutputGroup().runNumber(runNumber).timestamp(timestamp).build()
    )


def testReductionPixelMaskName():
    assert (  # "reduction_pixelmask,{runNumber},{timestamp}"
        f"_pixelmask_{fRunNumber}_{fTimestamp}"
        == wng.reductionPixelMask().runNumber(runNumber).timestamp(timestamp).build()
    )


def testUserPixelMaskName():
    numberTag = 1
    # <number tag> is only included when > 1
    assert (  # "reduction_pixelmask,{runNumber},{version}"
        "MaskWorkspace" == wng.reductionUserPixelMask().numberTag(numberTag).build()
    )
    numberTag = 2
    assert f"MaskWorkspace_{numberTag}" == wng.reductionUserPixelMask().numberTag(numberTag).build()


def testWorkspaceName_with_BaseModel():
    # Test that complete `WorkspaceName` are created during model construction.
    class ModelWithName(BaseModel):
        name: WorkspaceName

        model_config = ConfigDict(arbitrary_types_allowed=True)

    name = wng.reductionOutput().runNumber("123456").group("bank").timestamp(time.time()).build()
    test1 = ModelWithName(name=name)
    test2 = ModelWithName.model_validate({"name": name})
    test3 = ModelWithName.model_construct(name=name)
    test4 = test3.model_copy()
    test5 = ModelWithName(name="joe")

    # positive tests
    assert test1.name._builder is not None
    assert test2.name._builder is not None
    assert test3.name._builder is not None
    assert test4.name._builder is not None

    # negative test
    assert test5.name._builder is None


def testWorkspaceNameTokens():
    runNumber = "456789"
    unit = wng.Units.DSP
    group = wng.Groups.COLUMN
    testName = wng.run().runNumber(runNumber).unit(unit).group(group).build()
    runNumber_, unit_, group_ = testName.tokens("runNumber", "unit", "group")
    assert runNumber_ == runNumber
    assert unit_ == unit.lower()
    assert group_ == group.lower()


def testWorkspaceNameToString():
    runNumber = "456789"
    unit = wng.Units.DSP
    group = wng.Groups.COLUMN
    testName = wng.run().runNumber(runNumber).unit(unit).group(group).build()
    # WARNING: `WorkspaceName` is an `Annotated` type.
    # TODO: this shouldn't be so cryptic => FIX THIS!
    assert isinstance(testName, typing.get_args(WorkspaceName)[0])
    assert isinstance(testName.toString(), str)


def testWorkspaceNameTokens_bad_key():
    runNumber = "456789"
    unit = wng.Units.DSP
    group = wng.Groups.COLUMN
    testName = wng.run().runNumber(runNumber).unit(unit).group(group).build()
    with pytest.raises(RuntimeError, match=r".*not a valid property for workspace-type.*"):
        runNumber, useLiteMode = testName.tokens("runNumber", "useLiteMode")


def testWorkspaceNameTokens_incomplete():
    runNumber = "456789"
    unit = wng.Units.DSP
    group = wng.Groups.COLUMN
    testName = wng.run().runNumber(runNumber).unit(unit).group(group).build()
    testName._builder = None
    with pytest.raises(RuntimeError, match=r".*no '_builder' attribute is retained.*"):
        runNumber, unit = testName.tokens("runNumber", "unit")


def testNumberTagFormat():
    fmt = wnvf.numberTagFormat
    number = 24
    assert wnvf.formatNumberTag(number) == fmt.WORKSPACE.format(number=number)
    assert wnvf.pathNumberTag(number) == fmt.PATH.format(number=number)

    # numberTag == 1 => omit
    number = 1
    assert wnvf.formatNumberTag(number) == ""
    assert wnvf.pathNumberTag(number) == ""


def testNumberTagFormatConfig():
    fmt = wnvf.numberTagFormat
    assert fmt.WORKSPACE == Config["mantid.workspace.nameTemplate.formatter.numberTag.workspace"]
    assert fmt.PATH == Config["mantid.workspace.nameTemplate.formatter.numberTag.path"]


def testRunNumberFormat():
    fmt = wnvf.runNumberFormat
    runNumber = "012345"
    assert wnvf.formatRunNumber(runNumber) == fmt.WORKSPACE.format(runNumber=runNumber)
    assert wnvf.pathRunNumber(runNumber) == fmt.PATH.format(runNumber=runNumber)


def testRunNumberFormatConfig():
    fmt = wnvf.runNumberFormat
    assert fmt.WORKSPACE == Config["mantid.workspace.nameTemplate.formatter.runNumber.workspace"]
    assert fmt.PATH == Config["mantid.workspace.nameTemplate.formatter.runNumber.path"]


def testStateIdFormat():
    testHash = "abcdef0123456798"
    assert wnvf.formatStateId(testHash) == testHash[0:8]
    assert wnvf.pathStateId(testHash) == testHash


def testTimestampFormat():
    fmt = wnvf.timestampFormat
    timestamp = time.time()
    assert wnvf.formatTimestamp(timestamp) == datetime.fromtimestamp(timestamp).strftime(fmt.WORKSPACE)
    assert wnvf.pathTimestamp(timestamp) == datetime.fromtimestamp(timestamp).strftime(fmt.PATH)


def testTimestampFormatLegacy():
    # key 'timestamp' in format string => legacy ms format
    fmt = "ts_{timestamp}"
    timestamp = time.time()
    time_ms = int(round(timestamp * 1000.0))
    assert wnvf.formatTimestamp(timestamp, fmt) == fmt.format(timestamp=time_ms)


def testTimestampFormatConfig():
    fmt = wnvf.timestampFormat
    assert fmt.WORKSPACE == Config["mantid.workspace.nameTemplate.formatter.timestamp.workspace"]
    assert fmt.PATH == Config["mantid.workspace.nameTemplate.formatter.timestamp.path"]


def testInvalidKey():
    with pytest.raises(RuntimeError, match=r"Key 'unit' not a valid property for workspace-type"):
        wng.diffCalMask().runNumber(runNumber).unit(wng.Units.TOF).build()


def test_formatVersion_none():
    expected = ""
    ans = wnvf.formatVersion(None)
    assert ans == expected


def test_pathVersion_none():
    expected = ""
    ans = wnvf.pathVersion(None)
    assert ans == expected


def test_pathversion_default():
    expected = f"v_{VersionState.DEFAULT}"
    ans = wnvf.pathVersion(VersionState.DEFAULT)
    assert ans == expected


def test_pathVersion_str_to_int():
    expected = "v_0001"
    ans = wnvf.pathVersion("1")
    assert ans == expected


def test_pathVersion_int():
    expected = "v_0001"
    ans = wnvf.pathVersion(1)
    assert ans == expected


def test_formatVersion_nonsense():
    expected = ""
    ans = wnvf.formatVersion("matt")
    assert ans == expected
