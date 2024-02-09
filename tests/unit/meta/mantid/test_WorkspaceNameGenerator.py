from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng


def testRunNames():
    runNumber = 123
    fRunNumber = str(runNumber).zfill(6)
    assert "tof_all_" + fRunNumber == wng.run().runNumber(runNumber).build()
    assert "dsp_all_" + fRunNumber == wng.run().runNumber(runNumber).unit(wng.Units.DSP).build()
    assert (
        "dsp_column_" + fRunNumber
        == wng.run().runNumber(runNumber).unit(wng.Units.DSP).group(wng.Groups.COLUMN).build()
    )
    assert (
        "dsp_column_test_" + fRunNumber
        == wng.run().runNumber(runNumber).unit(wng.Units.DSP).group(wng.Groups.COLUMN).auxilary("Test").build()
    )


def testDiffCalInputNames():
    runNumber = 123
    fRunNumber = str(runNumber).zfill(6)
    assert "_tof_" + fRunNumber + "_raw" == wng.diffCalInput().runNumber(runNumber).build()
    assert "_dsp_" + fRunNumber + "_raw" == wng.diffCalInput().runNumber(runNumber).unit(wng.Units.DSP).build()


def testDiffCalTableName():
    runNumber = 123
    fRunNumber = str(runNumber).zfill(6)
    assert "_diffract_consts_" + fRunNumber == wng.diffCalTable().runNumber(runNumber).build()


def testDiffCalMetricsName():
    runNumber = 123
    fRunNumber = str(runNumber).zfill(6)
    version = 1
    fVersion = "v" + str(version).zfill(4)
    assert (
        "_calib_metrics_strain_" + fRunNumber + "_" + fVersion
        == wng.diffCalMetric().metricName("strain").runNumber(runNumber).version(version).build()
    )
