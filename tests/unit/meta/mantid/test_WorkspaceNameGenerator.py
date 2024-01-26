from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng


def testRunNames():
    assert "tof_all_123" == wng.run().runNumber(123).build()
    assert "dsp_all_123" == wng.run().runNumber(123).unit(wng.Units.DSP).build()
    assert "dsp_column_123" == wng.run().runNumber(123).unit(wng.Units.DSP).group(wng.Groups.COLUMN).build()
    # assert (
    #     "dsp_column_123_test"
    #     == wng.run().runNumber(123).unit(wng.Units.DSP).group(wng.Groups.COLUMN).auxilary("Test").build()
    assert (
        "dsp_column_test_123"
        == wng.run().runNumber(123).unit(wng.Units.DSP).group(wng.Groups.COLUMN).auxilary("Test").build()
    )


def testDiffCalInputNames():
    assert "_tof_123_raw" == wng.diffCalInput().runNumber(123).build()
    assert "_dsp_123_raw" == wng.diffCalInput().runNumber(123).unit(wng.Units.DSP).build()


def testDiffCalTableName():
    assert "_diffract_consts_123" == wng.diffCalTable().runNumber(123).build()


def testDiffCalMetricsName():
    assert "_calib_metrics_strain_123_1" == wng.diffCalMetrics().metricName("strain").runNumber(123).version(1).build()


def testParseRun():
    assert wng.parseRun("tof_all_lite_sometext_123")["auxilary"] == "sometext"
