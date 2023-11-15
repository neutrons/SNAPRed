from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng


def testRunNames():
    assert "tof_all_123" == wng.run().runNumber(123).build()
    assert "dsp_all_123" == wng.run().runNumber(123).unit(wng.Units.DSP).build()
    assert "dsp_column_123" == wng.run().runNumber(123).unit(wng.Units.DSP).group(wng.Groups.COLUMN).build()
    assert (
        "dsp_column_123_Test"
        == wng.run().runNumber(123).unit(wng.Units.DSP).group(wng.Groups.COLUMN).auxilary("Test").build()
    )


def testDiffCalInputNames():
    assert "_tof_123_raw" == wng.diffCalInput().runNumber(123).build()
    assert "_dsp_123_raw" == wng.diffCalInput().runNumber(123).unit(wng.Units.DSP).build()


def testDiffCalTableName():
    assert "_DIFC_123" == wng.diffCalTable().runNumber(123).build()
