from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng


def testRunNames():
    assert "TOFAll123" == wng.run().runNumber(123).build()
    assert "DSPAll123" == wng.run().runNumber(123).unit(wng.Units.DSP).build()
    assert "DSPColumn123" == wng.run().runNumber(123).unit(wng.Units.DSP).group(wng.Groups.COLUMN).build()
    assert (
        "DSPColumnTest123"
        == wng.run().runNumber(123).unit(wng.Units.DSP).group(wng.Groups.COLUMN).auxilary("Test").build()
    )


def testDiffCalInputNames():
    assert "_TOF_123_raw" == wng.diffCalInput().runNumber(123).build()
    assert "_DSP_123_raw" == wng.diffCalInput().runNumber(123).unit(wng.Units.DSP).build()


def testDiffCalTableName():
    assert "_DIFC_123" == wng.diffCalTable().runNumber(123).build()
