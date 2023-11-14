from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng


def testRunNames():
    assert "TOFAll123" == wng.run(123)
    assert "DSPAll123" == wng.run(123, unit=wng.Units.DSP)
    assert "DSPColumn123" == wng.run(123, unit=wng.Units.DSP, group=wng.Groups.COLUMN)
    assert "DSPColumnTest123" == wng.run(123, unit=wng.Units.DSP, auxilary="Test", group=wng.Groups.COLUMN)


def testDiffCalInputNames():
    assert "_TOF_123_raw" == wng.diffCalInput(123)
    assert "_DSP_123_raw" == wng.diffCalInput(123, unit=wng.Units.DSP)


def testDiffCalTableName():
    assert "_DIFC_123" == wng.diffCalTable(123)
