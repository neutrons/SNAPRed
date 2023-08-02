from .init import aII, localz


def test_pullCorrectVals():
    assert "dummy" in aII
    assert "restricted_dummy" in aII
    assert len(aII) == 2
    assert "dummy" in localz
    assert "restricted_dummy" in localz
    assert "super_secret_dummy" not in localz
    assert "myUnsecurePassword" not in localz
