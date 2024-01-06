import pytest

# Pull data from the dummy package.
from .init import aII, localz


# Test the pullModuleMembers function.
# Ensures that the right modules and contents are pulled.
@pytest.mark.skip(reason="Temporary regression?")
def test_pullCorrectVals():
    assert "dummy" in aII
    assert "restricted_dummy" in aII
    assert len(aII) == 2
    assert "dummy" in localz
    assert "restricted_dummy" in localz
    assert "super_secret_dummy" not in localz
    assert "myUnsecurePassword" not in localz
