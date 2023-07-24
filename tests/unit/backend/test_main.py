import pytest
from snapred.__main__ import main


@pytest.mark.parametrize("option", ["-h", "--help", "-v", "--version"])
def test_simple(option):
    with pytest.raises(SystemExit):
        main([option])
