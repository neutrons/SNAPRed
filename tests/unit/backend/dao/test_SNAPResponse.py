import pytest
from pydantic import ValidationError

from snapred.backend.dao.SNAPResponse import ResponseCode, SNAPResponse


def test_enum():
    assert ResponseCode.OK == 200
    assert ResponseCode.ERROR == 500


def test_constructors():
    assert SNAPResponse(code=200).code == ResponseCode.OK
    assert SNAPResponse(code=500).code == ResponseCode.ERROR

    # cannot be a teapot
    with pytest.raises(ValidationError):
        SNAPResponse(code=418)
