from snapred.backend.error.RecoverableException import RecoverableException


def test_RecoverableException():
    inst = RecoverableException("Recoverable exception")
    assert inst.message == "Recoverable exception"
    assert inst.flags == RecoverableException.Type.UNSET


def test_RecoverableException_parse_raw():
    raw = '{"message": "Recoverable exception", "flags": 0, "data": 1 }'
    inst = RecoverableException.parse_raw(raw)
    assert inst.message == "Recoverable exception"
    assert inst.flags == RecoverableException.Type.UNSET
    assert inst.data == 1
    assert isinstance(inst, RecoverableException)
