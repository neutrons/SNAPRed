import logging

# Test-related imports go last:
from unittest import mock

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


def test_RecoverableException_log_stacktrace(caplog):
    with (
        caplog.at_level(logging.DEBUG, logger=RecoverableException.__module__),
        mock.patch("snapred.backend.error.RecoverableException.extractTrueStacktrace") as mockStackTrace,
    ):
        mockStackTrace.return_value = "my little chickadee"
        try:
            try:
                # Ensure that there actually is a stack trace
                raise RuntimeError("...")
            except RuntimeError:
                raise RecoverableException("lah dee dah")
        except RecoverableException:
            pass
    assert mockStackTrace.return_value in caplog.text


def test_RecoverableException_no_log_stacktrace(caplog):
    with (
        caplog.at_level(logging.INFO, logger=RecoverableException.__module__),
        mock.patch("snapred.backend.error.RecoverableException.extractTrueStacktrace") as mockStackTrace,
    ):
        mockStackTrace.return_value = "my little chickadee"
        try:
            try:
                # Ensure that there actually is a stack trace
                raise RuntimeError("...")
            except RuntimeError:
                raise RecoverableException("lah dee dah")
        except RecoverableException:
            pass
    assert mockStackTrace.return_value not in caplog.text
