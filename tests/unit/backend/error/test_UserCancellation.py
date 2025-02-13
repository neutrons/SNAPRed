# Test-related imports go last:

from snapred.backend.error.UserCancellation import UserCancellation


def test_UserCancellation():
    msg = "User cancellation request"
    e = UserCancellation(msg)
    assert e.message == msg


def test_UserCancellation_parse_raw():
    msg = "User cancellation request"
    raw = f'{{"message": "{msg}"}}'
    e = UserCancellation.parse_raw(raw)
    assert e.message == msg
    assert isinstance(e, UserCancellation)
