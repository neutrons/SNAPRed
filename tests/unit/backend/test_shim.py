from mantid.kernel import ConfigService
from snapred.backend.shims import amend_mantid_config


def test_amend_config():
    config = ConfigService.Instance()
    old_instrument = config["instrumentName"]
    with amend_mantid_config({"instrumentName": "42"}):
        assert config["instrumentName"] == "42"
    assert config["instrumentName"] == old_instrument
