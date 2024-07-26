from snapred.backend.error.ContinueWarning import ContinueWarning


def test_ContinueWarning():
    inst = ContinueWarning("Continue with warning")
    assert inst.message == "Continue with warning"
    assert inst.flags == ContinueWarning.Type.UNSET


def test_ContinueWarning_parse_raw():
    raw = '{"message": "Continue with warning", "flags": 0 }'
    inst = ContinueWarning.parse_raw(raw)
    assert inst.message == "Continue with warning"
    assert inst.flags == ContinueWarning.Type.UNSET
    assert isinstance(inst, ContinueWarning)


def test_ContinueWarning_multiFlags():
    inst = ContinueWarning(
        "Continue with warning",
        ContinueWarning.Type.MISSING_DIFFRACTION_CALIBRATION | ContinueWarning.Type.MISSING_NORMALIZATION,
    )
    assert inst.message == "Continue with warning"
    assert (
        inst.flags == ContinueWarning.Type.MISSING_DIFFRACTION_CALIBRATION | ContinueWarning.Type.MISSING_NORMALIZATION
    )
