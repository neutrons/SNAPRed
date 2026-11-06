from snapred.backend.error.CalibrationHomeError import CalibrationHomeError


class CalibrationWriteGuardError(CalibrationHomeError):
    "Raised when a write would land outside of the calibration home, or in a calibration folder of another environment"
