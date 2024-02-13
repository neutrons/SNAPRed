from pydantic import BaseModel


class CalibrationMetric(BaseModel):
    """
    Metrics Instruments Scientists use to determine the quality of a Calibration
    This is relative to known standards and previous Calibrations.
    """

    sigmaAverage: float
    sigmaStandardDeviation: float
    strainAverage: float
    strainStandardDeviation: float
    
    # units: degrees (?)
    twoThetaAverage: float
