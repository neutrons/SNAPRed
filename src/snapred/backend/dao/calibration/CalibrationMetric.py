from pydantic import BaseModel


class CalibrationMetric(BaseModel):
    """

    The CalibrationMetric class, built with Pydantic, is designed to capture and quantify the quality
    of a calibration relative to established standards and prior calibrations. It includes metrics such
    as sigmaAverage and sigmaStandardDeviation for assessing calibration consistency, alongside strainAverage
    and strainStandardDeviation to evaluate calibration strain. Additionally, it tracks the twoThetaAverage,
    possibly measured in degrees

    """

    sigmaAverage: float
    sigmaStandardDeviation: float
    strainAverage: float
    strainStandardDeviation: float

    # units: degrees (?)
    twoThetaAverage: float
