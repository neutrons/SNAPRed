from pydantic import BaseModel


class CalibrationMetric(BaseModel):
    sigmaAverage: float
    sigmaStandardDeviation: float
    strainAverage: float
    strainStandardDeviation: float
    twoThetaAverage: float
