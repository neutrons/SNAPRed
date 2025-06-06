from pydantic import BaseModel

from snapred.backend.dao.Limit import Limit


class ParticleBounds(BaseModel):
    """
    `ParticleBounds` specifies the allowable ranges for wavelength and time-of-flight (TOF)
    parameters for particles, using the `Limit` class to define the upper and lower bounds
    for each.
    """

    """
    # pydantic v2 has an issue with Generic-derived types.
    wavelength: Limit[float]  # lambda
    tof: Limit[float]
    """
    wavelength: Limit  # lambda
    tof: Limit
