from pydantic import BaseModel

from snapred.backend.dao.Limit import Limit


class ParticleBounds(BaseModel):
    wavelength: Limit[float]  # lambda
    tof: Limit[float]
