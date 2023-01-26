from dataclasses import dataclass



# https://docs.python.org/3/library/dataclasses.html
@dataclass
class DiffractionCalibrant:
    runNumber: int
    name: str
    latticeParameters: str # though it is a csv string of floats
    reference: str