from dataclasses import dataclass



# https://docs.python.org/3/library/dataclasses.html
@dataclass
class DiffractionCalibrant:
    name: str
    latticeParameters: str # though it is a csv string of floats
    reference: str