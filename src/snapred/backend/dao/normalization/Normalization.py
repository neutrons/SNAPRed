from datetime import datetime

from pydantic import BaseModel

from snapred.backend.dao.state import InstrumentState


class Normalization(BaseModel):
    """
    Normalization object class.

    Attributes:
        instrumentState (InstrumentState): Represents the current state of the instrument. This
            is critical for ensuring that the normalization is appropriate for the instrument's
            conditions at the time of the run.
        seedRun (int): Identifier for the initial run from which this normalization object is
            generated. It is essential for traceability and reproducibility of the normalization process.
        creationDate (datetime): Records the exact date and time when this normalization object was
            created, providing a timestamp for versioning and historical reference.
        name (str): A descriptive name given to this normalization object for easy identification
            and reference within a dataset or system.
        version (int, default=0): Version number of this normalization object, allowing for
            version control and updates to normalization parameters or methods over time.

    This class represents a normalization entity with essential attributes to track its origin, application,
    and metadata. It is designed to work within a system that requires understanding of the instrument state,
    facilitating data normalization processes in a structured and version-controlled manner.

    """

    instrumentState: InstrumentState
    seedRun: int
    creationDate: datetime
    name: str
    version: int = 0
