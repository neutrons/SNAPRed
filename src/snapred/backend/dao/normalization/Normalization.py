from snapred.backend.dao.indexing.CalculationParameters import CalculationParameters


class Normalization(CalculationParameters):
    """

    This class represents a normalization opject with essential attributes to track its origin,
    application, and metadata. It is designed to work within a system that requires understanding
    of the instrument state, facilitating data normalization processes in a structured and
    version-controlled manner.

    """

    # inherits from CalculationParameters
    # - instrumentState: InstrumentState
    # - seedRun: str
    # - useLiteMode: bool
    # - creationDate: datetime
    # - name: str
    # - version: Union[int, UNINITIALIZED]

    pass
