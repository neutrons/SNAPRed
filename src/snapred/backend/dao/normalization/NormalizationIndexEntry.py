from snapred.backend.dao.IndexEntry import IndexEntry


class NormalizationIndexEntry(IndexEntry):
    """

    This class represents a Normalization Index Entry object with various attributes and a custom validator.
    The purpose of this class is to model a normalization index entry with attributes like runNumber,
    backgroundRunNumber, version, appliesTo, comments, author, and timestamp. It also includes a custom
    validator method called appliesToFormatChecker to validate the format of the appliesTo attribute if it
    is present.

    """

    backgroundRunNumber: str
