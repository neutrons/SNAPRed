from typing import Optional

from pydantic import BaseModel, validator


class NormalizationIndexEntry(BaseModel):
    """
    Represents a Normalization Index Entry object with various attributes and a custom validator.

    Attributes:
        runNumber (str): Unique identifier for the run. This is used to distinguish between
            different normalization entries and ensure each is properly associated with its
            corresponding run.
        backgroundRunNumber (str): Identifier for the background run associated with this entry,
            facilitating the differentiation and contextual understanding of normalization data.
        version (Optional[str]): Version of the entry; optional. Allows for version tracking of
            the normalization entry for updates and revisions.
        appliesTo (Optional[str]): Specifies applicable run numbers in 'runNumber', '>runNumber',
            or '<runNumber' format; optional. The format is validated to ensure consistency and
            correctness.
        comments (Optional[str]): Additional comments about the entry; optional. Provides a space
            for annotating the entry with relevant observations or notes.
        author (Optional[str]): Author's name who created or modified the entry; optional. Records
            the individual responsible for the entry, aiding in traceability.
        timestamp (Optional[int]): Unix timestamp of creation or last update; optional.Timestamps
            offer temporal context to the entry's creation or modification.

    Validator ensures 'appliesTo' adheres to the specified format if present, enhancing the
    integrity of data referencing.
    """

    runNumber: str
    backgroundRunNumber: str
    version: Optional[str]
    appliesTo: Optional[str]
    comments: Optional[str]
    author: Optional[str]
    timestamp: Optional[int]

    @validator("appliesTo", allow_reuse=True)
    def appliesToFormatChecker(cls, v):
        """
        Validates the 'appliesTo' format: 'runNumber', '>runNumber', or '<runNumber'.

        Ensures that the 'appliesTo' attribute, if present, follows a predefined format for
        consistency and correctness across entries. Raises a ValueError if the format does not
        match expectations, safeguarding against invalid data entry.

        Parameters:
            v (str): The value to validate, representing the 'appliesTo' field.

        Returns:
            str: The validated value, confirming it adheres to the correct format.
        """
        testValue = v
        if testValue is not None:
            if testValue.startswith(">") or testValue.startswith("<"):
                testValue = testValue[1:]
                try:
                    int(testValue)
                except ValueError:
                    raise ValueError("appliesTo must be in the format of 'runNumber', '>runNumber', or '<runNumber'.")

        return v
