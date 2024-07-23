NormalizationIndexEntry Class Documentation
===========================================

NormalizationIndexEntry.py introduces NormalizationIndexEntry, a class for encapsulating normalization index entries. It models entries through
attributes like runNumber, backgroundRunNumber, version, appliesTo, comments, author, and timestamp, and includes appliesToFormatChecker, a
validation method to ensure the appliesTo attribute conforms to the expected format.

Initialization requires no explicit inputs; attributes are set by providing values upon instantiation, resulting in a fully populated object
reflecting the specified attributes.

The appliesToFormatChecker method validates the appliesTo attribute's format against 'runNumber', '>runNumber', or '<runNumber', ensuring correct
data incorporation by examining the attribute's prefix and validating the subsequent string as a legitimate integer run number. A ValueError is
raised for format mismatches, maintaining data integrity.


Attributes:
-----------

- runNumber (str): Unique identifier for the run. This is used to distinguish between
  different normalization entries and ensure each is properly associated with its
  corresponding run.

- backgroundRunNumber (str): Identifier for the background run associated with this entry,
  facilitating the differentiation and contextual understanding of normalization data.

- version (Optional[str]): Version of the entry; optional. Allows for version tracking of
  the normalization entry for updates and revisions.

- appliesTo (Optional[str]): Specifies applicable run numbers in 'runNumber', '>runNumber',
  or '<runNumber' format; optional. The format is validated to ensure consistency and
  correctness.

- comments (Optional[str]): Additional comments about the entry; optional. Provides a space
  for annotating the entry with relevant observations or notes.

- author (Optional[str]): Author's name who created or modified the entry; optional. Records
  the individual responsible for the entry, aiding in traceability.

- timestamp (Optional[float]): Unix timestamp of creation or last update; optional.Timestamps
  offer temporal context to the entry's creation or modification.


Validator Logic:
----------------

Validates the 'appliesTo' format: 'runNumber', '>runNumber', or '<runNumber'.

Ensures that the 'appliesTo' attribute, if present, follows a predefined format for
consistency and correctness across entries. Raises a ValueError if the format does not
match expectations, safeguarding against invalid data entry.


Parameters:
'''''''''''

- v (str): testValue
    The value to validate, representing the 'appliesTo' field.


Outputs:
''''''''

Returns the validated value, confirming it adheres to the correct format.
