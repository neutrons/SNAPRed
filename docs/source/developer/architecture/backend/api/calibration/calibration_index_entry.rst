CalibrationIndexEntry Class Documentation
=========================================

CalibrationIndexEntry is a Pydantic model structured to encapsulate and manage the details pertinent to calibration
index entries within a system. This class is specifically designed to hold crucial information such as runNumber,
version, appliesTo, comments, author, and timestamp, effectively organizing calibration data. It introduces a
sophisticated method, parseAppliesTo, for interpreting the appliesTo field which signifies the applicability of the
calibration entry across different scenarios.


Attributes:
-----------

- runNumber (str): Identifies the run number associated with the calibration entry, serving as a primary reference point
  for identifying specific calibration data.

- version (Optional[str]): Represents the version of the calibration entry, allowing for tracking of changes and updates
  over time. It is optional, acknowledging that not all entries may be versioned.

- appliesTo (Optional[str]): Indicates the applicability of the calibration entry, using comparisons (e.g., '>', '<', '>=', '<=')
  to define its scope relative to run numbers. This field supports complex applicability conditions, enhancing the flexibility
  of calibration data usage.

- comments (str): Provides space for annotations or notes related to the calibration entry, aiding in documentation and contextual
  understanding.

- author (str): The name of the individual responsible for the calibration entry, ensuring traceability and accountability.

- timestamp (Optional[float]): A Unix timestamp marking the creation or last update of the calibration entry, offering a chronological
  reference for data management.


Functionalities:
----------------

- parseAppliesTo: A method to interpret the appliesTo field, extracting comparison symbols and run numbers to clarify the entry's
  applicability conditions. It plays a crucial role in enabling precise and flexible management of calibration applicability.

- appliesToFormatChecker: A validator ensuring the appliesTo field adheres to accepted formats, either a simple 'runNumber' or a
  comparison format (e.g., '>runNumber'). This validator reinforces data integrity by mandating consistent entry formats across
  the dataset.
