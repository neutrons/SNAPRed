Calibration Class Documentation
===============================

Calibration is a model class derived from BaseModel within the Pydantic library, specifically designed to encapsulate a group of parameters
predominantly used for conducting fitting operations. It serves as a structured representation of calibration parameters, tying them closely
to a particular instrument state and facilitating traceability and versioning of calibration data. The class design emphasizes immutability
and consistency in calibration data.


Attributes:
-----------

- instrumentState (InstrumentState): Represents the state of the instrument at the time of calibration. This attribute is crucial for
  ensuring that calibration parameters are relevant and applicable to the specific instrument conditions, enhancing the accuracy and
  reliability of fitting operations.

- seedRun (int): An identifier for the initial run from which the calibration data is derived. This facilitates traceability back to
  the experimental data that contributed to the calibration, enabling reproducibility and verification of calibration results.

- creationDate (datetime): Records the exact date and time when the calibration was created. This timestamp is vital for managing
  calibration versions and tracking the evolution of calibration parameters over time.

- name (str): A descriptive name assigned to the calibration. This name aids in the identification and differentiation of calibration
 sets within datasets or systems, enhancing data management and retrieval.

- version (int, default=0): Specifies the version number of the calibration, starting from 0. Versioning allows for the systematic
  tracking of changes and updates to calibration parameters, supporting iterative improvements and the maintenance of calibration
  data integrity.
