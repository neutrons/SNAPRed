CalibrationLoadAssessmentRequest Class Documentation
====================================================

CalibrationLoadAssessmentRequest is a Pydantic model designed to facilitate the initiation
of generating and loading assessments for specific calibration versions related to a run's
instrument state. This class allows for precise identification and retrieval of calibration
assessments by specifying a runId and version. Additionally, it incorporates a checkExistent
flag to streamline the assessment process, offering the option to bypass regeneration of the
assessment if it already exists, thus enhancing system efficiency and resource utilization.


Attributes:
-----------

- runId (str): Specifies the identifier of the run for which the calibration assessment is
  to be generated or retrieved. This ID is crucial for locating the correct calibration data
  associated with a particular run's instrument state.

- version (str): Identifies the specific version of the calibration of interest. The versioning
  system allows for precise tracking and management of calibration data over time, facilitating
  access to historical or updated calibration assessments.

- checkExistent (bool): A flag that controls the regeneration behavior of the calibration assessment.
  When set to true, the system will not regenerate the assessment if it already exists, thereby avoiding
  unnecessary computational overhead and ensuring efficient use of resources.
