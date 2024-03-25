CalibrationAssessmentResponse Class Documentation
=================================================

CalibrationAssessmentResponse is a Pydantic model developed to encapsulate and communicate the
outcomes of calibration assessments in a structured and comprehensive manner. By incorporating
detailed calibration information alongside identifiers for relevant metric workspaces, this class
serves as an effective response model for conveying the results of calibration processes.


Attributes:
-----------

- record (CalibrationRecord): Embeds a CalibrationRecord instance that details the specific
  calibration process undertaken. This record provides a comprehensive overview of the calibration,
  including the parameters used, the run configuration, and any related calibration metrics.

- metricWorkspaces (List[str]): A list of strings identifying the workspaces where the calibration
  metrics are stored. These identifiers allow for direct access to the data and metrics resulting
  from the calibration assessment, supporting subsequent analysis or review processes.
