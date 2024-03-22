CalibrationRecord Class Documentation
=====================================

CalibrationRecord is a Pydantic model designed to serve as a detailed log for tracking the inputs, parameters,
and outcomes associated with a specific calibration effort. This class methodically compiles a comprehensive set
of data elements crucial for the calibration process, including run numbers, crystallographic information,
calibration parameters, and related workspace details. By encapsulating this information, CalibrationRecord provides
a structured approach to documenting the calibration process, facilitating analysis, replication, and quality assessment
of calibrations over time.


Attributes:
-----------

- runNumber (str): Identifies the specific run or dataset that the calibration pertains to, serving as a key reference
  point for the calibration record.

- crystalInfo (CrystallographicInfo): Contains detailed crystallographic information relevant to the calibration process.

- calibrationFittingIngredients (Calibration): Captures the set of calibration parameters used in the calibration fitting
  process, providing a snapshot of the calibration configuration for analysis and replication.

- pixelGroups (Optional[List[PixelGroup]]): A list of PixelGroup objects representing the pixel groupings used in the
  calibration. While currently optional, future updates intend to make this attribute mandatory to ensure comprehensive
  documentation of calibration inputs.

- focusGroupCalibrationMetrics (FocusGroupMetric): Includes metrics from FocusGroupMetric to evaluate the quality and
  effectiveness of the calibration across different focus groups, supporting quality assessment and improvement efforts.

- workspaces (Dict[WorkspaceType, List[WorkspaceName]]): Organizes Mantid workspace names associated with the calibration
  by their type, facilitating easy access and management of related workspace data.

- version (Optional[int]): An optional attribute to track the versioning of the calibration record, allowing for the
  historical analysis of calibration evolution and improvements over time.
