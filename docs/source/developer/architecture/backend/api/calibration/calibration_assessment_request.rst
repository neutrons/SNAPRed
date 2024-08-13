CalibrationAssessmentRequest Class Documentation
================================================

CalibrationAssessmentRequest is a Pydantic model designed to facilitate the initiation and execution
of calibration assessments for specific runs in scientific analysis. By incorporating essential details
such as run configurations, workspace mappings, focus group specifications, and operational parameters,
this class streamlines the calibration assessment process.


Attributes:
-----------

- run (RunConfig): Specifies the configuration for the run being assessed, including details such as run
  number and instrument settings. This configuration is vital for tailoring the assessment to the specific
  characteristics of the run.

- workspaces (Dict[WorkspaceType, List[WorkspaceName]]): Maps various workspace types to their corresponding
  workspace names, providing a structured analytical context for the assessment. This mapping is crucial for
  organizing and accessing the data required for calibration analysis.

- focusGroup (FocusGroup): Identifies the focus group for targeted assessment, enabling a focused analysis on
  specific segments or aspects of the data. This specification enhances the relevance and precision of the calibration
  assessment.

- calibrantSamplePath (str): Points to the path of the sample data, usually a CIF file, that provides the standard
  crystallographic data for comparison. This path is essential for basing the calibration assessment on accurate and
  standardized reference data.

- useLiteMode (bool): Indicates whether the assessment should operate in a lite mode, optimizing for reduced resource
  consumption and faster execution. This mode is suitable for preliminary assessments or scenarios where resource
  constraints are a consideration.

- nBinsAcrossPeakWidth (int, default=Config["calibration.diffraction.nBinsAcrossPeakWidth"]): Specifies the number of
  bins to distribute across the width of a peak in the diffraction pattern, as defined in the system configurations.
  This parameter affects the resolution and granularity of the peak analysis.

- peakFunction (ALLOWED_PEAK_TYPES, default="Gaussian"): Defines the type of peak model to be used in the assessment, with a
  default set to Gaussian. The choice of peak type influences the modeling and analysis of peak data, impacting the
  assessment outcomes.
