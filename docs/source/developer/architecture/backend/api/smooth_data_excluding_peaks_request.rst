SmoothingDataExcludingPeaksRequest Class Documentation
======================================================

SmoothDataExcludingPeaksRequest is a class derived from BaseModel in the pydantic library. It encapsulates all necessary parameters for requests
aimed at smoothing data while excluding peaks. To ensure ease of use and consistency across executions, it includes default values sourced from the
configuration. This class provides a detailed framework for specifying the details of a data smoothing operation, focusing on preserving significant
data features while minimizing noise.


Attributes:
-----------

- runNumber (str): The unique identifier for the run to be processed. This is crucial for
  identifying and tracking the specific dataset undergoing the smoothing operation.

- useLiteMode (bool, default=True): A flag indicating if the process should run in lite
  mode, optimizing for faster execution with minimal resource usage. This option can
  significantly affect the efficiency and scalability of the data processing workflow.

- focusGroup (FocusGroup): An instance of FocusGroup representing the specific group or set
  of samples being focused on during data smoothing. This parameter ensures that the
  smoothing operation is appropriately tailored to the characteristics of the focus
  group.

- calibrantSamplePath (str): The file path to the calibrant sample data used in the
  calibration and smoothing process. This is essential for accurate calibration and
  adjustment of the smoothing operation to the specific characteristics of the sample.

- inputWorkspace (str): The name of the input workspace containing the data to be smoothed.
  Identifying the input workspace is key to sourcing the raw data for the smoothing
  process.

- outputWorkspace (str): The name of the output workspace where the smoothed data will be
  stored. This defines the destination for the processed data, facilitating subsequent
  analysis or visualization steps.

- smoothingParameter (float, default=Config["calibration.parameters.default.smoothing"]):
  Controls the amount of smoothing applied to the data. The default value is sourced
  from application.yml, providing a baseline smoothing level that can be adjusted as
  needed.

- crystalDMin (float, default=Config["constants.CrystallographicInfo.dMin"]): The minimum
  d-spacing value to consider in the smoothing process, sourced from application.yml.
  This parameter helps to define the relevant data range for the smoothing operation.

- crystalDMax (float, default=Config["constants.CrystallographicInfo.dMax"]): The maximum
  d-spacing value to consider in the smoothing process, sourced from application.yml.
  Like crystalDMin, this parameter delimits the data range of interest.

- peakIntensityThreshold (float, default=Config["constants.PeakIntensityFractionThreshold"]):
  The threshold for peak intensity; peaks below this threshold will not be considered
  in the smoothing process. Sourced from application.yml, this parameter helps to
  exclude insignificant peaks from the smoothing operation, focusing on the most
  relevant features of the data.
