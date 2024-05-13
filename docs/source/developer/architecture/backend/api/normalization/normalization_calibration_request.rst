NormalizationRequest Class Documentation
===================================================

NormalizationRequest.py introduces the NormalizationRequest class for initiating normalization calibration processes. It
incorporates default values for simplification and consistency, requiring the setting of attributes like runNumber, backgroundRunNumber, and specific
calibration settings upon instantiation.


Attributes:
-----------

- runNumber (str): The unique identifier for the run to be normalized, crucial for
  identifying and tracking the specific run undergoing the normalization calibration
  process.

- backgroundRunNumber (str): The identifier for the background run associated with the
  normalization process, providing context and control data for the normalization.

- useLiteMode (bool, default=True): A flag to determine if the process should run in lite
  mode, optimizing for faster execution with reduced resource usage. This setting
  affects the computational complexity and resource demands of the normalization
  calibration.

- focusGroup (FocusGroup): An instance of FocusGroup representing the specific group or set
  of samples being focused on during normalization. This parameter is key to tailoring
  the normalization process to the characteristics of a particular focus group.

- calibrantSamplePath (str): The file path to the calibrant sample data used for calibration,
  essential for ensuring the accuracy and relevance of the calibration to the sample
  being normalized.

- smoothingParameter (float, default=Config["calibration.parameters.default.smoothing"]): A
  parameter to control the smoothing applied in the calibration process. Default value
  is sourced from application.yml, ensuring consistency across runs unless explicitly
  overridden.

- crystalDMin (float, default=Config["constants.CrystallographicInfo.dMin"]): The minimum
  crystal d-spacing value to consider, sourced from application.yml. This parameter
  defines the lower bound for d-spacing values considered in the calibration.

- crystalDMax (float, default=Config["constants.CrystallographicInfo.dMax"]): The maximum
  crystal d-spacing value to consider, sourced from application.yml. This parameter
  defines the upper bound for d-spacing values considered in the calibration.

- peakIntensityThreshold (float, default=Config["constants.PeakIntensityFractionThreshold"]):
  The threshold for peak intensity, below which peaks will be ignored, from
  application.yml. This threshold is crucial for distinguishing significant peaks from
  noise in the diffraction pattern.

- nBinsAcrossPeakWidth (int, default=Config["calibration.diffraction.nBinsAcrossPeakWidth"]):
  The number of bins to distribute across the width of a peak in the diffraction pattern,
  as defined in application.yml. This parameter affects the resolution of the
  diffraction pattern analysis.
