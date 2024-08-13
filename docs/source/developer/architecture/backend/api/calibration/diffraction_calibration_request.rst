DiffractionCalibrationRequest Class Documentation
=================================================

DiffractionCalibrationRequest is a Pydantic model meticulously designed to initiate the
diffraction calibration process for specific runs. By benchmarking against established
crystallographic data typically sourced from CIF files, this class ensures a detailed
and precise calibration framework. It incorporates essential parameters such as runNumber,
calibrantSamplePath, and the targeted focusGroup, alongside operational settings and
calibration parameters to optimize the calibration process. Pre-configured default
values from the system configuration underpin these parameters


Attributes:
-----------

- runNumber (str): Specifies the identifier of the run to be calibrated, serving as a
  critical reference throughout the calibration process.

- calibrantSamplePath (str): Points to the path of the crystallographic data file (CIF file),
  providing a standard against which the run data will be calibrated.

- focusGroup (FocusGroup): Identifies the focus group involved in the calibration, enabling
  targeted calibration efforts within specific segments of the data.

- useLiteMode (bool): When set to true, activates a simplified processing mode designed to
  minimize computational resource usage while maintaining calibration effectiveness.

- crystalDMin (float, default=Config["constants.CrystallographicInfo.dMin"]): Defines the
  minimum crystallographic d-spacing considered in the calibration, based on system configuration
  defaults.

- crystalDMax (float, default=Config["constants.CrystallographicInfo.dMax"]): Sets the maximum
  crystallographic d-spacing for the calibration, adhering to predefined system configurations.

- peakFunction (SymmetricPeakEnum, default=SymmetricPeakEnum[Config["calibration.diffraction.peakFunction"]]):
  Specifies the peak function model to be used in calibration, chosen from allowed peak types within system
  configurations.

- convergenceThreshold (float, default=Config["calibration.diffraction.convergenceThreshold"]):
  Establishes the threshold for calibration convergence, guiding the precision and termination
  of the calibration process.

- nBinsAcrossPeakWidth (int, default=Config["calibration.diffraction.nBinsAcrossPeakWidth"]):
  Specifies the number of bins to distribute across the peak width in the diffraction pattern,
  affecting peak resolution.

- maximumOffset (float, default=Config["calibration.diffraction.maximumOffset"]): Limits the
  maximum allowable calibration offset, ensuring calibration adjustments remain within acceptable
  bounds.
