DiffractionCalibrationIngredients Class Documentation
=====================================================

DiffractionCalibrationIngredients is a Pydantic model that consolidates essential elements
required for executing diffraction calibration procedures. This class is thoughtfully designed
to contain comprehensive details about the calibration run settings, pixel grouping considerations,
peak identification, and calibration accuracy parameters. By grouping these components,
DiffractionCalibrationIngredients ensures a structured approach to diffraction calibration, enhancing
the precision and effectiveness of the calibration process.


Attributes:
-----------

- runConfig (RunConfig): Specifies the calibration run settings, encapsulating parameters such as run
  number, instrument configuration, and any specific conditions under which the calibration is performed.
  This attribute is crucial for defining the operational context of the calibration process.

- pixelGroup (PixelGroup): Details the specific group of pixels being considered for calibration. This
  granularity allows for targeted calibration efforts, focusing on particular pixel groups to optimize
  calibration accuracy and relevance.

- groupedPeakLists (List[GroupPeakList]): Contains a list of GroupPeakList instances, each detailing the
  peaks identified within different groups of pixels. This comprehensive listing of peak data is essential
  for the calibration process, facilitating the accurate modeling and adjustment of peak positions.

- convergenceThreshold (float): Defines the threshold for calibration accuracy, setting a criterion for the
  convergence of the calibration process. This threshold ensures that calibration efforts meet a predetermined
  level of precision before being deemed satisfactory.

- peakFunction (SymmetricPeakEnum): Specifies the peak function selected for modeling the peaks during calibration,
  based on system configurations. This function is central to the peak fitting process, influencing how peaks are
  represented and adjusted within the calibration model.

- maxOffset (float): Sets a limit for the maximum allowable adjustment during calibration, preventing excessive offsets
  that could compromise calibration validity. This parameter acts as a safeguard, ensuring that calibration adjustments
  remain within reasonable bounds.
