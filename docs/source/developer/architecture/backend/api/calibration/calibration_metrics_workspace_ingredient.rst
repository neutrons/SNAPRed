CalibrationMetricsWorkspaceIngredients Class Documentation
==========================================================

CalibrationMetricsWorkspaceIngredients is a Pydantic model conceived to encapsulate key components
essential for the creation of workspaces tailored to calibration metrics analysis. This class serves
as a container for the foundational elements required to generate and analyze workspaces that are
specifically dedicated to exploring and understanding calibration data. By incorporating a reference
to a specific CalibrationRecord alongside an optional timestamp marking the generation time, this class
facilitates structured and time-aware analyses of calibration metrics within dedicated workspace environments.


Attributes:
-----------

- calibrationRecord (CalibrationRecord): Holds a reference to the CalibrationRecord instance being analyzed.
  This reference is pivotal as it directly links the workspace ingredients to the specific calibration data
  set, ensuring that the workspace is accurately populated with relevant calibration metrics.

- timestamp (Optional[float]): An optional Unix timestamp that marks the time of workspace generation. This
  attribute is useful for tracking when the workspace was created, enabling temporal analyses of calibration
  metrics and supporting versioning or historical comparisons of calibration data.
