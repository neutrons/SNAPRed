GenerateCalibrationMetricsWorkspaceRecipe Class Documentation
=============================================================

GenerateCalibrationMetricsWorkspaceRecipe orchestrates the aggregation and visualization of
calibration metrics into Mantid workspaces, facilitating scientific data analysis. Utilizing
MantidSnapper for operations and taking CalibrationMetricsWorkspaceIngredients as input, this
recipe constructs a table workspace of calibration metrics keyed by run ID and, optionally,
by timestamp or calibration version. It meticulously processes these metrics to generate
matrix workspaces that graphically depict sigma and strain values against twoTheta averages,
offering an in-depth look at calibration quality through visual means


Key Process:
------------

1. Initializes with MantidSnapper, setting up the foundation for its operations.
2. Executes executeRecipe using CalibrationMetricsWorkspaceIngredients, preparing and processing
   calibration metrics for visualization.
3. Depending on the presence of a timestamp or calibration version, it identifies the correct
   workspace naming convention for storing metrics.
4. Generates a table workspace of calibration metrics and subsequently converts this table into
   matrix workspaces for sigma and strain versus twoTheta visualization.
5. Cleans up any interim workspaces post-conversion, maintaining workflow integrity.


Operation Details:
------------------

- Ingredient Preparation: Parses CalibrationMetricsWorkspaceIngredients to extract run ID and
  optional timestamp or calibration version, forming the basis for workspace identification.

- Table Workspace Generation: Constructs a table workspace containing detailed calibration metrics,
  facilitating a structured representation of calibration data.

- Matrix Workspace Conversion: Transforms the table workspace into matrix workspaces, enabling
  graphical visualization of sigma and strain metrics against twoTheta averages.

- Cleanup Process: Ensures the removal of interim workspaces post-conversion, streamlining the
  workflow and preventing clutter.
