Algorithm: `PixelGroupingParametersCalculationAlgorithm`
========================================================

Description:
------------
This algorithm calculates state-derived parameters for pixel groupings.
It uses information from the provided grouping workspace and optional
mask workspace to compute various grouping parameters such as mean L2,
twoTheta, azimuth, and dSpacing resolution.

Expected Inputs:
----------------
1. **Ingredients**:
   - **Type**: `String`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: JSON string of the PixelGroupingIngredients.

2. **GroupingWorkspace**:
   - **Type**: `MatrixWorkspace`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: The grouping workspace defining this grouping scheme,
   with instrument-location parameters initialized according to the run number.

3. **MaskWorkspace**:
   - **Type**: `MaskWorkspace`
   - **Direction**: `Input`
   - **Property Mode**: `Optional`
   - **Description**: The mask workspace for a specified calibration run number and version.

Expected Outputs:
-----------------
1. **OutputParameters**:
   - **Type**: `String`
   - **Direction**: `Output`
   - **Property Mode**: `Mandatory`
   - **Description**: JSON string containing the calculated grouping parameters.
