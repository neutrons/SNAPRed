Recipe: `GroupDiffCalRecipe`
========================================

Description:
------------
This algorithm calculates the group-aligned DIFC associated with a given workspace,
as part of diffraction calibration. It processes input neutron data, applies
diffraction calibration, and outputs various calibration and diagnostic workspaces.

Expected Inputs:
----------------
1. **InputWorkspace**:
   - **Type**: `MatrixWorkspace`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: Workspace containing TOF neutron data.

2. **GroupingWorkspace**:
   - **Type**: `MatrixWorkspace`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: Workspace containing the grouping information.

3. **PreviousCalibrationTable**:
   - **Type**: `ITableWorkspace`
   - **Direction**: `Input`
   - **Property Mode**: `Optional`
   - **Description**: Table workspace with previous pixel-calibrated DIFC values; if none given,
   will be calculated using the instrument geometry.

4. **Ingredients**:
   - **Type**: `String`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: JSON string containing the ingredients for the algorithm.

Expected Outputs:
-----------------
1. **OutputWorkspace**:
   - **Type**: `MatrixWorkspace`
   - **Direction**: `Output`
   - **Property Mode**: `Optional`
   - **Description**: A diffraction-focused workspace in dSpacing, after calibration constants have been adjusted.

2. **DiagnosticWorkspace**:
   - **Type**: `MatrixWorkspace`
   - **Direction**: `Output`
   - **Property Mode**: `Optional`
   - **Description**: A workspace group containing the fitted peaks, fit parameters, and the TOF-focused data for comparison to fits.

3. **MaskWorkspace**:
   - **Type**: `MaskWorkspace`
   - **Direction**: `Output`
   - **Property Mode**: `Optional`
   - **Description**: If mask workspace exists: incoming mask values will be used (1.0 => dead-pixel, 0.0 => live-pixel).

4. **FinalCalibrationTable**:
   - **Type**: `ITableWorkspace`
   - **Direction**: `Output`
   - **Property Mode**: `Optional`
   - **Description**: Table workspace with group-corrected DIFC values.
