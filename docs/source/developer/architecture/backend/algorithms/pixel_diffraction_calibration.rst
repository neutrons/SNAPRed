### Algorithm: `PixelDiffractionCalibration`

#### Description:
This algorithm calculates the offset-corrected DIFC associated with a given workspace as part of diffraction calibration.
It may be re-called iteratively with `execute` to ensure convergence.

#### Expected Inputs:
1. **InputWorkspace**:
   - **Type**: `MatrixWorkspace`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: Workspace containing the TOF neutron data.

2. **GroupingWorkspace**:
   - **Type**: `MatrixWorkspace`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: Workspace containing the grouping information.

3. **Ingredients**:
   - **Type**: `String`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: JSON string of the DiffractionCalibrationIngredients.

#### Expected Outputs:
1. **CalibrationTable**:
   - **Type**: `ITableWorkspace`
   - **Direction**: `Output`
   - **Property Mode**: `Optional`
   - **Description**: Workspace containing the corrected calibration constants.

2. **MaskWorkspace**:
   - **Type**: `MaskWorkspace`
   - **Direction**: `Output`
   - **Property Mode**: `Optional`
   - **Description**: Mask workspace (1.0 => dead-pixel, 0.0 => live-pixel).

3. **data**:
    - **Type**: `String`
    - **Direction**: `Output`
    - **Property Mode**: `Optional`
    - **Description**: JSON string containing statistics of the offsets for testing convergence.
