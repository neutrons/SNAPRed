### Algorithm: `CalculateDiffCalTable`

#### Description:
This algorithm calculates a diffraction calibration table based on the input workspace. It uses the instrument definition within the workspace to create a DIFC table and then converts this table into a calibration table.

#### Expected Inputs:
1. **InputWorkspace**:
   - **Type**: `MatrixWorkspace`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: Workspace containing the instrument definition.

2. **OffsetMode**:
   - **Type**: `String`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: Mode for the offset calculation. Valid options are `Signed`, `Relative`, and `Absolute`.

3. **BinWidth**:
   - **Type**: `Double`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: Width of the bin.

#### Expected Outputs:
1. **CalibrationTable**:
   - **Type**: `ITableWorkspace`
   - **Direction**: `Output`
   - **Property Mode**: `Optional`
   - **Description**: The resulting calibration table.
