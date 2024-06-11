### Algorithm: `LoadCalibrationWorkspaces`

#### Description:
This algorithm creates a table workspace and a mask workspace from a calibration-data
HDF5 file. It uses an instrument donor workspace to load the calibration constants and
mask data.

#### Expected Inputs:
1. **Filename**:
   - **Type**: `FileProperty`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: Path to the HDF5 format file to be loaded.

2. **InstrumentDonor**:
   - **Type**: `MatrixWorkspace`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: Workspace to use as an instrument donor.

#### Expected Outputs:
1. **CalibrationTable**:
   - **Type**: `ITableWorkspace`
   - **Direction**: `Output`
   - **Property Mode**: `Mandatory`
   - **Description**: Name of the output table workspace.

2. **MaskWorkspace**:
   - **Type**: `MaskWorkspace`
   - **Direction**: `Output`
   - **Property Mode**: `Mandatory`
   - **Description**: Name of the output mask workspace.
