### Algorithm: `MaskDetectorFlags`

#### Description:
This algorithm initializes detector flags from a mask workspace. Unlike Mantid's `MaskDetectors`,
it does not clear any masked spectra or move masked detectors to group zero for grouping
workspaces.

#### Expected Inputs:
1. **MaskWorkspace**:
   - **Type**: `MaskWorkspace`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: Workspace containing the mask.

2. **OutputWorkspace**:
   - **Type**: `MatrixWorkspace`
   - **Direction**: `InOut`
   - **Property Mode**: `Mandatory`
   - **Description**: The workspace for which to set the detector flags.
