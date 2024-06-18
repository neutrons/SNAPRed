Algorithm: `FitMultiplePeaksAlgorithm`
======================================

Description:
------------
This algorithm fits multiple peaks in a given workspace using a specified peak function.
It processes the input workspace, fits the peaks, and outputs the results in various
workspaces, grouped together.

Expected Inputs:
----------------
1. **InputWorkspace**:
   - **Type**: `MatrixWorkspace`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: Workspace containing the peaks to be fit.

2. **DetectorPeaks**:
   - **Type**: `String`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: Input list of peaks to be fit.

3. **PeakFunction**:
   - **Type**: `String`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: Type of peak function to use. Valid options are those in `allowed_peak_type_list`.

Expected Outputs:
-----------------
1. **OutputWorkspaceGroup**:
   - **Type**: `WorkspaceGroup`
   - **Direction**: `Output`
   - **Property Mode**: `Mandatory`
   - **Description**: The group of workspaces containing the fit results.
