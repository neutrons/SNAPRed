Algorithm: `DiffractionSpectrumWeightCalculator`
================================================

Description:
------------
This algorithm calculates smoothing weights for the smoothed cubic spline method
to allow it to exclude peaks from its fit of the background. It clones the input
workspace to create a weight workspace, applies zero weights to regions around
the predicted peaks, and handles event data by converting it to histogram data
if necessary.

Expected Inputs:
----------------
1. **InputWorkspace**:
   - **Type**: `MatrixWorkspace`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: Workspace peaks to be weighted.

2. **DetectorPeaks**:
   - **Type**: `String`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: JSON string of predicted peaks.

Expected Outputs:
-----------------
1. **WeightWorkspace**:
   - **Type**: `MatrixWorkspace`
   - **Direction**: `Output`
   - **Property Mode**: `Mandatory`
   - **Description**: The output workspace with calculated weights.
