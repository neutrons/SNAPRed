Algorithm: `SmoothDataExcludingPeaksAlgo`
=========================================

Description:
------------
This algorithm removes peaks from a given workspace and smooths the data using a spline function. It processes the
input workspace to exclude specified peaks and then applies a smoothing spline to the remaining data.

Expected Inputs:
----------------
1. **InputWorkspace**:
    - **Type**: `MatrixWorkspace`
    - **Direction**: `Input`
    - **Property Mode**: `Mandatory`
    - **Description**: Workspace containing the peaks to be removed.

2. **OutputWorkspace**:
    - **Type**: `MatrixWorkspace`
    - **Direction**: `Output`
    - **Property Mode**: `Mandatory`
    - **Description**: Histogram Workspace with removed peaks.

3. **DetectorPeaks**:
    - **Type**: `String`
    - **Direction**: `Input`
    - **Property Mode**: `Mandatory`
    - **Description**: JSON string containing the detector peaks.

4. **SmoothingParameter**:
    - **Type**: `Float`
    - **Direction**: `Input`
    - **Property Mode**: `Mandatory`
    - **Description**: Smoothing parameter for the spline function.
