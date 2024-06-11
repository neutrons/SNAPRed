### Algorithm: `CalibrationMetricExtractionAlgorithm`

#### Description:
This algorithm extracts calibration metrics from the output of the peak
fitting algorithm `FitMultiplePeaks`. It calculates the sigma and strain
averages and standard deviations, and then packages them with the
two-theta average. This is done per group, with strain and sigma
collected over spectra within the group.

#### Expected Inputs:
1. **InputWorkspace**:
   - **Type**: `WorkspaceGroup`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: Group of workspaces containing the peak fitting results.

2. **PixelGroup**:
   - **Type**: `String`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: JSON string defining the pixel group.

#### Expected Outputs:
1. **OutputMetrics**:
   - **Type**: `String`
   - **Direction**: `Output`
   - **Property Mode**: `Optional`
   - **Description**: JSON string containing the extracted calibration metrics.
