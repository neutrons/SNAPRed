### Algorithm: `RawVanadiumCorrectionAlgorithm`

#### Description:
This algorithm processes raw vanadium data, correcting it by subtracting background data,
scaling, and applying absorption corrections based on the sample geometry. The result is
a corrected dataset suitable for further analysis.

#### Expected Inputs:
1. **InputWorkspace**:
    - **Type**: `MatrixWorkspace`
    - **Direction**: `Input`
    - **Property Mode**: `Mandatory`
    - **Description**: Workspace containing the raw vanadium data.

2. **BackgroundWorkspace**:
    - **Type**: `MatrixWorkspace`
    - **Direction**: `Input`
    - **Property Mode**: `Mandatory``
    - **Description**: Workspace containing the raw vanadium background data.

3. **OutputWorkspace**:
    - **Type**: `MatrixWorkspace`
    - **Direction**: `Input`
    - **Property Mode**: `Mandatory`
    - **Description**: Workspace containing corrected data; if none given, the
    InputWorkspace will be overwritten.

4. **Ingredients**:
    - **Type**: `String`
    - **Direction**: `Input`
    - **Property Mode**: `Mandatory`
    - **Description**: JSON string containing the NormalizationIngredients.
