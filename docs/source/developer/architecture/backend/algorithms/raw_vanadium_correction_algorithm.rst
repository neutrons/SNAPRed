Algorithm: `RawVanadiumCorrectionAlgorithm`
===========================================

Description:
------------
This algorithm creates the raw vanadium data which is unfocused and
is later used in the reduction process.

Expected Inputs:
----------------
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
    - **Description**: Workspace containing corrected data; if none given, the InputWorkspace will be overwritten.

4. **Ingredients**:
    - **Type**: `String`
    - **Direction**: `Input`
    - **Property Mode**: `Mandatory`
    - **Description**: JSON string containing the NormalizationIngredients.
