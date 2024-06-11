### Algorithm: `FocusSpectraAlgorithm`

#### Description:
This algorithm focuses diffraction data using a specified grouping workspace.
It converts the input workspace to d-spacing, applies diffraction focusing,
and optionally rebins the output.

#### Expected Inputs:
1. **InputWorkspace**:
   - **Type**: `MatrixWorkspace`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: Workspace containing values at each pixel.

2. **GroupingWorkspace**:
   - **Type**: `MatrixWorkspace`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: Workspace defining the grouping for diffraction focusing.

3. **Ingredients**:
   - **Type**: `String`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: JSON string containing the ingredients for focusing.

4. **RebinOutput**:
   - **Type**: `Bool`
   - **Direction**: `Input`
   - **Property Mode**: `Optional`
   - **Description**: Flag indicating whether to rebin the output workspace.

#### Expected Outputs:
1. **OutputWorkspace**:
   - **Type**: `MatrixWorkspace`
   - **Direction**: `Output`
   - **Property Mode**: `Mandatory`
   - **Description**: The diffraction-focused data.
