Algorithm: `LiteDataCreationAlgo`
=================================

Description:
------------
This algorithm converts a full-resolution dataset to a lite resolution dataset
using a specified grouping workspace. It optionally compresses the output and
can delete the non-lite workspace.

Expected Inputs:
----------------
1. **InputWorkspace**:
   - **Type**: `MatrixWorkspace`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: Workspace containing the full resolution dataset to be converted to lite.

2. **LiteDataMapWorkspace**:
   - **Type**: `MatrixWorkspace`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: Grouping workspace which maps full pixel resolution to lite data.

3. **LiteInstrumentDefinitionFile**:
   - **Type**: `String`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: Path to the lite instrument definition file.

4. **Ingredients**:
   - **Type**: `String`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: JSON-format 'LiteDataCreationIngredients': compression-related args.

Expected Outputs:
-----------------
1. **OutputWorkspace**:
   - **Type**: `MatrixWorkspace`
   - **Direction**: `Output`
   - **Property Mode**: `Mandatory`
   - **Description**: The workspace reduced to lite resolution and compressed.

2. **Tolerance**:
   - **Type**: `float`
   - **Direction**: `Output`
   - **Property Mode**: `Mandatory`
   - **Description**: The compression tolerance 'deltaT' when compression has been used.
