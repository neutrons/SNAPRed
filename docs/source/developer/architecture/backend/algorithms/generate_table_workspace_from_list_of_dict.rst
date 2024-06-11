### Algorithm: `GenerateTableWorkspaceFromListOfDict`

#### Description:
This algorithm generates a table workspace from a list of dictionaries.
Each dictionary represents a row in the table, and the keys in the
dictionaries are used as column names.

#### Expected Inputs:
1. **ListOfDict**:
   - **Type**: `String`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: JSON string representing a list of dictionaries.

#### Expected Outputs:
1. **OutputWorkspace**:
   - **Type**: `ITableWorkspace`
   - **Direction**: `Output`
   - **Property Mode**: `Mandatory`
   - **Description**: The table workspace created from the input list of dictionaries.
