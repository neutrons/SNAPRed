### Algorithm: `ConjoinTableWorkspaces`

#### Description:
This algorithm combines two table workspaces by adding the rows of the second workspace
to the first. It ensures that the columns of both workspaces match in number, names, and
types.

#### Expected Inputs:
1. **InputWorkspace1**:
   - **Type**: `ITableWorkspace`
   - **Direction**: `InOut`
   - **Property Mode**: `Mandatory`
   - **Description**: The first workspace, which will have the rows added to it.

2. **InputWorkspace2**:
   - **Type**: `ITableWorkspace`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: The second workspace to be conjoined to the first.

3. **AutoDelete**:
   - **Type**: `Bool`
   - **Direction**: `Input`
   - **Property Mode**: `Optional`
   - **Description**: Flag indicating whether to automatically delete the second workspace after conjoining.

#### Expected Outputs:
- **InputWorkspace1**: The modified workspace with rows from `InputWorkspace2` added.
