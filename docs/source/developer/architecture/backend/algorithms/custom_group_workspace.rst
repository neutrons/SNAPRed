Algorithm: `CustomGroupWorkspace`
=================================

Description:
------------
This algorithm orchestrates loading grouping workspaces and then grouping them
into a `WorkspaceGroup`.

Expected Inputs:
----------------
1. **GroupingWorkspaces**:
   - **Type**: `StringArrayProperty`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: List of workspace names to be grouped.

2. **FocusGroups**:
   - **Type**: `String`
   - **Direction**: `Input`
   - **Property Mode**: `Optional`
   - **Description**: Focus group information (additional metadata, usage not explicitly defined in the provided code).

Expected Outputs:
-----------------
1. **OutputWorkspace**:
   - **Type**: `WorkspaceGroup`
   - **Direction**: `Output`
   - **Property Mode**: `Optional`
   - **Description**: The group of grouping workspaces.
