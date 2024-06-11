### Algorithm: `WashDishes`

#### Description:
This algorithm deletes specified workspaces from the Mantid workspace list unless the CIS mode is enabled. It ensures that only existing workspaces are attempted to be deleted.

#### Expected Inputs:
1. **Workspace**:
    - **Type**: `String`
    - **Direction**: `Input`
    - **Property Mode**: Optional
    - **Description**: The name of the workspace to be deleted.

2. **WorkspaceList**:
    - **Type**: `StringArrayProperty`
    - **Direction**: `Input`
    - **Property Mode**: Optional
    - **Description**: List of workspaces to be deleted.
